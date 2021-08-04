#include <algorithm>
#include <csignal>
#include <Access/AllowedClientHosts.h>
#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Disks/DiskRestartProxy.h>
#include <Disks/IStoragePolicy.h>
#include <IO/copyData.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageFactory.h>
#include <Common/ActionLock.h>
#include <Common/DNSResolver.h>
#include <Common/ShellCommand.h>
#include <Common/SymbolIndex.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/typeid_cast.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_KILL;
    extern const int NOT_IMPLEMENTED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int NO_ZOOKEEPER;
}


namespace ActionLocks
{
    extern StorageActionBlockType PartsMerge;
    extern StorageActionBlockType DistributedSend;
    extern StorageActionBlockType PartsTTLMerge;
    extern StorageActionBlockType PartsMove;
}


namespace
{
    ExecutionStatus getOverallExecutionStatusOfCommands() { return ExecutionStatus(0); }

    /// Consequently tries to execute all commands and generates final exception message for failed commands
    template <typename Callable, typename... Callables>
    ExecutionStatus getOverallExecutionStatusOfCommands(Callable && command, Callables &&... commands)
    {
        ExecutionStatus status_head(0);
        try
        {
            command();
        }
        catch (...)
        {
            status_head = ExecutionStatus::fromCurrentException();
        }

        ExecutionStatus status_tail = getOverallExecutionStatusOfCommands(std::forward<Callables>(commands)...);

        auto res_status = status_head.code != 0 ? status_head.code : status_tail.code;
        auto res_message = status_head.message + (status_tail.message.empty() ? "" : ("\n" + status_tail.message));

        return ExecutionStatus(res_status, res_message);
    }

    /// Consequently tries to execute all commands and throws exception with info about failed commands
    template <typename... Callables>
    void executeCommandsAndThrowIfError(Callables &&... commands)
    {
        auto status = getOverallExecutionStatusOfCommands(std::forward<Callables>(commands)...);
        if (status.code != 0)
            throw Exception(status.message, status.code);
    }


    AccessType getRequiredAccessType(StorageActionBlockType action_type)
    {
        if (action_type == ActionLocks::PartsMerge)
            return AccessType::SYSTEM_MERGES;
        else if (action_type == ActionLocks::DistributedSend)
            return AccessType::SYSTEM_DISTRIBUTED_SENDS;
        else if (action_type == ActionLocks::PartsTTLMerge)
            return AccessType::SYSTEM_TTL_MERGES;
        else if (action_type == ActionLocks::PartsMove)
            return AccessType::SYSTEM_MOVES;
        else
            throw Exception("Unknown action type: " + std::to_string(action_type), ErrorCodes::LOGICAL_ERROR);
    }


}

/// Implements SYSTEM [START|STOP] <something action from ActionLocks>
void InterpreterSystemQuery::startStopAction(StorageActionBlockType action_type, bool start)
{
    auto manager = getContext()->getActionLocksManager();
    manager->cleanExpired();

    if (volume_ptr && action_type == ActionLocks::PartsMerge)
    {
        volume_ptr->setAvoidMergesUserOverride(!start);
    }
    else if (table_id)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
        if (table)
        {
            if (start)
            {
                manager->remove(table, action_type);
                table->onActionLockRemove(action_type);
            }
            else
                manager->add(table, action_type);
        }
    }
    else
    {
        auto access = getContext()->getAccess();
        for (auto & elem : DatabaseCatalog::instance().getDatabases())
        {
            for (auto iterator = elem.second->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
                StoragePtr table = iterator->table();
                if (!table)
                    continue;

                if (!access->isGranted(getRequiredAccessType(action_type), elem.first, iterator->name()))
                {
                    LOG_INFO(
                        log,
                        "Access {} denied, skipping {}.{}",
                        toString(getRequiredAccessType(action_type)),
                        elem.first,
                        iterator->name());
                    continue;
                }

                if (start)
                {
                    manager->remove(table, action_type);
                    table->onActionLockRemove(action_type);
                }
                else
                    manager->add(table, action_type);
            }
        }
    }
}


InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_->clone()), log(&Poco::Logger::get("InterpreterSystemQuery"))
{
}


BlockIO InterpreterSystemQuery::execute()
{
    auto & query = query_ptr->as<ASTSystemQuery &>();

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext(), getRequiredAccessForDDLOnCluster());

    using Type = ASTSystemQuery::Type;

    /// Use global context with fresh system profile settings
    auto system_context = Context::createCopy(getContext()->getGlobalContext());
    system_context->setSetting("profile", getContext()->getSystemProfileName());

    /// Make canonical query for simpler processing
    if (query.type == Type::RELOAD_DICTIONARY)
    {
        if (!query.database.empty())
            query.table = query.database + "." + query.table;
    }
    else if (!query.table.empty())
    {
        table_id = getContext()->resolveStorageID(StorageID(query.database, query.table), Context::ResolveOrdinary);
    }


    volume_ptr = {};
    if (!query.storage_policy.empty() && !query.volume.empty())
        volume_ptr = getContext()->getStoragePolicy(query.storage_policy)->getVolumeByName(query.volume);

    switch (query.type)
    {
        case Type::SHUTDOWN: {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            if (kill(0, SIGTERM))
                throwFromErrno("System call kill(0, SIGTERM) failed", ErrorCodes::CANNOT_KILL);
            break;
        }
        case Type::KILL: {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            /// Exit with the same code as it is usually set by shell when process is terminated by SIGKILL.
            /// It's better than doing 'raise' or 'kill', because they have no effect for 'init' process (with pid = 0, usually in Docker).
            LOG_INFO(log, "Exit immediately as the SYSTEM KILL command has been issued.");
            _exit(128 + SIGKILL);
            // break; /// unreachable
        }
        case Type::SUSPEND: {
            auto command = fmt::format("kill -STOP {0} && sleep {1} && kill -CONT {0}", getpid(), query.seconds);
            LOG_DEBUG(log, "Will run {}", command);
            auto res = ShellCommand::execute(command);
            res->in.close();
            WriteBufferFromOwnString out;
            copyData(res->out, out);
            copyData(res->err, out);
            if (!out.str().empty())
                LOG_DEBUG(log, "The command returned output: {}", command, out.str());
            res->wait();
            break;
        }
        case Type::DROP_DNS_CACHE: {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_DNS_CACHE);
            DNSResolver::instance().dropCache();
            /// Reinitialize clusters to update their resolved_addresses
            system_context->reloadClusterConfig();
            break;
        }
        case Type::DROP_MARK_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MARK_CACHE);
            system_context->dropMarkCache();
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE);
            system_context->dropUncompressedCache();
            break;
        case Type::DROP_MMAP_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MMAP_CACHE);
            system_context->dropMMappedFileCache();
            break;
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_COMPILED_EXPRESSION_CACHE);
            if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
                cache->reset();
            break;
#endif
        case Type::RELOAD_DICTIONARY: {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);

            auto & external_dictionaries_loader = system_context->getExternalDictionariesLoader();
            external_dictionaries_loader.reloadDictionary(query.table, getContext());


            ExternalDictionariesLoader::resetAll();
            break;
        }
        case Type::RELOAD_DICTIONARIES: {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);
            executeCommandsAndThrowIfError(
                [&] { system_context->getExternalDictionariesLoader().reloadAllTriedToLoad(); },
                [&] { system_context->getEmbeddedDictionaries().reload(); });
            ExternalDictionariesLoader::resetAll();
            break;
        }
        case Type::RELOAD_MODEL: {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);

            auto & external_models_loader = system_context->getExternalModelsLoader();
            external_models_loader.reloadModel(query.target_model);
            break;
        }
        case Type::RELOAD_MODELS: {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);

            auto & external_models_loader = system_context->getExternalModelsLoader();
            external_models_loader.reloadAllTriedToLoad();
            break;
        }
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES);
            system_context->getEmbeddedDictionaries().reload();
            break;
        case Type::RELOAD_CONFIG:
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            system_context->reloadConfig();
            break;
        case Type::RELOAD_SYMBOLS: {
#if defined(__ELF__) && !defined(__FreeBSD__)
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_SYMBOLS);
            (void)SymbolIndex::instance(true);
            break;
#else
            throw Exception("SYSTEM RELOAD SYMBOLS is not supported on current platform", ErrorCodes::NOT_IMPLEMENTED);
#endif
        }
        case Type::STOP_MERGES:
            startStopAction(ActionLocks::PartsMerge, false);
            break;
        case Type::START_MERGES:
            startStopAction(ActionLocks::PartsMerge, true);
            break;
        case Type::STOP_TTL_MERGES:
            startStopAction(ActionLocks::PartsTTLMerge, false);
            break;
        case Type::START_TTL_MERGES:
            startStopAction(ActionLocks::PartsTTLMerge, true);
            break;
        case Type::STOP_MOVES:
            startStopAction(ActionLocks::PartsMove, false);
            break;
        case Type::START_MOVES:
            startStopAction(ActionLocks::PartsMove, true);
            break;
        case Type::STOP_DISTRIBUTED_SENDS:
            startStopAction(ActionLocks::DistributedSend, false);
            break;
        case Type::START_DISTRIBUTED_SENDS:
            startStopAction(ActionLocks::DistributedSend, true);
            break;
        case Type::FLUSH_DISTRIBUTED:
            flushDistributed(query);
            break;
        case Type::RESTART_DISK:
            restartDisk(query.disk);
            break;
        case Type::FLUSH_LOGS: {
            getContext()->checkAccess(AccessType::SYSTEM_FLUSH_LOGS);
            executeCommandsAndThrowIfError(
                [&] {
                    if (auto query_log = getContext()->getQueryLog())
                        query_log->flush(true);
                },
                [&] {
                    if (auto part_log = getContext()->getPartLog(""))
                        part_log->flush(true);
                },
                [&] {
                    if (auto query_thread_log = getContext()->getQueryThreadLog())
                        query_thread_log->flush(true);
                },
                [&] {
                    if (auto trace_log = getContext()->getTraceLog())
                        trace_log->flush(true);
                },
                [&] {
                    if (auto text_log = getContext()->getTextLog())
                        text_log->flush(true);
                },
                [&] {
                    if (auto metric_log = getContext()->getMetricLog())
                        metric_log->flush(true);
                },
                [&] {
                    if (auto asynchronous_metric_log = getContext()->getAsynchronousMetricLog())
                        asynchronous_metric_log->flush(true);
                },
                [&] {
                    if (auto opentelemetry_span_log = getContext()->getOpenTelemetrySpanLog())
                        opentelemetry_span_log->flush(true);
                });
            break;
        }
        case Type::STOP_LISTEN_QUERIES:
        case Type::START_LISTEN_QUERIES:
            throw Exception(String(ASTSystemQuery::typeToString(query.type)) + " is not supported yet", ErrorCodes::NOT_IMPLEMENTED);
        default:
            throw Exception("Unknown type of SYSTEM query", ErrorCodes::BAD_ARGUMENTS);
    }

    return BlockIO();
}

void InterpreterSystemQuery::flushDistributed(ASTSystemQuery &)
{
    getContext()->checkAccess(AccessType::SYSTEM_FLUSH_DISTRIBUTED, table_id);

    if (auto * storage_distributed = dynamic_cast<StorageDistributed *>(DatabaseCatalog::instance().getTable(table_id, getContext()).get()))
        storage_distributed->flushClusterNodesAllData(getContext());
    else
        throw Exception("Table " + table_id.getNameForLogs() + " is not distributed", ErrorCodes::BAD_ARGUMENTS);
}

void InterpreterSystemQuery::restartDisk(String & name)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_DISK);

    auto disk = getContext()->getDisk(name);

    if (DiskRestartProxy * restart_proxy = dynamic_cast<DiskRestartProxy *>(disk.get()))
        restart_proxy->restart();
    else
        throw Exception("Disk " + name + " doesn't have possibility to restart", ErrorCodes::BAD_ARGUMENTS);
}


AccessRightsElements InterpreterSystemQuery::getRequiredAccessForDDLOnCluster() const
{
    const auto & query = query_ptr->as<const ASTSystemQuery &>();
    using Type = ASTSystemQuery::Type;
    AccessRightsElements required_access;

    switch (query.type)
    {
        case Type::SHUTDOWN:
            [[fallthrough]];
        case Type::KILL:
            [[fallthrough]];
        case Type::SUSPEND: {
            required_access.emplace_back(AccessType::SYSTEM_SHUTDOWN);
            break;
        }
        case Type::DROP_DNS_CACHE:
            [[fallthrough]];
        case Type::DROP_MARK_CACHE:
            [[fallthrough]];
        case Type::DROP_MMAP_CACHE:
            [[fallthrough]];
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
            [[fallthrough]];
#endif
        case Type::DROP_UNCOMPRESSED_CACHE: {
            required_access.emplace_back(AccessType::SYSTEM_DROP_CACHE);
            break;
        }
        case Type::RELOAD_DICTIONARY:
            [[fallthrough]];
        case Type::RELOAD_DICTIONARIES:
            [[fallthrough]];
        case Type::RELOAD_EMBEDDED_DICTIONARIES: {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_DICTIONARY);
            break;
        }
        case Type::RELOAD_MODEL:
            [[fallthrough]];
        case Type::RELOAD_MODELS: {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_MODEL);
            break;
        }
        case Type::RELOAD_CONFIG: {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_CONFIG);
            break;
        }
        case Type::RELOAD_SYMBOLS: {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_SYMBOLS);
            break;
        }
        case Type::STOP_MERGES:
            [[fallthrough]];
        case Type::START_MERGES: {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MERGES, query.database, query.table);
            break;
        }
        case Type::STOP_TTL_MERGES:
            [[fallthrough]];
        case Type::START_TTL_MERGES: {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES, query.database, query.table);
            break;
        }
        case Type::STOP_MOVES:
            [[fallthrough]];
        case Type::START_MOVES: {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_MOVES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MOVES, query.database, query.table);
            break;
        }
        case Type::STOP_DISTRIBUTED_SENDS:
            [[fallthrough]];
        case Type::START_DISTRIBUTED_SENDS: {
            if (query.table.empty())
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS);
            else
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS, query.database, query.table);
            break;
        }
        case Type::FLUSH_DISTRIBUTED: {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_DISTRIBUTED, query.database, query.table);
            break;
        }
        case Type::FLUSH_LOGS: {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_LOGS);
            break;
        }
        case Type::RESTART_DISK: {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_DISK);
            break;
        }
        case Type::STOP_LISTEN_QUERIES:
            break;
        case Type::START_LISTEN_QUERIES:
            break;
        case Type::UNKNOWN:
            break;
        case Type::END:
            break;
    }
    return required_access;
}

void InterpreterSystemQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr) const
{
    elem.query_kind = "System";
}

}
