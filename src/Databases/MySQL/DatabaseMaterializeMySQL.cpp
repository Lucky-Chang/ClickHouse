#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#    include <Databases/MySQL/DatabaseMaterializeMySQL.h>

#    include <Interpreters/Context.h>
#    include <Databases/DatabaseOrdinary.h>
#    include <Databases/MySQL/DatabaseMaterializeTablesIterator.h>
#    include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Storages/StorageMaterializeMySQL.h>
#    include <Poco/Logger.h>
#    include <Common/setThreadName.h>
#    include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

DatabaseMaterializeMySQL::DatabaseMaterializeMySQL(
    ContextPtr context_,
    const String & database_name_,
    UUID uuid,
    const String & metadata_path_,
    const String & mysql_database_name_,
    mysqlxx::Pool && pool_,
    MySQLClient && client_,
    std::unique_ptr<MaterializeMySQLSettings> settings_)
    : DatabaseOrdinary(
        database_name_,
        uuid,
        metadata_path_,
        "data/" + escapeForFileName(database_name_) + "/",
        "DatabaseMaterializeMySQL<Ordinary> (" + database_name_ + ")",
        context_)
    , settings(std::move(settings_))
    , materialize_thread(context_, database_name_, mysql_database_name_, std::move(pool_), std::move(client_), settings.get())
{
}


void DatabaseMaterializeMySQL::rethrowExceptionIfNeed() const
{
    std::unique_lock<std::mutex> lock(DatabaseOrdinary::mutex);

    if (!settings->allows_query_when_mysql_lost && exception)
    {
        try
        {
            std::rethrow_exception(exception);
        }
        catch (Exception & ex)
        {
            /// This method can be called from multiple threads
            /// and Exception can be modified concurrently by calling addMessage(...),
            /// so we rethrow a copy.
            throw Exception(ex);
        }
    }
}

void DatabaseMaterializeMySQL::setException(const std::exception_ptr & exception_)
{
    std::unique_lock<std::mutex> lock(DatabaseOrdinary::mutex);
    exception = exception_;
}

void DatabaseMaterializeMySQL::loadStoredObjects(ContextMutablePtr context_, bool has_force_restore_data_flag, bool force_attach)
{
    DatabaseOrdinary::loadStoredObjects(context_, has_force_restore_data_flag, force_attach);
    if (!force_attach)
        materialize_thread.assertMySQLAvailable();

    materialize_thread.startSynchronization();
    started_up = true;
}

void DatabaseMaterializeMySQL::createTable(ContextPtr context_, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    assertCalledFromSyncThreadOrDrop("create table");
    DatabaseOrdinary::createTable(context_, name, table, query);
}

void DatabaseMaterializeMySQL::dropTable(ContextPtr context_, const String & name, bool no_delay)
{
    assertCalledFromSyncThreadOrDrop("drop table");
    DatabaseOrdinary::dropTable(context_, name, no_delay);
}

void DatabaseMaterializeMySQL::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assertCalledFromSyncThreadOrDrop("attach table");
    DatabaseOrdinary::attachTable(name, table, relative_table_path);
}

StoragePtr DatabaseMaterializeMySQL::detachTable(const String & name)
{
    assertCalledFromSyncThreadOrDrop("detach table");
    return DatabaseOrdinary::detachTable(name);
}


void DatabaseMaterializeMySQL::renameTable(ContextPtr context_, const String & name, IDatabase & to_database, const String & to_name, bool exchange, bool dictionary)
{
    assertCalledFromSyncThreadOrDrop("rename table");

    if (exchange)
        throw Exception("MaterializeMySQL database not support exchange table.", ErrorCodes::NOT_IMPLEMENTED);

    if (dictionary)
        throw Exception("MaterializeMySQL database not support rename dictionary.", ErrorCodes::NOT_IMPLEMENTED);

    if (to_database.getDatabaseName() != DatabaseOrdinary::getDatabaseName())
        throw Exception("Cannot rename with other database for MaterializeMySQL database.", ErrorCodes::NOT_IMPLEMENTED);

    DatabaseOrdinary::renameTable(context_, name, *this, to_name, exchange, dictionary);
}


void DatabaseMaterializeMySQL::alterTable(ContextPtr context_, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    assertCalledFromSyncThreadOrDrop("alter table");
    DatabaseOrdinary::alterTable(context_, table_id, metadata);
}


void DatabaseMaterializeMySQL::drop(ContextPtr context_)
{
    /// Remove metadata info
    fs::path metadata(DatabaseOrdinary::getMetadataPath() + "/.metadata");

    if (fs::exists(metadata))
        fs::remove(metadata);

    DatabaseOrdinary::drop(context_);
}


StoragePtr DatabaseMaterializeMySQL::tryGetTable(const String & name, ContextPtr context_) const
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
    {
        StoragePtr nested_storage = DatabaseOrdinary::tryGetTable(name, context_);

        if (!nested_storage)
            return {};

        return std::make_shared<StorageMaterializeMySQL>(std::move(nested_storage), this);
    }

    return DatabaseOrdinary::tryGetTable(name, context_);
}

DatabaseTablesIteratorPtr
DatabaseMaterializeMySQL::getTablesIterator(ContextPtr context_, const DatabaseOnDisk::FilterByNameFunction & filter_by_table_name)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
    {
        DatabaseTablesIteratorPtr iterator = DatabaseOrdinary::getTablesIterator(context_, filter_by_table_name);
        return std::make_unique<DatabaseMaterializeTablesIterator>(std::move(iterator), this);
    }

    return DatabaseOrdinary::getTablesIterator(context_, filter_by_table_name);
}


void DatabaseMaterializeMySQL::assertCalledFromSyncThreadOrDrop(const char * method) const
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread() && started_up)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MaterializeMySQL database not support {}", method);
}


void DatabaseMaterializeMySQL::shutdownSynchronizationThread()
{
    materialize_thread.stopSynchronization();
    started_up = false;
}

template<typename Database, template<class> class Helper, typename... Args>
auto castToMaterializeMySQLAndCallHelper(Database * database, Args && ... args)
{
    using Ordinary = DatabaseMaterializeMySQL;
    using ToOrdinary = typename std::conditional_t<std::is_const_v<Database>, const Ordinary *, Ordinary *>;
    if (auto * database_materialize = typeid_cast<ToOrdinary>(database))
        return (database_materialize->*Helper<Ordinary>::v)(std::forward<Args>(args)...);

    throw Exception("LOGICAL_ERROR: cannot cast to DatabaseMaterializeMySQL, it is a bug.", ErrorCodes::LOGICAL_ERROR);
}

template<typename T> struct HelperSetException { static constexpr auto v = &T::setException; };
void setSynchronizationThreadException(const DatabasePtr & materialize_mysql_db, const std::exception_ptr & exception)
{
    castToMaterializeMySQLAndCallHelper<IDatabase, HelperSetException>(materialize_mysql_db.get(), exception);
}

template<typename T> struct HelperStopSync { static constexpr auto v = &T::shutdownSynchronizationThread; };
void stopDatabaseSynchronization(const DatabasePtr & materialize_mysql_db)
{
    castToMaterializeMySQLAndCallHelper<IDatabase, HelperStopSync>(materialize_mysql_db.get());
}

template<typename T> struct HelperRethrow { static constexpr auto v = &T::rethrowExceptionIfNeed; };
void rethrowSyncExceptionIfNeed(const IDatabase * materialize_mysql_db)
{
    castToMaterializeMySQLAndCallHelper<const IDatabase, HelperRethrow>(materialize_mysql_db);
}

}

#endif
