#include <filesystem>
#include <Core/BackgroundSchedulePool.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/IDatabase.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/loadMetadata.h>
#include <Parsers/formatAST.h>
#include <Storages/IStorage.h>
#include <Storages/LiveView/TemporaryLiveViewCleaner.h>
#include <Storages/StorageMemory.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/CurrentMetrics.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>
#include <Common/renameat2.h>
#include <common/logger_useful.h>
#include "Core/UUID.h"

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#    include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#    include <Storages/StorageMaterializeMySQL.h>
#endif

#if USE_LIBPQXX
#    include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#endif

namespace fs = std::filesystem;

namespace CurrentMetrics
{
extern const Metric TablesToDropQueueSize;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int DATABASE_NOT_EMPTY;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
}

TemporaryTableHolder::TemporaryTableHolder(ContextPtr context_, const TemporaryTableHolder::Creator & creator, const ASTPtr & query)
    : WithContext(context_->getGlobalContext()), temporary_tables(DatabaseCatalog::instance().getDatabaseForTemporaryTables().get())
{
    ASTPtr original_create;
    ASTCreateQuery * create = dynamic_cast<ASTCreateQuery *>(query.get());
    String global_name;
    if (create)
    {
        original_create = create->clone();
        if (create->uuid == UUIDHelpers::Nil)
            create->uuid = UUIDHelpers::generateV4();
        id = create->uuid;
        create->table = "_tmp_" + toString(id);
        global_name = create->table;
        create->database = DatabaseCatalog::TEMPORARY_DATABASE;
    }
    else
    {
        id = UUIDHelpers::generateV4();
        global_name = "_tmp_" + toString(id);
    }
    auto table_id = StorageID(DatabaseCatalog::TEMPORARY_DATABASE, global_name, id);
    auto table = creator(table_id);
    temporary_tables->createTable(getContext(), global_name, table, original_create);
    table->startup();
}


TemporaryTableHolder::TemporaryTableHolder(
    ContextPtr context_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    const ASTPtr & query,
    bool create_for_global_subquery)
    : TemporaryTableHolder(
        context_,
        [&](const StorageID & table_id) {
            auto storage = StorageMemory::create(table_id, ColumnsDescription{columns}, ConstraintsDescription{constraints}, String{});

            if (create_for_global_subquery)
                storage->delayReadForGlobalSubqueries();

            return storage;
        },
        query)
{
}

TemporaryTableHolder::TemporaryTableHolder(TemporaryTableHolder && rhs)
    : WithContext(rhs.context), temporary_tables(rhs.temporary_tables), id(rhs.id)
{
    rhs.id = UUIDHelpers::Nil;
}

TemporaryTableHolder & TemporaryTableHolder::operator=(TemporaryTableHolder && rhs)
{
    id = rhs.id;
    rhs.id = UUIDHelpers::Nil;
    return *this;
}

TemporaryTableHolder::~TemporaryTableHolder()
{
    if (id != UUIDHelpers::Nil)
        temporary_tables->dropTable(getContext(), "_tmp_" + toString(id));
}

StorageID TemporaryTableHolder::getGlobalTableID() const
{
    return StorageID{DatabaseCatalog::TEMPORARY_DATABASE, "_tmp_" + toString(id), id};
}

StoragePtr TemporaryTableHolder::getTable() const
{
    auto table = temporary_tables->tryGetTable("_tmp_" + toString(id), getContext());
    if (!table)
        throw Exception("Temporary table " + getGlobalTableID().getNameForLogs() + " not found", ErrorCodes::LOGICAL_ERROR);
    return table;
}

void DatabaseCatalog::initializeAndLoadTemporaryDatabase()
{
    auto db_for_temporary_and_external_tables
        = std::make_shared<DatabaseMemory>(TEMPORARY_DATABASE, UUIDHelpers::generateV4(), getContext());
    attachDatabase(TEMPORARY_DATABASE, db_for_temporary_and_external_tables);
}

void DatabaseCatalog::loadDatabases()
{
    /// Another background thread which drops temporary LiveViews.
    /// We should start it after loadMarkedAsDroppedTables() to avoid race condition.
    TemporaryLiveViewCleaner::instance().startup();
}

void DatabaseCatalog::shutdownImpl()
{
    TemporaryLiveViewCleaner::shutdown();

    /** At this point, some tables may have threads that block our mutex.
      * To shutdown them correctly, we will copy the current list of tables,
      *  and ask them all to finish their work.
      * Then delete all objects with tables.
      */

    Databases current_databases;
    {
        std::lock_guard lock(databases_mutex);
        current_databases = databases;
    }

    /// We still hold "databases" (instead of std::move) for Buffer tables to flush data correctly.

    for (auto & database : current_databases)
        database.second->shutdown();

    std::lock_guard lock(databases_mutex);

    databases.clear();
    view_dependencies.clear();
}


DatabaseAndTable DatabaseCatalog::getTableImpl(const StorageID & table_id, ContextPtr context_, std::optional<Exception> * exception) const
{
    if (!table_id)
    {
        if (exception)
            exception->emplace(ErrorCodes::UNKNOWN_TABLE, "Cannot find table: StorageID is empty");
        return {};
    }

    if (table_id.database_name == TEMPORARY_DATABASE)
    {
        /// For temporary tables UUIDs are set in Context::resolveStorageID(...).
        /// If table_id has no UUID, then the name of database was specified by user and table_id was not resolved through context.
        /// Do not allow access to TEMPORARY_DATABASE because it contains all temporary tables of all contexts and users.
        if (exception)
            exception->emplace(
                ErrorCodes::DATABASE_ACCESS_DENIED, "Direct access to `{}` database is not allowed", String(TEMPORARY_DATABASE));
        return {};
    }

    DatabasePtr database;
    {
        std::lock_guard lock{databases_mutex};
        auto it = databases.find(table_id.getDatabaseName());
        if (databases.end() == it)
        {
            if (exception)
                exception->emplace(ErrorCodes::UNKNOWN_DATABASE, "Database {} doesn't exist", backQuoteIfNeed(table_id.getDatabaseName()));
            return {};
        }
        database = it->second;
    }

    auto table = database->tryGetTable(table_id.table_name, context_);
    if (!table && exception)
        exception->emplace(ErrorCodes::UNKNOWN_TABLE, "Table {} doesn't exist", table_id.getNameForLogs());
    if (!table)
        database = nullptr;

    return {database, table};
}

void DatabaseCatalog::assertDatabaseExists(const String & database_name) const
{
    std::lock_guard lock{databases_mutex};
    assertDatabaseExistsUnlocked(database_name);
}

void DatabaseCatalog::assertDatabaseDoesntExist(const String & database_name) const
{
    std::lock_guard lock{databases_mutex};
    assertDatabaseDoesntExistUnlocked(database_name);
}

void DatabaseCatalog::assertDatabaseExistsUnlocked(const String & database_name) const
{
    assert(!database_name.empty());
    if (databases.end() == databases.find(database_name))
        throw Exception("Database " + backQuoteIfNeed(database_name) + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
}


void DatabaseCatalog::assertDatabaseDoesntExistUnlocked(const String & database_name) const
{
    assert(!database_name.empty());
    if (databases.end() != databases.find(database_name))
        throw Exception("Database " + backQuoteIfNeed(database_name) + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
}

void DatabaseCatalog::attachDatabase(const String & database_name, const DatabasePtr & database)
{
    std::lock_guard lock{databases_mutex};
    assertDatabaseDoesntExistUnlocked(database_name);
    databases.emplace(database_name, database);
}


DatabasePtr DatabaseCatalog::detachDatabase(const String & database_name, bool drop, bool check_empty)
{
    if (database_name == TEMPORARY_DATABASE)
        throw Exception("Cannot detach database with temporary tables.", ErrorCodes::DATABASE_ACCESS_DENIED);

    DatabasePtr db;
    {
        std::lock_guard lock{databases_mutex};
        assertDatabaseExistsUnlocked(database_name);
        databases.erase(database_name);
    }

    if (check_empty)
    {
        try
        {
            if (!db->empty())
                throw Exception("New table appeared in database being dropped or detached. Try again.", ErrorCodes::DATABASE_NOT_EMPTY);
            if (!drop)
                db->assertCanBeDetached(false);
        }
        catch (...)
        {
            attachDatabase(database_name, db);
            throw;
        }
    }

    db->shutdown();

    if (drop)
    {
        /// Delete the database.
        db->drop(getContext());

        /// Old ClickHouse versions did not store database.sql files
        fs::path database_metadata_file = fs::path(getContext()->getPath()) / "metadata" / (escapeForFileName(database_name) + ".sql");
        if (fs::exists(database_metadata_file))
            fs::remove(database_metadata_file);
    }

    return db;
}

void DatabaseCatalog::updateDatabaseName(const String & old_name, const String & new_name)
{
    std::lock_guard lock{databases_mutex};
    assert(databases.find(new_name) == databases.end());
    auto it = databases.find(old_name);
    assert(it != databases.end());
    auto db = it->second;
    databases.erase(it);
    databases.emplace(new_name, db);
}

DatabasePtr DatabaseCatalog::getDatabase(const String & database_name) const
{
    std::lock_guard lock{databases_mutex};
    assertDatabaseExistsUnlocked(database_name);
    return databases.find(database_name)->second;
}

DatabasePtr DatabaseCatalog::tryGetDatabase(const String & database_name) const
{
    assert(!database_name.empty());
    std::lock_guard lock{databases_mutex};
    auto it = databases.find(database_name);
    if (it == databases.end())
        return {};
    return it->second;
}

bool DatabaseCatalog::isDatabaseExist(const String & database_name) const
{
    assert(!database_name.empty());
    std::lock_guard lock{databases_mutex};
    return databases.end() != databases.find(database_name);
}

Databases DatabaseCatalog::getDatabases() const
{
    std::lock_guard lock{databases_mutex};
    return databases;
}

bool DatabaseCatalog::isTableExist(const DB::StorageID & table_id, ContextPtr context_) const
{
    DatabasePtr db;
    {
        std::lock_guard lock{databases_mutex};
        auto iter = databases.find(table_id.database_name);
        if (iter != databases.end())
            db = iter->second;
    }
    return db && db->isTableExist(table_id.table_name, context_);
}

void DatabaseCatalog::assertTableDoesntExist(const StorageID & table_id, ContextPtr context_) const
{
    if (isTableExist(table_id, context_))
        throw Exception("Table " + table_id.getNameForLogs() + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}

DatabasePtr DatabaseCatalog::getDatabaseForTemporaryTables() const
{
    return getDatabase(TEMPORARY_DATABASE);
}

DatabasePtr DatabaseCatalog::getSystemDatabase() const
{
    return getDatabase(SYSTEM_DATABASE);
}

std::unique_ptr<DatabaseCatalog> DatabaseCatalog::database_catalog;

DatabaseCatalog::DatabaseCatalog(ContextMutablePtr global_context_)
    : WithMutableContext(global_context_), log(&Poco::Logger::get("DatabaseCatalog"))
{
    TemporaryLiveViewCleaner::init(global_context_);
}

DatabaseCatalog & DatabaseCatalog::init(ContextMutablePtr global_context_)
{
    if (database_catalog)
    {
        throw Exception("Database catalog is initialized twice. This is a bug.", ErrorCodes::LOGICAL_ERROR);
    }

    database_catalog.reset(new DatabaseCatalog(global_context_));

    return *database_catalog;
}

DatabaseCatalog & DatabaseCatalog::instance()
{
    if (!database_catalog)
    {
        throw Exception("Database catalog is not initialized. This is a bug.", ErrorCodes::LOGICAL_ERROR);
    }

    return *database_catalog;
}

void DatabaseCatalog::shutdown()
{
    // The catalog might not be initialized yet by init(global_context). It can
    // happen if some exception was thrown on first steps of startup.
    if (database_catalog)
    {
        database_catalog->shutdownImpl();
    }
}

DatabasePtr DatabaseCatalog::getDatabase(const String & database_name, ContextPtr local_context) const
{
    String resolved_database = local_context->resolveDatabase(database_name);
    return getDatabase(resolved_database);
}

void DatabaseCatalog::addDependency(const StorageID & from, const StorageID & where)
{
    std::lock_guard lock{databases_mutex};
    // FIXME when loading metadata storage may not know UUIDs of it's dependencies, because they are not loaded yet,
    // so UUID of `from` is not used here. (same for remove, get and update)
    view_dependencies[{from.getDatabaseName(), from.getTableName()}].insert(where);
}

void DatabaseCatalog::removeDependency(const StorageID & from, const StorageID & where)
{
    std::lock_guard lock{databases_mutex};
    view_dependencies[{from.getDatabaseName(), from.getTableName()}].erase(where);
}

Dependencies DatabaseCatalog::getDependencies(const StorageID & from) const
{
    std::lock_guard lock{databases_mutex};
    auto iter = view_dependencies.find({from.getDatabaseName(), from.getTableName()});
    if (iter == view_dependencies.end())
        return {};
    return Dependencies(iter->second.begin(), iter->second.end());
}

void DatabaseCatalog::updateDependency(
    const StorageID & old_from, const StorageID & old_where, const StorageID & new_from, const StorageID & new_where)
{
    std::lock_guard lock{databases_mutex};
    if (!old_from.empty())
        view_dependencies[{old_from.getDatabaseName(), old_from.getTableName()}].erase(old_where);
    if (!new_from.empty())
        view_dependencies[{new_from.getDatabaseName(), new_from.getTableName()}].insert(new_where);
}

DDLGuardPtr DatabaseCatalog::getDDLGuard(const String & database, const String & table)
{
    std::unique_lock lock(ddl_guards_mutex);
    auto db_guard_iter = ddl_guards.try_emplace(database).first;
    DatabaseGuard & db_guard = db_guard_iter->second;
    return std::make_unique<DDLGuard>(db_guard.first, db_guard.second, std::move(lock), table, database);
}

std::unique_lock<std::shared_mutex> DatabaseCatalog::getExclusiveDDLGuardForDatabase(const String & database)
{
    DDLGuards::iterator db_guard_iter;
    {
        std::unique_lock lock(ddl_guards_mutex);
        db_guard_iter = ddl_guards.try_emplace(database).first;
        assert(db_guard_iter->second.first.count(""));
    }
    DatabaseGuard & db_guard = db_guard_iter->second;
    return std::unique_lock{db_guard.second};
}

bool DatabaseCatalog::isDictionaryExist(const StorageID & table_id) const
{
    auto storage = tryGetTable(table_id, getContext());
    bool storage_is_dictionary = storage && storage->isDictionary();

    return storage_is_dictionary;
}

StoragePtr DatabaseCatalog::getTable(const StorageID & table_id, ContextPtr local_context) const
{
    std::optional<Exception> exc;
    auto res = getTableImpl(table_id, local_context, &exc);
    if (!res.second)
        throw Exception(*exc);
    return res.second;
}

StoragePtr DatabaseCatalog::tryGetTable(const StorageID & table_id, ContextPtr local_context) const
{
    return getTableImpl(table_id, local_context, nullptr).second;
}

DatabaseAndTable DatabaseCatalog::getDatabaseAndTable(const StorageID & table_id, ContextPtr local_context) const
{
    std::optional<Exception> exc;
    auto res = getTableImpl(table_id, local_context, &exc);
    if (!res.second)
        throw Exception(*exc);
    return res;
}

DatabaseAndTable DatabaseCatalog::tryGetDatabaseAndTable(const StorageID & table_id, ContextPtr local_context) const
{
    return getTableImpl(table_id, local_context, nullptr);
}


DDLGuard::DDLGuard(
    Map & map_, std::shared_mutex & db_mutex_, std::unique_lock<std::mutex> guards_lock_, const String & elem, const String & database_name)
    : map(map_), db_mutex(db_mutex_), guards_lock(std::move(guards_lock_))
{
    it = map.emplace(elem, Entry{std::make_unique<std::mutex>(), 0}).first;
    ++it->second.counter;
    guards_lock.unlock();
    table_lock = std::unique_lock(*it->second.mutex);
    is_database_guard = elem.empty();
    if (!is_database_guard)
    {
        bool locked_database_for_read = db_mutex.try_lock_shared();
        if (!locked_database_for_read)
        {
            releaseTableLock();
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} is currently dropped or renamed", database_name);
        }
    }
}

void DDLGuard::releaseTableLock() noexcept
{
    if (table_lock_removed)
        return;

    table_lock_removed = true;
    guards_lock.lock();
    UInt32 counter = --it->second.counter;
    table_lock.unlock();
    if (counter == 0)
        map.erase(it);
    guards_lock.unlock();
}

DDLGuard::~DDLGuard()
{
    if (!is_database_guard)
        db_mutex.unlock_shared();
    releaseTableLock();
}

}
