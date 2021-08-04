#pragma once
#include <Databases/IDatabase.h>

#include <Interpreters/DatabaseCatalog.h>

namespace DB
{

template<typename StorageT, typename... StorageArgs>
void attach(IDatabase & system_database, const String & table_name, StorageArgs && ... args)
{
    {
        /// Attach to Ordinary database
        auto table_id = StorageID(DatabaseCatalog::SYSTEM_DATABASE, table_name);
        system_database.attachTable(table_name, StorageT::create(table_id, std::forward<StorageArgs>(args)...));
    }
}

}
