#pragma once

#include <Databases/IDatabase.h>
#include <Common/ThreadPool.h>

namespace DB
{
class ASTCreateQuery;

class DatabaseFactory
{
public:
    /// TODO@json.lrj add to IDatabaseCatalog
    static DatabasePtr get(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context);

    static DatabasePtr getImpl(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context);
};

}
