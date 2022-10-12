#pragma once

#include <map>
#include <optional>
#include <string>
#include <Core/Types.h>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class UserDefinedCatalog
{
public:
    UserDefinedCatalog(
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings,
        const String & config_prefix,
        const String & user_catalog_name);

    UserDefinedCatalog(const UserDefinedCatalog &) = delete;
    UserDefinedCatalog & operator=(const UserDefinedCatalog &) = delete;

    String getCatalogPrefixPath() const { return path; }
    String getDefaultDatabase() const { return default_database; }
    std::optional<String> getCatalogShard() const { return shard; }
    std::optional<String> getCatalogReplica() const { return replica; }

private:
    void initMisc();

    String name;
    String type = "local";

    String path;
    String default_database = "default";

    std::optional<String> shard;
    std::optional<String> replica;
};

using UserDefinedCatalogPtr = std::shared_ptr<UserDefinedCatalog>;


class UserDefinedCatalogs
{
public:
    UserDefinedCatalogs(
        const Poco::Util::AbstractConfiguration & config, const Settings & settings, const String & config_prefix = "user_catalogs");

    UserDefinedCatalogs(const UserDefinedCatalogs &) = delete;
    UserDefinedCatalogs & operator=(const UserDefinedCatalogs &) = delete;

    size_t getUserDefinedCatalogCount() const { return impl.size(); }
    Strings getUserDefinedCatalogNames() const;
    UserDefinedCatalogPtr getUserDefinedCatalog(const String & user_catalog_name) const;

    using Impl = std::map<String, UserDefinedCatalogPtr>;

protected:
    Impl impl;
};

}
