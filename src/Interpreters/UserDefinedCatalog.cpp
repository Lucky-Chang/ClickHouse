#include "UserDefinedCatalog.h"
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_GET;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}

UserDefinedCatalog::UserDefinedCatalog(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & /*settings*/,
    const String & config_prefix,
    const String & user_catalog_name)
    : name(user_catalog_name), path(user_catalog_name)
{
    Poco::Util::AbstractConfiguration::Keys catalog_keys;
    config.keys(config_prefix, catalog_keys);

    for (const auto & catalog_key : catalog_keys)
    {
        if (startsWith(catalog_key, "type"))
            type = config.getString(config_prefix + "." + catalog_key);
        else if (startsWith(catalog_key, "relative_path"))
        {
            path = config.getString(config_prefix + "." + catalog_key);
            if (!std::all_of(path.begin(), path.end(), [](auto & c) { return isWordCharASCII(c) || c == '/'; }))
                throw Exception(
                    "DatabaseCatalog path can contain only alphanumeric and '_' : " + backQuote(path),
                    ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        }
        else if (startsWith(catalog_key, "default_database"))
            default_database = config.getString(config_prefix + "." + catalog_key);
        else if (startsWith(catalog_key, "shard"))
            shard = config.getString(config_prefix + "." + catalog_key);
        else if (startsWith(catalog_key, "replica"))
            replica = config.getString(config_prefix + "." + catalog_key);
    }

    /// get canonical path
    if (!path.empty() && !path.ends_with("/"))
        path = path + "/";
}

void UserDefinedCatalog::initMisc()
{
    /// TODO@json.lrj add rsync task
    /// TODO@json.lrj add back-compatibility task: store文件夹位置变化, atomic db soft link方式, 本地meta.sql
}


UserDefinedCatalogs::UserDefinedCatalogs(
    const Poco::Util::AbstractConfiguration & config, const Settings & settings, const String & config_prefix)
{
    Poco::Util::AbstractConfiguration::Keys user_catalog_names;
    config.keys(config_prefix, user_catalog_names);

    for (const auto & user_catalog_name : user_catalog_names)
    {
        if (!std::all_of(user_catalog_name.begin(), user_catalog_name.end(), isWordCharASCII))
            throw Exception(
                "DatabaseCatalog name can contain only alphanumeric and '_' : " + backQuote(user_catalog_name),
                ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
    }

    for (const auto & user_catalog_name : user_catalog_names)
    {
        auto user_defined_catalog
            = std::make_shared<UserDefinedCatalog>(config, settings, config_prefix + "." + user_catalog_name, user_catalog_name);
        impl.emplace(user_catalog_name, user_defined_catalog);
    }
}

Strings UserDefinedCatalogs::getUserDefinedCatalogNames() const
{
    Strings names;
    names.reserve(impl.size());
    for (auto && [name, _] : impl)
        names.push_back(name);
    return names;
}

UserDefinedCatalogPtr UserDefinedCatalogs::getUserDefinedCatalog(const String & user_catalog_name) const
{
    auto iter = impl.find(user_catalog_name);
    if (iter == impl.end())
        throw Exception("Requested user defined catalog '" + user_catalog_name + "' not found", ErrorCodes::BAD_GET);
    return iter->second;
}

}
