#include <Parsers/ASTUseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Common/typeid_cast.h>


namespace DB
{

BlockIO InterpreterUseQuery::execute()
{
    /// TODO@json.lrj add use catalog syntax
    const String & new_database = query_ptr->as<ASTUseQuery &>().database;
    getContext()->checkAccess(AccessType::SHOW_DATABASES, new_database);
    getContext()->getSessionContext()->setCurrentDatabase(new_database);
    return {};
}

}
