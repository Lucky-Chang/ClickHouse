#pragma once
#include <Parsers/IAST_fwd.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
ASTPtr tryGetTableOverride(ContextPtr context, const String & mapped_database, const String & table);
}
