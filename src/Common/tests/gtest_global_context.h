#pragma once

#include <Interpreters/Context.h>

struct ContextHolder
{
    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr context;

    ContextHolder()
        : shared_context(DB::Context::createShared())
        , context(DB::Context::createCatalog(shared_context.get()))
    {
        context->makeSystemCatalogContext();
        context->setPath("./");
    }

    ContextHolder(ContextHolder &&) = default;

    void destroy()
    {
        context->shutdown();
        context.reset();
        shared_context.reset();
    }
};

const ContextHolder & getContext();

ContextHolder & getMutableContext();

void destroyContext();
