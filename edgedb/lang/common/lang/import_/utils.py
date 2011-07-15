##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import imp
import importlib
import os.path

from .context import ImportContext

def cache_from_source(path, debug_override=None, cache_ext=None):
    cachepath = imp.cache_from_source(path, debug_override)

    if cache_ext is not None:
        pathext = cache_ext
    else:
        pathpre, pathext = os.path.splitext(path)

    if pathext != '.py' or cache_ext is not None:
        cachepre, cacheext = os.path.splitext(cachepath)
        cachepath = cachepre + pathext + 'c'

    return cachepath


def source_from_cache(path):
    cachepath = path
    path = imp.source_from_cache(cachepath)

    cachepre, cacheext = os.path.splitext(cachepath)
    if cacheext != '.pyc':
        pathpre, pathext = os.path.splitext(path)
        path = pathpre + cacheext[:-1]

    return path


def import_module(full_module_name, *, loader=None):
    if loader is not None:
        if isinstance(full_module_name, ImportContext):
            context = full_module_name.__class__.copy(full_module_name)
            context.loader = loader
        else:
            context = ImportContext(full_module_name, loader=loader)

        full_module_name = context

    return importlib.import_module(full_module_name)
