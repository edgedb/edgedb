##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import imp
import importlib
import sys

from . import module as module_types
from .context import ImportContext

from .finder import install, update_finders


def reload(module):
    if isinstance(module, module_types.BaseProxyModule):
        sys.modules[module.__name__] = module.__wrapped__

        # XXX: imp.reload has a hardcoded check that fails on instances of module subclasses
        new_mod = module.__wrapped__.__loader__.load_module(module.__name__)
        if isinstance(new_mod, module_types.BaseProxyModule):
            module.__wrapped__ = new_mod.__wrapped__
        else:
            module.__wrapped__ = new_mod

        sys.modules[module.__name__] = module
        return module

    else:
        return imp.reload(module)


class ObjectImportError(Exception):
    pass


def get_object(cls):
    modname, _, name = cls.rpartition('.')

    try:
        mod = importlib.import_module(modname)
    except ImportError as e:
        raise ObjectImportError('could not load object %s' % cls) from e
    else:
        try:
            result = getattr(mod, name)
        except AttributeError as e:
            raise ObjectImportError('could not load object %s' % cls) from e

        return result
