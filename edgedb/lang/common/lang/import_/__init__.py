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


def reload(module):
    if isinstance(module, module_types.BaseProxyModule):
        sys.modules[module.__name__] = module.__wrapped__

        new_mod = imp.reload(module.__wrapped__)
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
    try:
        mod, _, name = cls.rpartition('.')
        return getattr(importlib.import_module(mod), name)
    except (ImportError, AttributeError) as e:
        raise ObjectImportError('could not load object %s' % cls) from e
