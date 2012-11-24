##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import imp
import importlib
import os
import sys

from .context import ImportContext

from .finder import install, update_finders


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
