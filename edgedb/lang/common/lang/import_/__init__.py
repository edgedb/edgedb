##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import imp
import sys

from . import module as module_types


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


class ImportContext(str):
    def __getitem__(self, key):
        result = super().__getitem__(key)
        return self.__class__.copy(result, self)

    @classmethod
    def copy(cls, name, other):
        return cls(name)

    @classmethod
    def from_parent(cls, name, parent):
        return cls(name)
