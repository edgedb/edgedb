##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import imp
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
