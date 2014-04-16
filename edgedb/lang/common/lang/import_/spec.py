##
# Copyright (c) 2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib.util
import sys

try:
    from importlib._bootstrap import _find_spec, _spec_from_module
except ImportError:
    from ._spec import _find_deep_spec, _spec_from_module
else:
    def _find_deep_spec(name, path):
        steps = name.split('.')

        path = None

        for i in range(len(steps)):
            modname = '.'.join(steps[:i + 1])
            spec = _find_spec(modname, path)

            if spec is None:
                return None
            else:
                path = spec.submodule_search_locations

        return spec


def find_spec(name, package=None, path=None):
    """Return the spec for the specified module.

    Like importlib.util.find_spec, but does not require parent
    modules of the specified module to be imported.
    """

    if name.startswith('.'):
        name = importlib.util.resolve_name(name, package)

    try:
        module = sys.modules[name]
    except KeyError:
        return _find_deep_spec(name, path)
    else:
        if module is None:
            return None

        return _spec_from_module(module)
