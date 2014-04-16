##
# Copyright (c) 2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib._bootstrap
import importlib.util
import os.path
import sys


class ModuleSpec:
    """Module specification (PEP 451), for compatibility with Python pre-3.4"""

    def __init__(self, name, loader, *, origin=None, loader_state=None, is_package=None):
        self.name = name
        self.loader = loader
        self.origin = origin
        self.loader_state = loader_state
        self.submodule_search_locations = [] if is_package else None


def find_spec(name, package=None):
    """Implementation of importlib.util.find_spec for Python 3.3."""

    if name.startswith('.'):
        name = importlib.util.resolve_name(name, package)

    try:
        module = sys.modules[name]
    except KeyError:
        return _find_spec(name)
    else:
        if module is None:
            return None

        return _spec_from_module(module)


def _find_spec(name):
    steps = name.split('.')
    path = None

    for i in range(len(steps)):
        loader = None
        modname = '.'.join(steps[:i + 1])

        # Some modules (mostly builtin and frozen) may not have the __loader__ attribute)
        try:
            mod = sys.modules[modname]
        except KeyError:
            has_loader = True
        else:
            has_loader = hasattr(mod, '__loader__')

        if has_loader:
            loader = importlib.find_loader(modname, path=path)

        if loader is not None:
            if isinstance(loader, importlib._bootstrap.NamespaceLoader):
                path = loader._path._path
            else:
                modfile = loader.get_filename(modname)
                # os.path.dirname(__file__) is a common importlib assumption for __path__
                path = [os.path.dirname(modfile)]

    if loader is None:
        return None

    if hasattr(loader, 'get_filename'):
        return _spec_from_file_location(name, loader)
    else:
        if hasattr(loader, 'is_package'):
            try:
                is_package = loader.is_package(name)
            except ImportError:
                is_package = None
        else:
            is_package = False

    return ModuleSpec(name, loader, is_package=is_package)


def _spec_from_file_location(name, loader):
    try:
        location = loader.get_filename(name)
    except ImportError:
        location = '<unknown>'

    if hasattr(loader, 'is_package'):
        try:
            is_package = loader.is_package(name)
        except ImportError:
            is_package = None
    else:
        is_package = False

    spec = ModuleSpec(name, loader, origin=location, is_package=is_package)

    if spec.submodule_search_locations == []:
        if location:
            dirname = os.path.dirname(location)
            spec.submodule_search_locations.append(dirname)

    return spec


def _spec_from_module(module):
    name = module.__name__

    try:
        loader = module.__loader__
    except AttributeError:
        loader = None

    try:
        location = module.__file__
    except AttributeError:
        location = None

    try:
        submodule_search_locations = list(module.__path__)
    except AttributeError:
        submodule_search_locations = None

    spec = ModuleSpec(name, loader, origin=location)
    spec.submodule_search_locations = submodule_search_locations
    return spec
