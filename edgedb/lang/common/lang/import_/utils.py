##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import imp
import importlib
import os.path
import sys
import types


from semantix.utils.algos import topological

from .context import ImportContext
from . import cache as caches
from . import module as module_types


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


_RELOADING = {}

def reload(module):
    # XXX: imp.reload has a hardcoded check that fails on instances of module subclasses,
    # so we have to reimplement reload here

    if not isinstance(module, types.ModuleType):
        raise TypeError('reload() argument must be module')

    modname = module.__name__
    if modname not in sys.modules:
        raise ImportError('module {!r} is not in sys.modules'.format(modname), name=modname)

    try:
        return _RELOADING[modname]
    except KeyError:
        _RELOADING[modname] = module

        try:
            parent_name = modname.rpartition('.')[0]
            if parent_name and parent_name not in sys.modules:
                msg = 'parent {!r} not in sys.modules'
                raise ImportError(msg.format(parent_name), name=parent_name)
            return module.__loader__.load_module(modname)
        finally:
            _RELOADING.pop(modname, None)


def modules_from_import_statements(package, imports):
    modules = []

    for name, fromlist in imports:
        level = 0

        while level < len(name) and name[level] == '.':
            level += 1

        if level > 0:
            steps = package.rsplit('.', level - 1)
            if len(steps) < level:
                raise ValueError('relative import reaches beyond top-level package')

            suffix = name[level:]

            if suffix:
                fq_name = '{}.{}'.format(steps[0], name[level:])
            else:
                fq_name = steps[0]
        else:
            fq_name = name

        path = None
        steps = fq_name.split('.')

        add_package = True

        for i in range(len(steps)):
            modname = '.'.join(steps[:i + 1])

            loader = importlib.find_loader(modname, path=path)

            if loader is None:
                raise ValueError('could not find loader for module {}'.format(modname))

            if not isinstance(loader, importlib._bootstrap.NamespaceLoader) and not loader.is_package(modname):
                break

            if isinstance(loader, importlib._bootstrap.NamespaceLoader):
                path = loader._path._path
            else:
                modfile = loader.get_filename(modname)
                # os.path.dirname(__file__) is a common importlib assumption for __path__
                path = [os.path.dirname(modfile)]
        else:
            if fromlist:
                add_package = False

                for entry in fromlist:
                    modname = '{}.{}'.format(fq_name, entry)
                    entry_loader = importlib.find_loader(modname, path=path)

                    if entry_loader is not None and entry_loader.path != loader.path:
                        modules.append(modname)
                    else:
                        add_package = True

        if add_package:
            modules.append(fq_name)

    return modules


def modified_modules():
    caches.invalidate_modver_cache()

    for module in list(sys.modules.values()):
        try:
            loader = module.__loader__
        except AttributeError:
            # Weird custom module, skip
            continue

        try:
            loaded_modver = module.__sx_modversion__
        except AttributeError:
            # Unmanaged module
            continue

        try:
            imports = module.__sx_imports__
        except AttributeError:
            imports = ()

        current_modver = loader.get_module_version(module.__name__, imports)

        if loaded_modver != current_modver:
            yield module


def reload_modified(modified=None):
    if modified is None:
        modified = modified_modules()

    modified = tuple(modified)
    modified_names = {m.__name__ for m in modified}

    modg = {}

    for module in modified:
        imports = set(getattr(module, '__sx_imports__', ())) & modified_names
        modg[module.__name__] = {'item': module, 'deps': imports}

    reloaded = []

    for module in topological.sort(modg):
        try:
            module = reload(module)
        except ImportError as e:
            if isinstance(e.__cause__, FileNotFoundError):
                del sys.modules[module.__name__]
            else:
                raise
        else:
            reloaded.append(module)

    return reloaded
