##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import imp
import importlib
import os
import re
import sys
import types


from metamagic.utils.algos import topological
from metamagic.utils.debug import debug

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


class _NotFoundModuleError(Exception): pass
_init_re = re.compile(r'^(.*)\{sep}__init__\.(\w+)$'.format(sep=os.sep))
def import_path(path):
    def _import_module(name):
        try:
            return importlib.import_module(name)
        except ImportError as ex:
            if ex.args[0].startswith('No module named'):
                raise _NotFoundModuleError from ex
            raise

    cwd = os.path.abspath(os.path.realpath(os.getcwd()))
    path = os.path.abspath(os.path.realpath(path))

    paths = []
    for p in sys.path:
        p = os.path.abspath(os.path.realpath(p))
        if path.startswith(p):
            paths.append(p)
    if not paths:
        raise ImportError('unable to find module with path {!r}: not in a sys.path'.format(path))

    paths.sort(key=lambda x: len(x), reverse=True)

    init_match = _init_re.match(path)
    if init_match:
        path = init_match.group(1)
    else:
        path = path.rpartition('.')[0]

    was_in_cwd = False
    for syspath in paths:
        if syspath == cwd:
            was_in_cwd = True
            continue

        tail = path[len(syspath.rstrip(os.sep)):]
        if tail[-1] != os.sep:
            tail = path

        modname = tail.strip(os.sep).replace(os.sep, '.')
        try:
            return _import_module(modname)
        except _NotFoundModuleError:
            pass

    if was_in_cwd:
        modname = path[len(cwd):].strip(os.sep).replace(os.sep, '.')
        try:
            return _import_module(modname)
        except _NotFoundModuleError:
            pass

    raise ImportError('unable to find module with path {!r}'.format(path))


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

        proxied = isinstance(module, module_types.BaseProxyModule)

        if proxied:
            sys.modules[modname] = module.__wrapped__

        try:
            parent_name = modname.rpartition('.')[0]
            if parent_name and parent_name not in sys.modules:
                msg = 'parent {!r} not in sys.modules'
                raise ImportError(msg.format(parent_name), name=parent_name)
            mod = module.__loader__.load_module(modname)

            if proxied:
                if isinstance(mod, module_types.BaseProxyModule):
                    module.__wrapped__ = mod.__wrapped__
                else:
                    module.__wrapped__ = mod

                mod = module

            return mod

        finally:
            if proxied:
                sys.modules[modname] = module

            _RELOADING.pop(modname, None)


def resolve_module_name(name, package):
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

    return fq_name


def modules_from_import_statements(package, imports, ignore_missing=False):
    modules = []

    for name, fromlist in imports:
        path = None
        fq_name = resolve_module_name(name, package)

        if fq_name == 'builtins':
            continue

        steps = fq_name.split('.')

        add_package = True
        loader = None

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

            if loader is None:
                if ignore_missing:
                    add_package = False
                    break
                else:
                    raise ValueError('could not find loader for module {}'.format(modname))

            if not isinstance(loader, importlib._bootstrap.NamespaceLoader):
                if not loader.is_package(modname):
                    break

            if isinstance(loader, importlib._bootstrap.NamespaceLoader):
                path = loader._path._path
            else:
                modfile = loader.get_filename(modname)
                # os.path.dirname(__file__) is a common importlib assumption for __path__
                path = [os.path.dirname(modfile)]
        else:
            if fromlist and not isinstance(fromlist, str):
                # Try to obtain package loader path and do it with care
                # as NamespaceLoader/NamespacePath have slightly different
                # semantics.
                try:
                    loader_path = loader.path
                except AttributeError:
                    try:
                        loader_path = loader._path
                    except AttributeError:
                        loader_path = []

                if loader_path:
                    loader_path = list(loader_path)

                add_package = False

                for entry in fromlist:
                    modname = '{}.{}'.format(fq_name, entry)
                    entry_loader = importlib.find_loader(modname, path=path)

                    if entry_loader is not None and list(entry_loader.path) != loader_path:
                        modules.append(modname)
                    else:
                        add_package = True

        if add_package:
            modules.append(fq_name)

    return modules


def get_module_version(module):
    try:
        metadata = module.__mm_metadata__
    except AttributeError:
        return None
    else:
        return getattr(metadata, 'modver', None)


def modified_modules():
    caches.invalidate_modver_cache()

    for module in list(sys.modules.values()):
        try:
            loader = module.__loader__
        except AttributeError:
            # Weird custom module, skip
            continue

        try:
            loaded_metainfo = module.__mm_metadata__
        except AttributeError:
            # Unmanaged module
            continue

        current_modver = loader.get_module_version(module.__name__, loaded_metainfo)

        if loaded_metainfo.modver != current_modver:
            yield module


@debug
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
            """LINE [lang.import.reload] RELOADED MODULE
            module.__name__
            """
            reloaded.append(module)

    return reloaded
