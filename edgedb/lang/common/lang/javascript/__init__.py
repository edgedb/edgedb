##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import os
import collections
import itertools
import logging
import importlib
import types
import sys
import zlib

from semantix.utils.datastructures import OrderedSet
from semantix.utils import resource, abc
from semantix.utils.lang import meta as lang_meta
from semantix.utils.lang.import_ import module, loader, utils as imp_utils


class BaseJavaScriptModule(module.Module):
    pass


class JavaScriptModule(BaseJavaScriptModule, resource.File):
    """Whenever you import a javascript file, the resulting module
    will be a subclass of this class"""

    def __init__(self, path, name):
        BaseJavaScriptModule.__init__(self, name)

        public_path = name + '.js'
        resource.File.__init__(self, path, public_path=public_path)


class VirtualJavaScriptResource(BaseJavaScriptModule, resource.VirtualFile):
    def __init__(self, source, name):
        BaseJavaScriptModule.__init__(self, name)
        resource.VirtualFile.__init__(self, source, name + '.js')


_SX_MODULE_ = '''
_sx_module = (function(global) {
    var cbs = {}, loaded = {};

    var module = function(name, code) {
        var mod = code.call(global, name);
        if (cbs[name]) {
            for (var i = 0, c = cbs[name], len = c.length; i < len; i++) {
                c[i][0].call(c[i][1], name, mod);
            }

            delete cbs[name];
        }
        loaded[name] = mod;
    };

    module.onload = function(name, cb, scope) {
        if (loaded.hasOwnProperty(name)) {
            cb.call(scope || global, name, loaded[name]);
            return;
        }
        if (!cbs[name]) {
            cbs[name] = [];
        }
        cbs[name].push([cb, scope || global]);
    };

    module.is_loaded = function(name) {
        return loaded.hasOwnProperty(name);
    };

    return module;
})(this);
''';

_sx_module = VirtualJavaScriptResource(_SX_MODULE_, 'semantix.utils.lang.javascript.module')


class BaseModuleHook(metaclass=abc.AbstractMeta):
    pass


class ModuleHook(BaseModuleHook):
    @abc.abstractmethod
    def __call__(self, module, resource):
        pass


class ModuleAttributeHook(BaseModuleHook):
    @abc.abstractmethod
    def __call__(self, module, resource, name, obj):
        pass


ParsedImport = collections.namedtuple('ParsedImport', 'name, frm, weak')


class _SemantixImportsHook:
    import_re = re.compile(r'''
        ^(\s*)//(
            \s*(%import|%from)\s+
                (?P<name>.+?)
                (
                    \s+import\s+
                    (?P<fromlist>[^\n]+)
                )?
            )
        \s*$
    ''', re.X | re.M)

    @classmethod
    def parse(cls, source):
        imports = []

        if '%import' in source or '%from' in source:
            match = cls.import_re.search(source)
            while match is not None:
                name = match.group('name').strip()
                fromlist = match.group('fromlist')

                if fromlist is None:
                    for nm in name.split(','):
                        imports.append(ParsedImport(nm.strip(), None, False))
                else:
                    if ',' in name:
                        raise RuntimeError('invalid import statement: "%from {} import {}"'. \
                                           format(name, fromlist))

                    fromlist = (frm.strip() for frm in fromlist.split(','))
                    for frm in fromlist:
                        imports.append(ParsedImport(name, frm, False))

                match = cls.import_re.search(source, match.end())

        return imports

    def __call__(self, module, imports, source):
        imports.extend(self.parse(source))


class Loader(loader.SourceFileLoader):
    logger = logging.getLogger('semantix')

    #: version of cache format
    CACHE_MAGIC_BASE = 2

    # That's the attribute where the actual magic number will be stored.
    # The magic number depends on the 'CACHE_MAGIC_BASE' constant +
    # hash of all registered "import detect" hooks.
    # See "add_import_detect_hook" method for details.
    #
    _cache_magic = CACHE_MAGIC_BASE

    _import_detect_hooks = {}
    _module_hooks = {}

    _module_hooks_cache = set()
    _module_cache = {}

    @classmethod
    def _recalc_magic(cls):
        # Calculate new magic value, which is a crc32 hash of a special string,
        # which incorporates the CACHE_MAGIC_BASE constant + names of all registered
        # hooks
        #
        hooks_hames = ', '.join(sorted(itertools.chain(cls._import_detect_hooks.keys(),
                                                      cls._module_hooks.keys())))

        hash_key = '{};{}'.format(cls.CACHE_MAGIC_BASE, hooks_hames).encode('latin-1')
        cls._cache_magic = zlib.crc32(hash_key)

    @classmethod
    def add_module_hook(cls, hook):
        if not isinstance(hook, (ModuleHook, ModuleAttributeHook)):
            raise TypeError('invalid javascript loader hook, instance of ModuleHook '
                            'or ModuleAttributeHook expected: {}'.format(hook))

        hook_name = cls._get_hook_key(hook)

        if hook_name in cls._module_hooks:
            # Already registered
            #
            return

        cls._module_hooks[hook_name] = hook
        cls._recalc_magic()

    @classmethod
    def add_import_detect_hook(cls, hook):
        hook_name = cls._get_hook_key(hook)

        if hook_name in cls._import_detect_hooks:
            # Already registered
            #
            return

        cls._import_detect_hooks[hook_name] = hook
        cls._recalc_magic()

    @classmethod
    def _get_hook_key(cls, hook):
        return '{}.{}'.format(hook.__class__.__module__, hook.__class__.__name__)

    @classmethod
    def run_hooks(cls, mod, parent_resource=None, parent_weak=False):
        module_key = mod.__name__

        try:
            res = cls._module_cache[module_key]
        except KeyError:
            res = cls._module_cache[module_key] = VirtualJavaScriptResource(None, mod.__name__)

            if hasattr(mod, '__sx_moduleclass__'):
                for hook in cls._module_hooks.values():
                    if isinstance(hook, ModuleHook):
                        key = cls._get_hook_key(hook) + module_key
                        if key not in cls._module_hooks_cache:
                            cls._module_hooks_cache.add(key)
                            source = hook(mod, res)
                            if source:
                                res.__sx_resource_source__ += source
            else:
                for attr_name in dir(mod):
                    attr = getattr(mod, attr_name)
                    for hook in cls._module_hooks.values():
                        if isinstance(hook, ModuleAttributeHook):
                            if isinstance(attr, type):
                                if attr.__module__ != module_key:
                                    continue
                                key = cls._get_hook_key(hook) + module_key + attr.__name__
                            else:
                                key = cls._get_hook_key(hook) + module_key + attr_name
                            if key not in cls._module_hooks_cache:
                                cls._module_hooks_cache.add(key)
                                source = hook(mod, res, attr_name, attr)
                                if source:
                                    res.__sx_resource_source__ += source

        if not res.__sx_resource_source__:
            return

        res.__sx_add_required_resource__(_sx_module)

        source = res.__sx_resource_source__
        head = '// !autogenerated!\n_sx_module({!r}, function($module) {{\n"use strict";\n'
        head = head.format(mod.__name__)
        source = head + source
        source += '});\n'
        res.__sx_resource_source__ = source

        if parent_resource is not None:
            parent_resource.__sx_add_required_resource__(res, parent_weak)

        return res

    def __init__(self, fullname, filename, language):
        super().__init__(fullname, filename)
        self._lang = language

    def get_cache_magic(self):
        return self.__class__._cache_magic

    def cache_path_from_source_path(self, source_path):
        return imp_utils.cache_from_source(source_path, cache_ext='.js')

    def code_from_source(self, module, source_bytes, log=True):
        if not len(self._import_detect_hooks):
            # No import hooks?  We can't find any imports then.
            #
            return ()

        source = source_bytes.decode()

        # If any imports detect hooks are registered - process the source.
        #
        raw_imports = []

        if log and len(self._import_detect_hooks):
            self.logger.debug('parsing javascript module: {!r}'.format(module.__name__))

        for hook in self._import_detect_hooks.values():
            hook(module, raw_imports, source)

        if not len(raw_imports):
            # Source was analyzed and no imports found.
            #
            return ()

        module_name = module.__name__
        module_file = module.__file__
        is_package = self.is_package(module_file)

        imports = OrderedSet()
        for imp in raw_imports:
            if imp.frm is None:
                name = imp.name
            else:
                # Here we have to unwind any relative imports, for instance:
                # 'from .. import foo' in 'a.b.c' module should be transformed
                # to 'import a.foo'

                imp_package = imp.name
                if imp_package.startswith('.'):
                    mod_package = module_name

                    if is_package:
                        # Import from __init__
                        #
                        imp_package = imp_package[1:]

                    while imp_package.startswith('.'):
                        imp_package = imp_package[1:]
                        mod_package = mod_package.rpartition('.')[0]

                    if imp_package:
                        imp_package = mod_package + '.' + imp_package
                    else:
                        imp_package = mod_package

                name = imp_package + '.' + imp.frm

            imports.add((name, imp.weak))

        return tuple(imports)

    def _process_imports(self, imports, parent=None):
        for imp_name, weak in imports:
            try:
                mod = sys.modules[imp_name]
            except KeyError:
                # Module was not imported before; import it
                #
                mod = importlib.import_module(imp_name)

            if isinstance(mod, resource.Resource):
                # We're interested in tracking only resources
                #
                parent.__sx_add_required_resource__(mod, weak)
            elif self._module_hooks:
                # Python module?  YAML module?  Let's try to get some
                # Resources out of it, if any module hooks registered.
                #
                self.run_hooks(mod, parent, weak)

    def load_module(self, fullname):
        if fullname in sys.modules:
            # XXX mask bug in importlib; to be removed
            return sys.modules[fullname]

        module = JavaScriptModule(self._path, fullname)
        module.__file__ = self._path

        is_package = os.path.splitext(os.path.basename(self._path))[0] == '__init__'

        if is_package:
            module.__path__ = [os.path.dirname(self._path)]
        else:
            module.__package__ = module.__name__.rpartition('.')[0]

        module.__loader__ = self
        module.__language__ = self._lang

        # Time to add newly created module to 'sys.modules'. Important to do so before
        # processing inner modules to avoid recursion.
        #
        sys.modules[module.__name__] = module

        imports = self.get_code(module)
        if imports:
            self._process_imports(imports, module)

        if '.' in module.__name__:
            # Link parent package
            #
            parent_name = module.__name__.rpartition('.')[0]
            parent = sys.modules[parent_name]

            if isinstance(parent, resource.Resource):
                module.__sx_resource_parent__ = parent

        module.__loaded__ = True

        return module


# XXX Do this implicitly?
Loader.add_import_detect_hook(_SemantixImportsHook())


class Language(lang_meta.Language):
    file_extensions = ('js',)
    loader = Loader

