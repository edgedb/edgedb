##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import os
import collections
import importlib
import types
import sys
import zlib

from semantix.utils.lang import meta as lang_meta
from semantix.utils.lang.import_ import module, loader, utils as imp_utils
from semantix.utils.datastructures import OrderedSet


class JavaScriptModule(module.Module):
    """Whenever you import a javascript file, the resulting module
    will be a subclass of this class"""

    def list_deps_modules(self):
        """Builds the full list of modules that current module depends on"""

        def _collect_deps(module, deps, recs):
            recs.add(module)
            for mod in module.__sx_js_module_deps__:
                if mod not in recs:
                    _collect_deps(mod, deps, recs)
            deps.add(module)

        recs = set()
        deps = OrderedSet()
        _collect_deps(self, deps, recs)
        return tuple(deps)


class _SemantixImportsHook:
    import_re = re.compile(r'''
        ^//(
            \s*(%import|%from)\s+
                (?P<name>[^\s]+)
                (
                    \s+import\s+
                    (?P<fromlist>[^\n]+)
                )?
            )
        \s*$
    ''', re.X | re.M)

    def __call__(self, module, imports, source):
        if '%import' in source or '%from' in source:
            match = self.import_re.search(source)
            while match is not None:
                name = match.group('name')
                fromlist = match.group('fromlist')

                if fromlist is None:
                    imports.add((name.strip(),))
                else:
                    imports.add((name.strip(), tuple(frm.strip() for frm in fromlist.split(','))))

                match = self.import_re.search(source, match.end())


class Loader(loader.SourceFileLoader):
    #: version of cache format
    CACHE_MAGIC_BASE = 0

    # That's the attribute where the actual magic number will be stored.
    # The magic number depends on the 'CACHE_MAGIC_BASE' constant +
    # hash of all registered "import detect" hooks.
    # See "add_import_detect_hook" method for details.
    #
    _cache_magic = CACHE_MAGIC_BASE

    _import_detect_hooks = collections.OrderedDict()

    @classmethod
    def add_import_detect_hook(cls, hook):
        hook_name = '{}.{}'.format(hook.__class__.__module__, hook.__class__.__name__)

        if hook_name in cls._import_detect_hooks:
            # Already registered
            #
            return

        cls._import_detect_hooks[hook_name] = hook

        # Calculate new magic value, which is a crc32 hash of a special string,
        # which incorporates the CACHE_MAGIC_BASE constant + names of all registered
        # hooks
        #
        hooks_hames = ','.join(cls._import_detect_hooks.keys())
        hash_key = '{};{}'.format(cls.CACHE_MAGIC_BASE, hooks_hames).encode('latin-1')
        cls._cache_magic = zlib.crc32(hash_key)

    def __init__(self, fullname, filename, language):
        super().__init__(fullname, filename)
        self._lang = language

    def get_cache_magic(self):
        return self.__class__._cache_magic

    def cache_from_source(self, source_path):
        return imp_utils.cache_from_source(source_path, cache_ext='.js')

    def code_from_source(self, module, source_bytes):
        if not len(self._import_detect_hooks):
            # No import hooks?  We can't find any imports then.
            #
            return ()

        source = source_bytes.decode()

        # If any imports detect hooks are registered - process the source.
        #
        raw_imports = OrderedSet()
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
            if len(imp) == 1:
                imports.add(imp[0])
            else:
                # Here we have to unwind any relative imports, for instance:
                # 'from .. import foo' in 'a.b.c' module should be transformed
                # to 'import a.foo'

                imp_package, imp_froms = imp

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

                for imp_from in imp_froms:
                    imports.add(imp_package + '.' + imp_from)

        return tuple(imports)

    def load_module(self, fullname):
        module = JavaScriptModule(fullname)
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

        deps = []
        if imports:
            for imp in imports:
                try:
                    mod = sys.modules[imp]
                except KeyError:
                    # Module was not imported before; import it
                    #
                    mod = importlib.import_module(imp)

                if isinstance(mod, JavaScriptModule):
                    # We're interested in tracking only javascript modules
                    #
                    deps.append(mod)

        # Here, in 'deps' list we have all immediate modules that our module
        # depends on.  Store them in a special attribute to be able to build
        # modules index later.
        #
        module.__sx_js_module_deps__ = tuple(deps)
        module.__loaded__ = True

        return module


# XXX Do this implicitly?
Loader.add_import_detect_hook(_SemantixImportsHook())


class Language(lang_meta.Language):
    file_extensions = ('js',)
    loader = Loader
