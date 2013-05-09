##
# Copyright (c) 2011-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import io
import importlib
import marshal
import os
import sys

from metamagic.utils.lang.import_ import loader
from metamagic.utils.lang.import_ import module as module_types

from .context import DocumentContext


class LanguageCodeObject:
    def __init__(self, code, imports=(), runtime_imports=()):
        self.code = code
        self.imports = imports
        self.runtime_imports = runtime_imports


class LangModuleCacheMetaInfo(loader.ModuleCacheMetaInfo):
    def __init__(self, modname, *, magic=None, modver=None, code_offset=None):
        super().__init__(modname, magic=magic, modver=modver, code_offset=code_offset)
        self.dependencies = None
        self.runtime_dependencies = None

    def update_from_code(self, code):
        self.dependencies = code.imports
        self.runtime_dependencies = code.runtime_imports

    def marshal_extras(self):
        extras = self.get_extras()
        if extras:
            return marshal.dumps(extras)
        else:
            return None

    def get_extras(self):
        result = {}

        if self.dependencies:
            result['deps'] = self.dependencies

        if self.runtime_dependencies:
            result['runtime_deps'] = self.runtime_dependencies

        return result or None

    def unmarshal_extras(self, data):
        try:
            data = marshal.loads(data)
        except Exception as e:
            raise ImportError('could not unmarshal metainfo extras') from e

        self.set_extras(data)

    def set_extras(self, data):
        deps = data.get('deps')
        if deps:
            self.dependencies = deps

        runtime_deps = data.get('runtime_deps')
        if runtime_deps:
            self.runtime_dependencies = runtime_deps

    def get_dependencies(self):
        return self.dependencies


class LangModuleCache(loader.ModuleCache):
    metainfo_class = LangModuleCacheMetaInfo

    def validate(self):
        super().validate()
        self._loader._language.validate_code(self.code)

    def get_magic(self):
        return self._loader._language.get_language_version()

    def update_module_attributes_early(self, module):
        super().update_module_attributes_early(module)
        module.__sx_imports__ = self.metainfo.dependencies or ()
        module.__mm_runtime_imports__ = self.metainfo.runtime_dependencies or ()
        module.__language__ = self._loader._language


class LanguageLoaderBase:
    def load_module_for_runtimes(self, modname, runtimes):
        # Make sure the module is imported regularly first
        try:
            mod = sys.modules[modname]
        except KeyError:
            mod = importlib.import_module(modname)

        for runtime in runtimes:
            runtime.load_module(mod, loader=self)

        for dep in getattr(mod, '__sx_imports__', ()):
            self.load_module_for_runtimes(dep, runtimes)


class LanguageLoader(LanguageLoaderBase):
    def __init__(self, fullname, filename, language):
        super().__init__(fullname, filename)
        self._language = language
        self._context = fullname

    def create_cache(self, modname):
        return LangModuleCache(modname, self)

    def get_proxy_module_class(self):
        return self._language.proxy_module_cls

    def code_from_source(self, modname, source_bytes, *, cache=None):
        filename = self.get_filename(modname)
        if self.is_package(modname):
            path = [os.path.dirname(filename)]
            package = modname
        else:
            path = None
            package = modname.rpartition('.')[0]

        modinfo = module_types.ModuleInfo(name=modname, package=package, path=path, file=filename)
        context = DocumentContext(module=modinfo, import_context=modname)

        stream = io.BytesIO(source_bytes)
        code = self._language.load_code(stream, context=context)

        if isinstance(code, LanguageCodeObject) and cache is not None:
            cache.metainfo.update_from_code(code)

        return code

    def update_module_attributes_from_code(self, module, code):
        super().update_module_attributes_from_code(module, code)
        module.__sx_imports__ = code.imports or ()
        module.__mm_runtime_imports__ = code.runtime_imports or ()
        module.__language__ = self._language

    def _get_module_version(self, modname, imports):
        my_modver = super()._get_module_version(modname, imports)

        if imports:
            deps_modver = self._get_deps_modver(imports)

            if deps_modver > my_modver:
                my_modver = deps_modver

        return my_modver

    def _get_deps_modver(self, deps):
        max_modver = 0

        for dep in deps:
            impmod = importlib.import_module(dep)

            dep_modver = None

            try:
                loader = impmod.__loader__
            except AttributeError:
                pass
            else:
                try:
                    getmodver = loader.get_module_version
                except AttributeError:
                    pass
                else:
                    dep_modver = getmodver(dep, getattr(impmod, '__sx_imports__', ()))

            if dep_modver is None:
                # Module not handled by any of our loaders, fallback to native loader check
                dep_loader = self._get_loader(dep)
                try:
                    getfn = dep_loader.get_filename
                    pstats = dep_loader.path_stats
                except AttributeError:
                    # pytest's AssertionRewritingHook does not implement the above
                    dep_modver = 0
                else:
                    dep_modpath = getfn(dep)
                    dep_modver = self.modver_from_path_stats(pstats(dep_modpath))

            if dep_modver > max_modver:
                max_modver = dep_modver

        return max_modver

    def _get_loader(self, modname):
        if '.' in modname:
            dep_parent, _, _ = modname.rpartition('.')
            dep_parent = importlib.import_module(dep_parent)
            dep_path = dep_parent.__path__
        else:
            dep_path = None

        dep_loader = importlib.find_loader(modname, path=dep_path)
        if dep_loader is None:
            raise ImportError('could not find loader for dependency module {}'.format(modname))

        return dep_loader

    def _execute(self, module, data, method):
        modinfo = module_types.ModuleInfo(module)
        context = DocumentContext(module=modinfo, import_context=self._context)
        attributes = getattr(self._language, method)(data, context=context)
        self.set_module_attributes(module, attributes)

    def set_module_attributes(self, module, attributes):
        module.__odict__ = collections.OrderedDict()

        for attribute_name, attribute_value in attributes:
            attribute_name = str(attribute_name)
            module.__odict__[attribute_name] = attribute_value
            setattr(module, attribute_name, attribute_value)

    def execute_module_code(self, module, code):
        self._execute(module, code, 'execute_code')

    def execute_module(self, module):
        source = self.get_source_bytes(module.__name__)
        stream = io.BytesIO(source)
        self._execute(module, stream, 'load_dict')

    def invalidate_module(self, module):
        try:
            odict = module.__odict__
        except AttributeError:
            pass
        else:
            for k in odict.keys():
                try:
                    del module.__dict__[k]
                except KeyError:
                    pass


class LanguageSourceFileLoader(LanguageLoader, loader.SourceFileLoader):
    pass


class LanguageSourceBufferLoader(LanguageLoader, loader.SourceBufferLoader):
    pass
