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
from metamagic.utils.lang.import_ import spec as module_spec

from .context import DocumentContext
from . import exceptions as lang_errors
from . import runtimes as lang_runtimes


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

    def get_magic(self, metadata):
        return self._loader._language.get_language_version(metadata)

    def update_module_attributes_early(self, module):
        super().update_module_attributes_early(module)
        module.__sx_imports__ = self.metainfo.dependencies or ()
        module.__mm_runtime_imports__ = self.metainfo.runtime_dependencies or ()
        module.__language__ = self._loader._language


class LanguageLoaderBase:
    pass


class LanguageLoader(LanguageLoaderBase):
    def __init__(self, fullname, filename, language):
        super().__init__(fullname, filename)
        self._language = language
        self._context = fullname

    def create_cache(self, modname):
        return LangModuleCache(modname, self)

    def get_proxy_module_class(self):
        return self._language.get_proxy_module_cls()

    def check_runtime_compatibility(cls, module1, module2):
        runtimes1 = lang_runtimes.get_compatible_runtimes(module1, consider_derivatives=True)
        runtimes2 = lang_runtimes.get_compatible_runtimes(module2, consider_derivatives=True)
        return lang_runtimes.runtimes_compatible(runtimes1, runtimes2)

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

        module = sys.modules[modname]

        stream = io.BytesIO(source_bytes)
        code = self._language.load_code(stream, context=context)
        self.update_module_attributes_from_code(module, code)

        runtimes = lang_runtimes.get_compatible_runtimes(module)

        if runtimes:
            for impmodname in code.imports:
                try:
                    impmod = sys.modules[impmodname]
                except KeyError:
                    # Lazy import
                    continue

                lang_runtimes.load_module_for_runtimes(impmodname, runtimes)

                if not self.check_runtime_compatibility(module, impmod):
                    msg = '{} cannot import {}: they are not runtime compatible' \
                                        .format(module.__name__, impmod.__name__)

                    impruntimes = lang_runtimes.get_compatible_runtimes(impmod,
                                                                        consider_derivatives=True)

                    details = 'Module {} runtimes: {}\nModule {} runtimes: {}'.\
                                format(modname,
                                       ', '.join(str(r) for r in runtimes) or '<none>',
                                       impmodname,
                                       ', '.join(str(r) for r in impruntimes) or '<none>')

                    hint = ('Ensure that the importing module is tagged properly and that the '
                            'imported module is capable of being executed in every runtime '
                            'of the importing module; add runtime adapters where necessary.')

                    raise lang_errors.LanguageError(msg, details=details, hint=hint)

        if isinstance(code, LanguageCodeObject) and cache is not None:
            cache.metainfo.update_from_code(code)

        return code

    def update_module_attributes_from_code(self, module, code):
        super().update_module_attributes_from_code(module, code)
        module.__sx_imports__ = code.imports or ()
        module.__mm_runtime_imports__ = code.runtime_imports or ()
        module.__language__ = self._language

    def _get_module_version(self, modname, metadata):
        my_modver = super()._get_module_version(modname, metadata)

        try:
            get_deps = metadata.get_dependencies
        except AttributeError:
            imports = None
        else:
            imports = get_deps()

        if imports:
            deps_modver = self._get_deps_modver(modname, imports)

            if deps_modver > my_modver:
                my_modver = deps_modver

        return my_modver

    def _get_deps_modver(self, modname, deps):
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
                    try:
                        dep_metadata = impmod.__mm_metadata__
                    except AttributeError:
                        pass
                    else:
                        dep_modver = getmodver(dep, dep_metadata)

            if dep_modver is None:
                # Module not handled by any of our loaders, fallback to native loader check
                dep_loader = self._get_loader(dep)
                try:
                    getfn = dep_loader.get_filename
                    pstats = dep_loader.path_stats
                except AttributeError:
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
        else:
            dep_parent = None

        dep_loader = module_spec.find_spec(modname, dep_parent)
        if dep_loader is None:
            raise ImportError('could not find loader for dependency module {}'.format(modname))

        return dep_loader

    def _execute(self, module, data, method):
        modinfo = module_types.ModuleInfo(module)
        context = DocumentContext(module=modinfo, import_context=self._context)
        attributes = getattr(self._language, method)(data, context=context)
        self._language.set_module_attributes(module, attributes)

    def execute_module_code(self, module, code):
        runtimes = lang_runtimes.get_compatible_runtimes(module)

        imports = getattr(code, 'imports', None)
        if imports:
            if runtimes:
                for impname in imports:
                    lang_runtimes.load_module_for_runtimes(impname, runtimes)

        self._execute(module, code, 'execute_code')

        # Support reload
        all_runtimes = lang_runtimes.get_compatible_runtimes(module, consider_derivatives=True)
        if all_runtimes != runtimes:
            try:
                module.__mm_loaded_runtimes__.clear()
            except AttributeError:
                pass

            try:
                module.__mm_runtime_derivatives__.clear()
            except AttributeError:
                pass

            lang_runtimes.load_module_for_runtimes(module.__name__, all_runtimes)

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
                    delattr(module, k)
                except AttributeError:
                    pass


class LanguageSourceFileLoader(LanguageLoader, loader.SourceFileLoader):
    pass


class LanguageSourceBufferLoader(LanguageLoader, loader.SourceBufferLoader):
    pass
