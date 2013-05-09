##
# Copyright (c) 2008-2010, 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import marshal
import os
import sys
try:
    from importlib._bootstrap import _SourceFileLoader
except ImportError:
    from importlib._bootstrap import SourceFileLoader as _SourceFileLoader

from metamagic.utils.lang import meta as lang_meta, loader as lang_loader
from metamagic.utils.lang.import_ import utils as imputils
from metamagic.utils.lang.import_ import loader as imploader
from . import ast
from . import utils as pyutils


class LangModuleCache(lang_loader.LangModuleCache):
    @property
    def metainfo_path(self):
        path = self.path
        path += '.metainfo'
        return path

    def unmarshal_code(self, code_bytes):
        return marshal.loads(code_bytes[12:])


class Loader(imploader.LoaderCommon, _SourceFileLoader, imploader.SourceLoader,
                                                        lang_loader.LanguageLoaderBase):
    def __init__(self, fullname, filename, language):
        super().__init__(fullname, filename)
        self._language = language
        self._imports = {}

    def create_cache(self, modname):
        return LangModuleCache(modname, self)

    def process_code(self, modname, code, cache):
        import_stmts = pyutils.get_top_level_imports(code)
        package = modname if self.is_package(modname) else modname.rpartition('.')[0]
        imports = imputils.modules_from_import_statements(package, import_stmts,
                                                          ignore_missing=True)
        cache.metainfo.dependencies = imports

    def get_cache_path(self, modname):
        source_path = self.get_filename(modname)
        return imputils.cache_from_source(source_path)

    def get_code(self, fullname):
        code = super().get_code(fullname)

        if not self.is_deptracked(fullname):
            return code

        cache = self.create_cache(fullname)
        source_path = self.get_filename(fullname)

        if cache is not None:
            try:
                cache.validate()
            except ImportError:
                # Fix any inconsistencies in cache object before re-compiling,
                # so that code_from_source can safely work with it.
                cache.fix()

                self.process_code(fullname, code, cache)

                if not sys.dont_write_bytecode:
                    try:
                        cache.dump()
                    except NotImplementedError:
                        pass

            self._imports[fullname] = cache.metainfo.dependencies

        return code

    def _load_module_impl(self, fullname):
        _SourceFileLoader.load_module(self, fullname)
        mod = sys.modules[fullname]

        imports = self._imports.get(fullname, ())

        try:
            mod.__sx_imports__ = imports
        except AttributeError:
            pass

        modtags = self.get_modtags(fullname)
        if modtags:
            mod.__mm_module_tags__ = modtags

        return mod


class Language(lang_meta.Language):
    file_extensions = ('py',)
    loader = Loader
