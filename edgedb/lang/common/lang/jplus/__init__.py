##
# Copyright (c) 2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
import marshal
import types

from metamagic.utils import resource

from metamagic.utils.lang import meta as lang_meta, loader
from metamagic.utils.lang.import_ import module as module_types
from metamagic.utils.lang.import_ import loader as base_loader
from metamagic.utils.lang.import_ import utils as import_utils

from metamagic.utils.debug import debug
from metamagic.utils.markup import dump, dump_code

from metamagic.utils.lang.jplus import transpiler, parser
from metamagic.utils.lang.javascript import codegen
from metamagic.utils.lang import javascript
from metamagic.utils.lang import runtimes as lang_runtimes


class JPlusModule(javascript.BaseJavaScriptModule, resource.VirtualFile):
    def __init__(self, name):
        javascript.BaseJavaScriptModule.__init__(self, name)
        resource.VirtualFile.__init__(self, None, name + '.js')


class ProxyJPlusModule(module_types.ProxyModule, JPlusModule):
    pass


class JPlusModuleCache(loader.LangModuleCache):
    MAGIC = 1

    def get_magic(self, metadata):
        return self.MAGIC

    def marshal_code(self, code):
        return code.marshal()

    def unmarshal_code(self, data):
        try:
            return JPlusCodeObject.unmarshal(data)
        except Exception as e:
            raise ImportError('could not unmarshal jplus cache code') from e


class Compiler:
    def __init__(self):
        self.parser = parser.Parser()
        self.transpiler = transpiler.Transpiler()

    def compile_file(self, filename):
        with open(filename, 'rt') as f:
            source = f.read()

        return self.compile_source(filename, source)

    @debug
    def compile_source(self, filename, source, modname, package):
        jsp_ast = self.parser.parse(source, filename=filename)

        """LOG [jsp] JS+ AST
        dump(jsp_ast)
        """

        js_ast, deps = self.transpiler.transpile(jsp_ast,
                                                 module=modname,
                                                 package=package)

        """LOG [jsp] JS AST
        dump(js_ast)
        """

        js_src = codegen.JavascriptSourceGenerator.to_source(js_ast)

        """LOG [jsp] Resultant JS Source
        dump_code(js_src, lexer='javascript', header='Resultant JS Source')
        """

        return js_src, deps


class BaseLoader:
    def create_cache(self, modname):
        return JPlusModuleCache(modname, self)


class Loader(BaseLoader, loader.LanguageSourceFileLoader):
    def new_module(self, fullname):
        return JPlusModule(fullname)


class BufferLoader(BaseLoader, loader.LanguageSourceBufferLoader):
    def new_module(self, fullname):
        return JPlusModule(fullname)


class JPlusCodeObject(loader.LanguageCodeObject):
    def marshal(self):
        return marshal.dumps((self.code, self.imports))

    @classmethod
    def unmarshal(cls, data):
        code, imports = marshal.loads(data)
        return cls(code, imports)


class Language(lang_meta.Language):
    file_extensions = ('jp',)
    loader = Loader
    default_runtime = javascript.JavaScriptRuntime

    @classmethod
    def get_proxy_module_cls(cls):
        return ProxyJPlusModule

    @classmethod
    def load_code(cls, stream, context):
        modname = context.module.__name__

        jp_source = stream.read().decode('utf-8')
        filename = context.module.__file__

        package = getattr(context.module, '__package__', None)
        if not package:
            package = modname

        js_code, imports = Compiler().compile_source(source=jp_source,
                                                     filename=filename,
                                                     modname=modname,
                                                     package=package)

        imports = import_utils.modules_from_import_statements(package, imports)
        imports.append('metamagic.utils.lang.jplus.support.builtins')

        return JPlusCodeObject(js_code, imports)

    @classmethod
    def execute_code(cls, code, context):
        for modname in code.imports:
            importlib.import_module(modname)

        yield '__sx_resource_source_value__', code.code.encode('utf-8')
