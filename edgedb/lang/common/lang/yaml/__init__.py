##
# Copyright (c) 2008-2010, 2012, 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
import sys
import yaml

from metamagic.utils.lang import meta, context as lang_context, loader as lang_loader
from metamagic.utils.lang.import_ import utils as import_utils
from metamagic.utils.lang.yaml import loader, dumper
from metamagic.utils.lang.yaml import schema as yaml_schema
from metamagic.utils.functional import Adapter
from metamagic.utils.datastructures import OrderedSet

from . import types


class YAMLModuleCacheMetaInfo(lang_loader.LangModuleCacheMetaInfo):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lazy_dependencies = []
        self.schemas = []

    def update_from_code(self, code):
        super().update_from_code(code)
        self.lazy_dependencies = code.lazy_imports
        self.schemas = ['{}.{}'.format(s.__module__, s.__name__) for s in code.schemas]
        self.schemas.sort()

    def get_extras(self):
        result = super().get_extras()

        if result is None:
            result = {}

        if self.lazy_dependencies:
            result['lazy_deps'] = self.lazy_dependencies

        result['schemas'] = self.schemas

        return result or None

    def set_extras(self, data):
        super().set_extras(data)

        lazy_deps = data.get('lazy_deps')
        if lazy_deps:
            self.lazy_dependencies = lazy_deps

        schemas = data.get('schemas')
        if schemas:
            self.schemas = schemas

    def get_dependencies(self):
        if self.dependencies and self.lazy_dependencies:
            return OrderedSet(self.dependencies) - self.lazy_dependencies
        else:
            return self.dependencies


class YAMLModuleCache(lang_loader.LangModuleCache):
    metainfo_class = YAMLModuleCacheMetaInfo


class YAMLCodeObject(lang_loader.LanguageCodeObject):
    def __init__(self, code, imports, yaml_event_stream, schemas, module_schema, lazy_imports):
        super().__init__(code, imports)
        self.yaml_event_stream = yaml_event_stream
        self.schemas = schemas
        self.module_schema = module_schema
        self.lazy_imports = lazy_imports


class Loader(lang_loader.LanguageSourceFileLoader):
    def create_cache(self, modname):
        return YAMLModuleCache(modname, self)


class Language(meta.Language):
    file_extensions = ('yml',)
    loader = Loader
    LANG_VERSION = 5

    @classmethod
    def get_language_version(cls, metadata):
        schemas = getattr(metadata, 'schemas', None)

        if not schemas:
            schemas_magic = 0
        else:
            schemas_magic = []

            for schema in schemas:
                schema_magic = -1

                schema_modname, _, schema_attrname = schema.rpartition('.')

                try:
                    schema_mod = importlib.import_module(schema_modname)
                except ImportError as e:
                    pass
                else:
                    try:
                        schema = getattr(schema_mod, schema_attrname)
                    except AttributeError as e:
                        pass
                    else:
                        schema_magic = schema.get_schema_magic()

                schemas_magic.append(schema_magic)

            schemas_magic = hash(tuple(schemas_magic))

        return cls.LANG_VERSION << 64 | schemas_magic & 0xFFFFFFFFFFFFFFFF

    @classmethod
    def load(cls, stream, context=None):
        if not context:
            context = lang_context.DocumentContext()

        ldr = loader.Loader(stream, context)
        while ldr.check_data():
            yield ldr.get_data()

    @classmethod
    def dump(cls, data):
        return yaml.dump(data, Dumper=dumper.Dumper)

    @classmethod
    def load_dict(cls, stream, context=None):
        if not context:
            context = lang_context.DocumentContext()

        ldr = loader.Loader(stream, context)
        for d in ldr.get_dict():
            yield d

    @classmethod
    def load_code(cls, stream, context=None):
        if not context:
            context = lang_context.DocumentContext()

        ldr = loader.RecordingLoader(stream, context)

        yaml_code = ldr.get_code()
        documents = ldr.get_documents()
        caching_schemas = False
        lazy_import_schemas = False
        schemas = []
        schema_mods = []
        module_schema = None
        imports = {}

        for document in documents:
            if document.schema is not None:
                schema_modname, _, schema_attrname = document.schema.rpartition('.')

                try:
                    schema_mod = importlib.import_module(schema_modname)
                except ImportError as e:
                    raise ValueError('could not import YAML document schema') from e
                else:
                    schema_mods.append(schema_modname)

                    try:
                        schema = getattr(schema_mod, schema_attrname)
                    except AttributeError as e:
                        raise ValueError('could not import YAML document schema') from e

                if issubclass(schema, yaml_schema.ModuleSchemaBase):
                    module_schema = schema
                else:
                    schemas.append(schema)

                if (not caching_schemas and issubclass(schema, yaml_schema.CachingSchema)
                                        and schema.cacheable()):
                    caching_schemas = True

                if getattr(schema, 'lazy_imports', False):
                    lazy_import_schemas = True

                imports.update(document.imports)

                implicit = schema.get_implicit_imports()
                for imp in implicit:
                    if imp != context.module.__name__:
                        imports[imp] = imp

        getmods = import_utils.modules_from_import_statements

        modloader = sys.modules[context.module.__name__].__loader__

        if modloader.is_package(context.module.__name__):
            pkg = context.module.__name__
        else:
            pkg = context.module.__package__

        all_imports = getmods(pkg, list(imports.items()))
        # Add schemas as imports
        all_imports.extend(schema_mods)
        all_imports.sort()
        lazy_imports = set()

        if caching_schemas or lazy_import_schemas:
            # To obtain caches produced by schemas, the stream has to be replayed
            rldr = loader.ReplayLoader(yaml_code, context)
            code = []
            lazy_import_refs = set()

            for d in rldr.get_dict():
                context.namespace.update((d,))
                code.append(d)

            if lazy_import_schemas:
                lazy_import_refs = getattr(context, 'lazy_import_refs', set())

                for k, v in context.namespace.items():
                    if isinstance(v, lang_context.LazyImportAttribute):
                        modname = v.module
                        if modname not in lazy_import_refs:
                            lazy_imports.add(modname)

                        if v.attribute:
                            modname = modname + '.' + v.attribute
                            if modname not in lazy_import_refs:
                                lazy_imports.add(modname)

            if not caching_schemas:
                code = None
        else:
            code = None

        return YAMLCodeObject(code, imports=all_imports, yaml_event_stream=ldr.get_code(),
                              schemas=schemas, module_schema=module_schema,
                              lazy_imports=frozenset(lazy_imports))

    @classmethod
    def execute_code(cls, code, context=None):
        if isinstance(code, YAMLCodeObject) and code.code is not None:
            imports = set()

            for imp in code.imports:
                imports.add(importlib.import_module(imp))

            if code.module_schema is not None:
                code.module_schema.normalize_code(code.code, imports)

                for d in code.code:
                    yield d
            else:
                for i, d in enumerate(code.code):
                    schema = code.schemas[i]
                    schema.normalize_code(d[1], imports)
                    yield d
        else:
            if isinstance(code, YAMLCodeObject):
                event_stream = code.yaml_event_stream
            else:
                event_stream = code

            if not context:
                context = lang_context.DocumentContext()

            ldr = loader.ReplayLoader(event_stream, context)
            for d in ldr.get_dict():
                context.namespace.update((d,))
                yield d

    @classmethod
    def validate_code(cls, code):
        if isinstance(code, YAMLCodeObject):
            pass
        else:
            if code is None:
                raise ImportError('invalid YAML eventlog')


class ObjectMeta(Adapter):
    def __new__(metacls, name, bases, clsdict, *, adapts=None, ignore_aliases=False, **kwargs):
        result = super(ObjectMeta, metacls).__new__(metacls, name, bases, clsdict, adapts=adapts,
                                                                                   **kwargs)
        if ignore_aliases:
            dumper.Dumper.add_ignore_aliases(adapts if adapts is not None else result)

        return result

    def __init__(cls, name, bases, clsdict, *, adapts=None, ignore_aliases=False, **kwargs):
        super(ObjectMeta, cls).__init__(name, bases, clsdict, adapts=adapts, **kwargs)

        if hasattr(cls, '__sx_getstate__'):
            representer = cls.represent_wrapper

            adaptee = cls.get_adaptee()
            if adaptee is not None:
                yaml.add_multi_representer(adaptee, representer, Dumper=dumper.Dumper)
            else:
                yaml.add_multi_representer(cls, representer, Dumper=dumper.Dumper)


class Object(meta.Object, metaclass=ObjectMeta):
    @classmethod
    def represent_wrapper(cls, dumper, data):
        return dumper.represent_data(cls.__sx_getstate__(data))
