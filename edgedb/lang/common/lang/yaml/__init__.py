##
# Copyright (c) 2008-2010, 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import importlib
import itertools
import os
import yaml

from metamagic.utils.lang import meta, context as lang_context, loader as lang_loader
from metamagic.utils.lang.import_ import utils as import_utils
from metamagic.utils.lang.yaml import loader, dumper
from metamagic.utils.lang.yaml import schema as yaml_schema
from metamagic.utils.functional import Adapter


class YAMLCodeObject(lang_loader.LanguageCodeObject):
    def __init__(self, code, imports, yaml_event_stream, schemas, module_schema):
        super().__init__(code, imports)
        self.yaml_event_stream = yaml_event_stream
        self.schemas = schemas
        self.module_schema = module_schema


class Language(meta.Language):
    file_extensions = ('yml',)

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
        schemas = []
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
                    try:
                        schema = getattr(schema_mod, schema_attrname)
                    except AttributeError as e:
                        raise ValueError('could not import YAML document schema') from e

                if issubclass(schema, yaml_schema.ModuleSchemaBase):
                    module_schema = schema
                else:
                    schemas.append(schema)

                if not caching_schemas and issubclass(schema, yaml_schema.CachingSchema):
                    caching_schemas = True

                imports.update(document.imports)

        if caching_schemas:
            # To obtain caches produced by schemas, the stream has to be replayed
            rldr = loader.ReplayLoader(yaml_code, context)
            data = list(rldr.get_dict())

            getmods = import_utils.modules_from_import_statements

            if hasattr(context.module, '__path__'):
                pkg = context.module.__name__
            else:
                pkg = context.module.__package__

            all_imports = getmods(pkg, list(imports.items()))
            all_imports.sort()
            return YAMLCodeObject(data, all_imports, yaml_event_stream=ldr.get_code(),
                                  schemas=schemas, module_schema=module_schema)

        else:
            return ldr.get_code()

    @classmethod
    def execute_code(cls, code, context=None):
        if isinstance(code, YAMLCodeObject):
            imports = set()

            for imp in code.imports:
                imports.add(importlib.import_module(imp))

            if code.module_schema is not None:
                implicit_imports = code.module_schema.get_implicit_imports()
                for imp in implicit_imports:
                    if imp != context.module.__name__:
                        imports.add(importlib.import_module(imp))

                code.module_schema.normalize_code(code.code, imports)

                for d in code.code:
                    yield d
            else:
                for i, d in enumerate(code.code):
                    implicit_imports = code.module_schema.get_implicit_imports()
                    for imp in implicit_imports:
                        if imp != context.module.__name__:
                            imports.add(importlib.import_module(imp))

                    code.schemas[i].normalize_code(d[1], imports)
                    yield d

            yield ('__sx_imports__', tuple(i.__name__ for i in imports))
        else:
            if not context:
                context = lang_context.DocumentContext()

            ldr = loader.ReplayLoader(code, context)
            for d in ldr.get_dict():
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
            representer = lambda dumper, data: cls.represent_wrapper(data, dumper)

            adaptee = cls.get_adaptee()
            if adaptee is not None:
                yaml.add_multi_representer(adaptee, representer, Dumper=dumper.Dumper)
            else:
                yaml.add_multi_representer(cls, representer, Dumper=dumper.Dumper)


class Object(meta.Object, metaclass=ObjectMeta):
    @classmethod
    def represent_wrapper(cls, data, dumper):
        result = cls.__sx_getstate__(data)

        if isinstance(result, dict):
            return dumper.represent_mapping('tag:yaml.org,2002:map', result)
        elif isinstance(result, list):
            return dumper.represent_sequence('tag:yaml.org,2002:seq', result)
        elif isinstance(result, str):
            return dumper.represent_scalar('tag:yaml.org,2002:str', result)
        elif isinstance(result, bool):
            return dumper.represent_scalar('tag:yaml.org,2002:bool', str(result))
        elif isinstance(result, int):
            return dumper.represent_scalar('tag:yaml.org,2002:int', str(result))
        else:
            assert False, 'unhandled representer result type: %s' % result
