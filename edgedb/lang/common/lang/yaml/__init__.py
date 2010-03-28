##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import yaml
from semantix.utils.lang import meta
from semantix.utils.lang.yaml import loader, dumper


class YamlImportError(Exception):
    pass


class Language(meta.Language):
    @classmethod
    def recognize_file(cls, filename, try_append_extension=False):
        if try_append_extension and os.path.exists(filename + '.yml'):
            if os.path.exists(filename + '.py'):
                raise YamlImportError('ambiguous yaml module name')
            return filename + '.yml'
        elif os.path.exists(filename) and filename.endswith('.yml'):
            return filename

    @classmethod
    def load(cls, stream, context=None):
        if not context:
            context = meta.DocumentContext()

        ldr = loader.Loader(stream, context)
        while ldr.check_data():
            yield ldr.get_data()

    @classmethod
    def dump(cls, data):
        return yaml.dump(data, Dumper=dumper.Dumper)

    @classmethod
    def load_dict(cls, stream, context=None):
        if not context:
            context = meta.DocumentContext()

        document_number = getattr(context.import_context, 'private', 0)

        ldr = loader.Loader(stream, context)
        for d in ldr.get_dict(document_number):
            yield d


class ObjectMeta(type):
    def __new__(metacls, name, bases, clsdict, *, wraps=None, ignore_aliases=False):
        if wraps:
            bases = bases + (wraps,)

        result = super(ObjectMeta, metacls).__new__(metacls, name, bases, clsdict)
        result._wraps = wraps

        if ignore_aliases:
            dumper.Dumper.add_ignore_aliases(wraps or result)

        return result

    def __init__(cls, name, bases, clsdict, *, wraps=None, ignore_aliases=False):
        super(ObjectMeta, cls).__init__(name, bases, clsdict)

        if hasattr(cls, 'represent'):
            representer = lambda dumper, data: cls.represent_wrapper(data, dumper)

            if cls._wraps:
                yaml.add_multi_representer(cls._wraps, representer, Dumper=dumper.Dumper)
            else:
                yaml.add_multi_representer(cls, representer, Dumper=dumper.Dumper)


class Object(meta.Object, metaclass=ObjectMeta):
    @classmethod
    def represent_wrapper(cls, data, dumper):
        result = cls.represent(data)

        if isinstance(result, dict):
            return dumper.represent_mapping('tag:yaml.org,2002:map', result)
        elif isinstance(result, list):
            return dumper.represent_sequence('tag:yaml.org,2002:seq', result)
        elif isinstance(result, str):
            return dumper.represent_scalar('tag:yaml.org,2002:str', result)
        else:
            assert False, 'unhandled representer result type: %s' % result
