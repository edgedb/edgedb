##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import yaml
from semantix.utils.lang import meta
from semantix.utils.lang.yaml import loader


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
        return yaml.dump(data)

    @classmethod
    def load_dict(cls, stream, context=None):
        if not context:
            context = meta.DocumentContext()

        document_number = getattr(context.import_context, 'private', 0)

        ldr = loader.Loader(stream, context)
        for d in ldr.get_dict(document_number):
            yield d


class ObjectMeta(type):
    def __new__(metacls, name, bases, clsdict, *, wraps=None):
        if wraps:
            bases = bases + (wraps,)

        result = super(ObjectMeta, metacls).__new__(metacls, name, bases, clsdict)
        result._wraps = wraps

        return result

    def __init__(cls, name, bases, clsdict, *, wraps=None):
        super(ObjectMeta, cls).__init__(name, bases, clsdict)

        if hasattr(cls, 'represent'):
            if cls._wraps:
                yaml.add_multi_representer(cls._wraps, lambda dumper, data: cls.represent(data, dumper))
            else:
                yaml.add_multi_representer(cls, lambda dumper, data: cls.represent(data, dumper))



class Object(meta.Object, metaclass=ObjectMeta):
    pass
