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
    def load_dict(cls, stream, context=None):
        if not context:
            context = meta.DocumentContext()

        document_number = getattr(context.import_context, 'private', 0)

        ldr = loader.Loader(stream, context)
        for d in ldr.get_dict(document_number):
            yield d
