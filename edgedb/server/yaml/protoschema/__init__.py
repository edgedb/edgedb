##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools
import types

from importkit import context as lang_context
from importkit import yaml

from metamagic.caos import protoschema


class SchemaError(protoschema.SchemaError):
    def __init__(self, msg, *, hint=None, details=None, context=None):
        super().__init__(msg, hint=hint, details=details)
        self.context = context

    def __str__(self):
        result = super().__str__()
        if self.context and self.context.start:
            result += '\ncontext: %s, line %d, column %d' % \
                        (self.context.name, self.context.start.line, self.context.start.column)
        return result


class ProtoSchemaAdapter(yaml.Object):
    def __sx_setstate__(self, data):
        context = lang_context.SourceContext.from_object(self)

        proto_schema_class = self.get_proto_schema_class()
        proto_module_class = self.get_proto_module_class()

        module_name = context.document.module.__name__
        self.module = proto_module_class(name=module_name)

        # Local schema is the module itself plus direct imports.
        # There must be zero references to prototypes outside this schema in the module
        # being loaded.
        #
        self.localschema = localschema = proto_schema_class()
        localschema.add_module(self.module, alias=None)

        self.load_imports(context, localschema)

        self.process_data(data, localschema)

    def load_imports(self, context, localschema):
        pass

    def get_proto_schema_class(self):
        return protoschema.ProtoSchema

    def get_proto_module_class(self):
        return protoschema.ProtoModule

    def get_schema_name_class(self):
        return protoschema.SchemaName

    def process_data(self, data, localschema):
        pass

    def items(self):
        yield ('__sx_prototypes__', self.module)
