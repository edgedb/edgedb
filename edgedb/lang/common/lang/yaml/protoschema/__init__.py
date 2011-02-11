##
# Copyright (c) 2011 M.C. Dean, Inc.
# All rights reserved.
##


import itertools


from semantix.utils.lang import protoschema
from semantix.utils.lang import yaml


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
    def construct(self):
        data = self.data
        context = self.context

        if context.document.import_context.builtin:
            self.include_builtin = True
            realm_meta_class = self.get_proto_schema_class(builtin=True)
        else:
            self.include_builtin = False
            realm_meta_class = self.get_proto_schema_class(builtin=False)

        self.toplevel = context.document.import_context.toplevel
        globalschema = context.document.import_context.protoschema

        self.localschema = localschema = realm_meta_class()
        self.module = data.get('module', None)
        if not self.module:
            self.module = context.document.module.__name__
        localschema.add_module(self.module, None)

        if self.toplevel and self.module and protoschema.SchemaName.is_qualified(self.module):
            main_module = self.get_schema_name_class()(self.module)
        else:
            main_module = None
        self.finalschema = realm_meta_class(main_module=main_module)

        for alias, module in context.document.imports.items():
            localschema.add_module(module.__name__, alias)

        self.read_elements(data, globalschema, localschema)

        if self.toplevel:
            self.order_elements(globalschema)

    def get_proto_schema_class(self, builtin):
        return protoschema.BuiltinProtoSchema if builtin else protoschema.ProtoSchema

    def get_schema_name_class(self):
        return protoschema.SchemaName

    def read_elements(self, data, globalschema, localschema):
        pass

    def order_elements(self, globalschema):
        pass

    def items(self):
        return itertools.chain([('_index_', self.finalschema), ('_module_', self.module)])
