##
# Copyright (c) 2008-2010, 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.bootstrap.yaml import validator
from semantix.utils.lang.import_ import get_object

from semantix.utils.lang.yaml import constructor as yaml_constructor


class Schema(validator.Schema):
    @classmethod
    def prepare_class(cls, context, data):
        cls._schema_data = data
        cls._context = context

    def init_constructor(self):
        return yaml_constructor.Constructor()

    def _build(self, dct):
        dct_id = id(dct)

        if dct_id in self.refs:
            return self.refs[dct_id]

        if isinstance(dct, type) and issubclass(dct, Schema):
            # This happens when top-level anchor is assigned to the schema
            dct = dct._schema_data

        elif isinstance(dct, str):
            imported_schema = self._get_imported_schema(dct)
            dct = imported_schema._schema_data

        elif dct.get('extends'):
            imported_schema = self._get_imported_schema(dct['extends'])()

            imported_schema._build(imported_schema._schema_data)
            self.refs.update(imported_schema.refs)

            _dct = imported_schema._schema_data.copy()
            _dct.update(dct)
            dct = _dct

        return super()._build(dct)

    def _get_imported_schema(self, schema_name):
        # Reference to an external schema
        head, _, tail = schema_name.partition('.')

        imported = self.__class__._context.document.imports.get(head)
        if imported:
            head = imported.__name__

        schema = get_object(head + '.' + tail)

        return schema
