##
# Copyright (c) 2018-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.schema import basetypes as s_types


class _GQLType:
    def __init__(self, name=None, edb_base=None, schema=None, shadow=False):
        assert not shadow or (edb_base and schema)
        assert name or edb_base
        # __typename
        if name is None:
            self._name = f'{edb_base.name.module}::{edb_base.name.name}'
        else:
            self._name = name
        # what EdgeDB entity will be the root for queries, if any
        self._edb_base = edb_base
        self._shadow = shadow
        self._schema = schema
        self._fields = {}

    @property
    def name(self):
        return self._name

    @property
    def short_name(self):
        return self._name.split('::')[-1]

    @property
    def edb_base(self):
        return self._edb_base

    @property
    def shadow(self):
        return self._shadow

    @property
    def schema(self):
        return self._schema

    def get_fields(self):
        try:
            return getattr(self, '_fields')
        except AttributeError:
            raise NotImplementedError

    def get_field_type(self, name):
        # this is just shadowing a real EdgeDB type
        if self.shadow:
            target = self._fields.get(name)
            if target is None:
                target = self.edb_base.resolve_pointer(self._schema, name)

                if target is not None:
                    target = _GQLType(
                        edb_base=target.target,
                        schema=self._schema,
                        shadow=True,
                    )

            self._fields[name] = target
            return target

    def issubclass(self, other):
        if isinstance(other, _GQLType):
            if self.shadow:
                return self.edb_base.issubclass(other.edb_base)

        return False

    def get_implementation_type(self):
        if self.shadow:
            return self.edb_base.get_implementation_type()


class GQLSchema(_GQLType):
    def __init__(self, schema):
        edb_base = schema.get('graphql::Query')
        super().__init__('__Schema', edb_base, schema)

    def get_field_type(self, name):
        if name == 'directives':
            target = self._fields.get(name)
            if target is None:
                target = GQLDirective(self.schema)

            self._fields[name] = target
            return target

        return super().get_field_type(name)


class GQLType(_GQLType):
    def __init__(self, schema):
        edb_base = schema.get('graphql::Query')
        super().__init__('__Type', edb_base, schema)

    # def get_field_type(self, name):
    #     if name == 'directives':
    #         target = self._fields.get(name)
    #         if target is None:
    #             target = GQLDirective(self.schema)

    #         self._fields[name] = target
    #         return target

    #     return super().get_field_type(name)


class GQLDirective(_GQLType):
    def __init__(self, schema):
        edb_base = schema.get(('graphql', 'Directive'))
        super().__init__('__Directive', edb_base, schema, True)


PY_COERCION_MAP = {
    str: (s_types.string.Str, s_types.uuid.UUID),
    int: (s_types.int.Int, s_types.numeric.Float, s_types.numeric.Decimal,
          s_types.uuid.UUID),
    float: (s_types.numeric.Float, s_types.numeric.Decimal),
    bool: s_types.boolean.Bool,
}

GQL_TYPE_NAMES_MAP = {
    'String': s_types.string.Str,
    'Int': s_types.int.Int,
    'Float': s_types.numeric.Float,
    'Boolean': s_types.boolean.Bool,
    'ID': s_types.uuid.UUID,
}
