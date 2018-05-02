##
# Copyright (c) 2018-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.schema import basetypes as s_types
from edgedb.lang.schema import pointers as s_pointers


# TODO: this class needs a generic mechanism for handling available
# fields that allows introspection. So rather than field names being
# hardcoded in the methods, there must be a registry.
class _GQLType:
    def __init__(self, schema=None, name=None, edb_base=None, shadow=False):
        assert not shadow or (edb_base and schema)
        assert name or edb_base
        # __typename
        if name is None:
            self._name = f'{edb_base.name.module}::{edb_base.name.name}'
        else:
            self._name = name
        # determine module from name is not already specified
        if not hasattr(self, '_module'):
            if '::' in self._name:
                self._module = self._name.split('::', 1)[0]
            else:
                self._module = None

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
    def module(self):
        return self._module

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

    def convert_edb_to_gql_type(self, base, name):
        if isinstance(base, s_pointers.Pointer):
            base = base.target
        return _GQLType(
            edb_base=base,
            schema=self.schema,
            shadow=True,
        )

    def get_field_type(self, name):
        # this is just shadowing a real EdgeDB type
        target = self._fields.get(name)

        if target is None:
            # special handling of '__typename'
            if name == '__typename':
                target = self.convert_edb_to_gql_type(
                    self.schema.get('std::str'), name)

            elif self.shadow:
                target = self.edb_base.resolve_pointer(self._schema, name)

                if target is not None:
                    target = self.convert_edb_to_gql_type(target, name)

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
    def __init__(self, schema, module):
        edb_base = schema.get('graphql::Schema')
        self._module = module
        super().__init__(schema, '__Schema', edb_base)

    def get_field_type(self, name):
        if name == 'directives':
            target = self._fields.get(name)
            if target is None:
                target = GQLDirective(self.schema)

            self._fields[name] = target
            return target
        elif name == 'queryType':
            target = self._fields.get(name)
            if target is None:
                target = GQLQuery(self.schema, self.module)

            self._fields[name] = target
            return target
        elif name == 'mutationType':
            target = self._fields.get(name)
            if target is None:
                target = GQLMutation(self.schema, self.module)

            self._fields[name] = target
            return target

        return super().get_field_type(name)


class GQLType(_GQLType):
    def __init__(self, schema):
        edb_base = schema.get('graphql::Query')
        super().__init__(schema, '__Type', edb_base)


class GQLQuery(_GQLType):
    def __init__(self, schema, module):
        edb_base = schema.get('graphql::Query')
        self._module = module
        # we give unusual full name, so that it doesn't clash with a
        # potential ObjectType `Query` in one of the modules
        super().__init__(schema, f'{module}--Query', edb_base)

    @property
    def short_name(self):
        return self._name.rsplit('--', 1)[-1]

    def get_field_type(self, name):
        assert isinstance(name, str)
        target = super().get_field_type(name)

        # special handling of '__type' and '__schema'
        if name == '__type':
            target = GQLType(self.schema)
        elif name == '__schema':
            target = GQLSchema(self.schema, self.module)
            # populate the query type
            target._fields['queryType'] = self

        if target is None:
            target = self.schema.get((self._module, name))

            if target is not None:
                target = self.convert_edb_to_gql_type(target, name)

        self._fields[name] = target
        return target


class GQLMutation(_GQLType):
    def __init__(self, schema, module):
        edb_base = schema.get('graphql::Mutation')
        self._module = module
        # we give unusual full name, so that it doesn't clash with a
        # potential ObjectType `Mutation` in one of the modules
        super().__init__(schema, f'{module}--Mutation', edb_base)

    @property
    def short_name(self):
        return self._name.rsplit('--', 1)[-1]


class GQLField(_GQLType):
    def __init__(self, schema):
        edb_base = schema.get('graphql::Field')
        super().__init__(schema, '__Field', edb_base)


class GQLEnumValue(_GQLType):
    def __init__(self, schema):
        edb_base = schema.get('graphql::EnumValue')
        super().__init__(schema, '__EnumValue', edb_base)


class GQLInputValue(_GQLType):
    def __init__(self, schema):
        edb_base = schema.get('graphql::InputValue')
        super().__init__(schema, '__InputValue', edb_base, True)


class GQLDirective(_GQLType):
    def __init__(self, schema):
        edb_base = schema.get(('graphql', 'Directive'))
        super().__init__(schema, '__Directive', edb_base, True)


class GQLTypeKind(_GQLType):
    def __init__(self, schema):
        edb_base = schema.get(('graphql', 'typeKind'))
        super().__init__(schema, '__TypeKind', edb_base, True)


class GQLDirectiveLocation(_GQLType):
    def __init__(self, schema):
        edb_base = schema.get(('graphql', 'directiveLocation'))
        super().__init__(schema, '__DirectiveLocation', edb_base, True)

    def convert_edb_to_gql_type(self, base, name):
        if name == 'args':
            return GQLInputValue(
                edb_base=base.target,
                schema=self.schema,
                shadow=True,
            )
        else:
            return super().convert_edb_to_gql_type(base, name)


PY_COERCION_MAP = {
    str: (s_types.string.Str, s_types.uuid.UUID),
    int: (s_types.int.Int64, s_types.numeric.Float64, s_types.numeric.Numeric,
          s_types.uuid.UUID),
    float: (s_types.numeric.Float64, s_types.numeric.Numeric),
    bool: s_types.boolean.Bool,
}

GQL_TYPE_NAMES_MAP = {
    'String': s_types.string.Str,
    'Int': s_types.int.Int64,
    'Float': s_types.numeric.Float64,
    'Boolean': s_types.boolean.Bool,
    'ID': s_types.uuid.UUID,
}
