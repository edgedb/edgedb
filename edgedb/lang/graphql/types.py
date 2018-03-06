##
# Copyright (c) 2018-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.schema import basetypes as s_types


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


class GQLType:
    def __init__(self, fields):
        self.fields = fields


__SCHEMA = GQLType({
    'types': '[__Type!]!',
    'queryType': '__Type!',
    'mutationType': '__Type',
    'directives': '[__Directive!]!',
})

__TYPE = GQLType({
    'kind': '__TypeKind!',
    'name': 'String',
    'description': 'String',

    # OBJECT and INTERFACE only
    'fields': '[__Field!]',

    # OBJECT only
    'interfaces': '[__Type!]',

    # INTERFACE and UNION only
    'possibleTypes': '[__Type!]',

    # ENUM only
    'enumValues': '[__EnumValue!]',

    # INPUT_OBJECT only
    'inputFields': '[__InputValue!]',

    # NON_NULL and LIST only
    'ofType': '__Type',
})

__FIELD = GQLType({
    'name': 'String!',
    'description': 'String',
    'args': '[__InputValue!]!',
    'type': '__Type!',
    'isDeprecated': 'Boolean!',
    'deprecationReason': 'String',
})

__INPUTVALUE = GQLType({
    'name': 'String!',
    'description': 'String',
    'type': '__Type!',
    'defaultValue': 'String',
})

__ENUMVAUE = GQLType({
    'name': 'String!',
    'description': 'String',
    'isDeprecated': 'Boolean!',
    'deprecationReason': 'String',
})

__DIRECTIVE = GQLType({
    'name': 'String!',
    'description': 'String',
    'locations': '[__DirectiveLocation!]!',
    'args': '[__InputValue!]!',
})


GQL_TYPE_MAP = {
    '__schema': __SCHEMA,
    '__type': __TYPE,
}
