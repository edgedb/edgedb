#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from collections import OrderedDict
from functools import partial
from graphql import (
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLInterfaceType,
    GraphQLInputObjectType,
    GraphQLField,
    GraphQLInputObjectField,
    GraphQLArgument,
    GraphQLList,
    GraphQLNonNull,
    GraphQLString,
    GraphQLInt,
    GraphQLFloat,
    GraphQLBoolean,
    GraphQLID,
    GraphQLEnumType,
)
from graphql.type import GraphQLEnumValue
import itertools

from edb.lang.common import ast
from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import codegen
from edb.lang.edgeql.parser import parse_fragment

from edb.lang.schema import pointers as s_pointers
from edb.lang.schema import types as s_types
from edb.lang.schema.objtypes import ObjectType
from edb.lang.schema.scalars import ScalarType

from .errors import GraphQLCoreError, GraphQLTranslationError


EDB_TO_GQL_SCALARS_MAP = {
    'str': GraphQLString,
    'anyint': GraphQLInt,
    'int16': GraphQLInt,
    'int32': GraphQLInt,
    'int64': GraphQLInt,
    'anyfloat': GraphQLFloat,
    'float32': GraphQLFloat,
    'float64': GraphQLFloat,
    'anyreal': GraphQLFloat,
    'decimal': GraphQLFloat,
    'bool': GraphQLBoolean,
    'uuid': GraphQLID,
    'datetime': GraphQLString,
    'date': GraphQLString,
    'time': GraphQLString,
}


GQL_TO_OPS_MAP = {
    'eq': ast.ops.EQ,
    'neq': ast.ops.NE,
    'gt': ast.ops.GT,
    'gte': ast.ops.GE,
    'lt': ast.ops.LT,
    'lte': ast.ops.LE,
    'like': qlast.LIKE,
    'ilike': qlast.ILIKE,
}


class GQLCoreSchema:
    def __init__(self, edb_schema):
        '''Create a graphql schema based on edgedb schema.'''

        self.edb_schema = edb_schema
        # extract and sort modules to have a consistent type ordering
        self.modules = {
            m.name for m in
            self.edb_schema.get_modules()
        } - {'schema', 'graphql'}
        self.modules = list(self.modules)
        self.modules.sort()

        self._gql_interfaces = {}
        self._gql_objtypes = {}
        self._gql_inobjtypes = {}
        self._gql_ordertypes = {}

        self._define_types()

        query = self._gql_objtypes['Query'] = GraphQLObjectType(
            name='Query',
            fields=self.get_fields('Query'),
        )

        # get a sorted list of types relevant for the Schema
        types = [
            objt for name, objt in
            itertools.chain(self._gql_objtypes.items(),
                            self._gql_inobjtypes.items())
            # the Query is included separately
            if name != 'Query'
        ]
        types = sorted(types, key=lambda x: x.name)
        self._gql_schema = GraphQLSchema(query=query, types=types)

    def get_short_name(self, name):
        return name.split('::', 1)[-1]

    def _convert_edb_type(self, edb_target):
        target = None

        # only arrays can be validly wrapped, other containers don't
        # produce a valid graphql type
        if isinstance(edb_target, s_types.Array):
            el_type = self._convert_edb_type(edb_target.element_type)
            if el_type:
                target = GraphQLList(GraphQLNonNull(el_type))
        elif isinstance(edb_target, ObjectType):
            target = self._gql_interfaces.get(
                edb_target.name,
                self._gql_objtypes.get(edb_target.name)
            )
        else:
            target = EDB_TO_GQL_SCALARS_MAP.get(edb_target.name.name)

        return target

    def _get_target(self, ptr):
        edb_target = ptr.target
        target = self._convert_edb_type(edb_target)

        if target:
            # figure out any additional wrappers due to cardinality
            # and required flags
            if ptr.cardinality in {s_pointers.PointerCardinality.OneToMany,
                                   s_pointers.PointerCardinality.ManyToMany}:
                target = GraphQLList(GraphQLNonNull(target))

            if ptr.required:
                target = GraphQLNonNull(target)

        return target

    def _get_args(self, typename):
        return {
            'filter': GraphQLArgument(self._gql_inobjtypes[typename]),
            'order': GraphQLArgument(self._gql_ordertypes[typename]),
            'first': GraphQLArgument(GraphQLInt),
            'last': GraphQLArgument(GraphQLInt),
            # before and after are supposed to be opaque values
            # serialized to string
            'before': GraphQLArgument(GraphQLString),
            'after': GraphQLArgument(GraphQLString),
        }

    def get_fields(self, typename):
        fields = OrderedDict()

        if typename == 'Query':
            for name, gqltype in sorted(self._gql_interfaces.items(),
                                        key=lambda x: x[1].name):
                if name == typename:
                    continue
                fields[name.split('::', 1)[1]] = GraphQLField(
                    GraphQLList(GraphQLNonNull(gqltype)),
                    args=self._get_args(name),
                )
        else:
            edb_type = self.edb_schema.get(typename)
            for name in sorted(edb_type.pointers, key=lambda x: x.name):
                if name.name == '__type__':
                    continue

                ptr = edb_type.resolve_pointer(self.edb_schema, name)
                target = self._get_target(ptr)
                if target:
                    if isinstance(ptr.target, ObjectType):
                        args = self._get_args(ptr.target.name)
                    else:
                        args = None

                    fields[name.name] = GraphQLField(target, args=args)

        return fields

    def get_filter_fields(self, typename):
        selftype = self._gql_inobjtypes[typename]
        fields = OrderedDict()
        fields['and'] = GraphQLInputObjectField(
            GraphQLList(GraphQLNonNull(selftype)))
        fields['or'] = GraphQLInputObjectField(
            GraphQLList(GraphQLNonNull(selftype)))
        fields['not'] = GraphQLInputObjectField(selftype)

        edb_type = self.edb_schema.get(typename)
        for name in sorted(edb_type.pointers, key=lambda x: x.name):
            if name.name == '__type__':
                continue
            if name.name in fields:
                raise GraphQLCoreError(
                    f"{name.name!r} of {typename} clashes with special "
                    "reserved fields required for GraphQL conversion"
                )

            ptr = edb_type.resolve_pointer(self.edb_schema, name)

            if not isinstance(ptr.target, ScalarType):
                continue

            target = self._convert_edb_type(ptr.target)
            intype = self._gql_inobjtypes.get(f'Filter{target.name}')
            if intype:
                fields[name.name] = GraphQLInputObjectField(intype)

        return fields

    def define_generic_filter_types(self):
        eq = ['eq', 'neq']
        comp = eq + ['gte', 'gt', 'lte', 'lt']
        string = comp + ['like', 'ilike']

        self._make_generic_input_type(GraphQLBoolean, eq)
        self._make_generic_input_type(GraphQLID, eq)
        self._make_generic_input_type(GraphQLInt, comp)
        self._make_generic_input_type(GraphQLFloat, comp)
        self._make_generic_input_type(GraphQLString, string)

    def _make_generic_input_type(self, base, ops):
        name = f'Filter{base.name}'
        self._gql_inobjtypes[name] = GraphQLInputObjectType(
            name=name,
            fields={op: GraphQLInputObjectField(base) for op in ops},
        )

    def define_generic_order_types(self):
        self._gql_ordertypes['directionEnum'] = GraphQLEnumType(
            'directionEnum',
            values=OrderedDict(
                ASC=GraphQLEnumValue(),
                DESC=GraphQLEnumValue()
            )
        )
        self._gql_ordertypes['nullsOrderingEnum'] = GraphQLEnumType(
            'nullsOrderingEnum',
            values=OrderedDict(
                SMALLEST=GraphQLEnumValue(),
                BIGGEST=GraphQLEnumValue(),
            )
        )
        self._gql_ordertypes['Ordering'] = GraphQLInputObjectType(
            'Ordering',
            fields=OrderedDict(
                dir=GraphQLInputObjectField(
                    GraphQLNonNull(self._gql_ordertypes['directionEnum']),
                ),
                nulls=GraphQLInputObjectField(
                    self._gql_ordertypes['nullsOrderingEnum'],
                    default_value='SMALLEST',
                ),
            )
        )

    def get_order_fields(self, typename):
        fields = OrderedDict()

        edb_type = self.edb_schema.get(typename)
        for name in sorted(edb_type.pointers, key=lambda x: x.name):
            if name.name == '__type__':
                continue

            ptr = edb_type.resolve_pointer(self.edb_schema, name)

            if not isinstance(ptr.target, ScalarType):
                continue

            target = self._convert_edb_type(ptr.target)
            # this makes sure that we can only order by properties
            # that can be reflected into GraphQL
            intype = self._gql_inobjtypes.get(f'Filter{target.name}')
            if intype:
                fields[name.name] = GraphQLInputObjectField(
                    self._gql_ordertypes['Ordering']
                )

        return fields

    def _define_types(self):
        interface_types = []
        obj_types = []

        self.define_generic_filter_types()
        self.define_generic_order_types()

        for modname in self.modules:
            # get all descendants of this abstract type
            module = self.edb_schema.get_module(modname)

            # every ObjectType is reflected as an interface
            interface_types += [t for t in module.get_objects()
                                if isinstance(t, ObjectType)]

        # concrete types are also reflected as Type (with a 'Type' postfix)
        obj_types += [t for t in interface_types if not t.is_abstract]

        # interfaces
        for t in interface_types:
            short_name = self.get_short_name(t.name)
            gqltype = GraphQLInterfaceType(
                name=short_name,
                fields=partial(self.get_fields, t.name),
                resolve_type=lambda obj, info: obj,
            )
            self._gql_interfaces[t.name] = gqltype

            # input object types corresponding to this interface
            gqlfiltertype = GraphQLInputObjectType(
                name='Filter' + short_name,
                fields=partial(self.get_filter_fields, t.name),
            )
            self._gql_inobjtypes[t.name] = gqlfiltertype

            # ordering input type
            gqlordertype = GraphQLInputObjectType(
                name='Order' + short_name,
                fields=partial(self.get_order_fields, t.name),
            )
            self._gql_ordertypes[t.name] = gqlordertype

        # object types
        for t in obj_types:
            interfaces = []

            # views are currently skipped
            if t.is_view():
                continue

            for st in t.get_mro():
                if (isinstance(st, ObjectType) and
                        st.name in self._gql_interfaces):
                    interfaces.append(self._gql_interfaces[st.name])

            short_name = self.get_short_name(t.name)
            gqltype = GraphQLObjectType(
                name=short_name + 'Type',
                fields=partial(self.get_fields, t.name),
                interfaces=interfaces,
            )
            self._gql_objtypes[t.name] = gqltype


def get_fkey(*args):
    '''Make a hashable key from args.'''

    if not args:
        return None
    else:
        result = []
        for arg in args:
            if isinstance(arg, (list, tuple)):
                result.append(get_fkey(*arg))

            elif isinstance(arg, dict):
                newargs = [(key, val) for key, val in arg.items()]
                newargs.sort()
                result.append(get_fkey(*newargs))

            else:
                result.append(arg)

    if len(result) == 1:
        return result[0]
    else:
        return tuple(result)


class CompleteShadowingFields:
    def __contains__(self, item):
        return True


ALL_FIELDS = CompleteShadowingFields()
NO_FIELDS = set()


class Schema:
    '''This is the schema from GQL perspective.'''

    def __init__(self, gqlcore):
        self._schema = gqlcore.edb_schema
        self._type_map = {}
        self.modules = gqlcore.modules

    @property
    def edb_schema(self):
        return self._schema

    def get(self, name, **kwargs):
        '''Get a GQL type either by name or based on EdgeDB type.'''
        # normalize name and possibly add 'edb_base' to kwargs
        edb_base = None

        if isinstance(name, str):
            if '::' not in name:
                if name in {'__Schema', '__Type', 'Query'}:
                    name = f'graphql::{name}'

        else:
            edb_base = name
            name = f'{edb_base.name.module}::{edb_base.name.name}'

        if not name.startswith('graphql::'):
            if edb_base is None:
                if '::' in name:
                    edb_base = self.edb_schema.get(name)
                else:
                    for module in self.modules:
                        edb_base = self.edb_schema.get(
                            f'{module}::{name}', None)
                        if edb_base:
                            break

            kwargs['edb_base'] = edb_base

        # check if the type already exists
        fkey = get_fkey(name, kwargs)
        gqltype = self._type_map.get(fkey)

        if not gqltype:
            _type = GQLTypeMeta.edb_map.get(name, GQLShadowType)
            gqltype = _type(schema=self, **kwargs)
            self._type_map[fkey] = gqltype

        return gqltype


class GQLTypeMeta(type):
    edb_map = {}

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)

        edb_type = dct.get('edb_type')
        if edb_type:
            mcls.edb_map[edb_type] = cls

        return cls


class GQLBaseType(metaclass=GQLTypeMeta):
    edb_type = None
    shadow_fields = NO_FIELDS

    def __init__(self, schema, **kwargs):
        name = kwargs.get('name', None)
        edb_base = kwargs.get('edb_base', None)
        dummy = kwargs.get('dummy', False)

        if edb_base is None and self.edb_type:
            edb_base = schema.edb_schema.get(self.edb_type)

        if edb_base is None:
            raise GraphQLTranslationError(
                "cannot determine the EdgeDB base type")
        if schema is None:
            raise GraphQLTranslationError("schema is missing")

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
        self._schema = schema
        self._fields = {}

        # XXX clean up needed, but otherwise it means that the type is
        # used to validate the fields/types/args/etc., but is not
        # expected to generate non-empty results, so messy EQL is not
        # needed.
        self.dummy = dummy

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
    def edb_base_name(self):
        base = self.edb_base.name
        return codegen.generate_source(
            qlast.ObjectRef(
                module=base.module,
                name=base.name,
            )
        )

    @property
    def shadow(self):
        return self.shadow_fields == ALL_FIELDS

    @property
    def schema(self):
        return self._schema

    @property
    def edb_schema(self):
        return self._schema.edb_schema

    def convert_edb_to_gql_type(self, base, **kwargs):
        if isinstance(base, s_pointers.Pointer):
            base = base.target

        if self.dummy:
            kwargs['dummy'] = True

        return self.schema.get(base, **kwargs)

    def is_field_shadowed(self, name):
        return name in self.shadow_fields

    def get_field_type(self, name, argsmap=None):
        if self.dummy:
            return None

        # this is just shadowing a real EdgeDB type
        fkey = get_fkey(name, argsmap)
        target = self._fields.get(fkey)

        if target is None:
            # special handling of '__typename'
            if name == '__typename':
                target = self.convert_edb_to_gql_type('std::str')

            else:
                target = self.edb_base.resolve_pointer(self.edb_schema, name)

                if target is not None:
                    target = self.convert_edb_to_gql_type(target)

            self._fields[fkey] = target

        return target

    def has_native_field(self, name):
        return self.edb_base.resolve_pointer(
            self.edb_schema, name) is not None

    def issubclass(self, other):
        if isinstance(other, GQLBaseType):
            if self.shadow:
                return self.edb_base.issubclass(other.edb_base)

        return False

    def get_template(self):
        '''Provide an EQL AST template to be filled.

        Return the overall ast, a reference to where the shape element
        with placeholder is, and a reference to the element which may
        be filtered.
        '''

        if self.dummy:
            return parse_fragment(f'''<json>"xxx"'''), None, None

        eql = parse_fragment(f'''
            SELECT {self.edb_base_name} {{
                xxx
            }}
        ''')

        filterable = eql
        shape = filterable.result

        return eql, shape, filterable

    def get_field_template(self, name, *, parent=None, has_shape=False):
        eql = shape = filterable = None
        if self.dummy:
            return eql, shape, filterable

        if name == '__typename':
            eql = parse_fragment(
                f'''graphql::short_name(
                    {codegen.generate_source(parent)}.__type__.name)''')

        elif has_shape:
            eql = filterable = parse_fragment(
                f'''SELECT {codegen.generate_source(parent)}.
                        {codegen.generate_source(qlast.ObjectRef(name=name))}
                        {{ xxx }}
                ''')
            filterable = eql
            shape = filterable.result

        else:
            eql = filterable = parse_fragment(
                f'''SELECT {codegen.generate_source(parent)}.
                        {codegen.generate_source(qlast.ObjectRef(name=name))}
                ''')

        return eql, shape, filterable


class GQLShadowType(GQLBaseType):
    shadow_fields = ALL_FIELDS


class GQLSchema(GQLBaseType):
    edb_type = 'graphql::__Schema'


class GQLType(GQLBaseType):
    edb_type = 'graphql::__Type'


class GQLQuery(GQLBaseType):
    edb_type = 'graphql::Query'
    shadow_fields = {'__typename'}

    def __init__(self, schema, **kwargs):
        self.modules = schema.modules
        super().__init__(schema, **kwargs)

    @property
    def short_name(self):
        return self._name.rsplit('--', 1)[-1]

    def get_field_type(self, name, argsmap=None):
        fkey = get_fkey(name, argsmap)
        target = None

        if not isinstance(name, str):
            raise GraphQLTranslationError(
                f"field name must be a str, got {type(name).__name__}")

        if name == '__type':
            if fkey in self._fields:
                return self._fields[fkey]

            target = self.convert_edb_to_gql_type('__Type', dummy=True)

        elif name == '__schema':
            if fkey in self._fields:
                return self._fields[fkey]

            target = self.convert_edb_to_gql_type('__Schema', dummy=True)

        else:
            target = super().get_field_type(name, argsmap)

            if target is None:
                for module in self.modules:
                    target = self.edb_schema.get((module, name), None)
                    if target:
                        break

                if target is not None:
                    target = self.convert_edb_to_gql_type(target)

        self._fields[fkey] = target
        return target


class GQLMutation(GQLBaseType):
    edb_type = 'graphql::Mutation'

    def __init__(self, schema, **kwargs):
        self.modules = schema.modules
        super().__init__(schema, **kwargs)

    @property
    def short_name(self):
        return self._name.rsplit('--', 1)[-1]
