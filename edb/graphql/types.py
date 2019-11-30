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


from __future__ import annotations

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

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import codegen
from edb.edgeql.parser import parse_fragment

from edb.schema import modules as s_mod
from edb.schema import pointers as s_pointers
from edb.schema import objtypes as s_objtypes
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema
from edb.schema import objects as s_objects

from . import errors as g_errors


EDB_TO_GQL_SCALARS_MAP = {
    # For compatibility with GraphQL we cast json into a String, since
    # GraphQL doesn't have an equivalent type with arbitrary fields.
    'std::json': GraphQLString,
    'std::str': GraphQLString,
    'std::anyint': GraphQLInt,
    'std::int16': GraphQLInt,
    'std::int32': GraphQLInt,
    'std::int64': GraphQLInt,
    'std::anyfloat': GraphQLFloat,
    'std::float32': GraphQLFloat,
    'std::float64': GraphQLFloat,
    'std::anyreal': GraphQLFloat,
    'std::decimal': GraphQLFloat,
    'std::bigint': GraphQLFloat,
    'std::bool': GraphQLBoolean,
    'std::uuid': GraphQLID,
    'std::datetime': GraphQLString,
    'std::duration': GraphQLString,
    'std::local_datetime': GraphQLString,
    'std::local_date': GraphQLString,
    'std::local_time': GraphQLString,
    'std::bytes': None,
}


# used for casting input values from GraphQL to EdgeQL
GQL_TO_EDB_SCALARS_MAP = {
    'String': 'str',
    'Int': 'int64',
    'Float': 'float64',
    'Boolean': 'bool',
    'ID': 'uuid',
}


GQL_TO_OPS_MAP = {
    'eq': '=',
    'neq': '!=',
    'gt': '>',
    'gte': '>=',
    'lt': '<',
    'lte': '<=',
    'like': 'LIKE',
    'ilike': 'ILIKE',
}


HIDDEN_MODULES = s_schema.STD_MODULES - {'std'}
TOP_LEVEL_TYPES = {'Query', 'Mutation'}


class GQLCoreSchema:
    def __init__(self, edb_schema):
        '''Create a graphql schema based on edgedb schema.'''

        self.edb_schema = edb_schema
        # extract and sort modules to have a consistent type ordering
        self.modules = {
            m.get_name(self.edb_schema)
            for m in self.edb_schema.get_objects(type=s_mod.Module)
        } - HIDDEN_MODULES
        self.modules = list(self.modules)
        self.modules.sort()

        self._gql_interfaces = {}
        self._gql_objtypes = {}
        self._gql_inobjtypes = {}
        self._gql_ordertypes = {}
        self._gql_enums = {}

        self._define_types()

        query = self._gql_objtypes['Query'] = GraphQLObjectType(
            name='Query',
            fields=self.get_fields('Query'),
        )

        # If a database only has abstract types and scalars, no
        # mutations will be possible (such as in a blank database),
        # but we would still want the reflection to work without
        # error, even if all that can be discovered through GraphQL
        # then is the schema.
        fields = self.get_fields('Mutation')
        if fields:
            mutation = self._gql_objtypes['Mutation'] = GraphQLObjectType(
                name='Mutation',
                fields=fields,
            )
        else:
            mutation = None

        # get a sorted list of types relevant for the Schema
        types = [
            objt for name, objt in
            itertools.chain(self._gql_objtypes.items(),
                            self._gql_inobjtypes.items())
            # the Query is included separately
            if name not in TOP_LEVEL_TYPES
        ]
        types = sorted(types, key=lambda x: x.name)
        self._gql_schema = GraphQLSchema(
            query=query, mutation=mutation, types=types)

        # this map is used for GQL -> EQL translator needs
        self._type_map = {}

    @property
    def edgedb_schema(self):
        return self.edb_schema

    @property
    def graphql_schema(self):
        return self._gql_schema

    def get_gql_name(self, name):
        module, shortname = name.split('::', 1)
        if module in {'default', 'std'}:
            return shortname
        else:
            return f'{module}__{shortname}'

    def get_input_name(self, inputtype, name):
        if '__' in name:
            module, shortname = name.split('__', 1)
            return f'{module}__{inputtype}{shortname}'
        else:
            return f'{inputtype}{name}'

    def gql_to_edb_name(self, name):
        '''Convert the GraphQL field name into an EdgeDB type/view name.'''

        if '__' in name:
            return name.replace('__', '::', 1)
        else:
            return name

    def _convert_edb_type(self, edb_target):
        target = None

        # only arrays can be validly wrapped, other containers don't
        # produce a valid graphql type
        if edb_target.is_array():
            subtype = edb_target.get_subtypes(self.edb_schema)[0]
            if subtype.get_name(self.edb_schema) == 'std::json':
                # cannot expose arrays of JSON
                return None

            el_type = self._convert_edb_type(subtype)
            if el_type is None:
                # we can't expose an array of unexposable type
                return el_type
            else:
                target = GraphQLList(GraphQLNonNull(el_type))

        elif edb_target.is_object_type():
            target = self._gql_interfaces.get(
                edb_target.get_name(self.edb_schema),
                self._gql_objtypes.get(edb_target.get_name(self.edb_schema))
            )

        elif edb_target.is_scalar() and edb_target.is_enum(self.edb_schema):
            name = self.get_gql_name(edb_target.get_name(self.edb_schema))

            if name in self._gql_enums:
                target = self._gql_enums.get(name)

        else:
            base_target = edb_target.get_topmost_concrete_base(self.edb_schema)
            bt_name = base_target.get_name(self.edb_schema)
            try:
                target = EDB_TO_GQL_SCALARS_MAP[bt_name]
            except KeyError:
                # this is the scalar base case, where all potentially
                # unrecognized scalars should end up
                edb_typename = edb_target.get_verbosename(self.edb_schema)
                raise g_errors.GraphQLCoreError(
                    f"could not convert {edb_typename!r} type to"
                    f" a GraphQL type")

        return target

    def _get_target(self, ptr):
        edb_target = ptr.get_target(self.edb_schema)
        target = self._convert_edb_type(edb_target)

        if target is not None:
            # figure out any additional wrappers due to cardinality
            # and required flags
            target = self._wrap_target(ptr, target)

        return target

    def _wrap_target(self, ptr, target, *,
                     for_input=False, ignore_required=False):
        # figure out any additional wrappers due to cardinality
        # and required flags
        if not ptr.singular(self.edb_schema):
            target = GraphQLList(GraphQLNonNull(target))

        if not ignore_required:
            # for input values having a default cancels out being required
            if (ptr.get_required(self.edb_schema) and
                    # for_input -> no default
                    (not for_input or
                     ptr.get_default(self.edb_schema) is None)):
                target = GraphQLNonNull(target)

        return target

    def _get_query_args(self, typename):
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

    def _get_insert_args(self, typename):
        return {
            'data': GraphQLArgument(
                GraphQLNonNull(GraphQLList(GraphQLNonNull(
                    self._gql_inobjtypes[f'Insert{typename}'])))),
        }

    def _get_update_args(self, typename):
        # some types have no updates
        uptype = self._gql_inobjtypes.get(f'Update{typename}')
        if uptype is None:
            return {}

        # the update args are same as for query + data
        args = self._get_query_args(typename)
        args['data'] = GraphQLArgument(GraphQLNonNull(uptype))
        return args

    def get_fields(self, typename):
        fields = OrderedDict()

        if typename == 'Query':
            for name, gqltype in sorted(self._gql_interfaces.items(),
                                        key=lambda x: x[1].name):
                if name in TOP_LEVEL_TYPES:
                    continue
                fields[gqltype.name] = GraphQLField(
                    GraphQLList(GraphQLNonNull(gqltype)),
                    args=self._get_query_args(name),
                )
        elif typename == 'Mutation':
            for name, gqltype in sorted(self._gql_objtypes.items(),
                                        key=lambda x: x[1].name):
                if name in TOP_LEVEL_TYPES:
                    continue
                gname = self.get_gql_name(name)
                fields[f'delete_{gname}'] = GraphQLField(
                    GraphQLList(GraphQLNonNull(gqltype)),
                    args=self._get_query_args(name),
                )
                fields[f'insert_{gname}'] = GraphQLField(
                    GraphQLList(GraphQLNonNull(gqltype)),
                    args=self._get_insert_args(name),
                )

            for name, gqltype in sorted(self._gql_interfaces.items(),
                                        key=lambda x: x[1].name):
                if (name in TOP_LEVEL_TYPES or
                        f'Update{name}' not in self._gql_inobjtypes):
                    continue
                gname = self.get_gql_name(name)
                args = self._get_update_args(name)
                if args:
                    fields[f'update_{gname}'] = GraphQLField(
                        GraphQLList(GraphQLNonNull(gqltype)),
                        args=args,
                    )
        else:
            edb_type = self.edb_schema.get(typename)
            pointers = edb_type.get_pointers(self.edb_schema)

            for name, ptr in sorted(pointers.items(self.edb_schema)):
                if name == '__type__':
                    continue

                # We want to look at the pointer lineage because that
                # will be reflected into GraphQL interface that is
                # being extended and the type cannot be changed.
                lineage = s_objects.compute_lineage(self.edb_schema, ptr)

                # We want the first non-generic ancestor of this
                # pointer as its target type will dictate the target
                # types of all its derived pointers.
                #
                # NOTE: We're guaranteed to have a non-generic one
                # since we're inspecting the lineage of a pointer
                # belonging to an actual type.
                for ancestor in reversed(lineage):
                    if not ancestor.generic(self.edb_schema):
                        ptr = ancestor
                        break

                target = self._get_target(ptr)
                if target is not None:
                    if ptr.get_target(self.edb_schema).is_object_type():
                        args = self._get_query_args(
                            ptr.get_target(self.edb_schema).get_name(
                                self.edb_schema))
                    else:
                        args = None

                    fields[name] = GraphQLField(target, args=args)

        return fields

    def get_filter_fields(self, typename, nested=False):
        selftype = self._gql_inobjtypes[typename]
        fields = OrderedDict()
        if not nested:
            fields['and'] = GraphQLInputObjectField(
                GraphQLList(GraphQLNonNull(selftype)))
            fields['or'] = GraphQLInputObjectField(
                GraphQLList(GraphQLNonNull(selftype)))
            fields['not'] = GraphQLInputObjectField(selftype)

        edb_type = self.edb_schema.get(typename)
        pointers = edb_type.get_pointers(self.edb_schema)
        names = sorted(pointers.keys(self.edb_schema))
        for name in names:
            if name == '__type__':
                continue
            if name in fields:
                raise g_errors.GraphQLCoreError(
                    f"{name!r} of {typename} clashes with special "
                    "reserved fields required for GraphQL conversion"
                )

            ptr = edb_type.getptr(self.edb_schema, name)
            edb_target = ptr.get_target(self.edb_schema)

            if edb_target.is_object_type():
                t_name = edb_target.get_name(self.edb_schema)
                gql_name = self.get_input_name(
                    'NestedFilter', self.get_gql_name(t_name))

                intype = self._gql_inobjtypes.get(gql_name)
                if intype is None:
                    # construct a nested insert type
                    intype = GraphQLInputObjectType(
                        name=gql_name,
                        fields=partial(self.get_filter_fields, t_name, True),
                    )
                    self._gql_inobjtypes[gql_name] = intype

            elif not edb_target.is_scalar():
                continue

            else:
                target = self._convert_edb_type(edb_target)
                if target is None:
                    # don't expose this
                    continue

                intype = self._gql_inobjtypes.get(f'Filter{target.name}')

            if intype:
                fields[name] = GraphQLInputObjectField(intype)

        return fields

    def get_insert_fields(self, typename):
        fields = OrderedDict()

        edb_type = self.edb_schema.get(typename)
        pointers = edb_type.get_pointers(self.edb_schema)
        names = sorted(pointers.keys(self.edb_schema))
        for name in names:
            if name == '__type__':
                continue

            ptr = edb_type.getptr(self.edb_schema, name)
            edb_target = ptr.get_target(self.edb_schema)

            if edb_target.is_object_type():
                typename = edb_target.get_name(self.edb_schema)

                intype = self._gql_inobjtypes.get(f'NestedInsert{typename}')
                if intype is None:
                    # construct a nested insert type
                    intype = self._make_generic_nested_insert_type(edb_target)

                intype = self._wrap_target(ptr, intype, for_input=True)
                fields[name] = GraphQLInputObjectField(intype)

            elif edb_target.is_scalar() or edb_target.is_array():
                target = self._convert_edb_type(edb_target)
                if target is None:
                    # don't expose this
                    continue

                if edb_target.is_array():
                    intype = self._gql_inobjtypes.get(
                        f'Insert{target.of_type.of_type.name}')
                    intype = GraphQLList(GraphQLNonNull(intype))
                else:
                    intype = self._gql_inobjtypes.get(f'Insert{target.name}')

                intype = self._wrap_target(ptr, intype, for_input=True)

                if intype:
                    fields[name] = GraphQLInputObjectField(intype)

            else:
                continue

        return fields

    def get_update_fields(self, typename):
        fields = OrderedDict()

        edb_type = self.edb_schema.get(typename)
        pointers = edb_type.get_pointers(self.edb_schema)
        names = sorted(pointers.keys(self.edb_schema))
        for name in names:
            if name == '__type__':
                continue

            ptr = edb_type.getptr(self.edb_schema, name)
            edb_target = ptr.get_target(self.edb_schema)

            if edb_target.is_object_type():
                intype = self._gql_inobjtypes.get(
                    f'UpdateOp{typename}__{name}')
                if intype is None:
                    # the links can only be updated by selecting some
                    # objects, meaning that the basis is the same as for
                    # query of whatever is the link type
                    intype = self._gql_inobjtypes.get(
                        f'NestedUpdate{edb_target.get_name(self.edb_schema)}')
                    if intype is None:
                        # construct a nested insert type
                        intype = self._make_generic_nested_update_type(
                            edb_target)

                    # depending on whether this is a multilink or not wrap
                    # it in a List
                    intype = self._wrap_target(
                        ptr, intype, ignore_required=True)
                    # wrap into additional layer representing update ops
                    intype = self._make_generic_update_op_type(
                        ptr, name, edb_type, intype)

                fields[name] = GraphQLInputObjectField(intype)

            elif edb_target.is_scalar() or edb_target.is_array():
                target = self._convert_edb_type(edb_target)
                if target is None or ptr.get_readonly(self.edb_schema):
                    # don't expose this
                    continue

                intype = self._gql_inobjtypes.get(
                    f'UpdateOp{typename}__{name}')
                if intype is None:
                    # construct a nested insert type
                    target = self._wrap_target(
                        ptr, target, ignore_required=True)
                    intype = self._make_generic_update_op_type(
                        ptr, name, edb_type, target)

                if intype:
                    fields[name] = GraphQLInputObjectField(intype)

            else:
                continue

        return fields

    def _make_generic_update_op_type(self, ptr, fname, edb_base, target):
        typename = edb_base.get_name(self.edb_schema)
        name = f'UpdateOp{typename}__{fname}'
        edb_target = ptr.get_target(self.edb_schema)

        fields = {
            'set': GraphQLInputObjectField(target)
        }

        # get additional commands based on the pointer type
        if not ptr.get_required(self.edb_schema):
            fields['clear'] = GraphQLInputObjectField(GraphQLBoolean)

        if edb_target.is_scalar():
            base_target = edb_target.get_topmost_concrete_base(self.edb_schema)
            bt_name = base_target.get_name(self.edb_schema)
        else:
            bt_name = None

        # first check for this being a multi-link
        if not ptr.singular(self.edb_schema):
            fields['add'] = GraphQLInputObjectField(target)
            fields['remove'] = GraphQLInputObjectField(target)
        elif target is GraphQLInt or target is GraphQLFloat:
            # anything that maps onto the numeric types is a fair game
            fields['increment'] = GraphQLInputObjectField(target)
            fields['decrement'] = GraphQLInputObjectField(target)
        elif bt_name == 'std::str' or edb_target.is_array():
            # only actual strings and arrays have append, prepend and
            # slice ops
            fields['prepend'] = GraphQLInputObjectField(target)
            fields['append'] = GraphQLInputObjectField(target)
            # slice [from, to]
            fields['slice'] = GraphQLInputObjectField(
                GraphQLList(GraphQLNonNull(GraphQLInt))
            )

        nitype = GraphQLInputObjectType(
            name=self.get_input_name(
                f'UpdateOp_{fname}_', self.get_gql_name(typename)),
            fields=fields,
        )
        self._gql_inobjtypes[name] = nitype

        return nitype

    def _make_generic_nested_update_type(self, edb_base):
        typename = edb_base.get_name(self.edb_schema)
        name = f'NestedUpdate{typename}'
        nitype = GraphQLInputObjectType(
            name=self.get_input_name(
                'NestedUpdate', self.get_gql_name(typename)),
            fields={
                'filter': GraphQLInputObjectField(
                    self._gql_inobjtypes[typename]),
                'order': GraphQLInputObjectField(
                    self._gql_ordertypes[typename]),
                'first': GraphQLInputObjectField(GraphQLInt),
                'last': GraphQLInputObjectField(GraphQLInt),
                # before and after are supposed to be opaque values
                # serialized to string
                'before': GraphQLInputObjectField(GraphQLString),
                'after': GraphQLInputObjectField(GraphQLString),
            },
        )

        self._gql_inobjtypes[name] = nitype

        return nitype

    def _make_generic_nested_insert_type(self, edb_base):
        typename = edb_base.get_name(self.edb_schema)
        name = f'NestedInsert{typename}'
        nitype = GraphQLInputObjectType(
            name=self.get_input_name(
                'NestedInsert', self.get_gql_name(typename)),
            fields={
                'data': GraphQLInputObjectField(
                    self._gql_inobjtypes[f'Insert{typename}']),
                'filter': GraphQLInputObjectField(
                    self._gql_inobjtypes[typename]),
                'order': GraphQLInputObjectField(
                    self._gql_ordertypes[typename]),
                'first': GraphQLInputObjectField(GraphQLInt),
                'last': GraphQLInputObjectField(GraphQLInt),
                # before and after are supposed to be opaque values
                # serialized to string
                'before': GraphQLInputObjectField(GraphQLString),
                'after': GraphQLInputObjectField(GraphQLString),
            },
        )

        self._gql_inobjtypes[name] = nitype

        return nitype

    def define_enums(self):
        self._gql_enums['directionEnum'] = GraphQLEnumType(
            'directionEnum',
            values=OrderedDict(
                ASC=GraphQLEnumValue(),
                DESC=GraphQLEnumValue()
            )
        )
        self._gql_enums['nullsOrderingEnum'] = GraphQLEnumType(
            'nullsOrderingEnum',
            values=OrderedDict(
                SMALLEST=GraphQLEnumValue(),
                BIGGEST=GraphQLEnumValue(),
            )
        )

        scalar_types = list(
            self.edb_schema.get_objects(included_modules=self.modules,
                                        type=s_scalars.ScalarType))
        for st in scalar_types:
            if st.is_enum(self.edb_schema):

                name = self.get_gql_name(st.get_name(self.edb_schema))
                self._gql_enums[name] = GraphQLEnumType(
                    name,
                    values=OrderedDict(
                        (key, GraphQLEnumValue()) for key in
                        st.get_enum_values(self.edb_schema)
                    )
                )

    def define_generic_filter_types(self):
        eq = ['eq', 'neq']
        comp = eq + ['gte', 'gt', 'lte', 'lt']
        string = comp + ['like', 'ilike']

        self._make_generic_input_type(GraphQLBoolean, eq)
        self._make_generic_input_type(GraphQLID, eq)
        self._make_generic_input_type(GraphQLInt, comp)
        self._make_generic_input_type(GraphQLFloat, comp)
        self._make_generic_input_type(GraphQLString, string)

        for name, etype in self._gql_enums.items():
            if name not in {'directionEnum', 'nullsOrderingEnum'}:
                self._make_generic_input_type(etype, comp)

    def _make_generic_input_type(self, base, ops):
        name = f'Filter{base.name}'
        self._gql_inobjtypes[name] = GraphQLInputObjectType(
            name=name,
            fields={op: GraphQLInputObjectField(base) for op in ops},
        )

    def define_generic_insert_types(self):
        for itype in [GraphQLBoolean, GraphQLID, GraphQLInt,
                      GraphQLFloat, GraphQLString]:
            self._gql_inobjtypes[f'Insert{itype.name}'] = itype

    def define_generic_order_types(self):
        self._gql_ordertypes['directionEnum'] = \
            self._gql_enums['directionEnum']
        self._gql_ordertypes['nullsOrderingEnum'] = \
            self._gql_enums['nullsOrderingEnum']
        self._gql_ordertypes['Ordering'] = GraphQLInputObjectType(
            'Ordering',
            fields=OrderedDict(
                dir=GraphQLInputObjectField(
                    GraphQLNonNull(self._gql_enums['directionEnum']),
                ),
                nulls=GraphQLInputObjectField(
                    self._gql_enums['nullsOrderingEnum'],
                    default_value='SMALLEST',
                ),
            )
        )

    def get_order_fields(self, typename):
        fields = OrderedDict()

        edb_type = self.edb_schema.get(typename)
        pointers = edb_type.get_pointers(self.edb_schema)
        names = sorted(pointers.keys(self.edb_schema))

        for name in names:
            if name == '__type__':
                continue

            ptr = edb_type.getptr(self.edb_schema, name)

            if not ptr.get_target(self.edb_schema).is_scalar():
                continue

            target = self._convert_edb_type(ptr.get_target(self.edb_schema))
            if target is None:
                # don't expose this
                continue

            # this makes sure that we can only order by properties
            # that can be reflected into GraphQL
            intype = self._gql_inobjtypes.get(f'Filter{target.name}')
            if intype:
                fields[name] = GraphQLInputObjectField(
                    self._gql_ordertypes['Ordering']
                )

        return fields

    def _define_types(self):
        interface_types = []
        obj_types = []

        self.define_enums()
        self.define_generic_filter_types()
        self.define_generic_order_types()
        self.define_generic_insert_types()

        # Every ObjectType is reflected as an interface.
        interface_types = list(
            self.edb_schema.get_objects(included_modules=self.modules,
                                        type=s_objtypes.BaseObjectType))

        # concrete types are also reflected as Type (with a 'Type' postfix)
        obj_types += [t for t in interface_types
                      if not t.get_is_abstract(self.edb_schema)]

        # interfaces
        for t in interface_types:
            t_name = t.get_name(self.edb_schema)
            gql_name = self.get_gql_name(t_name)
            gqltype = GraphQLInterfaceType(
                name=gql_name,
                fields=partial(self.get_fields, t_name),
                resolve_type=lambda obj, info: obj,
            )
            self._gql_interfaces[t_name] = gqltype

            # input object types corresponding to this interface
            gqlfiltertype = GraphQLInputObjectType(
                name=self.get_input_name('Filter', gql_name),
                fields=partial(self.get_filter_fields, t_name),
            )
            self._gql_inobjtypes[t_name] = gqlfiltertype

            # ordering input type
            gqlordertype = GraphQLInputObjectType(
                name=self.get_input_name('Order', gql_name),
                fields=partial(self.get_order_fields, t_name),
            )
            self._gql_ordertypes[t_name] = gqlordertype

            # update object types corresponding to this object (all
            # non-views can appear as update types)
            if not t.is_view(self.edb_schema):
                # only objects that have at least one non-readonly
                # link/property are eligible
                pointers = t.get_pointers(self.edb_schema)
                if any(not p.get_readonly(self.edb_schema)
                       for _, p in pointers.items(self.edb_schema)):
                    gqlupdatetype = GraphQLInputObjectType(
                        name=self.get_input_name('Update', gql_name),
                        fields=partial(self.get_update_fields, t_name),
                    )
                    self._gql_inobjtypes[f'Update{t_name}'] = gqlupdatetype

        # object types
        for t in obj_types:
            interfaces = []
            t_name = t.get_name(self.edb_schema)

            if t_name in self._gql_interfaces:
                interfaces.append(self._gql_interfaces[t_name])

            ancestors = t.get_ancestors(self.edb_schema)
            for st in ancestors.objects(self.edb_schema):
                if (st.is_object_type() and
                        st.get_name(self.edb_schema) in self._gql_interfaces):
                    interfaces.append(
                        self._gql_interfaces[st.get_name(self.edb_schema)])

            gql_name = self.get_gql_name(t_name)
            gqltype = GraphQLObjectType(
                name=gql_name + 'Type',
                fields=partial(self.get_fields, t_name),
                interfaces=interfaces,
            )
            self._gql_objtypes[t_name] = gqltype

            # input object types corresponding to this object (only
            # real objects can appear as input objects)
            gqlinserttype = GraphQLInputObjectType(
                name=self.get_input_name('Insert', gql_name),
                fields=partial(self.get_insert_fields, t_name),
            )
            self._gql_inobjtypes[f'Insert{t_name}'] = gqlinserttype

    def get(self, name, *, dummy=False):
        '''Get a special GQL type either by name or based on EdgeDB type.'''
        # normalize name and possibly add 'edb_base' to kwargs
        edb_base = None
        kwargs = {'dummy': dummy}

        if isinstance(name, str):
            if name in TOP_LEVEL_TYPES:
                name = f'stdgraphql::{name}'

        else:
            edb_base = name
            edb_base_name = edb_base.get_name(self.edb_schema)
            name = f'{edb_base_name.module}::{edb_base_name.name}'

        if not name.startswith('stdgraphql::'):
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
        fkey = (name, dummy)
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

    def __init__(self, schema, **kwargs):
        name = kwargs.get('name', None)
        edb_base = kwargs.get('edb_base', None)
        dummy = kwargs.get('dummy', False)
        self._shadow_fields = ()

        if edb_base is None and self.edb_type:
            edb_base = schema.edb_schema.get(self.edb_type)

        edb_base_name = edb_base.get_name(schema.edb_schema)

        # __typename
        if name is None:
            self._name = f'{edb_base_name.module}::{edb_base_name.name}'
        else:
            self._name = name
        # determine module from name if not already specified
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
        # JSON and bool need some special treatment so we want to know if
        # we're dealing with it
        if edb_base.is_scalar():
            bt = edb_base.get_topmost_concrete_base(self.edb_schema)
            bt_name = bt.get_name(self.edb_schema)
            self._is_json = bt_name == 'std::json'
            self._is_bool = bt_name == 'std::bool'
        else:
            self._is_json = self._is_bool = False

    @property
    def is_json(self):
        return self._is_json

    @property
    def is_enum(self):
        return False

    @property
    def is_bool(self):
        return self._is_bool

    @property
    def is_array(self):
        return self.edb_base.is_array()

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
    def edb_base_name_ast(self):
        if self.is_array:
            el = self.edb_base.get_element_type(self.edb_schema)
            base = el.get_name(self.edb_schema)
            return qlast.ObjectRef(
                module=base.module,
                name=base.name,
            )
        else:
            base = self.edb_base.get_name(self.edb_schema)
            return qlast.ObjectRef(
                module=base.module,
                name=base.name,
            )

    @property
    def edb_base_name(self):
        return codegen.generate_source(self.edb_base_name_ast)

    @property
    def gql_typename(self):
        name = self.name
        module, shortname = name.split('::', 1)
        if module in {'default', 'std'}:
            return f'{shortname}Type'
        else:
            return f'{module}__{shortname}Type'

    @property
    def schema(self):
        return self._schema

    @property
    def edb_schema(self):
        return self._schema.edb_schema

    @edb_schema.setter
    def edb_schema(self, schema):
        self._schema.edb_schema = schema

    def convert_edb_to_gql_type(self, base, **kwargs):
        if isinstance(base, s_pointers.Pointer):
            base = base.get_target(self.edb_schema)

        if self.dummy:
            kwargs['dummy'] = True

        return self.schema.get(base, **kwargs)

    def is_field_shadowed(self, name):
        return name in self._shadow_fields

    def get_field_type(self, name):
        if self.dummy:
            return None

        # this is just shadowing a real EdgeDB type
        fkey = (name, self.dummy)
        target = self._fields.get(fkey)

        if target is None:
            # special handling of '__typename'
            if name == '__typename':
                target = self.convert_edb_to_gql_type('std::str')

            else:
                target = self.edb_base.getptr(self.edb_schema, name)

                if target is not None:
                    target = self.convert_edb_to_gql_type(target)

            self._fields[fkey] = target

        return target

    def has_native_field(self, name):
        ptr = self.edb_base.getptr(self.edb_schema, name)
        return ptr is not None

    def issubclass(self, other):
        if isinstance(other, GQLShadowType):
            return self.edb_base.issubclass(self._schema.edb_schema,
                                            other.edb_base)

        return False

    def get_template(self):
        '''Provide an EQL AST template to be filled.

        Return the overall ast, a reference to where the shape element
        with placeholder is, and a reference to the element which may
        be filtered.
        '''

        if self.dummy:
            return parse_fragment(f'''to_json("xxx")'''), None, None

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

        if name == '__typename' and not self.is_field_shadowed(name):
            is_view = self.edb_base.is_view(self.edb_schema)
            if is_view:
                eql = parse_fragment(f'{self.gql_typename!r}')
            else:
                eql = parse_fragment(
                    f'''stdgraphql::short_name(
                        {codegen.generate_source(parent)}.__type__.name)''')

        elif has_shape:
            eql = filterable = parse_fragment(
                f'''SELECT {codegen.generate_source(parent)}.
                        {codegen.generate_source(qlast.ObjectRef(name=name))}
                        {{ xxx }}
                ''')
            filterable = eql
            shape = filterable.result

        elif self.get_field_type(name).is_json:
            eql = filterable = parse_fragment(
                f'''SELECT to_str({codegen.generate_source(parent)}.
                        {codegen.generate_source(qlast.ObjectRef(name=name))})
                ''')

        else:
            eql = filterable = parse_fragment(
                f'''SELECT {codegen.generate_source(parent)}.
                        {codegen.generate_source(qlast.ObjectRef(name=name))}
                ''')

        return eql, shape, filterable

    def get_field_cardinality(self, name):
        if not self.is_field_shadowed(name):
            return None

        ptr = self.edb_base.getptr(self.edb_schema, name)
        if not ptr.singular(self.edb_schema):
            return qltypes.Cardinality.MANY

        return None


class GQLShadowType(GQLBaseType):
    def is_field_shadowed(self, name):
        if name == '__typename':
            return False

        ftype = self.get_field_type(name)
        # JSON fields are not shadowed
        if ftype is None or ftype.is_json:
            return False

        return True

    @property
    def is_enum(self):
        return self.edb_base.is_enum(self.edb_schema)


class GQLBaseQuery(GQLBaseType):
    def __init__(self, schema, **kwargs):
        self.modules = schema.modules
        super().__init__(schema, **kwargs)
        self._shadow_fields = ('__typename',)

    def get_module_and_name(self, name):
        if name == 'Object':
            return ('std', name)
        elif '__' in name:
            return name.split('__', 1)
        else:
            return ('default', name)


class GQLQuery(GQLBaseQuery):
    edb_type = 'stdgraphql::Query'

    def get_field_type(self, name):
        fkey = (name, self.dummy)
        target = None

        if name in {'__type', '__schema'}:
            if fkey in self._fields:
                return self._fields[fkey]

            target = self.convert_edb_to_gql_type('Query', dummy=True)

        else:
            target = super().get_field_type(name)

            if target is None:
                module, edb_name = self.get_module_and_name(name)
                target = self.edb_schema.get((module, edb_name), None)
                if target is not None:
                    target = self.convert_edb_to_gql_type(target)

        self._fields[fkey] = target
        return target


class GQLMutation(GQLBaseQuery):
    edb_type = 'stdgraphql::Mutation'

    def get_field_type(self, name):
        fkey = (name, self.dummy)
        target = None

        op, name = name.split('_')
        if op in {'delete', 'insert', 'update'}:
            target = super().get_field_type(name)

            if target is None:
                module, edb_name = self.get_module_and_name(name)
                target = self.edb_schema.get((module, edb_name), None)
                if target is not None:
                    target = self.convert_edb_to_gql_type(target)

        self._fields[fkey] = target
        return target
