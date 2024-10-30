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
from typing import (
    Any,
    ClassVar,
    Optional,
    Tuple,
    Type,
    Union,
    Dict,
    List,
    Set,
    cast,
)

from functools import partial
from graphql import (
    GraphQLAbstractType,
    GraphQLSchema,
    GraphQLInputType,
    GraphQLNamedType,
    GraphQLOutputType,
    GraphQLObjectType,
    GraphQLWrappingType,
    GraphQLInterfaceType,
    GraphQLInputObjectType,
    GraphQLResolveInfo,
    GraphQLField,
    GraphQLInputField,
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
from graphql.type import GraphQLEnumValue, GraphQLScalarType
from graphql.language import ast as gql_ast
import itertools

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import codegen
from edb.edgeql.parser import parse_fragment

from edb.schema import modules as s_mod
from edb.schema import name as s_name
from edb.schema import pointers as s_pointers
from edb.schema import objtypes as s_objtypes
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from . import errors as g_errors


'''
This module is responsible for mapping Gel types onto the GraphQL
types. However, this is an imperfect mapping because not all the types
or relationships between them can be expressed.

# Aliased Types

Aliased types present a particular problem. Basically, they break
inheritance and GraphQL fragments become useless for them.

Consider a link friends in this alias:
```
    type User {... multi link friends -> User}
    type SpecialUser extending User {property special-> str}
    alias UserAlias := User {friends: {some_new_prop := 'foo'}}
```

In GraphQL we will have type UserType implementing interfaces User,
Object and SpecialUserType implementing SpecialUser, User, Object. The
trouble starts with our implicit aliased type for the aliased friends
link that targets __UserAlias__friends. This type gets reflected into
a GraphQL _edb__UserAlias__friends that implements... what? We have 2
options:

    1) Implement interfaces mirroring the Gel types: User, Object.

    2) Implement it's own interface (or just omit interfaces here,
       since if the interface is unique it's not adding anything).

Case 2) preserves all the fields defined in the alias to be accessible
and filterable, etc., but loses inheritance information.

Case 1) leads to the following additional choices:

    a) The friends target is still interface User, then the field
       some_new_prop will not appear in the nesting, but will require
       a specialized fragment:

        query {
            UserAlias {
                friends {
                    ... on _edb__UserAlias_friends {
                        some_new_prop
                    }
                }
            }
        }

    b) We can make the target of friends of UserAlias to be the actual
       type _edb__UserAlias__friends (similar to case 2), but then we
       cannot use the typed fragment `... on SpecialUser` construct
       inside it because the SpecialUser is a sibling of our aliased
       type and will cause a GraphQL validation error.

    c) The field target can be a union type, but it will still only
       have fields that are common to all union members and will
       require awkward inlined typed fragments to work with just like
       the first bullet point.

In the end I think that rather than preserving the inheritance and
then essentially forcing the use of `... on _edb__UserAlias_friends`
just to access the very field for which the alias was created in the
first place it's better to bite the bullet accept that in GraphQL
aliased types reflection removes all inheritance info, but at least
provide all the fields as per normal. The reasoning being that the
fields and data are probably much more important for practical
purposes than inheritance purity. To allow the SpecialUser
polymorphism I'd rather suggest to the user to bake it into the alias
like so:
```
    alias UserAlias := User {
        friends: {
            some_new_prop := 'foo',
            [IS SpecialUser].special,
        }
    }
```

Also, note that for the same reasons as outlined above that make
aliased types into a sibling branch in GraphQL, we can't give an
accurate __typename for them, like we can in EdgeQL (__type__.name)
because the "correct" types would violate the declared GraphQL type
hierarchy. So aliased types are necessarily opaque in GraphQL in all
these ways. Unlike in EdgeQL.
'''


def coerce_int64(value: Any) -> int:
    if isinstance(value, int):
        num = value
    else:
        num = int(value)
    if s_utils.MIN_INT64 <= num <= s_utils.MAX_INT64:
        return num

    raise Exception(
        f"Int64 cannot represent non 64-bit signed integer value: {value}")


def coerce_bigint(value: Any) -> int:
    if isinstance(value, int):
        num = value
    else:
        num = int(value)
    return num


def parse_int_literal(
    ast: gql_ast.Node,
    _variables: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    if isinstance(ast, gql_ast.IntValueNode):
        return int(ast.value)
    else:
        return None


GraphQLInt64 = GraphQLScalarType(
    name="Int64",
    description="The `Int64` scalar type represents non-fractional signed "
                "whole numeric values. Int can represent values between "
                "-2^63 and 2^63 - 1.",
    serialize=coerce_int64,
    parse_value=coerce_int64,
    parse_literal=parse_int_literal,
)


GraphQLBigint = GraphQLScalarType(
    name="Bigint",
    description="The `Bigint` scalar type represents non-fractional signed "
                "whole numeric values.",
    serialize=coerce_bigint,
    parse_value=coerce_bigint,
    parse_literal=parse_int_literal,
)


GraphQLJSON = GraphQLScalarType(
    name="JSON",
    description="The `JSON` scalar type represents arbitrary JSON values.",
)


def parse_decimal_literal(
    ast: gql_ast.Node,
    _variables: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    if isinstance(ast, (gql_ast.FloatValueNode, gql_ast.IntValueNode)):
        return float(ast.value)
    else:
        return None


GraphQLDecimal = GraphQLScalarType(
    name="Decimal",
    description="The `Decimal` scalar type represents signed "
                "unlimited-precision fractional values.",
    serialize=GraphQLFloat.serialize,
    parse_value=GraphQLFloat.parse_value,
    parse_literal=parse_decimal_literal,
)


EDB_TO_GQL_SCALARS_MAP = {
    # For compatibility with GraphQL we cast json into a String, since
    # GraphQL doesn't have an equivalent type with arbitrary fields.
    'std::json': GraphQLJSON,
    'std::str': GraphQLString,
    'std::anyint': GraphQLInt,
    'std::int16': GraphQLInt,
    'std::int32': GraphQLInt,
    'std::int64': GraphQLInt64,
    'std::bigint': GraphQLBigint,
    'std::anyfloat': GraphQLFloat,
    'std::float32': GraphQLFloat,
    'std::float64': GraphQLFloat,
    'std::anyreal': GraphQLFloat,
    'std::decimal': GraphQLDecimal,
    'std::bool': GraphQLBoolean,
    'std::uuid': GraphQLID,
    'std::datetime': GraphQLString,
    'std::duration': GraphQLString,
    'std::bytes': None,

    'std::cal::local_datetime': GraphQLString,
    'std::cal::local_date': GraphQLString,
    'std::cal::local_time': GraphQLString,
    'std::cal::relative_duration': GraphQLString,
    'std::cal::date_duration': GraphQLString,
}


# used for casting input values from GraphQL to EdgeQL
GQL_TO_EDB_SCALARS_MAP = {
    'String': 'str',
    'Int': 'int32',
    'Int64': 'int64',
    'Bigint': 'bigint',
    'Float': 'float64',
    'Decimal': 'decimal',
    'Boolean': 'bool',
    'ID': 'uuid',
    'JSON': 'json',
}


GQL_TO_EDB_RANGES_MAP = {
    'RangeOfString': 'json',
    'RangeOfInt': 'json',
    'RangeOfInt64': 'json',
    'RangeOfFloat': 'json',
    'RangeOfDecimal': 'json',
}


GQL_TO_OPS_MAP = {
    'exists': 'EXISTS',
    'in': 'IN',
    'eq': '=',
    'neq': '!=',
    'gt': '>',
    'gte': '>=',
    'lt': '<',
    'lte': '<=',
    'like': 'LIKE',
    'ilike': 'ILIKE',
}


HIDDEN_MODULES = set(s_schema.STD_MODULES) - {s_name.UnqualName('std')}
# The following are placeholders.
TOP_LEVEL_TYPES = {
    s_name.QualName(module='__graphql__', name='Query'),
    s_name.QualName(module='__graphql__', name='Mutation'),
}
# The following types should not be exposed as all.
HIDDEN_TYPES = {
    s_name.QualName(module='std', name='FreeObject'),
}


class GQLCoreSchema:

    _gql_interfaces: Dict[
        s_name.QualName,
        GraphQLInterfaceType,
    ]

    _gql_objtypes_from_alias: Dict[
        s_name.QualName,
        GraphQLObjectType,
    ]

    _gql_objtypes: Dict[
        s_name.QualName,
        GraphQLObjectType,
    ]

    _gql_inobjtypes: Dict[
        str,
        Union[
            GraphQLInputObjectType,
            GraphQLEnumType,
            GraphQLScalarType,
        ]
    ]

    _gql_ordertypes: Dict[str, GraphQLInputType]

    _gql_enums: Dict[str, GraphQLEnumType]

    _type_map: Dict[Tuple[str, bool], GQLBaseType]

    def __init__(self, edb_schema: s_schema.Schema) -> None:
        '''Create a graphql schema based on edgedb schema.'''

        self.edb_schema = edb_schema
        # extract and sort modules to have a consistent type ordering
        self.modules = list(sorted({
            m.get_name(self.edb_schema)
            for m in self.edb_schema.get_objects(type=s_mod.Module)
        } - HIDDEN_MODULES))

        self._gql_interfaces = {}
        self._gql_uniontypes: Set[s_name.QualName] = set()
        self._gql_objtypes_from_alias = {}
        self._gql_objtypes = {}
        self._gql_inobjtypes = {}
        self._gql_ordertypes = {}
        self._gql_enums = {}

        self._define_types()

        # Use a fake name as a placeholder.
        Query = s_name.QualName(module='__graphql__', name='Query')
        query = self._gql_objtypes[Query] = GraphQLObjectType(
            name='Query',
            fields=self.get_fields(Query),
        )

        # If a database only has abstract types and scalars, no
        # mutations will be possible (such as in a blank database),
        # but we would still want the reflection to work without
        # error, even if all that can be discovered through GraphQL
        # then is the schema.
        Mutation = s_name.QualName(module='__graphql__', name='Mutation')
        fields = self.get_fields(Mutation)
        if not fields:
            mutation = None
        else:
            mutation = self._gql_objtypes[Mutation] = GraphQLObjectType(
                name='Mutation',
                fields=fields,
            )

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
    def edgedb_schema(self) -> s_schema.Schema:
        return self.edb_schema

    @property
    def graphql_schema(self) -> GraphQLSchema:
        return self._gql_schema

    @classmethod
    def get_gql_name(cls, name: s_name.QualName) -> str:
        module, shortname = name.module, name.name

        # Adjust the shortname.
        if shortname.startswith('__'):
            # Use '_edb' prefix to mark derived and otherwise
            # internal types. We opt out of '__edb' because we
            # still rely on the first occurrence of '__' in
            # GraphQL names to separate the module from the rest
            # of the name in some code.
            shortname = f'_edb{shortname}'
        elif shortname.startswith('('):
            # Looks like a union type, so we'll need to process individual
            # parts of the name.
            names = []
            for part in shortname[1:-1].split(' | '):
                names.append(
                    cls.get_gql_name(s_name.QualName(*part.split(':', 1))))
            shortname = '_OR_'.join(names)

        if module in {'default', 'std'}:
            return shortname
        else:
            assert module != '', f'get_gl_name {name=}'
            return str(name).replace("::", "__")

    def get_input_name(self, inputtype: str, name: str) -> str:
        if '__' in name:
            module, shortname = name.rsplit('__', 1)
            assert module != '', f'get_input_name {name=}'
            return f'{module}__{inputtype}{shortname}'
        else:
            return f'{inputtype}{name}'

    def gql_to_edb_name(self, name: str) -> str:
        '''Convert the GraphQL field name into an Gel type/view name.'''
        if '__' in name:
            return name.replace('__', '::')
        else:
            return name

    def _get_description(self, edb_type: s_types.Type) -> Optional[str]:
        description_anno = edb_type.get_annotations(self.edb_schema).get(
            self.edb_schema, s_name.QualName('std', 'description'), None)
        if description_anno is not None:
            return description_anno.get_value(self.edb_schema)

        return None

    def _convert_edb_type(
        self,
        edb_target: s_types.Type,
    ) -> Optional[GraphQLOutputType]:
        target: Optional[GraphQLOutputType] = None

        if isinstance(edb_target, s_types.Array):
            subtype = edb_target.get_subtypes(self.edb_schema)[0]
            el_type = self._convert_edb_type(subtype)
            if el_type is None:
                # we can't expose an array of unexposable type
                return el_type
            else:
                target = GraphQLList(GraphQLNonNull(el_type))

        elif isinstance(edb_target, (s_types.Range, s_types.MultiRange)):
            # Represent ranges and multiranges as JSON. Same as reason as for
            # tuples: the values are atomic and cannot be fragmented via
            # GraphQL specification, so we cannot use objects with fields to
            # represent them.
            target = EDB_TO_GQL_SCALARS_MAP['std::json']

        elif edb_target.is_view(self.edb_schema):
            tname = edb_target.get_name(self.edb_schema)
            assert isinstance(tname, s_name.QualName)
            target = self._gql_objtypes.get(tname)

        elif isinstance(edb_target, s_objtypes.ObjectType):
            target = self._gql_interfaces.get(
                edb_target.get_name(self.edb_schema),
                self._gql_objtypes.get(edb_target.get_name(self.edb_schema))
            )

        elif (
            isinstance(edb_target, s_scalars.ScalarType)
            and edb_target.is_enum(self.edb_schema)
        ):
            name = self.get_gql_name(edb_target.get_name(self.edb_schema))

            if name in self._gql_enums:
                target = self._gql_enums.get(name)

        elif edb_target.is_tuple(self.edb_schema):
            # Represent tuples as JSON.
            target = EDB_TO_GQL_SCALARS_MAP['std::json']

        elif isinstance(edb_target, s_types.InheritingType):
            base_target = edb_target.get_topmost_concrete_base(self.edb_schema)
            bt_name = base_target.get_name(self.edb_schema)
            try:
                target = EDB_TO_GQL_SCALARS_MAP[str(bt_name)]
            except KeyError:
                # this is the scalar base case, where all potentially
                # unrecognized scalars should end up
                edb_typename = edb_target.get_verbosename(self.edb_schema)
                raise g_errors.GraphQLCoreError(
                    f"could not convert {edb_typename!r} type to"
                    f" a GraphQL type")
        else:
            raise AssertionError(f'unexpected schema object: {edb_target!r}')

        return target

    def _get_target(
        self,
        ptr: s_pointers.Pointer,
    ) -> Optional[GraphQLOutputType]:
        edb_target = ptr.get_target(self.edb_schema)
        if edb_target is None:
            raise AssertionError(f'unexpected abstract pointer: {ptr!r}')
        target = self._convert_edb_type(edb_target)

        if target is not None:
            # figure out any additional wrappers due to cardinality
            # and required flags
            target = self._wrap_output_type(ptr, target)

        return target

    def _wrap_output_type(
        self,
        ptr: s_pointers.Pointer,
        target: GraphQLOutputType,
        *,
        ignore_required: bool = False,
    ) -> GraphQLOutputType:
        # figure out any additional wrappers due to cardinality
        # and required flags
        if not ptr.singular(self.edb_schema):
            target = GraphQLList(GraphQLNonNull(target))

        if not ignore_required:
            # for input values having a default cancels out being required
            if ptr.get_required(self.edb_schema):
                target = GraphQLNonNull(target)

        return target

    def _wrap_input_type(
        self,
        ptr: s_pointers.Pointer,
        target: GraphQLInputType,
        *,
        ignore_required: bool = False,
    ) -> GraphQLInputType:
        # figure out any additional wrappers due to cardinality
        # and required flags
        if not ptr.singular(self.edb_schema):
            target = GraphQLList(GraphQLNonNull(target))

        if not ignore_required:
            if (
                ptr.get_required(self.edb_schema)
                and ptr.get_default(self.edb_schema) is None
            ):
                target = GraphQLNonNull(target)

        return target

    def _get_query_args(
        self,
        typename: s_name.QualName,
    ) -> Dict[str, GraphQLArgument]:
        return {
            'filter': GraphQLArgument(self._gql_inobjtypes[str(typename)]),
            'order': GraphQLArgument(self._gql_ordertypes[str(typename)]),
            'first': GraphQLArgument(GraphQLInt),
            'last': GraphQLArgument(GraphQLInt),
            # before and after are supposed to be opaque values
            # serialized to string
            'before': GraphQLArgument(GraphQLString),
            'after': GraphQLArgument(GraphQLString),
        }

    def _get_insert_args(
        self,
        typename: s_name.QualName,
    ) -> Dict[str, GraphQLArgument]:
        # The data can only be a specific non-interface type, if no
        # such type exists, skip it as we cannot accept unambiguous
        # data input. It's still possible to just select some existing
        # data.
        intype = self._gql_inobjtypes.get(f'Insert{typename}')
        if intype is None:
            return {}

        return {
            'data': GraphQLArgument(
                GraphQLNonNull(GraphQLList(GraphQLNonNull(intype)))),
        }

    def _get_update_args(
        self,
        typename: s_name.QualName,
    ) -> Dict[str, GraphQLArgument]:
        # some types have no updates
        uptype = self._gql_inobjtypes.get(f'Update{typename}')
        if uptype is None:
            return {}

        # the update args are same as for query + data
        args = self._get_query_args(typename)
        args['data'] = GraphQLArgument(GraphQLNonNull(uptype))
        return args

    def get_fields(
        self,
        typename: s_name.QualName,
    ) -> Dict[str, GraphQLField]:
        fields = {}

        if str(typename) == '__graphql__::Query':
            # The fields here will come from abstract types and aliases.
            queryable: List[Tuple[s_name.QualName, GraphQLNamedType]] = []
            queryable.extend(self._gql_interfaces.items())
            queryable.extend(self._gql_objtypes_from_alias.items())
            queryable.sort(key=lambda x: x[1].name)
            for name, gqliface in queryable:
                # '_edb' prefix indicates an internally generated type
                # (e.g. nested aliased type), which should not be
                # exposed as a top-level query option.
                if name in TOP_LEVEL_TYPES or gqliface.name.startswith('_edb'):
                    continue
                # Check that the underlying type is not a union type.
                if name in self._gql_uniontypes:
                    continue
                fields[gqliface.name] = GraphQLField(
                    GraphQLList(GraphQLNonNull(gqliface)),
                    args=self._get_query_args(name),
                )
        elif str(typename) == '__graphql__::Mutation':
            # Get a list of alias names, so that we don't generate inserts for
            # them.
            aliases = {t.name for t in self._gql_objtypes_from_alias.values()}
            for name, gqltype in sorted(self._gql_objtypes.items(),
                                        key=lambda x: x[1].name):
                # '_edb' prefix indicates an internally generated type
                # (e.g. nested aliased type), which should not be
                # exposed as a top-level mutation option.
                if name in TOP_LEVEL_TYPES or gqltype.name.startswith('_edb'):
                    continue
                gname = self.get_gql_name(name)
                fields[f'delete_{gname}'] = GraphQLField(
                    GraphQLList(GraphQLNonNull(gqltype)),
                    args=self._get_query_args(name),
                )
                if gname in aliases:
                    # Aliases can only have delete mutations
                    continue

                args = self._get_insert_args(name)
                fields[f'insert_{gname}'] = GraphQLField(
                    GraphQLList(GraphQLNonNull(gqltype)),
                    args=args,
                )

            for name, gqliface in sorted(self._gql_interfaces.items(),
                                         key=lambda x: x[1].name):
                if (name in TOP_LEVEL_TYPES or
                    gqliface.name.startswith('_edb') or
                        f'Update{name}' not in self._gql_inobjtypes):
                    continue
                gname = self.get_gql_name(name)
                args = self._get_update_args(name)
                if args:
                    # If there are no args, there's nothing to update.
                    fields[f'update_{gname}'] = GraphQLField(
                        GraphQLList(GraphQLNonNull(gqliface)),
                        args=args,
                    )
        else:
            edb_type = self.edb_schema.get(
                typename,
                type=s_objtypes.ObjectType,
            )
            pointers = edb_type.get_pointers(self.edb_schema)

            for unqual_pn, ptr in sorted(pointers.items(self.edb_schema)):
                pn = str(unqual_pn)
                if pn == '__type__':
                    continue
                assert isinstance(ptr, s_pointers.Pointer)

                tgt = ptr.get_target(self.edb_schema)
                assert tgt is not None
                # Aliased types ignore their ancestors in order to
                # allow all their fields appear properly in the
                # filters.
                #
                # If the target is not a view, but this is computed,
                # so we cannot later override it, thus we can use the
                # type as is.
                if (
                    not tgt.is_view(self.edb_schema) and
                    not ptr.is_pure_computable(self.edb_schema)
                ):
                    # We want to look at the pointer lineage because that
                    # will be reflected into GraphQL interface that is
                    # being extended and the type cannot be changed.
                    ancestors: Tuple[s_pointers.Pointer, ...]
                    ancestors = ptr.get_ancestors(
                        self.edb_schema).objects(self.edb_schema)

                    # We want the first non-generic ancestor of this
                    # pointer as its target type will dictate the target
                    # types of all its derived pointers.
                    #
                    # NOTE: We're guaranteed to have a non-generic one
                    # since we're inspecting the lineage of a pointer
                    # belonging to an actual type.
                    for ancestor in reversed((ptr,) + ancestors):
                        if not ancestor.is_non_concrete(self.edb_schema):
                            ptr = ancestor
                            break

                target = self._get_target(ptr)

                if target is not None:
                    ptgt = ptr.get_target(self.edb_schema)
                    if not isinstance(ptgt, s_objtypes.ObjectType):
                        objargs = None
                    else:
                        objargs = self._get_query_args(
                            ptgt.get_name(self.edb_schema))

                    fields[pn] = GraphQLField(target, args=objargs)

        return fields

    def get_filter_fields(
        self,
        typename: s_name.QualName,
        nested: bool = False,
    ) -> Dict[str, GraphQLInputField]:
        selftype = self._gql_inobjtypes[str(typename)]
        fields = {}

        if not nested:
            fields['and'] = GraphQLInputField(
                GraphQLList(GraphQLNonNull(selftype)))
            fields['or'] = GraphQLInputField(
                GraphQLList(GraphQLNonNull(selftype)))
            fields['not'] = GraphQLInputField(selftype)
        else:
            # Always include the 'exists' operation
            fields['exists'] = GraphQLInputField(GraphQLBoolean)

        edb_type = self.edb_schema.get(typename, type=s_objtypes.ObjectType)
        pointers = edb_type.get_pointers(self.edb_schema)
        names = sorted(pointers.keys(self.edb_schema))
        for unqual_name in names:
            name = str(unqual_name)
            if name == '__type__':
                continue
            if name in fields:
                raise g_errors.GraphQLCoreError(
                    f"{name!r} of {typename} clashes with special "
                    "reserved fields required for GraphQL conversion"
                )

            ptr = edb_type.getptr(self.edb_schema, unqual_name)
            edb_target = ptr.get_target(self.edb_schema)
            assert edb_target is not None

            if isinstance(edb_target, s_objtypes.ObjectType):
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

                if isinstance(target, GraphQLNamedType):
                    intype = self._gql_inobjtypes.get(f'Filter{target.name}')
                else:
                    raise AssertionError(
                        f'unexpected GraphQL type: {target!r}'
                    )

            if intype:
                fields[name] = GraphQLInputField(intype)

        return fields

    def get_insert_fields(
        self,
        typename: s_name.QualName,
    ) -> Dict[str, GraphQLInputField]:
        fields = {}

        edb_type = self.edb_schema.get(typename, type=s_objtypes.ObjectType)
        pointers = edb_type.get_pointers(self.edb_schema)
        names = sorted(pointers.keys(self.edb_schema))
        for unqual_name in names:
            name = str(unqual_name)
            if name in {'__type__', 'id'}:
                continue

            ptr = edb_type.getptr(self.edb_schema, unqual_name)
            edb_target = ptr.get_target(self.edb_schema)
            intype: GraphQLInputType

            if ptr.is_pure_computable(self.edb_schema):
                # skip computed pointer
                continue

            elif isinstance(edb_target, s_objtypes.ObjectType):
                typename = edb_target.get_name(self.edb_schema)

                inobjtype = self._gql_inobjtypes.get(f'NestedInsert{typename}')
                if inobjtype is not None:
                    intype = inobjtype
                else:
                    # construct a nested insert type
                    intype = self._make_generic_nested_insert_type(edb_target)

                intype = self._wrap_input_type(ptr, intype)
                fields[name] = GraphQLInputField(intype)

            elif (
                edb_target and
                edb_target.contains_array_of_tuples(self.edb_schema)
            ):
                # Can't insert array<tuple<...>>
                continue

            elif (
                isinstance(edb_target, s_scalars.ScalarType)
                or isinstance(edb_target, s_types.Array)
            ):
                target = self._convert_edb_type(edb_target)
                if target is None:
                    # don't expose this
                    continue

                if isinstance(target, GraphQLList):
                    # Check whether the edb_target is an array of enums,
                    # because enums need slightly different handling.
                    assert isinstance(edb_target, s_types.Array)
                    el = edb_target.get_element_type(self.edb_schema)
                    if el.is_enum(self.edb_schema):
                        tname = el.get_name(self.edb_schema)
                        assert isinstance(tname, s_name.QualName)
                    else:
                        tname = target.of_type.of_type.name

                    inobjtype = self._gql_inobjtypes.get(f'Insert{tname}')
                    assert inobjtype is not None
                    intype = GraphQLList(GraphQLNonNull(inobjtype))

                elif edb_target.is_enum(self.edb_schema):
                    enum_name = edb_target.get_name(self.edb_schema)
                    assert isinstance(enum_name, s_name.QualName)
                    inobjtype = self._gql_inobjtypes.get(f'Insert{enum_name}')
                    assert inobjtype is not None
                    intype = inobjtype

                elif isinstance(target, GraphQLNamedType):
                    inobjtype = self._gql_inobjtypes.get(
                        f'Insert{target.name}')
                    assert inobjtype is not None
                    intype = inobjtype

                else:
                    raise AssertionError(
                        f'unexpected GraphQL type" {target!r}'
                    )

                intype = self._wrap_input_type(ptr, intype)

                if intype:
                    fields[name] = GraphQLInputField(intype)

            elif isinstance(edb_target, s_types.Range):
                subtype = edb_target.get_subtypes(self.edb_schema)[0]
                intype = self.get_input_range_type(subtype)
                intype = self._wrap_input_type(ptr, intype)
                fields[name] = GraphQLInputField(intype)

            elif isinstance(edb_target, s_types.MultiRange):
                subtype = edb_target.get_subtypes(self.edb_schema)[0]
                intype = GraphQLList(GraphQLNonNull(
                    self.get_input_range_type(subtype)))
                intype = self._wrap_input_type(ptr, intype)
                fields[name] = GraphQLInputField(intype)

            else:
                continue

        return fields

    def get_update_fields(
        self,
        typename: s_name.QualName,
    ) -> Dict[str, GraphQLInputField]:
        fields = {}

        edb_type = self.edb_schema.get(typename, type=s_objtypes.ObjectType)
        pointers = edb_type.get_pointers(self.edb_schema)
        names = sorted(pointers.keys(self.edb_schema))
        # This is just a heavily re-used type variable
        target: Union[GraphQLInputType, Optional[GraphQLOutputType]]
        for unqual_name in names:
            name = str(unqual_name)
            if name == '__type__':
                continue

            ptr = edb_type.getptr(self.edb_schema, unqual_name)
            edb_target = ptr.get_target(self.edb_schema)

            if ptr.is_pure_computable(self.edb_schema):
                # skip computed pointer
                continue

            elif isinstance(edb_target, s_objtypes.ObjectType):
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
                    intype = cast(
                        GraphQLInputObjectType,
                        self._wrap_input_type(
                            ptr, intype, ignore_required=True),
                    )
                    # wrap into additional layer representing update ops
                    intype = self._make_generic_update_op_type(
                        ptr, name, edb_type, intype)

                fields[name] = GraphQLInputField(intype)

            elif (
                edb_target and
                edb_target.contains_array_of_tuples(self.edb_schema)
            ):
                # Can't update array<tuple<...>>
                continue

            elif isinstance(
                edb_target,
                (
                    s_scalars.ScalarType,
                    s_types.Array,
                )
            ):
                target = self._convert_edb_type(edb_target)
                if target is None or ptr.get_readonly(self.edb_schema):
                    # don't expose this
                    continue

                intype = self._gql_inobjtypes.get(
                    f'UpdateOp{typename}__{name}')
                if intype is None:
                    # construct a nested insert type
                    assert isinstance(
                        target,
                        (
                            GraphQLScalarType,
                            GraphQLEnumType,
                            GraphQLInputObjectType,
                            GraphQLWrappingType,
                        ),
                    ), f'got {target!r}, expected GraphQLInputType'
                    intype = self._make_generic_update_op_type(
                        ptr,
                        fname=name,
                        edb_base=edb_type,
                        target=self._wrap_input_type(
                            ptr,
                            target,
                            ignore_required=True,
                        ),
                    )

                if intype:
                    fields[name] = GraphQLInputField(intype)

            elif isinstance(
                edb_target,
                (
                    s_types.Range,
                    s_types.MultiRange,
                )
            ):
                subtype = edb_target.get_subtypes(self.edb_schema)[0]
                target = self.get_input_range_type(subtype)
                if isinstance(edb_target, s_types.MultiRange):
                    target = GraphQLList(GraphQLNonNull(target))

                intype = self._gql_inobjtypes.get(
                    f'UpdateOp{typename}__{name}')
                if intype is None:
                    # construct a nested insert type
                    intype = self._make_generic_update_op_type(
                        ptr,
                        fname=name,
                        edb_base=edb_type,
                        target=self._wrap_input_type(
                            ptr,
                            target,
                            ignore_required=True,
                        ),
                    )

                if intype:
                    fields[name] = GraphQLInputField(intype)

            else:
                continue

        return fields

    def _make_generic_update_op_type(
        self,
        ptr: s_pointers.Pointer,
        fname: str,
        edb_base: s_types.Type,
        target: GraphQLInputType,
    ) -> GraphQLInputObjectType:
        typename = edb_base.get_name(self.edb_schema)
        assert isinstance(typename, s_name.QualName)
        name = f'UpdateOp{typename}__{fname}'
        edb_target = ptr.get_target(self.edb_schema)

        fields = {
            'set': GraphQLInputField(target)
        }

        # get additional commands based on the pointer type
        if not ptr.get_required(self.edb_schema):
            fields['clear'] = GraphQLInputField(GraphQLBoolean)

        bt_name: Optional[s_name.QualName]
        if isinstance(edb_target, s_scalars.ScalarType):
            base_target = edb_target.get_topmost_concrete_base(self.edb_schema)
            bt_name = base_target.get_name(self.edb_schema)
        else:
            bt_name = None

        # first check for this being a multi-link
        if not ptr.singular(self.edb_schema):
            fields['add'] = GraphQLInputField(target)
            fields['remove'] = GraphQLInputField(target)
        elif target in {GraphQLInt, GraphQLInt64, GraphQLBigint,
                        GraphQLFloat, GraphQLDecimal}:
            # anything that maps onto the numeric types is a fair game
            fields['increment'] = GraphQLInputField(target)
            fields['decrement'] = GraphQLInputField(target)
        elif (
            bt_name == s_name.QualName(module='std', name='str')
            or isinstance(edb_target, s_types.Array)
        ):
            # only actual strings and arrays have append, prepend and
            # slice ops
            fields['prepend'] = GraphQLInputField(target)
            fields['append'] = GraphQLInputField(target)
            # slice [from, to]
            fields['slice'] = GraphQLInputField(
                GraphQLList(GraphQLNonNull(GraphQLInt))
            )

        nitype = GraphQLInputObjectType(
            name=self.get_input_name(
                f'UpdateOp_{fname}_',
                self.get_gql_name(typename),
            ),
            fields=fields,
        )
        self._gql_inobjtypes[name] = nitype

        return nitype

    def _make_generic_nested_update_type(
        self,
        edb_base: s_objtypes.ObjectType,
    ) -> GraphQLInputObjectType:
        typename = edb_base.get_name(self.edb_schema)
        name = f'NestedUpdate{typename}'
        nitype = GraphQLInputObjectType(
            name=self.get_input_name(
                'NestedUpdate', self.get_gql_name(typename)),
            fields={
                'filter': GraphQLInputField(
                    self._gql_inobjtypes[str(typename)]),
                'order': GraphQLInputField(
                    self._gql_ordertypes[str(typename)]),
                'first': GraphQLInputField(GraphQLInt),
                'last': GraphQLInputField(GraphQLInt),
                # before and after are supposed to be opaque values
                # serialized to string
                'before': GraphQLInputField(GraphQLString),
                'after': GraphQLInputField(GraphQLString),
            },
        )

        self._gql_inobjtypes[name] = nitype

        return nitype

    def _make_generic_nested_insert_type(
        self,
        edb_base: s_objtypes.ObjectType,
    ) -> GraphQLInputObjectType:
        typename = edb_base.get_name(self.edb_schema)
        name = f'NestedInsert{typename}'
        fields = {
            'filter': GraphQLInputField(
                self._gql_inobjtypes[str(typename)]),
            'order': GraphQLInputField(
                self._gql_ordertypes[str(typename)]),
            'first': GraphQLInputField(GraphQLInt),
            'last': GraphQLInputField(GraphQLInt),
            # before and after are supposed to be opaque values
            # serialized to string
            'before': GraphQLInputField(GraphQLString),
            'after': GraphQLInputField(GraphQLString),
        }

        # The data can only be a specific non-interface type, if no
        # such type exists, skip it as we cannot accept unambiguous
        # data input. It's still possible to just select some existing
        # data.
        data_t = self._gql_inobjtypes.get(f'Insert{typename}')
        if data_t:
            fields['data'] = GraphQLInputField(data_t)

        nitype = GraphQLInputObjectType(
            name=self.get_input_name(
                'NestedInsert', self.get_gql_name(typename)),
            fields=fields,
        )

        self._gql_inobjtypes[name] = nitype

        return nitype

    def define_enums(self) -> None:
        self._gql_enums['directionEnum'] = GraphQLEnumType(
            'directionEnum',
            values=dict(
                ASC=GraphQLEnumValue(),
                DESC=GraphQLEnumValue()
            ),
            description='Enum value used to specify ordering direction.',
        )
        self._gql_enums['nullsOrderingEnum'] = GraphQLEnumType(
            'nullsOrderingEnum',
            values=dict(
                SMALLEST=GraphQLEnumValue(),
                BIGGEST=GraphQLEnumValue(),
            ),
            description='Enum value used to specify how nulls are ordered.',
        )

        scalar_types = list(
            self.edb_schema.get_objects(
                included_modules=self.modules,
                type=s_scalars.ScalarType
            ),
        )
        for st in scalar_types:
            enum_values = st.get_enum_values(self.edb_schema)
            if enum_values is not None:
                t_name = st.get_name(self.edb_schema)
                gql_name = self.get_gql_name(t_name)
                enum_type = GraphQLEnumType(
                    gql_name,
                    values={key: GraphQLEnumValue() for key in enum_values},
                    description=self._get_description(st),
                )

                self._gql_enums[gql_name] = enum_type
                self._gql_inobjtypes[f'Insert{t_name}'] = enum_type

    def define_generic_filter_types(self) -> None:
        eq = ['eq', 'neq']
        comp = eq + ['gte', 'gt', 'lte', 'lt']
        string = comp + ['like', 'ilike']

        self._make_generic_filter_type(GraphQLBoolean, eq)
        self._make_generic_filter_type(GraphQLID, eq)
        self._make_generic_filter_type(GraphQLInt, comp)
        self._make_generic_filter_type(GraphQLInt64, comp)
        self._make_generic_filter_type(GraphQLBigint, comp)
        self._make_generic_filter_type(GraphQLFloat, comp)
        self._make_generic_filter_type(GraphQLDecimal, comp)
        self._make_generic_filter_type(GraphQLString, string)
        self._make_generic_filter_type(GraphQLJSON, comp)

        for name, etype in self._gql_enums.items():
            if name not in {'directionEnum', 'nullsOrderingEnum'}:
                self._make_generic_filter_type(etype, comp)

    def _make_generic_filter_type(
        self,
        base: Union[GraphQLScalarType, GraphQLEnumType],
        ops: List[str],
    ) -> None:
        name = f'Filter{base.name}'
        fields = {}

        # Always include the 'exists' operation
        fields['exists'] = GraphQLInputField(GraphQLBoolean)
        # Always include the 'in' operation
        fields['in'] = GraphQLInputField(GraphQLList(GraphQLNonNull(base)))
        for op in ops:
            fields[op] = GraphQLInputField(base)

        self._gql_inobjtypes[name] = GraphQLInputObjectType(
            name=name,
            fields=fields,
        )

    def define_generic_insert_types(self) -> None:
        for itype in [
            GraphQLBoolean,
            GraphQLID,
            GraphQLInt,
            GraphQLInt64,
            GraphQLBigint,
            GraphQLFloat,
            GraphQLDecimal,
            GraphQLString,
            GraphQLJSON,
        ]:
            self._gql_inobjtypes[f'Insert{itype.name}'] = itype

    def define_generic_order_types(self) -> None:
        self._gql_ordertypes['directionEnum'] = self._gql_enums['directionEnum']
        self._gql_ordertypes['nullsOrderingEnum'] = self._gql_enums[
            'nullsOrderingEnum'
        ]
        self._gql_ordertypes['Ordering'] = GraphQLInputObjectType(
            'Ordering',
            fields=dict(
                dir=GraphQLInputField(
                    GraphQLNonNull(self._gql_enums['directionEnum']),
                ),
                nulls=GraphQLInputField(
                    self._gql_enums['nullsOrderingEnum'],
                    default_value='SMALLEST',
                ),
            )
        )

    def get_order_fields(
        self,
        typename: s_name.QualName,
    ) -> Dict[str, GraphQLInputField]:
        fields: Dict[str, GraphQLInputField] = {}

        edb_type = self.edb_schema.get(typename, type=s_objtypes.ObjectType)
        pointers = edb_type.get_pointers(self.edb_schema)
        names = sorted(pointers.keys(self.edb_schema))

        for unqual_name in names:
            name = str(unqual_name)
            if name == '__type__':
                continue

            ptr = edb_type.getptr(self.edb_schema, unqual_name)

            if not ptr.singular(self.edb_schema):
                continue

            t = ptr.get_target(self.edb_schema)
            assert t is not None

            target = self._convert_edb_type(t)
            if target is None:
                # Don't expose this
                continue

            if isinstance(t, s_scalars.ScalarType):
                assert isinstance(target, GraphQLNamedType)
                # This makes sure that we can only order by properties
                # that can be reflected into GraphQL
                intype = self._gql_inobjtypes.get(f'Filter{target.name}')

                if intype:
                    fields[name] = GraphQLInputField(
                        self._gql_ordertypes['Ordering']
                    )
            elif isinstance(t, s_objtypes.ObjectType):
                # It's a link so we need the link's type order input
                t_name = t.get_name(self.edb_schema)
                fields[name] = GraphQLInputField(
                    self._gql_ordertypes[str(t_name)]
                )
            else:
                # We ignore pointers that aren't scalars or objects.
                pass

        return fields

    def get_input_range_type(
        self, subtype: s_types.Type
    ) -> GraphQLInputObjectType:
        sub_gqltype = self._convert_edb_type(subtype)
        assert isinstance(sub_gqltype, GraphQLScalarType)
        r_name = f'RangeOf{sub_gqltype.name}'
        # Check the type cache...
        if (res := self._gql_inobjtypes.get(r_name)) is not None:
            assert isinstance(res, GraphQLInputObjectType)
            return res

        gqltype = GraphQLInputObjectType(
            name=r_name,
            fields=dict(
                lower=GraphQLInputField(sub_gqltype),
                inc_lower=GraphQLInputField(GraphQLBoolean),
                upper=GraphQLInputField(sub_gqltype),
                inc_upper=GraphQLInputField(GraphQLBoolean),
                empty=GraphQLInputField(GraphQLBoolean),
            ),
            description=f'Range of {sub_gqltype.name} values',
        )
        self._gql_inobjtypes[r_name] = gqltype

        return gqltype

    def _define_types(self) -> None:
        interface_types = []
        obj_types = []
        from_union = {}

        self.define_enums()
        self.define_generic_filter_types()
        self.define_generic_order_types()
        self.define_generic_insert_types()

        # Every ObjectType is reflected as an interface.
        interface_types = list(
            self.edb_schema.get_objects(included_modules=self.modules,
                                        type=s_objtypes.ObjectType))

        # concrete types are also reflected as Type (with a '_Type' postfix)
        obj_types += [t for t in interface_types
                      if not t.get_abstract(self.edb_schema)]

        # interfaces
        for t in interface_types:
            t_name = t.get_name(self.edb_schema)
            gql_name = self.get_gql_name(t_name)

            if t_name in HIDDEN_TYPES:
                continue

            if t.is_view(self.edb_schema):
                # The aliased types actually only reflect as an object
                # type, but the rest of the processing is identical to
                # interfaces.
                self._gql_objtypes_from_alias[t_name] = GraphQLObjectType(
                    name=gql_name,
                    fields=partial(self.get_fields, t_name),
                    description=self._get_description(t),
                )
            else:

                def _type_resolver(
                    obj: GraphQLObjectType,
                    info: GraphQLResolveInfo,
                    _t: GraphQLAbstractType,
                ) -> GraphQLObjectType:
                    return obj
                self._gql_interfaces[t_name] = GraphQLInterfaceType(
                    name=gql_name,
                    fields=partial(self.get_fields, t_name),
                    resolve_type=_type_resolver,
                    description=self._get_description(t),
                )

            if t.is_union_type(self.edb_schema):
                # NOTE: EdgeDB union types and GraphQL union types are
                # different in some important ways. In EdgeDB a union object
                # type will have all the common links and properties that are
                # shared among the members of the union. In GraphQL a union
                # type has *no fields* at all and must be accessed via typed
                # fragments. Effectively, EdgeDB union types behave exactly
                # like GraphQL interfaces, though, which is why they will be
                # reflected more naturally as interfaces.
                #
                # We still need to internally keep track of which interfaces
                # are actually union types so that we don't create any
                # top-level Query or Mutation entires for union types, but
                # stick to only use them in the nested structures they
                # actually appear in.

                self._gql_uniontypes.add(t_name)
                for member in t.get_union_of(self.edb_schema) \
                               .names(self.edb_schema):
                    # Union types must be interfaces for each of
                    # the individual components so we need to record that.
                    from_union[member] = t_name

            # input object types corresponding to this interface
            gqlfiltertype = GraphQLInputObjectType(
                name=self.get_input_name('Filter', gql_name),
                fields=partial(self.get_filter_fields, t_name),
            )
            self._gql_inobjtypes[str(t_name)] = gqlfiltertype

            # ordering input type
            gqlordertype = GraphQLInputObjectType(
                name=self.get_input_name('Order', gql_name),
                fields=partial(self.get_order_fields, t_name),
            )
            self._gql_ordertypes[str(t_name)] = gqlordertype

            # update object types corresponding to this object (all types
            # except views and union types can appear as update types)
            if not (t.is_view(self.edb_schema)
                    or t.is_union_type(self.edb_schema)):

                # only objects that have at least one non-readonly
                # link/property are eligible
                pointers = t.get_pointers(self.edb_schema)
                if any(not p.get_readonly(self.edb_schema) and
                       not p.is_pure_computable(self.edb_schema)
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
            gql_name = self.get_gql_name(t_name)

            if t_name in HIDDEN_TYPES:
                continue

            if t.is_view(self.edb_schema):
                # Just copy previously computed type.
                self._gql_objtypes[t_name] = \
                    self._gql_objtypes_from_alias[t_name]
                continue

            if t.is_union_type(self.edb_schema):
                continue

            if t_name in self._gql_interfaces:
                interfaces.append(self._gql_interfaces[t_name])

            if t_name in from_union:
                interfaces.append(self._gql_interfaces[from_union[t_name]])

            ancestors = t.get_ancestors(self.edb_schema)
            for st in ancestors.objects(self.edb_schema):
                if (st.is_object_type() and
                    st.get_name(self.edb_schema) in
                        self._gql_interfaces):
                    interfaces.append(
                        self._gql_interfaces[st.get_name(self.edb_schema)])

            gqltype = GraphQLObjectType(
                name=f'{gql_name}_Type',
                fields=partial(self.get_fields, t_name),
                interfaces=interfaces,
                description=self._get_description(t),
            )
            self._gql_objtypes[t_name] = gqltype

            # only objects that have at least one non-computed
            # link/property are eligible to be input objects
            pointers = t.get_pointers(self.edb_schema)
            if any(not p.is_pure_computable(self.edb_schema)
                   for pname, p in pointers.items(self.edb_schema)
                   if str(pname) not in {'__type__', 'id'}):

                # input object types corresponding to this object (only
                # real objects can appear as input objects)
                gqlinserttype = GraphQLInputObjectType(
                    name=self.get_input_name('Insert', gql_name),
                    fields=partial(self.get_insert_fields, t_name),
                )
                self._gql_inobjtypes[f'Insert{t_name}'] = gqlinserttype

    def get(self, name: str, *, dummy: bool = False) -> GQLBaseType:
        '''Get a special GQL type either by name or based on Gel type.'''
        # normalize name and possibly add 'edb_base' to kwargs
        edb_base = None
        kwargs: Dict[str, Any] = {'dummy': dummy}

        if not name.startswith('__graphql__::'):
            # The name may potentially contain the suffix "_Type",
            # which in 99% cases indicates that it's a GraphQL
            # internal type generated from the EdgeDB base type, but
            # we technically need to check both.
            if name.endswith('_Type'):
                names = [name[:-len('_Type')], name]
            else:
                names = [name]

            for tname in names:
                if edb_base is None:
                    module: Union[s_name.Name, str]

                    if '::' in tname:
                        edb_base = self.edb_schema.get(
                            tname,
                            type=s_types.Type,
                        )
                    elif '__' in tname:
                        # Looks like it's coming from a specific module
                        edb_base = self.edb_schema.get(
                            f"{tname.replace('__', '::')}",
                            type=s_types.Type,
                        )
                    else:
                        for module in self.modules:
                            edb_base = self.edb_schema.get(
                                f'{module}::{tname}',
                                type=s_types.Type,
                                default=None,
                            )
                            if edb_base:
                                break

                        # XXX: find a better way to do this
                        for stype in [s_types.Array, s_types.Tuple,
                                      s_types.Range, s_types.MultiRange]:
                            if edb_base is None:
                                edb_base = self.edb_schema.get_global(
                                    stype, tname, default=None
                                )
                            else:
                                break

            if edb_base is None:
                raise AssertionError(
                    f'unresolved type: {name}')

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
    edb_map: Dict[str, Type[GQLBaseType]] = {}

    def __new__(
        mcls,
        name: str,
        bases: Tuple[type, ...],
        dct: Dict[str, Any],
    ) -> GQLTypeMeta:
        cls = super().__new__(mcls, name, bases, dct)

        edb_type = dct.get('edb_type')
        if edb_type:
            mcls.edb_map[str(edb_type)] = cls  # type: ignore

        return cls


class GQLBaseType(metaclass=GQLTypeMeta):

    edb_type: ClassVar[Optional[s_name.QualName]] = None
    _edb_base: Optional[s_types.Type]
    _module: Optional[str]
    _fields: Dict[Tuple[str, bool], GQLBaseType]
    _shadow_fields: Tuple[str, ...]

    def __init__(
        self,
        schema: GQLCoreSchema,
        *,
        name: Optional[str] = None,
        edb_base: Optional[s_types.Type] = None,
        dummy: bool = False,
    ) -> None:
        self._shadow_fields = ()

        if edb_base is None:
            if self.edb_type:
                if self.edb_type.module == '__graphql__':
                    edb_base_name = str(self.edb_type)
                else:
                    edb_base = schema.edb_schema.get(
                        self.edb_type,
                        type=s_objtypes.ObjectType,
                    )
                    edb_base_name = str(edb_base.get_name(schema.edb_schema))
            else:
                raise AssertionError(
                    f'neither the constructor, nor the class attribute '
                    f'define a required edb_base for {type(self)!r}',
                )
        else:
            edb_base_name = str(edb_base.get_name(schema.edb_schema))

        # __typename
        if name is None:
            self._name = edb_base_name
        else:
            self._name = name

        # determine module from name if not already specified
        if '::' in self._name:
            self._module = self._name.rsplit('::', 1)[0]
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
        if isinstance(edb_base, s_scalars.ScalarType):
            bt = edb_base.get_topmost_concrete_base(self.edb_schema)
            bt_name = str(bt.get_name(self.edb_schema))
            self._is_json = bt_name == 'std::json'
            self._is_bool = bt_name == 'std::bool'
            self._is_float = edb_base.issubclass(
                self.edb_schema,
                self.edb_schema.get(
                    'std::anyfloat',
                    type=s_scalars.ScalarType,
                ),
            )
        else:
            self._is_json = self._is_bool = self._is_float = False

    @property
    def is_json(self) -> bool:
        return self._is_json

    @property
    def is_enum(self) -> bool:
        return False

    @property
    def is_bool(self) -> bool:
        return self._is_bool

    @property
    def is_float(self) -> bool:
        return self._is_float

    @property
    def is_array(self) -> bool:
        if self.edb_base is None:
            return False
        else:
            return self.edb_base.is_array()

    @property
    def is_range(self) -> bool:
        if self.edb_base is None:
            return False
        else:
            return self.edb_base.is_range()

    @property
    def is_multirange(self) -> bool:
        if self.edb_base is None:
            return False
        else:
            return self.edb_base.is_multirange()

    @property
    def is_object_type(self) -> bool:
        if self.edb_base is None:
            return False
        else:
            return self.edb_base.is_object_type()

    @property
    def name(self) -> str:
        return self._name

    @property
    def short_name(self) -> str:
        return self._name.split('::')[-1]

    @property
    def module(self) -> Optional[str]:
        return self._module

    @property
    def edb_base(self) -> Optional[s_types.Type]:
        return self._edb_base

    @property
    def edb_base_name_ast(self) -> Optional[qlast.ObjectRef]:
        if self.edb_base is None:
            return None
        if isinstance(self.edb_base, (s_types.Array,
                                      s_types.Range,
                                      s_types.MultiRange)):
            el = self.edb_base.get_element_type(self.edb_schema)
            base_name = el.get_name(self.edb_schema)
            assert isinstance(base_name, s_name.QualName)
            return qlast.ObjectRef(
                module=base_name.module,
                name=base_name.name,
            )
        else:
            base_name = self.edb_base.get_name(self.edb_schema)
            assert isinstance(base_name, s_name.QualName)
            return qlast.ObjectRef(
                module=base_name.module,
                name=base_name.name,
            )

    @property
    def edb_base_name(self) -> str:
        ast = self.edb_base_name_ast
        if ast is None:
            return ''
        else:
            return codegen.generate_source(ast)

    @property
    def gql_typename(self) -> str:
        name = self.name
        module, shortname = name.rsplit('::', 1)

        if self.edb_base is None:
            # We expect that this is one of the fake objects, that
            # only have an edb_type.
            assert self.edb_type is not None
            return self.edb_type.name
        elif self.edb_base.is_view(self.edb_schema):
            suffix = ''
        else:
            suffix = '_Type'

        if module in {'default', 'std'}:
            return f'{shortname}{suffix}'
        else:
            assert module != '', 'gql_typename ' + module
            return f'{name.replace("::", "__")}{suffix}'

    @property
    def schema(self) -> GQLCoreSchema:
        return self._schema

    @property
    def edb_schema(self) -> s_schema.Schema:
        return self._schema.edb_schema

    @edb_schema.setter
    def edb_schema(self, schema: s_schema.Schema) -> None:
        self._schema.edb_schema = schema

    def convert_edb_to_gql_type(
        self,
        base: Union[s_types.Type, s_pointers.Pointer],
        **kwargs: Any,
    ) -> GQLBaseType:
        if isinstance(base, s_pointers.Pointer):
            tgt = base.get_target(self.edb_schema)
            assert tgt is not None
            base = tgt

        if self.dummy:
            kwargs['dummy'] = True

        return self.schema.get(str(base.get_name(self.edb_schema)), **kwargs)

    def is_field_shadowed(self, name: str) -> bool:
        return name in self._shadow_fields

    def get_field_type(self, name: str) -> Optional[GQLBaseType]:
        if self.dummy:
            return None

        # this is just shadowing a real EdgeDB type
        fkey = (name, self.dummy)
        target = self._fields.get(fkey)

        if target is None:
            # special handling of '__typename'
            if name == '__typename':
                target = self.convert_edb_to_gql_type(
                    self.edb_schema.get(
                        s_name.QualName(
                            module='std',
                            name='str',
                        ),
                        type=s_scalars.ScalarType,
                    ),
                )

            elif isinstance(self.edb_base, s_objtypes.ObjectType):
                ptr = self.edb_base.maybe_get_ptr(
                    self.edb_schema,
                    s_name.UnqualName(name),
                )
                if ptr is not None:
                    target = self.convert_edb_to_gql_type(ptr)

            if target is not None:
                self._fields[fkey] = target

        return target

    def has_native_field(self, name: str) -> bool:
        if isinstance(self.edb_base, s_objtypes.ObjectType):
            ptr = self.edb_base.maybe_get_ptr(
                self.edb_schema, s_name.UnqualName(name))
            return ptr is not None
        else:
            return False

    def issubclass(self, other: Any) -> bool:
        if (
            self.edb_base is not None
            and other.edb_base is not None
            and isinstance(other, GQLShadowType)
        ):
            return self.edb_base.issubclass(
                self._schema.edb_schema, other.edb_base
            )
        else:
            return False

    def get_template(
        self,
    ) -> Tuple[qlast.Base, Optional[qlast.Expr], Optional[qlast.SelectQuery]]:
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
        assert isinstance(filterable, qlast.SelectQuery)
        shape = filterable.result

        return eql, shape, filterable

    def get_field_template(
        self,
        name: str,
        *,
        parent: qlast.Base,
        has_shape: bool = False,
    ) -> Tuple[
        Optional[qlast.Base],
        Optional[qlast.Expr],
        Optional[qlast.SelectQuery],
    ]:
        eql = shape = filterable = None
        if self.dummy:
            return eql, shape, filterable

        if name == '__typename' and not self.is_field_shadowed(name):
            if self.edb_base is None:
                # We expect that this is one of the fake objects, that
                # only have an edb_type.
                assert self.edb_type is not None
                eql = parse_fragment(f'{self.edb_type.name!r}')
            elif self.edb_base.is_view(self.edb_schema):
                eql = parse_fragment(f'{self.gql_typename!r}')
            else:
                # Construct the GraphQL type name from the actual type name.
                eql = parse_fragment(fr'''
                    WITH name := {codegen.generate_source(parent)}
                        .__type__.name
                    SELECT (
                        name[5:] IF name LIKE 'std::%' ELSE
                        name[9:] IF name LIKE 'default::%' ELSE
                        str_replace(name, '::', '__')
                    ) ++ '_Type'
                ''')

        elif has_shape:
            eql = parse_fragment(
                f'''SELECT {codegen.generate_source(parent)}.
                        {codegen.generate_source(qlast.ObjectRef(name=name))}
                        {{ xxx }}
                ''')
            assert isinstance(eql, qlast.SelectQuery)
            filterable = eql
            shape = filterable.result

        else:
            eql = parse_fragment(
                f'''SELECT {codegen.generate_source(parent)}.
                        {codegen.generate_source(qlast.ObjectRef(name=name))}
                ''')
            assert isinstance(eql, qlast.SelectQuery)
            filterable = eql

        return eql, shape, filterable

    def get_field_cardinality(
        self,
        name: str,
    ) -> Optional[qltypes.SchemaCardinality]:
        if not self.is_field_shadowed(name):
            return None

        elif isinstance(self.edb_base, s_objtypes.ObjectType):
            ptr = self.edb_base.getptr(
                self.edb_schema,
                s_name.UnqualName(name),
            )
            if not ptr.singular(self.edb_schema):
                return qltypes.SchemaCardinality.Many

        return None


class GQLShadowType(GQLBaseType):

    def is_field_shadowed(self, name: str) -> bool:
        if name == '__typename':
            return False

        ftype = self.get_field_type(name)
        # JSON fields are not shadowed
        if ftype is None:
            return False

        return True

    @property
    def is_enum(self) -> bool:
        if self.edb_base is None:
            return False
        else:
            return self.edb_base.is_enum(self.edb_schema)


class GQLBaseQuery(GQLBaseType):

    def __init__(
        self,
        schema: GQLCoreSchema,
        *,
        name: Optional[str] = None,
        edb_base: Optional[s_types.Type] = None,
        dummy: bool = False,
    ) -> None:
        self.modules = schema.modules
        super().__init__(schema, name=name, edb_base=edb_base, dummy=dummy)
        # Record names of std built-in object types
        self._std_obj_names = [
            t.get_name(self.edb_schema).name for t in
            self.edb_schema.get_objects(
                included_modules=[s_name.UnqualName('std')],
                type=s_objtypes.ObjectType,
            )
        ]

    def get_module_and_name(self, name: str) -> Tuple[str, ...]:
        if name in self._std_obj_names:
            return ('std', name)
        elif '__' in name:
            module, name = name.rsplit('__', 1)
            return (module.replace('__', '::'), name)
        else:
            return ('default', name)


class GQLQuery(GQLBaseQuery):
    edb_type = s_name.QualName(module='__graphql__', name='Query')

    def get_field_type(self, name: str) -> Optional[GQLBaseType]:
        fkey = (name, self.dummy)
        target = None

        if name in {'__type', '__schema'}:
            if fkey in self._fields:
                return self._fields[fkey]

            target = self.schema.get(str(self.edb_type), dummy=True)

        else:
            target = super().get_field_type(name)

            if target is None:
                module, edb_name = self.get_module_and_name(name)
                edb_qname = s_name.QualName(module=module, name=edb_name)
                edb_type = self.edb_schema.get(
                    edb_qname,
                    default=None,
                    type=s_types.Type,
                )
                if edb_type is not None:
                    target = self.convert_edb_to_gql_type(edb_type)

        if target is not None:
            self._fields[fkey] = target

        return target


class GQLMutation(GQLBaseQuery):
    edb_type = s_name.QualName(module='__graphql__', name='Mutation')

    def get_field_type(self, name: str) -> Optional[GQLBaseType]:
        fkey = (name, self.dummy)
        target = None

        if name == '__typename':
            # It's a valid field that doesn't start with a command
            target = super().get_field_type(name)
        else:
            op, name = name.split('_', 1)
            if op in {'delete', 'insert', 'update'}:
                target = super().get_field_type(name)

                if target is None:
                    module, edb_name = self.get_module_and_name(name)
                    edb_qname = s_name.QualName(module=module, name=edb_name)
                    edb_type = self.edb_schema.get(
                        edb_qname,
                        default=None,
                        type=s_types.Type,
                    )
                    if edb_type is not None:
                        target = self.convert_edb_to_gql_type(edb_type)

        if target is not None:
            self._fields[fkey] = target

        return target
