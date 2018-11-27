#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


"""EdgeQL compiler type-related helpers."""


import collections
import typing

from edb import errors

from edb.lang.ir import ast as irast

from edb.lang.schema import abc as s_abc
from edb.lang.schema import objects as s_obj
from edb.lang.schema import types as s_types

from edb.lang.edgeql import ast as qlast

from . import context
from . import schemactx


def type_to_ql_typeref(t: s_obj.Object, *,
                       _name=None,
                       ctx: context.ContextLevel) -> qlast.TypeName:
    if not isinstance(t, s_abc.Collection):
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                module=t.get_name(ctx.env.schema).module,
                name=t.get_name(ctx.env.schema).name
            )
        )
    elif isinstance(t, s_abc.Tuple) and t.named:
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                name=t.schema_name
            ),
            subtypes=[
                type_to_ql_typeref(st, _name=sn, ctx=ctx)
                for sn, st in t.element_types.items()
            ]
        )
    else:
        result = qlast.TypeName(
            name=_name,
            maintype=qlast.ObjectRef(
                name=t.schema_name
            ),
            subtypes=[
                type_to_ql_typeref(st, ctx=ctx)
                for st in t.get_subtypes()
            ]
        )

    return result


def ql_typeref_to_ir_typeref(
        ql_t: qlast.TypeExpr, *,
        ctx: context.ContextLevel) -> typing.Union[irast.Array, irast.TypeRef]:

    types = _ql_typeexpr_to_ir_typeref(ql_t, ctx=ctx)
    if len(types) > 1:
        return irast.Array(
            elements=types
        )
    else:
        return types[0]


def _ql_typeexpr_to_ir_typeref(
        ql_t: qlast.TypeExpr, *,
        ctx: context.ContextLevel) -> typing.List[irast.TypeRef]:

    # FIXME: currently this only handles type union
    if isinstance(ql_t, qlast.TypeOp):
        if ql_t.op == '|':
            return (_ql_typeexpr_to_ir_typeref(ql_t.left, ctx=ctx) +
                    _ql_typeexpr_to_ir_typeref(ql_t.right, ctx=ctx))

        raise errors.UnsupportedFeatureError(
            f'type operator {ql_t.op!r} is not implemented',
            context=ql_t.context)

    else:
        return [_ql_typeref_to_ir_typeref(ql_t, ctx=ctx)]


def _ql_typeref_to_ir_typeref(
        ql_t: qlast.TypeName, *,
        ctx: context.ContextLevel) -> irast.TypeRef:
    maintype = ql_t.maintype
    subtypes = ql_t.subtypes

    if subtypes:
        typ = irast.TypeRef(
            maintype=maintype.name,
            subtypes=[]
        )

        for subtype in subtypes:
            subtype = ql_typeref_to_ir_typeref(subtype, ctx=ctx)
            typ.subtypes.append(subtype)
    else:
        styp = schemactx.get_schema_type(maintype, ctx=ctx)
        if styp.is_any():
            typ = irast.AnyTypeRef()
        elif styp.is_anytuple():
            typ = irast.AnyTupleRef()
        else:
            typ = irast.TypeRef(
                maintype=styp.get_name(ctx.env.schema),
                subtypes=[]
            )

    return typ


def ql_typeref_to_type(
        ql_t: qlast.TypeName, *,
        ctx: context.ContextLevel) -> s_obj.Object:
    if ql_t.subtypes:
        coll = s_types.Collection.get_class(
            ql_t.maintype.name)

        if issubclass(coll, s_abc.Tuple):
            subtypes = collections.OrderedDict()
            named = False
            for si, st in enumerate(ql_t.subtypes):
                if st.name:
                    named = True
                    type_name = st.name
                else:
                    type_name = str(si)

                subtypes[type_name] = ql_typeref_to_type(st, ctx=ctx)

            return coll.from_subtypes(
                ctx.env.schema, subtypes, {'named': named})
        else:
            subtypes = []
            for st in ql_t.subtypes:
                subtypes.append(ql_typeref_to_type(st, ctx=ctx))

            return coll.from_subtypes(ctx.env.schema, subtypes)
    else:
        return schemactx.get_schema_type(ql_t.maintype, ctx=ctx)
