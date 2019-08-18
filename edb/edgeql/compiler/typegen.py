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


from __future__ import annotations

import collections
import typing

from edb import errors

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.schema import abc as s_abc
from edb.schema import objects as s_obj
from edb.schema import types as s_types

from edb.edgeql import ast as qlast

from . import astutils
from . import context
from . import dispatch
from . import schemactx
from . import setgen


def type_to_ql_typeref(t: s_obj.Object, *,
                       _name=None,
                       ctx: context.ContextLevel) -> qlast.TypeName:

    return astutils.type_to_ql_typeref(t, schema=ctx.env.schema)


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

    if isinstance(ql_t, qlast.TypeOf):
        with ctx.newscope(fenced=True, temporary=True) as subctx:
            ir_set = setgen.ensure_set(dispatch.compile(ql_t.expr, ctx=subctx),
                                       ctx=subctx)
            stype = setgen.get_set_type(ir_set, ctx=subctx)

        return [irtyputils.type_to_typeref(subctx.env.schema, stype)]

    elif isinstance(ql_t, qlast.TypeOp):
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

    stype = ql_typeref_to_type(ql_t, ctx=ctx)
    return irtyputils.type_to_typeref(ctx.env.schema, stype)


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
