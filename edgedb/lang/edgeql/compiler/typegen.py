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

from edgedb.lang.common import parsing

from edgedb.lang.ir import ast as irast

from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import types as s_types

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors

from . import context
from . import schemactx


def process_type_ref_expr(
        expr: irast.Base) -> typing.Union[irast.Array, irast.TypeRef]:
    if isinstance(expr.expr, irast.Tuple):
        elems = []

        for elem in expr.expr.elements:
            ref_elem = process_type_ref_elem(elem.val, elem.context)

            elems.append(ref_elem)

        expr = irast.Array(elements=elems)

    else:
        expr = process_type_ref_elem(expr, expr.context)

    return expr


def process_type_ref_elem(
        expr: irast.Base, qlcontext: parsing.ParserContext) -> irast.TypeRef:
    if isinstance(expr, irast.Set):
        if expr.rptr is not None:
            raise errors.EdgeQLSyntaxError(
                'expecting a type reference',
                context=qlcontext)

        result = irast.TypeRef(
            maintype=expr.scls.material_type().name,
        )

    else:
        raise errors.EdgeQLSyntaxError(
            'expecting a type reference',
            context=qlcontext)

    return result


def type_to_ql_typeref(t: s_obj.Object) -> qlast.TypeName:
    if not isinstance(t, s_types.Collection):
        result = qlast.TypeName(
            maintype=qlast.ObjectRef(
                module=t.name.module,
                name=t.name.name
            )
        )
    else:
        result = qlast.TypeName(
            maintype=qlast.ObjectRef(
                name=t.schema_name
            ),
            subtypes=[
                type_to_ql_typeref(st) for st in t.get_subtypes()
            ]
        )

    return result


def ql_typeref_to_ir_typeref(
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
        typ = irast.TypeRef(
            maintype=schemactx.get_schema_type(maintype, ctx=ctx).name,
            subtypes=[]
        )

    return typ


def ql_typeref_to_type(
        ql_t: qlast.TypeName, *,
        ctx: context.ContextLevel) -> s_obj.Object:
    if ql_t.subtypes:
        coll = s_types.Collection.get_class(ql_t.maintype.name)

        if issubclass(coll, s_types.Tuple):
            subtypes = collections.OrderedDict()
            named = False
            for si, st in enumerate(ql_t.subtypes):
                if st.name:
                    named = True
                    type_name = st.name
                else:
                    type_name = str(si)

                subtypes[type_name] = ql_typeref_to_type(st, ctx=ctx)

            return coll.from_subtypes(subtypes, {'named': named})
        else:
            subtypes = []
            for st in ql_t.subtypes:
                subtypes.append(ql_typeref_to_type(st, ctx=ctx))

            return coll.from_subtypes(subtypes)
    else:
        return schemactx.get_schema_type(ql_t.maintype, ctx=ctx)
