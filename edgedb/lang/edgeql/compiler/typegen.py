##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL compiler type-related helpers."""


import typing

from edgedb.lang.common import parsing

from edgedb.lang.ir import ast as irast

from edgedb.lang.schema import objects as s_obj

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
            maintype=expr.scls.name,
        )

    else:
        raise errors.EdgeQLSyntaxError(
            'expecting a type reference',
            context=qlcontext)

    return result


def type_to_ql_typeref(t: s_obj.Class) -> qlast.TypeName:
    if not isinstance(t, s_obj.Collection):
        result = qlast.TypeName(
            maintype=qlast.ClassRef(
                module=t.name.module,
                name=t.name.name
            )
        )
    else:
        result = qlast.TypeName(
            maintype=qlast.ClassRef(
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
            maintype=schemactx.get_schema_object(maintype, ctx=ctx).name,
            subtypes=[]
        )

    return typ
