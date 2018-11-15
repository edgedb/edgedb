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


"""Compilation helpers for shapes."""

import typing

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import pointers as s_pointers

from edb.server.pgsql import ast as pgast

from . import astutils
from . import context
from . import dispatch
from . import expr as expr_compiler  # NOQA
from . import relgen


def compile_shape(
        ir_set: irast.Set, shape: typing.List[irast.Set], *,
        ctx: context.CompilerContextLevel) -> pgast.TupleVar:
    elements = []

    with ctx.newscope() as shapectx:
        shapectx.disable_semi_join.add(ir_set.path_id)
        shapectx.unique_paths.add(ir_set.path_id)

        if (isinstance(ir_set.expr, irast.Stmt) and
                ir_set.expr.iterator_stmt is not None):
            # The source set for this shape is a FOR statement,
            # which is special in that besides set path_id it
            # should also expose the path_id of the FOR iterator
            # so that shape element expressions that might contain
            # an iterator reference find it properly.
            #
            # So, for:
            #    SELECT Bar {
            #        foo := (FOR x := ... UNION Foo { spam := x })
            #    }
            #
            # the path scope when processing the shape of Bar.foo
            # should be {'Bar.foo', 'x'}.
            iter_path_id = ir_set.expr.iterator_stmt.path_id
            shapectx.path_scope[iter_path_id] = ctx.rel

        for el in shape:
            rptr = el.rptr
            ptrcls = rptr.ptrcls
            ptrdir = rptr.direction or s_pointers.PointerDirection.Outbound
            is_singleton = ptrcls.singular(ctx.env.schema, ptrdir)

            if (irutils.is_subquery_set(el) or
                    isinstance(el.scls, s_objtypes.ObjectType) or
                    not is_singleton or
                    not ptrcls.get_required(ctx.env.schema)):
                wrapper = relgen.set_as_subquery(
                    el, as_value=True, ctx=shapectx)
                if not is_singleton:
                    value = relgen.set_to_array(
                        ir_set=el, query=wrapper, ctx=shapectx)
                else:
                    value = wrapper
            else:
                value = dispatch.compile(el, ctx=shapectx)

            elements.append(
                astutils.tuple_element_for_shape_el(el, value, ctx=shapectx))

    return pgast.TupleVar(elements=elements, named=True)
