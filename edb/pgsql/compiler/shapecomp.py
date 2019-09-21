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

from __future__ import annotations

from typing import *  # NoQA

from edb.edgeql import qltypes

from edb.ir import ast as irast
from edb.ir import utils as irutils

from edb.pgsql import ast as pgast

from . import astutils
from . import context
from . import dispatch
from . import expr as expr_compiler  # NOQA
from . import relgen


def compile_shape(
        ir_set: irast.Set, shape: List[irast.Set], *,
        ctx: context.CompilerContextLevel) -> pgast.TupleVar:
    elements = []

    with ctx.newscope() as shapectx:
        shapectx.disable_semi_join.add(ir_set.path_id)

        if isinstance(ir_set.expr, irast.Stmt):
            iterators = irutils.get_iterator_sets(ir_set.expr)
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
            if iterators:
                for iterator in iterators:
                    shapectx.path_scope[iterator.path_id] = ctx.rel

        for el in shape:
            rptr = el.rptr
            ptrref = rptr.ptrref
            is_singleton = ptrref.dir_cardinality is qltypes.Cardinality.ONE
            value: pgast.BaseExpr

            if (irutils.is_subquery_set(el) or
                    el.path_id.is_objtype_path() or
                    not is_singleton or
                    not ptrref.required):
                wrapper = relgen.set_as_subquery(
                    el, as_value=True, ctx=shapectx)
                if not is_singleton:
                    value = relgen.set_to_array(
                        ir_set=el, query=wrapper, ctx=shapectx)
                else:
                    value = wrapper
            else:
                value = dispatch.compile(el, ctx=shapectx)

            tuple_el = astutils.tuple_element_for_shape_el(
                el, value, ctx=shapectx)

            assert isinstance(tuple_el, pgast.TupleElement)
            elements.append(tuple_el)

    return pgast.TupleVar(elements=elements, named=True)
