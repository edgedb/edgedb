##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common import ast

from edgedb.lang.ir import ast2 as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import pointers as s_pointers

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types

from .context import TransformerContext


class Decompiler(ast.visitor.NodeVisitor):
    def transform(self, tree, local_to_source=None):
        context = TransformerContext()
        context.current.source = local_to_source

        if local_to_source:
            context.current.attmap = {}

            for l in local_to_source.pointers.values():
                name = l.normal_name()
                colname = common.edgedb_name_to_pg_name(l.normal_name())
                source = context.current.source.get_pointer_origin(
                    name, farthest=True)
                context.current.attmap[colname] = (name, source)

        return self._process_expr(context, tree)

    def _process_expr(self, context, expr):
        if isinstance(expr, pgast.BinOpNode):
            left = self._process_expr(context, expr.left)
            right = self._process_expr(context, expr.right)
            result = irast.BinOp(left=left, op=expr.op, right=right)

        elif isinstance(expr, pgast.FieldRefNode):
            if context.current.source:
                if isinstance(context.current.source, s_concepts.Concept):
                    id = irutils.LinearPath([context.current.source])
                    pointer, source = context.current.attmap[expr.field]
                    entset = irast.EntitySet(id=id, concept=source)
                    result = irast.AtomicRefSimple(ref=entset, name=pointer)
                else:
                    if context.current.source.generic():
                        name = context.current.attmap[expr.field][0]
                    else:
                        ptr_info = pg_types.get_pointer_storage_info(
                            context.current.source, resolve_type=False)

                        if ptr_info.table_type == 'concept':
                            # Singular pointer promoted into source table
                            name = sn.Name('std::target')
                        else:
                            name = context.current.attmap[expr.field][0]

                    id = irutils.LinearPath([None])
                    id.add(
                        context.current.source,
                        s_pointers.PointerDirection.Outbound, None)
                    entlink = irast.EntityLink(
                        ptrcls=context.current.source)
                    result = irast.LinkPropRefSimple(
                        ref=entlink, name=name, id=id)
            else:
                assert False

        elif isinstance(expr, pgast.RowExprNode):
            result = irast.Sequence(
                elements=[self._process_expr(context, e) for e in expr.args])

        elif isinstance(expr, pgast.FunctionCallNode):
            result = self._process_function(context, expr)

        else:
            assert False, "unexpected node type: %r" % expr

        return result

    def _process_function(self, context, expr):
        if expr.name in ('lower', 'upper'):
            fname = ('str', expr.name)
            args = [self._process_expr(context, a) for a in expr.args]
        elif expr.name == 'now':
            fname = ('std', 'current_datetime')
            args = [self._process_expr(context, a) for a in expr.args]
        elif expr.name in (
                ('edgedb', 'uuid_generate_v1mc'), 'uuid_generate_v1mc'):
            fname = ('std', 'uuid_generate_v1mc')
            args = [self._process_expr(context, a) for a in expr.args]
        else:
            raise ValueError('unexpected function: {}'.format(expr.name))

        return irast.FunctionCall(name=fname, args=args)
