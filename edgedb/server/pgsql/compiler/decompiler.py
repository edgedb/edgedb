##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common import ast

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import name as sn

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
                name = l.shortname
                colname = common.edgedb_name_to_pg_name(l.shortname)
                source = context.current.source.get_pointer_origin(
                    name, farthest=True)
                context.current.attmap[colname] = (name, source)

        return self.visit(tree)

    def generic_visit(self, node, *, combine_results=None):
        raise NotImplementedError(
            'no SQL decompiler handler for {}'.format(node.__class__))

    def visit_Expr(self, expr):
        if expr.lexpr is not None:
            left = self.visit(expr.lexpr)
        else:
            left = None

        if expr.rexpr is not None:
            right = self.visit(expr.rexpr)
        else:
            right = None

        if left is None:
            result = irast.UnaryOp(op=expr.op, operand=right)
        else:
            result = irast.BinOp(left=left, op=expr.op, right=right)

        return result

    def visit_ColumnRef(self, expr):
        ctx = self.context.current

        if not ctx.source:
            raise RuntimeError(
                'cannot decompile column references without source context')

        if isinstance(ctx.source, s_concepts.Concept):
            path_id = irutils.LinearPath([ctx.source])
            pointer, source = ctx.attmap[expr.field]
            srcset = irast.Set(scls=ctx.source, path_id=path_id)
            result = irutils.extend_path(ctx.schema, srcset, pointer)
        else:
            if ctx.source.generic():
                propname = ctx.attmap[expr.field][0]
            else:
                ptr_info = pg_types.get_pointer_storage_info(
                    ctx.source, resolve_type=False)

                if ptr_info.table_type == 'concept':
                    # Singular pointer promoted into source table
                    propname = sn.Name('std::target')
                else:
                    propname = ctx.attmap[expr.field][0]

            path = irast.Set()
            path.scls = ctx.schema.get('std::Object')
            path.path_id = irutils.LinearPath([path.scls])

            tgt_path = irutils.extend_path(ctx.schema, path, ctx.source)

            propcls = ctx.source.resolve_pointer(ctx.schema, propname)
            result = irutils.extend_path(ctx.schema, tgt_path, propcls)

        return result

    def visit_ImplicitRowExpr(self, expr):
        return irast.Sequence(elements=[self.visit(e) for e in expr.elements])

    def visit_FuncCall(self, expr):
        if expr.name in ('lower', 'upper'):
            fname = ('std', expr.name)
            args = [self.visit(a) for a in expr.args]
        elif expr.name == 'now':
            fname = ('std', 'current_datetime')
            args = [self.visit(a) for a in expr.args]
        elif expr.name in (
                ('edgedb', 'uuid_generate_v1mc'), 'uuid_generate_v1mc'):
            fname = ('std', 'uuid_generate_v1mc')
            args = [self.visit(a) for a in expr.args]
        else:
            raise ValueError('unexpected function: {}'.format(expr.name))

        return irast.FunctionCall(name=fname, args=args)
