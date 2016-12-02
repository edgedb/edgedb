##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import debug
from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import lproperties as s_lprops

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common

from .context import TransformerContext
from . import expr as expr_compiler


class SingletonExprIRCompiler(expr_compiler.IRCompilerBase):

    def transform_to_sql_tree(self, ir_expr, *, schema):
        # Transform to sql tree
        self.context = TransformerContext()
        ctx = self.context.current
        ctx.memo = self._memo
        ctx.schema = schema
        qtree = self.visit(ir_expr)

        if debug.flags.edgeql_compile:
            debug.header('SQL Tree')
            debug.dump(qtree)

        return qtree

    def visit_SelectStmt(self, node):
        return self.visit(node.result)

    def visit_Set(self, node):
        if node.expr is not None:
            return self.visit(node.expr)
        else:
            if node.rptr:
                ptrcls = node.rptr.ptrcls
                source = node.rptr.source

                if not isinstance(ptrcls, s_lprops.LinkProperty):
                    if source.rptr:
                        raise RuntimeError(
                            'unexpectedly long path in simple expr')

                colref = pgast.ColumnRef(
                    name=[
                        common.edgedb_name_to_pg_name(ptrcls.shortname)
                    ]
                )
            elif isinstance(node.scls, s_atoms.Atom):
                colref = pgast.ColumnRef(
                    name=[
                        common.edgedb_name_to_pg_name(node.scls.name)
                    ]
                )
            else:
                colref = pgast.ColumnRef(
                    name=[
                        common.edgedb_name_to_pg_name(node.scls.name)
                    ]
                )

            return colref
