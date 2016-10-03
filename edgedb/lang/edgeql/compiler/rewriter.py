##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import expr as s_expr
from edgedb.lang.schema import name as sn


class RewriteTransformer(ast.NodeTransformer):
    """Apply rewrites from policies."""

    def visit_EntitySet(self, expr):
        if expr.rlink:
            self.visit(expr.rlink)

        if ('access_rewrite' not in expr.rewrite_flags and
                expr.reference is None and expr.origin is None and
                getattr(self._context.current,
                        'apply_access_control_rewrite', False)):
            self._apply_rewrite_hooks(expr, 'filter')
            expr.rewrite_flags.add('access_rewrite')

        return expr

    def visit_EntityLink(self, expr):
        if expr.source is not None:
            self.visit(expr.source)

        if 'lang_rewrite' not in expr.rewrite_flags:
            schema = self._context.current.schema
            localizable = schema.get('std::localizable', default=None)

            link_class = expr.link_class

            if (localizable is not None and
                    link_class.issubclass(localizable)):
                cvars = self._context.current.context_vars

                lang = irast.Constant(
                    index='__context_lang', type=schema.get('std::str'))
                cvars['lang'] = 'en-US'

                propn = sn.Name('std::lang')

                for langprop in expr.proprefs:
                    if langprop.name == propn:
                        break
                else:
                    lprop_class = link_class.pointers[propn]
                    langprop = irast.LinkPropRefSimple(
                        name=propn, ref=expr, ptr_class=lprop_class)
                    expr.proprefs.add(langprop)

                eq_lang = irast.BinOp(
                    left=langprop, right=lang, op=ast.ops.EQ)
                lang_none = irast.NoneTest(expr=lang)
                # Test for property emptiness is for LEFT JOIN cases
                prop_none = irast.NoneTest(expr=langprop)
                lang_prop_none = irast.BinOp(
                    left=lang_none, right=prop_none, op=ast.ops.OR)
                lang_test = irast.BinOp(
                    left=lang_prop_none,
                    right=eq_lang,
                    op=ast.ops.OR,
                    strong=True)
                expr.propfilter = self.extend_binop(expr.propfilter,
                                                    lang_test)
                expr.rewrite_flags.add('lang_rewrite')

        if ('access_rewrite' not in expr.rewrite_flags and
                expr.source is not None
                # An optimization to avoid applying filtering rewrite
                # unnecessarily.
                and expr.source.reference is None and
                expr.source.origin is None and getattr(
                    self._context.current, 'apply_access_control_rewrite',
                    False)):
            self._apply_rewrite_hooks(expr, 'filter')
            expr.rewrite_flags.add('access_rewrite')

        if ('computable_rewrite' not in expr.rewrite_flags and
                expr.source is not None):
            self._apply_rewrite_hooks(expr, 'computable')
            expr.rewrite_flags.add('computable_rewrite')

        return expr

    def _apply_rewrite_hooks(self, expr, type):
        if isinstance(expr, irast.EntityLink):
            if (type == 'computable' and
                    expr.link_class.is_pure_computable()):
                deflt = expr.link_class.default[0]
                if isinstance(deflt, s_expr.ExpressionText):
                    edgeql_expr = deflt
                else:
                    edgeql_expr = "'" + str(deflt).replace("'", "''") + "'"
                    target_type = expr.link_class.target.name
                    edgeql_expr = 'CAST ({} AS [{}])'.format(edgeql_expr,
                                                             target_type)

                anchors = {'self': expr.source.concept}
                self._rewrite_with_edgeql_expr(expr, edgeql_expr, anchors)

    def _rewrite_with_edgeql_expr(self, expr, edgeql_expr, anchors):
        from edgedb.lang import edgeql

        schema = self._context.current.schema
        ir = edgeql.compile_fragment_to_ir(
            edgeql_expr, schema, anchors=anchors)

        node = expr.source
        rewrite_target = expr.target

        path_id = irutils.LinearPath([node.concept])
        nodes = ast.find_children(
            ir, lambda n: isinstance(n, irast.EntitySet) and n.id == path_id)

        for expr_node in nodes:
            expr_node.reference = node

        ptrname = expr.link_class.normal_name()

        expr_ref = irast.SubgraphRef(
            name=ptrname,
            force_inline=True,
            rlink=expr,
            is_rewrite_product=True,
            rewrite_original=rewrite_target)

        self._context.current.graph.replace_refs(
            [rewrite_target], expr_ref, deep=True)

        if not isinstance(ir, irast.GraphExpr):
            ir = irast.GraphExpr(selector=[irast.SelectorExpr(
                expr=ir, name=ptrname)])

        ir.referrers.append('exists')
        ir.referrers.append('generator')

        self._context.current.graph.subgraphs.add(ir)
        expr_ref.ref = ir
