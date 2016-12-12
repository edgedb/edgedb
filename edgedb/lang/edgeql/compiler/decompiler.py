##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast
from edgedb.lang.schema import lproperties as s_lprops

from edgedb.lang.ir import ast as irast
from edgedb.lang.edgeql import ast as qlast


class IRDecompilerContext:
    pass


class IRDecompiler(ast.visitor.NodeVisitor):
    def transform(self, ir_tree, inline_anchors=False):
        self.context = IRDecompilerContext()
        self.context.inline_anchors = inline_anchors

        edgeql_tree = self.visit(ir_tree)
        return edgeql_tree

    def generic_visit(self, node, *, combine_results=None):
        raise NotImplementedError(
            'no EdgeQL decompiler handler for {}'.format(node.__class__))

    def visit_SelectStmt(self, node):
        result = qlast.SelectQueryNode()

        if node.where is not None:
            result.where = self.visit(node.where)

        if node.groupby:
            result.groupby = self.visit(node.groupby)

        if node.orderby:
            result.orderby = self.visit(node.orderby)

        if node.offset is not None:
            result.offset = self.visit(node.offset)

        if node.limit is not None:
            result.limit = self.visit(node.limit)

        if node.result is not None:
            result.targets = [
                qlast.SelectExprNode(
                    expr=self.visit(node.result)
                )
            ]

        return result

    def visit_Shape(self, node):
        result = qlast.PathNode(
            steps=[qlast.PathStepNode(
                namespace=node.scls.name.module,
                expr=node.scls.name.name
            )],
            pathspec=[]
        )

        for el in node.elements:
            rptr = el.rptr
            ptrcls = rptr.ptrcls
            pn = ptrcls.shortname

            pn = qlast.SelectPathSpecNode(
                expr=qlast.PathNode(
                    steps=[
                        qlast.LinkExprNode(
                            expr=qlast.LinkNode(
                                namespace=pn.module,
                                name=pn.name,
                                direction=rptr.direction
                            )
                        )
                    ]
                )
            )

            result.pathspec.append(pn)

        return result

    def visit_Set(self, node):
        if node.expr is not None:
            return self.visit(node.expr)
        else:
            links = []

            while node.rptr and (not node.show_as_anchor or
                                 self.context.inline_anchors):
                rptr = node.rptr
                ptrcls = rptr.ptrcls
                pname = ptrcls.shortname

                target = rptr.target.scls.name
                target = qlast.ClassRefNode(
                    name=target.name, module=target.module)
                link = qlast.LinkNode(
                    name=pname.name, namespace=pname.module,
                    direction=rptr.direction, target=target)
                if isinstance(ptrcls, s_lprops.LinkProperty):
                    link.type = 'property'
                    link = qlast.LinkPropExprNode(expr=link)
                else:
                    link = qlast.LinkExprNode(expr=link)
                links.append(link)

                node = node.rptr.source

            path = qlast.PathNode()

            if node.show_as_anchor and not self.context.inline_anchors:
                step = qlast.PathStepNode(expr=node.show_as_anchor)
            else:
                step = qlast.PathStepNode(expr=node.scls.name.name,
                                          namespace=node.scls.name.module)

            path.steps.append(step)
            path.steps.extend(reversed(links))

            return path

    def visit_BinOp(self, node):
        result = qlast.BinOpNode()
        result.left = self.visit(node.left)
        result.right = self.visit(node.right)
        result.op = node.op
        return result

    def visit_UnaryOp(self, node):
        result = qlast.UnaryOpNode()
        result.operand = self.visit(node.expr)
        result.op = node.op
        return result

    def visit_Constant(self, node):
        if node.expr is not None:
            return self.visit(node.expr)
        else:
            result = qlast.ConstantNode(
                index=node.index, value=node.value
            )

            return result

    def visit_Sequence(self, node):
        result = qlast.SequenceNode(elements=[
            self.visit(e) for e in node.elements
        ])
        return result

    def visit_FunctionCall(self, node):
        result = qlast.FunctionCallNode(
            func=(node.func.name.module, node.func.name.name),
            args=self.visit(node.args)
        )

        return result

    def visit_TypeCast(self, node):
        if node.type.subtypes:
            typ = qlast.TypeNameNode(
                maintype=qlast.ClassRefNode(name=node.type.maintype),
                subtypes=[
                    qlast.ClassRefNode(
                        module=stn.module, name=stn.name)
                    for stn in node.type.subtypes
                ]
            )
        else:
            mtn = node.type.maintype
            mt = qlast.ClassRefNode(module=mtn.module, name=mtn.name)
            typ = qlast.TypeNameNode(maintype=mt)

        result = qlast.TypeCastNode(expr=self.visit(node.expr), type=typ)

        return result

    def visit_SortExpr(self, node):
        result = qlast.SortExprNode(
            path=self.visit(node.expr),
            direction=node.direction,
            nones_order=node.nones_order
        )

        return result

    def visit_SliceIndirection(self, node):
        start = self.visit(node.start) if node.start is not None else None
        stop = self.visit(node.stop) if node.stop is not None else None

        result = qlast.IndirectionNode(
            arg=self.visit(node.expr),
            indirection=[
                qlast.SliceNode(
                    start=(None if self._is_none(start) else start),
                    stop=(None if self._is_none(stop) else stop),
                )
            ]
        )

        return result

    def visit_IndexIndirection(self, node):
        result = qlast.IndirectionNode(
            arg=self.visit(node.expr),
            indirection=[
                qlast.IndexNode(
                    index=self.visit(node.index)
                )
            ]
        )

        return result

    def visit_ExistPred(self, node):
        result = qlast.ExistsPredicateNode(expr=self.visit(node.expr))
        return result

    def _is_none(self, expr):
        return (
            expr is None or (
                isinstance(expr, (irast.Constant, qlast.ConstantNode)) and
                expr.value is None and expr.index is None
            )
        )


def decompile_ir(ir_tree, inline_anchors=False, return_statement=False):
    decompiler = IRDecompiler()
    qltree = decompiler.transform(ir_tree, inline_anchors=inline_anchors)
    if return_statement and not isinstance(qltree, qlast.StatementNode):
        qltree = qlast.SelectQueryNode(targets=[
            qlast.SelectExprNode(expr=qltree)
        ])
    return qltree
