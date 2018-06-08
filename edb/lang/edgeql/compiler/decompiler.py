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


from edb.lang.common import ast

from edb.lang.schema import links as s_links
from edb.lang.schema import objtypes as s_objtypes

from edb.lang.ir import ast as irast
from edb.lang.edgeql import ast as qlast


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

    def visit_Statement(self, node):
        return self.visit(node.expr)

    def visit_SelectStmt(self, node):
        result = qlast.SelectQuery()

        if node.where is not None:
            result.where = self.visit(node.where)

        if node.orderby:
            result.orderby = self.visit(node.orderby)

        if node.offset is not None:
            result.offset = self.visit(node.offset)

        if node.limit is not None:
            result.limit = self.visit(node.limit)

        if node.result is not None:
            result.result = self.visit(node.result)

        return result

    def visit_Set(self, node):
        if node.expr is not None:
            result = self.visit(node.expr)
        else:
            links = []

            while node.rptr and (not node.show_as_anchor or
                                 self.context.inline_anchors):
                rptr = node.rptr
                ptrcls = rptr.ptrcls
                pname = ptrcls.shortname

                if isinstance(rptr.target.scls, s_objtypes.ObjectType):
                    target = rptr.target.scls.shortname
                    target = qlast.ObjectRef(
                        name=target.name,
                        module=target.module)
                else:
                    target = None

                link = qlast.Ptr(
                    ptr=qlast.ObjectRef(
                        name=pname.name,
                    ),
                    direction=rptr.direction,
                    target=target)
                if isinstance(ptrcls.source, s_links.Link):
                    link.type = 'property'
                links.append(link)

                node = node.rptr.source

            result = qlast.Path()

            if node.show_as_anchor and not self.context.inline_anchors:
                if issubclass(node.show_as_anchor, qlast.Expr):
                    step = node.show_as_anchor()
                else:
                    step = qlast.ObjectRef(name=node.show_as_anchor)
            else:
                step = qlast.ObjectRef(name=node.scls.shortname.name,
                                       module=node.scls.shortname.module)

            result.steps.append(step)
            result.steps.extend(reversed(links))

        if node.shape:
            result = qlast.Shape(
                expr=result,
                elements=[]
            )

            for el in node.shape:
                rptr = el.rptr
                ptrcls = rptr.ptrcls
                pn = ptrcls.shortname

                pn = qlast.ShapeElement(
                    expr=qlast.Path(
                        steps=[
                            qlast.Ptr(
                                ptr=qlast.ObjectRef(
                                    name=pn.name
                                ),
                                direction=rptr.direction
                            )
                        ]
                    )
                )

                result.elements.append(pn)

        return result

    def visit_BinOp(self, node):
        result = qlast.BinOp()
        result.left = self.visit(node.left)

        if isinstance(node.op, ast.ops.TypeCheckOperator):
            # Trim the trailing __type__ added by the compiler
            result.left.steps = result.left.steps[:-1]

        result.right = self.visit(node.right)
        result.op = node.op
        return result

    def visit_UnaryOp(self, node):
        result = qlast.UnaryOp()
        result.operand = self.visit(node.expr)
        result.op = node.op
        return result

    def visit_DistinctOp(self, node):
        result = qlast.UnaryOp()
        result.operand = self.visit(node.expr)
        result.op = qlast.DISTINCT
        return result

    def visit_Parameter(self, node):
        return qlast.Parameter(name=node.name)

    def visit_Constant(self, node):
        return qlast.Constant(value=node.value)

    def visit_Array(self, node):
        return qlast.Array(elements=[
            self.visit(e) for e in node.elements
        ])

    def visit_Tuple(self, node):
        if node.named:
            result = qlast.NamedTuple(
                elements=[
                    qlast.TupleElement(
                        name=el.name,
                        val=self.visit(el.val)
                    )
                    for el in node.elements
                ]
            )
        else:
            result = qlast.Tuple(elements=[
                self.visit(e.val) for e in node.elements
            ])

        return result

    def visit_FunctionCall(self, node):
        # FIXME: this is a temporary solution to bridge the gap to EdgeQL
        if node.agg_set_modifier == qlast.AggDISTINCT:
            args = qlast.UnaryOp(op=qlast.DISTINCT, operand=node.args[0])
        else:
            args = node.args

        # FIXME: hack to reconstruct args for a trivial aggregate function
        args = [qlast.FuncArg(arg=arg) for arg in self.visit(args)]
        if node.agg_filter or node.agg_sort:
            args[0].sort = node.agg_sort
            args[0].filter = (self.visit(node.agg_filter)
                              if node.agg_filter is not None else None)

        result = qlast.FunctionCall(
            func=(node.func.shortname.module, node.func.shortname.name),
            args=args,
        )

        return result

    def visit_TypeCast(self, node):
        if node.type.subtypes:
            typ = qlast.TypeName(
                maintype=qlast.ObjectRef(name=node.type.maintype),
                subtypes=[
                    qlast.ObjectRef(
                        module=stn.module, name=stn.name)
                    for stn in node.type.subtypes
                ]
            )
        else:
            mtn = node.type.maintype
            mt = qlast.ObjectRef(module=mtn.module, name=mtn.name)
            typ = qlast.TypeName(maintype=mt)

        result = qlast.TypeCast(expr=self.visit(node.expr), type=typ)

        return result

    def visit_SortExpr(self, node):
        result = qlast.SortExpr(
            path=self.visit(node.expr),
            direction=node.direction,
            nones_order=node.nones_order
        )

        return result

    def visit_SliceIndirection(self, node):
        start = self.visit(node.start) if node.start is not None else None
        stop = self.visit(node.stop) if node.stop is not None else None

        result = qlast.Indirection(
            arg=self.visit(node.expr),
            indirection=[
                qlast.Slice(
                    start=(None if self._is_none(start) else start),
                    stop=(None if self._is_none(stop) else stop),
                )
            ]
        )

        return result

    def visit_IndexIndirection(self, node):
        result = qlast.Indirection(
            arg=self.visit(node.expr),
            indirection=[
                qlast.Index(
                    index=self.visit(node.index)
                )
            ]
        )

        return result

    def visit_ExistPred(self, node):
        result = qlast.ExistsPredicate(expr=self.visit(node.expr))
        return result

    def visit_TypeRef(self, node):
        # Bare TypeRef only appears as rhs of IS [NOT] and is always
        # an object type reference.
        mtn = node.maintype

        result = qlast.Path(
            steps=[qlast.ObjectRef(module=mtn.module, name=mtn.name)]
        )

        return result

    def _is_none(self, expr):
        return (
            expr is None or (
                isinstance(expr, (irast.Constant, qlast.Constant)) and
                expr.value is None
            )
        )


def decompile_ir(ir_tree, inline_anchors=False, return_statement=False):
    decompiler = IRDecompiler()
    qltree = decompiler.transform(ir_tree, inline_anchors=inline_anchors)
    if return_statement and not isinstance(qltree, qlast.Statement):
        qltree = qlast.SelectQuery(result=qltree)
    return qltree
