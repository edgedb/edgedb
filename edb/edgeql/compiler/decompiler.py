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


from edb.common import ast

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as ft


class IRDecompilerContext:
    pass


class IRDecompiler(ast.visitor.NodeVisitor):
    def transform(self, ir_tree, inline_anchors=False, *, schema):
        self.context = IRDecompilerContext()
        self.context.inline_anchors = inline_anchors
        self.context.schema = schema

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
                ptrref = rptr.ptrref
                pname = ptrref.shortname

                if irtyputils.is_object(node.typeref):
                    if node.typeref.material_type is not None:
                        typeref = node.typeref.material_type
                    else:
                        typeref = node.typeref

                    stype = self.context.schema.get_by_id(typeref.id)
                    stype_name = stype.get_name(self.context.schema)

                    target = qlast.TypeName(
                        maintype=qlast.ObjectRef(
                            name=stype_name.name,
                            module=stype_name.module))
                else:
                    target = None

                link = qlast.Ptr(
                    ptr=qlast.ObjectRef(
                        name=pname.name,
                    ),
                    direction=rptr.direction,
                    target=target,
                )
                if ptrref.parent_ptr is not None:
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
                if node.typeref.material_type is not None:
                    typeref = node.typeref.material_type
                else:
                    typeref = node.typeref

                stype = self.context.schema.get_by_id(typeref.id)
                scls_shortname = stype.get_shortname(self.context.schema)
                step = qlast.ObjectRef(name=scls_shortname.name,
                                       module=scls_shortname.module)

            result.steps.append(step)
            result.steps.extend(reversed(links))

        if node.shape:
            result = qlast.Shape(
                expr=result,
                elements=[]
            )

            for el in node.shape:
                rptr = el.rptr
                ptrref = rptr.ptrref
                pn = ptrref.shortname

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

    def visit_TypeCheckOp(self, node):
        result = qlast.BinOp()
        result.left = self.visit(node.left)
        # Trim the trailing __type__ added by the compiler
        result.left.steps = result.left.steps[:-1]
        result.right = self.visit(node.right)
        result.op = node.op
        return result

    def visit_Parameter(self, node):
        return qlast.Parameter(name=node.name)

    def visit_StringConstant(self, node):
        return qlast.StringConstant.from_python(node.value)

    def visit_RawStringConstant(self, node):
        return qlast.RawStringConstant.from_python(node.value)

    def visit_BytesConstant(self, node):
        return qlast.BytesConstant.from_python(node.value)

    def visit_BooleanConstant(self, node):
        return qlast.BooleanConstant(value=node.value)

    def visit_FloatConstant(self, node):
        return qlast.FloatConstant(value=node.value)

    def visit_IntegerConstant(self, node):
        return qlast.IntegerConstant(value=node.value)

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
        args = node.args

        args = [qlast.FuncArg(arg=arg) for arg in self.visit(args)]

        result = qlast.FunctionCall(
            func=(node.func_shortname.module, node.func_shortname.name),
            args=args,
        )

        return result

    def visit_OperatorCall(self, node):
        args = node.args

        if node.operator_kind is ft.OperatorKind.INFIX:
            result = qlast.BinOp(
                left=self.visit(args[0]),
                right=self.visit(args[1]),
                op=node.func_shortname.name,
            )
        elif node.operator_kind is ft.OperatorKind.PREFIX:
            result = qlast.UnaryOp(
                operand=self.visit(args[0]),
                op=node.func_shortname.name,
            )
        else:
            raise RuntimeError(
                f'unexpected operator kind: {node.operator_kind}')

        return result

    def visit_TypeCast(self, node):
        if node.to_type.subtypes:
            typ = qlast.TypeName(
                maintype=qlast.ObjectRef(name=node.to_type.collection),
                subtypes=[
                    qlast.ObjectRef(
                        module=stn.module, name=stn.name)
                    for stn in node.to_type.subtypes
                ]
            )
        else:
            to_type = self.context.schema.get_by_id(node.to_type.id)
            mtn = to_type.get_name(self.context.schema)
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

    def visit_TypeRef(self, node):
        # Bare TypeRef only appears as rhs of IS [NOT] and is always
        # an object type reference.
        mtn = node.name

        result = qlast.Path(
            steps=[qlast.ObjectRef(module=mtn.module, name=mtn.name)]
        )

        return result

    def visit_IfElseExpr(self, node):
        result = qlast.IfElse()
        result.condition = self.visit(node.condition)
        result.if_expr = self.visit(node.if_expr)
        result.else_expr = self.visit(node.else_expr)
        return result

    def _is_none(self, expr):
        return (
            expr is None or (
                isinstance(expr, (irast.BaseConstant, qlast.BaseConstant)) and
                expr.value is None
            )
        )


def decompile_ir(ir_tree, inline_anchors=False, return_statement=False, *,
                 schema):
    decompiler = IRDecompiler()
    qltree = decompiler.transform(
        ir_tree, inline_anchors=inline_anchors, schema=schema)
    if return_statement and not isinstance(qltree, qlast.Statement):
        qltree = qlast.SelectQuery(result=qltree)
    return qltree
