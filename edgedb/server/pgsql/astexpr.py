##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos import proto

from metamagic.utils.functional import adapter

from metamagic.utils import ast
from metamagic.utils.ast import match as astmatch
from . import ast as pgast
from  . import astmatch as pgastmatch


class ConstantExpr:
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            self.pattern = astmatch.Or(
                               astmatch.group('value', pgastmatch.ConstantNode()),
                               pgastmatch.TypeCastNode(
                                   expr=astmatch.group('value', pgastmatch.ConstantNode())
                               )
                           )
        return self.pattern

    def match(self, tree):
        m = astmatch.match(self.get_pattern(), tree)
        if m:
            return m.value[0].node.value
        else:
            return None



class TextSearchExpr:
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            tscol = astmatch.group('column', pgastmatch.FunctionCallNode(
                        name='setweight',
                        args=[
                            pgastmatch.FunctionCallNode(
                                name='to_tsvector',
                                args=[
                                    astmatch.group('language', pgastmatch.ConstantNode()),

                                    astmatch.Or(
                                        pgastmatch.FunctionCallNode(
                                            name='coalesce',
                                            args=[
                                                astmatch.Or(
                                                    astmatch.group('column_name',
                                                        pgastmatch.FieldRefNode()),

                                                    pgastmatch.TypeCastNode(
                                                        expr=astmatch.group('column_name',
                                                                pgastmatch.FieldRefNode())
                                                    )
                                                ),

                                                pgastmatch.ConstantNode()
                                            ]
                                        ),

                                        pgastmatch.TypeCastNode(
                                            expr=pgastmatch.FunctionCallNode(
                                                name='coalesce',
                                                args=[
                                                    astmatch.group('column_name',
                                                                   pgastmatch.FieldRefNode()),
                                                    pgastmatch.ConstantNode()
                                                ]
                                            )
                                       )
                                   )
                               ]
                           ),
                           astmatch.group('weight', pgastmatch.ConstantNode())
                        ]
                    ))

            binop1 = pgastmatch.BinOpNode(
                        op='||',
                        left=tscol,
                        right=tscol
                     )

            binop1.left = astmatch.Or(tscol, binop1)

            self.pattern = astmatch.Or(tscol, binop1)

        return self.pattern

    def match(self, tree):
        m = astmatch.match(self.get_pattern(), tree)
        if m:
            result = {}
            for col in m.column:
                field_name = col.column_name[0].node.field
                language = col.language[0].node.value
                weight = col.weight[0].node.value

                result[field_name] = (language, weight)

            return result
        else:
            return None


class TypeExpr:
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            self.pattern = pgastmatch.TypeCastNode(
                                type=astmatch.group('value', pgastmatch.TypeNode()))
        return self.pattern

    def match(self, tree):
        m = astmatch.match(self.get_pattern(), tree)
        if m:
            if m.value[0].node.typmods:
                typmods = []

                for tm in m.value[0].node.typmods:
                    if isinstance(tm, pgast.FieldRefNode):
                        typmods.append(tm.field)
                    else:
                        typmods.append(tm.value)
            else:
                typmods = None
            typname = m.value[0].node.name
            if isinstance(typname, list):
                typname = tuple(typname)
            return (typname, typmods)
        else:
            return None


class AtomConstraintAdapterMeta(type(proto.AtomConstraint), adapter.Adapter):
    pass


class AtomConstraintExpr(metaclass=AtomConstraintAdapterMeta):
    pass


class AtomConstraintLengthExpr(AtomConstraintExpr, adapts=proto.AtomConstraintLength):
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            self.pattern = pgastmatch.BinOpNode(
                left=pgastmatch.FunctionCallNode(
                    args=[
                        astmatch.Or(
                            pgastmatch.FieldRefNode(),
                            pgastmatch.TypeCastNode(
                                expr=pgastmatch.FieldRefNode()
                            )
                        )
                    ],
                    name='length'
                ),
                right=astmatch.group('value', pgastmatch.ConstantNode())
            )

        return self.pattern

    def match(self, tree):
        m = astmatch.match(self.get_pattern(), tree)
        if m:
            return m.value[0].node.value
        else:
            return None


class AtomConstraintComparisonExpr(AtomConstraintExpr, adapts=proto.AtomConstraintComparison):
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            self.pattern = pgastmatch.BinOpNode(
                left=astmatch.Or(
                        pgastmatch.FieldRefNode(),
                        pgastmatch.TypeCastNode(
                            expr=pgastmatch.FieldRefNode()
                        )
                     ),
                right=astmatch.Or(
                          astmatch.group('value', astmatch.Or(
                            pgastmatch.ConstantNode(),
                            pgastmatch.UnaryOpNode(
                                operand=pgastmatch.ConstantNode()
                            )
                          )),
                          pgastmatch.TypeCastNode(
                            expr=astmatch.group('value', astmatch.Or(
                                    pgastmatch.ConstantNode(),
                                    pgastmatch.UnaryOpNode(
                                        operand=pgastmatch.ConstantNode()
                                    )
                                 ))
                          )
                      )
            )

        return self.pattern

    def match(self, tree):
        m = astmatch.match(self.get_pattern(), tree)
        if m:
            node = m.value[0].node
            if isinstance(node, pgast.UnaryOpNode):
                if node.op == ast.ops.UMINUS:
                    value = -node.operand.value
                else:
                    value = node.operand.value
            else:
                value = node.value

            return value
        else:
            return None


class AtomConstraintRegExpExpr(AtomConstraintExpr, adapts=proto.AtomConstraintRegExp):
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            match = pgastmatch.BinOpNode(
                op='~',
                left=astmatch.Or(
                        pgastmatch.FieldRefNode(),
                        pgastmatch.TypeCastNode(
                            expr=pgastmatch.FieldRefNode()
                        )
                    ),
                right=astmatch.group('regexp', pgastmatch.ConstantNode())
            )


            binop1 = pgastmatch.BinOpNode(
                        op=ast.ops.AND,
                        left=match,
                        right=match
                     )

            binop2 = pgastmatch.BinOpNode(
                        op=ast.ops.AND,
                        left=match,
                        right=binop1
                     )


            self.pattern = astmatch.Or(match, binop1, binop2)

        return self.pattern

    def match(self, tree):
        m = astmatch.match(self.get_pattern(), tree)
        if m:
            return [i.node.value for i in m.regexp]
        else:
            return None
