##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common.ast import match as astmatch
from . import ast as pgast
from . import astmatch as pgastmatch


class ConstantExpr:
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            self.pattern = astmatch.Or(
                astmatch.group('value', pgastmatch.ConstantNode()),
                pgastmatch.TypeCastNode(
                    expr=astmatch.group('value', pgastmatch.ConstantNode())))
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
            tscol = astmatch.group(
                'column', pgastmatch.FunctionCallNode(
                    name='setweight', args=[
                        pgastmatch.FunctionCallNode(
                            name='to_tsvector', args=[
                                astmatch.group(
                                    'language', pgastmatch.ConstantNode()),
                                astmatch.Or(
                                    pgastmatch.FunctionCallNode(
                                        name='coalesce', args=[
                                            astmatch.Or(
                                                astmatch.group(
                                                    'column_name',
                                                    pgastmatch.FieldRefNode()),
                                                pgastmatch.TypeCastNode(
                                                    expr=astmatch.group(
                                                        'column_name',
                                                        pgastmatch.
                                                        FieldRefNode()))),
                                            pgastmatch.ConstantNode()
                                        ]),
                                    pgastmatch.TypeCastNode(
                                        expr=pgastmatch.FunctionCallNode(
                                            name='coalesce', args=[
                                                astmatch.group(
                                                    'column_name',
                                                    pgastmatch.FieldRefNode()),
                                                pgastmatch.ConstantNode()
                                            ])))
                            ]),
                        astmatch.group('weight', pgastmatch.ConstantNode())
                    ]))

            binop1 = pgastmatch.BinOpNode(op='||', left=tscol, right=tscol)

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
