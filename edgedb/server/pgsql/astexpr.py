##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.ast import match as astmatch
from  . import astmatch as pgastmatch


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
                                                astmatch.group('column_name',
                                                               pgastmatch.FieldRefNode()),
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

            binop2 = pgastmatch.BinOpNode(
                        op='||',
                        left=tscol,
                        right=binop1
                     )


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
