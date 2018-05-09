#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
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


from edgedb.lang.common.ast import match as astmatch
from . import ast as pgast  # NOQA
from . import astmatch as pgastmatch


class ConstantExpr:
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            self.pattern = astmatch.Or(
                astmatch.group('value', pgastmatch.Constant()),
                pgastmatch.TypeCast(
                    arg=astmatch.group('value', pgastmatch.Constant())))
        return self.pattern

    def match(self, tree):
        m = astmatch.match(self.get_pattern(), tree)
        if m:
            return m.value[0].node.val
        else:
            return None


class TextSearchExpr:
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            tscol = astmatch.group(
                'column', pgastmatch.FuncCall(
                    name='setweight', args=[
                        pgastmatch.FuncCall(
                            name='to_tsvector', args=[
                                astmatch.group(
                                    'language', pgastmatch.Constant()),
                                astmatch.Or(
                                    pgastmatch.FuncCall(
                                        name='coalesce', args=[
                                            astmatch.Or(
                                                astmatch.group(
                                                    'column_name',
                                                    pgastmatch.ColumnRef()),
                                                pgastmatch.TypeCast(
                                                    arg=astmatch.group(
                                                        'column_name',
                                                        pgastmatch.
                                                        ColumnRef()))),
                                            pgastmatch.Constant()
                                        ]),
                                    pgastmatch.TypeCast(
                                        arg=pgastmatch.FuncCall(
                                            name='coalesce', args=[
                                                astmatch.group(
                                                    'column_name',
                                                    pgastmatch.ColumnRef()),
                                                pgastmatch.Constant()
                                            ])))
                            ]),
                        astmatch.group('weight', pgastmatch.Constant())
                    ]))

            binop1 = pgastmatch.Expr(name='||', larg=tscol, rarg=tscol)

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
            self.pattern = pgastmatch.TypeCast(
                type_name=astmatch.group('value', pgastmatch.TypeName()))
        return self.pattern

    def match(self, tree):
        m = astmatch.match(self.get_pattern(), tree)
        if m:
            if m.value[0].node.typmods:
                typmods = []

                for tm in m.value[0].node.typmods:
                    typmods.append(tm.value)
            else:
                typmods = None
            typname = m.value[0].node.name
            return (typname, typmods)
        else:
            return None
