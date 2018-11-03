#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2013-present MagicStack Inc. and the EdgeDB authors.
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


from edb.lang.common.ast import match as astmatch

from . import astmatch as irastmatch


class DistinctConjunctionExpr:
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            # Basic std::_is_exclusive(blah) expression
            pure_distinct_expr = irastmatch.FunctionCall(
                func_shortname='std::_is_exclusive',
                args=[astmatch.group('expr', irastmatch.Base())],
            )

            possibly_wrapped_distinct_expr = irastmatch.SelectStmt(
                result=pure_distinct_expr
            )

            distinct_expr = astmatch.Or(
                pure_distinct_expr, possibly_wrapped_distinct_expr
            )

            # A logical conjunction of unique constraint expressions
            binop = irastmatch.OperatorCall(func_shortname='std::AND')

            # Set expression with the above binop
            set_expr = irastmatch.Set(
                expr=astmatch.Or(
                    distinct_expr, binop
                )
            )

            # A unique constraint expression can be either one of the
            # three above
            constr_expr = astmatch.Or(
                distinct_expr, binop, set_expr
            )

            # Populate expression alternatives to complete recursive
            # pattern definition.
            binop.args = [constr_expr, constr_expr]

            self.pattern = constr_expr

        return self.pattern

    def match(self, tree):
        m = astmatch.match(self.get_pattern(), tree)
        if m:
            return [mg.node for mg in m.expr]
        else:
            return None
