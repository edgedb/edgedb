#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


import os.path

import edgedb

from edb.testbase import server as tb


class TestEdgeQLVolatility(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'volatility.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'volatility_setup.edgeql')

    async def test_edgeql_volatility_function_01(self):
        result = await self.con.fetchall(
            r"""
                WITH MODULE test
                SELECT Obj {
                    # immutable function should only be called once,
                    # generating the same value for all Objs
                    x := vol_immutable()
                };
            """
        )

        self.assertEqual(
            len(set(res.x for res in result)), 1,
            'more than one value for the same vol_immutable() call'
        )

    async def test_edgeql_volatility_function_02(self):
        result = await self.con.fetchall(
            r"""
                WITH MODULE test
                SELECT Obj {
                    # stable function should only be called once,
                    # generating the same value for all Objs
                    x := vol_stable()
                };
            """
        )

        self.assertEqual(
            len(set(res.x for res in result)), 1,
            'more than one value for the same vol_stable() call'
        )

    async def test_edgeql_volatility_function_03(self):
        result = await self.con.fetchall(
            r"""
                WITH MODULE test
                SELECT Obj {
                    # volatile function should be called once for each
                    # Obj, generating different values
                    x := vol_volatile()
                };
            """
        )

        self.assertNotEqual(
            len(set(res.x for res in result)), 1,
            'only one value for multiple vol_volatile() calls'
        )

    async def test_edgeql_volatility_function_04(self):
        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT Obj {
                    # this condition is true for all of the Objs, but
                    # a constant immutable function call can be
                    # factored out and called once per query
                    x := 1 IF Obj.n > 0 ELSE err_immutable()
                };
            ''')

    async def test_edgeql_volatility_function_05(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Obj {
                # this condition is true for all of the Objs and the
                # stable function call cannot be factored out
                x := 1 IF Obj.n > 0 ELSE err_stable()
            };
        ''', [
            {'x': 1},
            {'x': 1},
            {'x': 1},
        ])

    async def test_edgeql_volatility_function_06(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Obj {
                # this condition is true for all of the Objs and the
                # volatile function call cannot be factored out
                x := 1 IF Obj.n > 0 ELSE err_volatile()
            };
        ''', [
            {'x': 1},
            {'x': 1},
            {'x': 1},
        ])

    async def test_edgeql_volatility_operator_01(self):
        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT Obj {
                    # this condition is true for all of the Objs, but
                    # a constant immutable operation can be factored out
                    # and called once per query
                    x := 1 IF Obj.n > 0 ELSE (1/0)
                };
            ''')

    async def test_edgeql_volatility_cast_01(self):
        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT Obj {
                    # this condition is true for all of the Objs, but
                    # a constant immutable cast can be factored out
                    # and called once per query
                    x := 1 IF Obj.n > 0 ELSE (<int64>(<float64>1)/0)
                };
            ''')

    async def test_edgeql_volatility_cast_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT Obj {
                # this condition is true for all of the Objs and the
                # stable cast (<json>) cannot be factored out
                x := 1 IF Obj.n > 0 ELSE (<int64>(<json>1)/0)
            };
        ''', [
            {'x': 1},
            {'x': 1},
            {'x': 1},
        ])
