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

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLAdvancedTypes(tb.QueryTestCase):
    '''Test type expressions'''

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'advtypes.esdl')

    async def test_edgeql_advtypes_overlapping_union(self):
        await self.con.execute('''
            INSERT V {name:= 'v0', s := 's0', t := 't0', u := 'u0'};

            INSERT Z {
                name := 'z0',
                stw0 := (
                    SELECT V FILTER .name = 'v0'
                    LIMIT 1
                ),
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT Z {stw0: {name}} FILTER .name = 'z0';
            ''',
            [{
                'stw0': {'name': 'v0'}
            }]
        )

    async def test_edgeql_advtypes_overlapping_link_union(self):
        await self.con.execute("""
            INSERT A { name := 'a1' };
            INSERT V {
                name:= 'v1',
                s := 's1',
                t := 't1',
                u := 'u1',
                l_a := (SELECT A FILTER .name = 'a1'),
            };
        """)

        await self.assert_query_result(
            r"""
            SELECT (DISTINCT (SELECT S UNION T)) {
                cla := count(.l_a)
            }
            """,
            [{
                'cla': 1,
            }]
        )

    async def _setup_basic_data(self):
        await self.con.execute("""
            INSERT CBa {ba := 'cba0'};
            INSERT CBa {ba := 'cba1'};
            INSERT CBb {bb := 0};
            INSERT CBb {bb := 1};
            INSERT CBc {bc := 0.5};
            INSERT CBc {bc := 1.5};
            INSERT CBaBb {ba := 'cba2', bb := 2};
            INSERT CBaBb {ba := 'cba3', bb := 3};
            INSERT CBaBc {ba := 'cba4', bc := 4.5};
            INSERT CBaBc {ba := 'cba5', bc := 5.5};
            INSERT CBbBc {bb := 6, bc := 6.5};
            INSERT CBbBc {bb := 7, bc := 7.5};
            INSERT CBaBbBc {ba := 'cba8', bb := 8, bc := 8.5};
            INSERT CBaBbBc {ba := 'cba9', bb := 9, bc := 9.5};
        """)

    async def test_edgeql_advtypes_basic_union_01(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT (DISTINCT {Ba, Bb}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            } ORDER BY
                .ba EMPTY LAST THEN
                .bb EMPTY LAST THEN
                .bc EMPTY LAST;
            """,
            [
                {'ba': 'cba0', 'bb': None, 'bc': None, 'tn': 'default::CBa'},
                {'ba': 'cba1', 'bb': None, 'bc': None, 'tn': 'default::CBa'},
                {'ba': 'cba2', 'bb': 2, 'bc': None, 'tn': 'default::CBaBb'},
                {'ba': 'cba3', 'bb': 3, 'bc': None, 'tn': 'default::CBaBb'},
                {'ba': 'cba4', 'bb': None, 'bc': 4.5, 'tn': 'default::CBaBc'},
                {'ba': 'cba5', 'bb': None, 'bc': 5.5, 'tn': 'default::CBaBc'},
                {'ba': 'cba8', 'bb': 8, 'bc': 8.5, 'tn': 'default::CBaBbBc'},
                {'ba': 'cba9', 'bb': 9, 'bc': 9.5, 'tn': 'default::CBaBbBc'},
                {'ba': None, 'bb': 0, 'bc': None, 'tn': 'default::CBb'},
                {'ba': None, 'bb': 1, 'bc': None, 'tn': 'default::CBb'},
                {'ba': None, 'bb': 6, 'bc': 6.5, 'tn': 'default::CBbBc'},
                {'ba': None, 'bb': 7, 'bc': 7.5, 'tn': 'default::CBbBc'},
            ],
        )

    async def test_edgeql_advtypes_basic_union_02(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT {CBaBb, CBbBc} {
                tn := .__type__.name,
                bb,
            } ORDER BY .bb;
            """,
            [
                {'tn': 'default::CBaBb', 'bb': 2},
                {'tn': 'default::CBaBb', 'bb': 3},
                {'tn': 'default::CBbBc', 'bb': 6},
                {'tn': 'default::CBbBc', 'bb': 7},
            ],
        )

    async def test_edgeql_advtypes_basic_union_03(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT {CBaBb, CBaBbBc} {
                tn := .__type__.name,
                ba,
                bb,
            } ORDER BY .bb;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9},
            ],
        )

    async def test_edgeql_advtypes_basic_intersection_01(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Ba[IS Bb].__type__.name;
            """,
            {'default::CBaBb', 'default::CBaBbBc'},
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: column Bc~1.ba does not exist
    ''')
    async def test_edgeql_advtypes_basic_intersection_02(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Ba[IS Bb].ba;
            """,
            {'cba2', 'cba3', 'cba8', 'cba9'},
        )

    async def test_edgeql_advtypes_basic_intersection_03(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Ba[IS Bb].bb;
            """,
            {2, 3, 8, 9},
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: column Bb~1.ba does not exist
    ''')
    async def test_edgeql_advtypes_basic_intersection_04(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Ba[IS Bb][IS Bc] {
                tn := .__type__.name,
                ba,
                bb,
                bc,
            }
            ORDER BY .ba;
            """,
            [
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
            ],
        )
