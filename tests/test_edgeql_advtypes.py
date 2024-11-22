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
                ),
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT Z {stw0: {name}} FILTER .name = 'z0';
            ''',
            [{
                'stw0': [{'name': 'v0'}],
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
            INSERT XBa {ba := 'xba0'};
            INSERT XBa {ba := 'xba1'};
            INSERT XBb {bb := 90};
            INSERT XBb {bb := 91};
            INSERT XBc {bc := 90.5};
            INSERT XBc {bc := 90.5};
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
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
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

    async def test_edgeql_advtypes_complex_intersection_01(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Ba[IS Bb | Bc] {
                tn := .__type__.name,
                ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            ORDER BY .ba;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_02(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Ba[IS Bb & Bc] {
                tn := .__type__.name,
                ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            ORDER BY .ba;
            """,
            [
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_03(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Ba[IS CBa | Bb & Bc] {
                tn := .__type__.name,
                ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            ORDER BY .ba;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_04(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT {CBa, Ba[IS Bb & Bc]} {
                tn := .__type__.name,
                ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            ORDER BY .ba;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_05(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Ba[IS CBaBc | Bb][is Bc] {
                tn := .__type__.name,
                ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            ORDER BY .ba;
            """,
            [
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_06(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Ba[IS (CBaBc | Bb) & Bc] {
                tn := .__type__.name,
                ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            ORDER BY .ba;
            """,
            [
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_07(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Object[IS (Ba | Bb)][IS (Ba | Bc)] {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            ORDER BY
                .ba EMPTY LAST THEN
                .bb EMPTY LAST THEN
                .bc EMPTY LAST;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_08(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Object[IS (Ba | Bb) & (Ba | Bc)] {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            ORDER BY
                .ba EMPTY LAST THEN
                .bb EMPTY LAST THEN
                .bc EMPTY LAST;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_09(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Object[IS (Ba | Bb) | (Ba | Bc)] {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            ORDER BY
                .ba EMPTY LAST THEN
                .bb EMPTY LAST THEN
                .bc EMPTY LAST;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_10(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Object[IS (Ba & Bb) | (Ba & Bc)] {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            ORDER BY
                .ba EMPTY LAST THEN
                .bb EMPTY LAST THEN
                .bc EMPTY LAST;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_11(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT {Object[IS Ba & Bb], Object[IS Ba & Bc]} {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            ORDER BY
                .ba EMPTY LAST THEN
                .bb EMPTY LAST THEN
                .bc EMPTY LAST;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_12(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT {Ba, XBa}[is Bb | XBa] {
                tn := .__type__.name,
                ba,
            }
            ORDER BY .ba;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2'},
                {'tn': 'default::CBaBb', 'ba': 'cba3'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9'},
                {'tn': 'default::XBa', 'ba': 'xba0'},
                {'tn': 'default::XBa', 'ba': 'xba1'},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_13(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT {Ba[is Bb], XBa} {
                tn := .__type__.name,
                ba,
            }
            ORDER BY .ba;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2'},
                {'tn': 'default::CBaBb', 'ba': 'cba3'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9'},
                {'tn': 'default::XBa', 'ba': 'xba0'},
                {'tn': 'default::XBa', 'ba': 'xba1'},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_14(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Object[is (Ba & Bb) | XBa | XBb] {
                tn := .__type__.name,
                [is Ba | XBa].ba,
                [is Bb | XBb].bb,
            }
            ORDER BY
                .ba EMPTY LAST THEN
                .bb EMPTY LAST;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9},
                {'tn': 'default::XBa', 'ba': 'xba0', 'bb': None},
                {'tn': 'default::XBa', 'ba': 'xba1', 'bb': None},
                {'tn': 'default::XBb', 'ba': None, 'bb': 90},
                {'tn': 'default::XBb', 'ba': None, 'bb': 91},
            ],
        )

    async def test_edgeql_advtypes_complex_intersection_15(self):
        await self.con.execute("""
            INSERT A { name := 'a1' };
            INSERT A { name := 'a2' };
            INSERT A { name := 'a3' };
            INSERT S { name := 'sss', s := 's', l_a := (select A) };
            INSERT T { name := 'ttt', t := 't', l_a := (select A) };
            INSERT V {
                name := 'vvv',
                s := 'u',
                t := 'u',
                u := 'u',
                l_a := (select A)
            };
        """)

        await self.assert_query_result(
            r"""
            SELECT A.<l_a[is S | T] { name } ORDER BY .name;
            """,
            [{'name': 'sss'}, {'name': 'ttt'}, {'name': 'vvv'}],
        )

        await self.assert_query_result(
            r"""
            SELECT A.<l_a[is S & T] { name } ORDER BY .name;
            """,
            [{'name': 'vvv'}],
        )

    async def test_edgeql_advtypes_complex_intersection_16(self):
        # Testing finding path var for type intersections (#7656)
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT {CBa, Ba[is Bb]} {
                tn := .__type__.name,
                [IS Ba].ba,
            }
            ORDER BY .ba EMPTY LAST;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0'},
                {'tn': 'default::CBa', 'ba': 'cba1'},
                {'tn': 'default::CBaBb', 'ba': 'cba2'},
                {'tn': 'default::CBaBb', 'ba': 'cba3'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9'},
            ],
        )

        await self.assert_query_result(
            r"""
            SELECT {CBa, Bb[is Ba]} {
                tn := .__type__.name,
                [IS Ba].ba,
            }
            ORDER BY .ba EMPTY LAST;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0'},
                {'tn': 'default::CBa', 'ba': 'cba1'},
                {'tn': 'default::CBaBb', 'ba': 'cba2'},
                {'tn': 'default::CBaBb', 'ba': 'cba3'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9'},
            ],
        )

        await self.assert_query_result(
            r"""
            SELECT {Bb[is Ba], Bb[is Ba & Bc | CBaBb]} {
                tn := .__type__.name,
                [IS Ba].ba,
            }
            ORDER BY .ba EMPTY LAST;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2'},
                {'tn': 'default::CBaBb', 'ba': 'cba2'},
                {'tn': 'default::CBaBb', 'ba': 'cba3'},
                {'tn': 'default::CBaBb', 'ba': 'cba3'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9'},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9'},
            ],
        )

    async def test_edgeql_advtypes_complex_polymorphism_01(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Ba {
                tn := .__type__.name,
                ba,
                [is Bb & Bc].bb,
                [is (CBaBc | Bb) & Bc].bc,
            }
            ORDER BY .ba;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9', 'bb': 9, 'bc': 9.5},
            ],
        )

    async def test_edgeql_advtypes_complex_polymorphism_02(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Bb {
                tn := .__type__.name,
                bb,
                ua := [IS Ba | Bc].bb,
                ia := [IS Ba & Bc].bb,
            }
            ORDER BY .bb;
            """,
            [
                {'tn': 'default::CBb', 'bb': 0, 'ua': None, 'ia': None},
                {'tn': 'default::CBb', 'bb': 1, 'ua': None, 'ia': None},
                {'tn': 'default::CBaBb', 'bb': 2, 'ua': 2, 'ia': None},
                {'tn': 'default::CBaBb', 'bb': 3, 'ua': 3, 'ia': None},
                {'tn': 'default::CBbBc', 'bb': 6, 'ua': 6, 'ia': None},
                {'tn': 'default::CBbBc', 'bb': 7, 'ua': 7, 'ia': None},
                {'tn': 'default::CBaBbBc', 'bb': 8, 'ua': 8, 'ia': 8},
                {'tn': 'default::CBaBbBc', 'bb': 9, 'ua': 9, 'ia': 9},
            ],
        )

    async def test_edgeql_advtypes_complex_type_checking_01(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Object[IS Ba | Bb | Bc] {
                tn := .__type__.name,
                a := Object IS Ba,
                b := Object IS Bb,
                c := Object IS Bc,
            }
            ORDER BY .tn;
            """,
            [
                {'tn': 'default::CBa', 'a': True, 'b': False, 'c': False},
                {'tn': 'default::CBa', 'a': True, 'b': False, 'c': False},
                {'tn': 'default::CBaBb', 'a': True, 'b': True, 'c': False},
                {'tn': 'default::CBaBb', 'a': True, 'b': True, 'c': False},
                {'tn': 'default::CBaBbBc', 'a': True, 'b': True, 'c': True},
                {'tn': 'default::CBaBbBc', 'a': True, 'b': True, 'c': True},
                {'tn': 'default::CBaBc', 'a': True, 'b': False, 'c': True},
                {'tn': 'default::CBaBc', 'a': True, 'b': False, 'c': True},
                {'tn': 'default::CBb', 'a': False, 'b': True, 'c': False},
                {'tn': 'default::CBb', 'a': False, 'b': True, 'c': False},
                {'tn': 'default::CBbBc', 'a': False, 'b': True, 'c': True},
                {'tn': 'default::CBbBc', 'a': False, 'b': True, 'c': True},
                {'tn': 'default::CBc', 'a': False, 'b': False, 'c': True},
                {'tn': 'default::CBc', 'a': False, 'b': False, 'c': True},
            ],
        )

    async def test_edgeql_advtypes_complex_type_checking_02(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Object[IS Ba | Bb | Bc] {
                tn := .__type__.name,
                ab := Object IS (Ba | Bb),
                ac := Object IS (Ba | Bc),
                bc := Object IS (Bb | Bc),
            }
            ORDER BY .tn;
            """,
            [
                {'tn': 'default::CBa', 'ab': True, 'ac': True, 'bc': False},
                {'tn': 'default::CBa', 'ab': True, 'ac': True, 'bc': False},
                {'tn': 'default::CBaBb', 'ab': True, 'ac': True, 'bc': True},
                {'tn': 'default::CBaBb', 'ab': True, 'ac': True, 'bc': True},
                {'tn': 'default::CBaBbBc', 'ab': True, 'ac': True, 'bc': True},
                {'tn': 'default::CBaBbBc', 'ab': True, 'ac': True, 'bc': True},
                {'tn': 'default::CBaBc', 'ab': True, 'ac': True, 'bc': True},
                {'tn': 'default::CBaBc', 'ab': True, 'ac': True, 'bc': True},
                {'tn': 'default::CBb', 'ab': True, 'ac': False, 'bc': True},
                {'tn': 'default::CBb', 'ab': True, 'ac': False, 'bc': True},
                {'tn': 'default::CBbBc', 'ab': True, 'ac': True, 'bc': True},
                {'tn': 'default::CBbBc', 'ab': True, 'ac': True, 'bc': True},
                {'tn': 'default::CBc', 'ab': False, 'ac': True, 'bc': True},
                {'tn': 'default::CBc', 'ab': False, 'ac': True, 'bc': True},
            ],
        )

    async def test_edgeql_advtypes_complex_type_checking_03(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Object[IS Ba | Bb | Bc] {
                tn := .__type__.name,
                ab := Object IS (Ba & Bb),
                ac := Object IS (Ba & Bc),
                bc := Object IS (Bb & Bc),
            }
            ORDER BY .tn;
            """,
            [
                {'tn': 'default::CBa', 'ab': False, 'ac': False, 'bc': False},
                {'tn': 'default::CBa', 'ab': False, 'ac': False, 'bc': False},
                {'tn': 'default::CBaBb', 'ab': True, 'ac': False, 'bc': False},
                {'tn': 'default::CBaBb', 'ab': True, 'ac': False, 'bc': False},
                {'tn': 'default::CBaBbBc', 'ab': True, 'ac': True, 'bc': True},
                {'tn': 'default::CBaBbBc', 'ab': True, 'ac': True, 'bc': True},
                {'tn': 'default::CBaBc', 'ab': False, 'ac': True, 'bc': False},
                {'tn': 'default::CBaBc', 'ab': False, 'ac': True, 'bc': False},
                {'tn': 'default::CBb', 'ab': False, 'ac': False, 'bc': False},
                {'tn': 'default::CBb', 'ab': False, 'ac': False, 'bc': False},
                {'tn': 'default::CBbBc', 'ab': False, 'ac': False, 'bc': True},
                {'tn': 'default::CBbBc', 'ab': False, 'ac': False, 'bc': True},
                {'tn': 'default::CBc', 'ab': False, 'ac': False, 'bc': False},
                {'tn': 'default::CBc', 'ab': False, 'ac': False, 'bc': False},
            ],
        )

    async def test_edgeql_advtypes_complex_type_checking_04(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            SELECT Object[IS Ba | Bb | Bc] {
                tn := .__type__.name,
                u := Object IS (Ba | Bb | Bc),
                i := Object IS (Ba & Bb & Bc),
            }
            ORDER BY .tn;
            """,
            [
                {'tn': 'default::CBa', 'u': True, 'i': False},
                {'tn': 'default::CBa', 'u': True, 'i': False},
                {'tn': 'default::CBaBb', 'u': True, 'i': False},
                {'tn': 'default::CBaBb', 'u': True, 'i': False},
                {'tn': 'default::CBaBbBc', 'u': True, 'i': True},
                {'tn': 'default::CBaBbBc', 'u': True, 'i': True},
                {'tn': 'default::CBaBc', 'u': True, 'i': False},
                {'tn': 'default::CBaBc', 'u': True, 'i': False},
                {'tn': 'default::CBb', 'u': True, 'i': False},
                {'tn': 'default::CBb', 'u': True, 'i': False},
                {'tn': 'default::CBbBc', 'u': True, 'i': False},
                {'tn': 'default::CBbBc', 'u': True, 'i': False},
                {'tn': 'default::CBc', 'u': True, 'i': False},
                {'tn': 'default::CBc', 'u': True, 'i': False},
            ],
        )

    async def test_edgeql_advtypes_union_narrowing_supertype(self):
        await self.con.execute("""
            INSERT S { name := 'sss', s := 'sss' };
            INSERT T { name := 'ttt', t := 'ttt' };
            INSERT W { name := 'www' };
            INSERT Z {
                name := 'zzz',
                stw0 := {S, T, W},
            };
        """)

        await self.assert_query_result(
            r"""
            WITH My_Z := (SELECT Z FILTER .name = 'zzz')
            SELECT _ := My_Z.stw0[IS R].name
            ORDER BY _
            """,
            [
                'sss',
                'ttt',
            ]
        )

    async def test_edgeql_advtypes_union_narrowing_subtype(self):
        await self.con.execute("""
            INSERT S { name := 'sss', s := 'sss' };
            INSERT T { name := 'ttt', t := 'ttt' };
            INSERT W { name := 'www' };
            INSERT X { name := 'xxx', u := 'xxx_uuu' };
            INSERT Z {
                name := 'zzz',
                stw0 := {S, T, W},
            };
        """)

        await self.assert_query_result(
            r"""
            WITH My_Z := (SELECT Z FILTER .name = 'zzz')
            SELECT _ := My_Z.stw0[IS X].name
            ORDER BY _
            """,
            [
                'xxx',
            ]
        )

    async def test_edgeql_advtypes_union_opaque_narrowing_subtype(self):
        await self.con.execute("""
            INSERT W { name := 'www' };
            INSERT X {
                name := 'xxx',
                u := 'xxx_uuu',
                w := (SELECT DETACHED W LIMIT 1),
            };
            INSERT W {
                name := 'www-2',
                w := (SELECT (DETACHED W) FILTER .name = 'www'),
            };
        """)

        await self.assert_query_result(
            r"""
            SELECT W {
                w_of := .<w[IS X] {
                    name
                }
            }
            FILTER .name = 'www'
            """,
            [{
                'w_of': [{
                    'name': 'xxx',
                }],
            }]
        )

        await self.assert_query_result(
            r"""
            SELECT W {
                w_of := .<w[IS U] {
                    u
                }
            }
            FILTER .name = 'www'
            """,
            [{
                'w_of': [{
                    'u': 'xxx_uuu',
                }],
            }]
        )

    async def test_edgeql_advtypes_union_opaque_narrowing_nop(self):
        await self.con.execute("""
            INSERT A { name := 'aaa' };
            INSERT S { name := 'sss', s := 'sss', l_a := A };
        """)

        await self.assert_query_result(
            'SELECT A.<l_a[IS R].name',
            ['sss'],
        )

    async def test_edgeql_advtypes_intersection_with_comp(self):
        await self.con.execute("""
            INSERT A { name := 'aaa' };
        """)

        await self.assert_query_result(
            """
            WITH Rc := R
            SELECT Rc[IS A].name
            """,
            ['aaa'],
        )

    async def test_edgeql_advtypes_intersection_alias(self):
        await self.con.execute("""
            INSERT S { name := 'aaa', s := '' };
            INSERT Z { name := 'lol', stw0 := S };
        """)

        await self.assert_query_result(
            """
            WITH X := Z.stw0
            SELECT X { name }
            """,
            [{'name': 'aaa'}],
        )

    async def test_edgeql_advtypes_intersection_semijoin_01(self):
        await self.con.execute("""
            insert V {
                name := "x", s := "!", t := "!", u := '...',
                l_a := (insert A { name := "test" })
            };
        """)

        await self.assert_query_result(
            """
            select S[is T].l_a { name }
            """,
            [{"name": "test"}]
        )

        await self.assert_query_result(
            """
            select S[is T] { l_a: {name} }
            """,
            [{"l_a": [{"name": "test"}]}]
        )

    async def test_edgeql_advtypes_update_complex_type_01(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            with
                temp := (
                    update Ba[is Bb] set {
                        ba := .ba ++ '!',
                        bb := .bb + 1,
                    }
                )
            select temp {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2!', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3!', 'bb': 4, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 9, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 10, 'bc': 9.5},
            ],
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2!', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3!', 'bb': 4, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 9, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 10, 'bc': 9.5},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_update_complex_type_02(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            with
                temp := (
                    update Ba[is Bb][is Bc] set {
                        ba := .ba ++ '!',
                        bb := .bb + 1,
                        bc := .bc + 0.1,
                    }
                )
            select temp {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 9, 'bc': 8.6},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 10, 'bc': 9.6},
            ],
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 9, 'bc': 8.6},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 10, 'bc': 9.6},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_update_complex_type_03(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            with
                temp := (
                    update Ba[is Bb | Bc] set {
                        ba := .ba ++ '!',
                    }
                )
            select temp {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2!', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3!', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBaBc', 'ba': 'cba4!', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5!', 'bb': None, 'bc': 5.5},
            ],
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2!', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3!', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBaBc', 'ba': 'cba4!', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5!', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_update_complex_type_04(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            with
                temp := (
                    update Ba[is Bb & Bc] set {
                        ba := .ba ++ '!',
                        bb := .bb + 1,
                        bc := .bc + 0.1,
                    }
                )
            select temp {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 9, 'bc': 8.6},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 10, 'bc': 9.6},
            ],
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 9, 'bc': 8.6},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 10, 'bc': 9.6},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_update_complex_type_05(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            with
                temp := (
                    update Ba[IS CBa | Bb & Bc] set {
                        ba := .ba ++ '!',
                    }
                )
            select temp {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0!', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1!', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 9, 'bc': 9.5},
            ],
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0!', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1!', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_update_complex_type_06(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            with
                temp := (
                    update {CBa, Ba[IS Bb & Bc]} set {
                        ba := .ba ++ '!',
                    }
                )
            select temp {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0!', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1!', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 9, 'bc': 9.5},
            ],
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0!', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1!', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_update_complex_type_07(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            with
                temp := (
                    update Object[IS (Ba & Bb) | (Ba & Bc)] set {
                        ba := .ba ++ '!',
                    }
                )
            select temp {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2!', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3!', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBaBc', 'ba': 'cba4!', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5!', 'bb': None, 'bc': 5.5},
            ],
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2!', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3!', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBaBc', 'ba': 'cba4!', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5!', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_update_complex_type_08(self):
        await self._setup_basic_data()
        await self.assert_query_result(
            r"""
            with
                temp := (
                    update {Object[IS Ba & Bb], Object[IS Ba & Bc]} set {
                        ba := .ba ++ '!',
                    }
                )
            select temp {
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2!', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3!', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBaBc', 'ba': 'cba4!', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5!', 'bb': None, 'bc': 5.5},
            ],
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2!', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3!', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBbBc', 'ba': 'cba8!', 'bb': 8, 'bc': 8.5},
                {'tn': 'default::CBaBbBc', 'ba': 'cba9!', 'bb': 9, 'bc': 9.5},
                {'tn': 'default::CBaBc', 'ba': 'cba4!', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5!', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_delete_complex_type_01(self):
        await self._setup_basic_data()
        await self.con.execute(
            r"""
            delete Ba[is Bb];
            """
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_delete_complex_type_02(self):
        await self._setup_basic_data()
        await self.con.execute(
            r"""
            delete Ba[is Bb][is Bc];
            """
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_delete_complex_type_03(self):
        await self._setup_basic_data()
        await self.con.execute(
            r"""
            delete Ba[is Bb | Bc];
            """
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_delete_complex_type_04(self):
        await self._setup_basic_data()
        await self.con.execute(
            r"""
            delete Ba[is Bb & Bc];
            """
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_delete_complex_type_05(self):
        await self._setup_basic_data()
        await self.con.execute(
            r"""
            delete Ba[IS CBa | Bb & Bc];
            """
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_delete_complex_type_06(self):
        await self._setup_basic_data()
        await self.con.execute(
            r"""
            delete {CBa, Ba[IS Bb & Bc]};
            """
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBaBb', 'ba': 'cba2', 'bb': 2, 'bc': None},
                {'tn': 'default::CBaBb', 'ba': 'cba3', 'bb': 3, 'bc': None},
                {'tn': 'default::CBaBc', 'ba': 'cba4', 'bb': None, 'bc': 4.5},
                {'tn': 'default::CBaBc', 'ba': 'cba5', 'bb': None, 'bc': 5.5},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_delete_complex_type_07(self):
        await self._setup_basic_data()
        await self.con.execute(
            r"""
            delete Object[IS (Ba & Bb) | (Ba & Bc)];
            """
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_delete_complex_type_08(self):
        await self._setup_basic_data()
        await self.con.execute(
            r"""
            delete {Object[IS Ba & Bb], Object[IS Ba & Bc]};
            """
        )

        # Ensure the rest of the data is unchanged
        await self.assert_query_result(
            r"""
            select (DISTINCT {Ba, Bb, Bc}){
                tn := .__type__.name,
                [IS Ba].ba,
                [IS Bb].bb,
                [IS Bc].bc,
            }
            order by .tn then .ba then .bb then .bc;
            """,
            [
                {'tn': 'default::CBa', 'ba': 'cba0', 'bb': None, 'bc': None},
                {'tn': 'default::CBa', 'ba': 'cba1', 'bb': None, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 0, 'bc': None},
                {'tn': 'default::CBb', 'ba': None, 'bb': 1, 'bc': None},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 6, 'bc': 6.5},
                {'tn': 'default::CBbBc', 'ba': None, 'bb': 7, 'bc': 7.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 0.5},
                {'tn': 'default::CBc', 'ba': None, 'bb': None, 'bc': 1.5},
            ],
        )

    async def test_edgeql_advtypes_intersection_pointers_01(self):
        # Type intersections with incompatible pointers should produce errors.

        type_roots = [
            "SoloNonCompSingle",
            "SoloNonCompMulti",
            "SoloCompSingle",
            "SoloCompMulti",
            "DerivedNonCompSingle",
            "DerivedNonCompMulti",
            "DerivedCompSingle",
            "DerivedCompMulti",
        ]

        for type_root_a in type_roots:
            for type_root_b in type_roots:
                for type_suffix, ptr_name in (
                    ("Prop", "numbers"),
                    ("Link", "siblings"),
                ):
                    if (
                        # Either type has computed pointer
                        (
                            "NonComp" not in type_root_a
                            or "NonComp" not in type_root_b
                        )
                        # but the pointer doesn't come from a common base
                        and not (
                            "Derived" in type_root_a
                            and type_root_a == type_root_b
                        )
                    ):
                        async with self.assertRaisesRegexTx(
                            edgedb.SchemaError,
                            r"it is illegal to create a type intersection "
                            r"that causes a computed .* to mix "
                            r"with other versions of the same .*"
                        ):
                            await self.con.execute(f"""
                                select {type_root_a}{type_suffix}A {{
                                    x := (
                                        [is {type_root_b}{type_suffix}B]
                                        .{ptr_name}
                                    )
                                }};
                            """)

                    elif (
                        # differing pointer cardinalities
                        ("Single" in type_root_a) != ("Single" in type_root_b)
                    ):
                        async with self.assertRaisesRegexTx(
                            edgedb.SchemaError,
                            r"it is illegal to create a type intersection "
                            r"that causes a .* to mix "
                            r"with other versions of .* "
                            r"which have a different cardinality"
                        ):
                            await self.con.execute(f"""
                                select {type_root_a}{type_suffix}A {{
                                    x := (
                                        [is {type_root_b}{type_suffix}B]
                                        .{ptr_name}
                                    )
                                }};
                            """)

                    else:
                        await self.con.execute(f"""
                            select {type_root_a}{type_suffix}A {{
                                x := (
                                    [is {type_root_b}{type_suffix}B]
                                    .{ptr_name}
                                )
                            }};
                        """)

    async def test_edgeql_advtypes_intersection_pointers_02(self):
        # Intersection pointer should return nothing if they types are
        # unrelated.

        await self.con.execute("""
            INSERT SoloOriginA { dest := (INSERT Destination{ name := "A" }) };
            INSERT SoloOriginB { dest := (INSERT Destination{ name := "B" }) };
        """)

        await self.assert_query_result(
            r"""
            SELECT SoloOriginA {
                x := [is SoloOriginB].dest.name
            }
            """,
            [{'x': None}],
        )

    async def test_edgeql_advtypes_intersection_pointers_03(self):
        # Intersection pointer should return the correct values if the type
        # intersection is not empty.

        await self.con.execute("""
            INSERT BaseOriginA { dest := (INSERT Destination{ name := "A" }) };
            INSERT BaseOriginB { dest := (INSERT Destination{ name := "B" }) };
            INSERT DerivedOriginC {
                dest := (INSERT Destination{ name := "C" })
            };
        """)

        await self.assert_query_result(
            r"""
            SELECT BaseOriginA {
                x := [is BaseOriginB].dest.name
            }
            ORDER BY .x
            """,
            [{'x': None}, {'x': 'C'}],
        )
