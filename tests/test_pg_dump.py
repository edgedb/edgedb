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


class TestPGDump01(tb.StablePGDumpTestCase):

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'pg_dump01_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'pg_dump01_setup.edgeql')

    async def test_pgdump01_dump_restore_01(self):
        eqlres = await self.con.query('select A {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "A"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump01_dump_restore_02(self):
        eqlres = await self.con.query('select B {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "B"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump01_dump_restore_03(self):
        eqlres = await self.con.query('select C {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "C"')
        self.assert_shape(sqlres, eqlres)


class TestPGDump02(tb.StablePGDumpTestCase):

    SCHEMA_TEST = os.path.join(os.path.dirname(__file__), 'schemas',
                               'dump01_test.esdl')
    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump01_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump01_setup.edgeql')

    async def test_pgdump02_dump_restore_01(self):
        eqlres = await self.con.query('select A {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "A"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_02(self):
        props = [
            'p_bool', 'p_str', 'p_datetime', 'p_local_datetime',
            'p_local_date', 'p_local_time', 'p_duration', 'p_int16',
            'p_int32', 'p_int64', 'p_float32', 'p_float64', 'p_bigint',
            'p_decimal', 'p_json', 'p_bytes',
        ]
        eqlres = await self.con.query(f'select B {{id, {", ".join(props)}}}')

        sql = f'''
            SELECT
                id,
                {", ".join(self.multi_prop_subquery("B", p) for p in props)}
            FROM "B"

        '''
        sqlres = await self.scon.fetch(sql)
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_03(self):
        eqlres = await self.con.query('select C {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "C"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_04(self):
        eqlres = await self.con.query('''
            select D {
                id,
                num,
                single_link: {*},
                multi_link: {*}
            }
        ''')

        sql = f'''
            SELECT
                id,
                num,
                {self.single_link_subquery("D", "single_link", "C")},
                {self.multi_link_subquery("D", "multi_link", "C")}
            FROM "D"

        '''
        sqlres = await self.scon.fetch(sql)
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_05(self):
        eqlres = await self.con.query('''
            select E {
                id,
                num,
                _single_link := .single_link {
                    source := @source.id,
                    lp0 := @lp0,
                    target := .id,
                },
                _multi_link := .multi_link {
                    source := @source.id,
                    lp1 := @lp1,
                    target := .id,
                },
            }
        ''')

        sql = f'''
            SELECT
                id,
                num,
                {self.single_link_subquery("E", "single_link", "C", ["lp0"])},
                {self.multi_link_subquery("E", "multi_link", "C", ["lp1"])}
            FROM "E"
        '''
        sqlres = await self.scon.fetch(sql)
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_06(self):
        eqlres = await self.con.query('''
            select F {
                id,
                num,
                single_link: {*},
                multi_link: {*}
            }
        ''')

        sql = f'''
            SELECT
                id,
                num,
                {self.single_link_subquery("F", "single_link", "C")},
                {self.multi_link_subquery("F", "multi_link", "C")}
            FROM "F"

        '''
        sqlres = await self.scon.fetch(sql)
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_07(self):
        eqlres = await self.con.query('select M {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "M"')
        self.assert_shape(sqlres, eqlres)

        eqlres = await self.con.query('select N {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "N"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_08(self):
        eqlres = await self.con.query('select O {id, o0, o1}')
        sqlres = await self.scon.fetch('SELECT * FROM "O"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_09(self):
        eqlres = await self.con.query('select P {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "P"')
        self.assert_shape(sqlres, eqlres)

        eqlres = await self.con.query('select Q {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "Q"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_10(self):
        # Inheritance
        #
        # R - S - T
        #           \
        #         U - V

        eqlres = await self.con.query('select R {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "R"')
        self.assert_shape(sqlres, eqlres)

        eqlres = await self.con.query('select S {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "S"')
        self.assert_shape(sqlres, eqlres)

        eqlres = await self.con.query('select T {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "T"')
        self.assert_shape(sqlres, eqlres)

        eqlres = await self.con.query('select U {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "U"')
        self.assert_shape(sqlres, eqlres)

        eqlres = await self.con.query('select V {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "V"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_11(self):
        eqlres = await self.con.query('''
            select W {
                id,
                name,
                w_id := .w.id,
            }
        ''')
        sqlres = await self.scon.fetch('SELECT * FROM "W"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_12(self):
        eqlres = await self.con.query('''
            select X {
                id,
                name,
                y: {*},
            }
        ''')

        sql = f'''
            SELECT
                id,
                name,
                {self.single_link_subquery("X", "y", "Y")}
            FROM "X"

        '''
        sqlres = await self.scon.fetch(sql)
        self.assert_shape(sqlres, eqlres)

        eqlres = await self.con.query('''
            select Y {
                id,
                name,
                x: {*},
            }
        ''')

        sql = f'''
            SELECT
                id,
                name,
                {self.single_link_subquery("Y", "x", "X")}
            FROM "Y"

        '''
        sqlres = await self.scon.fetch(sql)
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump02_dump_restore_13(self):
        # We're using this type later, so we want to validate its integrity.
        eqlres = await self.con.query('select K {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "K"')
        self.assert_shape(sqlres, eqlres)

        # Emulating shapes with union types is messy in SQL and unnecessary
        # for validating the data as the individual types have been validated
        # in earlier tests.
        eqlres = await self.con.query('''
            select Z {
                id,
                ck_id := .ck.id,
                stw_ids := (select .stw order by <str>.id).id
            };
        ''')
        sqlres = await self.scon.fetch(f'''
            SELECT
                id,
                ck_id,
                (
                    SELECT array_agg(x.target ORDER BY x.target::text)
                    FROM "Z.stw" x
                    WHERE x.source = "Z".id
                ) as stw_ids
            FROM "Z"
        ''')
        self.assert_shape(sqlres, eqlres)


class TestPGDump03(tb.StablePGDumpTestCase):
    DEFAULT_MODULE = 'test'

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'pg_dump02_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'pg_dump02_setup.edgeql')

    TEARDOWN = '''
        CONFIGURE CURRENT DATABASE RESET allow_dml_in_functions;
    '''

    async def test_pgdump03_dump_restore_01(self):
        eqlres = await self.con.query('select `S p a M` {id, `🚀`}')
        sqlres = await self.scon.fetch('SELECT * FROM "S p a M"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump03_dump_restore_02(self):
        eqlres = await self.con.query('select A {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "A"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump03_dump_restore_03(self):
        eqlres = await self.con.query('''
            select Tree {
                id,
                val,
                parent_id := .parent.id,
            }
        ''')
        sqlres = await self.scon.fetch('SELECT * FROM "Tree"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump03_dump_restore_04(self):
        eqlres = await self.con.query('''
            select Łukasz {
                id,
                `Ł🤞`,
                `_Ł💯` := .`Ł💯` {
                    source := @source.id,
                    `🙀🚀🚀🚀🙀` := @`🙀🚀🚀🚀🙀`,
                    `🙀مرحبا🙀` := @`🙀مرحبا🙀`,
                    target := .id,
                },
            }
        ''')
        sql = f'''
            SELECT
                id,
                "Ł🤞",
                {self.single_link_subquery(
                    "Łukasz", "Ł💯", "A", ["🙀🚀🚀🚀🙀", "🙀مرحبا🙀"])}
            FROM "Łukasz"
        '''
        sqlres = await self.scon.fetch(sql)
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump03_dump_restore_05(self):
        eqlres = await self.con.query('select `💯💯💯`::`🚀🙀🚀Type` {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "💯💯💯"."🚀🙀🚀Type"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump03_dump_restore_06(self):
        eqlres = await self.con.query(
            'select `back``ticked`::`Ticked``Type` {*}')
        sqlres = await self.scon.fetch(
            'SELECT * FROM "back`ticked"."Ticked`Type"')
        self.assert_shape(sqlres, eqlres)


class TestPGDump04(tb.StablePGDumpTestCase):
    DEFAULT_MODULE = 'test'

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump03_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump03_setup.edgeql')

    async def test_pgdump04_dump_restore_01(self):
        eqlres = await self.con.query('select Test{*}')
        sqlres = await self.scon.fetch('SELECT * FROM "Test"')
        self.assert_shape(sqlres, eqlres)


class TestPGDump05(tb.StablePGDumpTestCase):
    DEFAULT_MODULE = 'test'

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump_v2_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump_v2_setup.edgeql')

    async def test_pgdump05_dump_restore_01(self):
        eqlres = await self.con.query('select Test1 {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "Test1"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump05_dump_restore_02(self):
        eqlres = await self.con.query('select Test2 {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "Test2"')
        self.assert_shape(sqlres, eqlres)

    async def test_pgdump05_dump_restore_03(self):
        eqlres = await self.con.query('select TargetA {*}')
        sqlres = await self.scon.fetch('SELECT * FROM "TargetA"')
        self.assert_shape(sqlres, eqlres)

        eqlres = await self.con.query('''
            select SourceA {
                id,
                name,
                link1: {*},
                link2: {*},
            }
        ''')
        sql = f'''
            SELECT
                id,
                name,
                {self.single_link_subquery(
                    "SourceA", "link1", "TargetA")},
                {self.single_link_subquery(
                    "SourceA", "link2", "TargetA")}
            FROM "SourceA"

        '''
        sqlres = await self.scon.fetch(sql)
        self.assert_shape(sqlres, eqlres)
