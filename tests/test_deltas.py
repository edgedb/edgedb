##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import textwrap
import uuid

from edgedb.client import exceptions
from edgedb.server import _testbase as tb


class TestDeltas(tb.DDLTestCase):
    async def test_delta_simple01(self):
        result = await self.con.execute("""
            # setup delta
            #
            CREATE MIGRATION test::d1 TO eschema $$
                link name:
                    linkproperty lang to str

                concept NamedObject:
                    required link name to str
            $$;

            COMMIT MIGRATION test::d1;

            # test updated schema
            #
            INSERT test::NamedObject {
                name := 'Test'
            };

            SELECT
                test::NamedObject {
                    name: {
                        @lang
                    }
                }
            FILTER
                test::NamedObject.name = 'Test';

            """)

        self.assert_data_shape(result, [
            None,

            None,

            [1],

            [{
                'id': uuid.UUID,
                'name': {'@target': 'Test', '@lang': None},
            }]
        ])


class TestDeltaLinkInheritance(tb.DDLTestCase):
    async def test_delta_link_inheritance(self):
        schema_f = os.path.join(os.path.dirname(__file__), 'schemas',
                                'links_1.eschema')

        with open(schema_f) as f:
            schema = f.read()

        await self.con.execute(f'''
            CREATE MIGRATION test::d_links01_0 TO eschema $${schema}$$;
            COMMIT MIGRATION test::d_links01_0;
            ''')

        await self.con.execute('''
            INSERT test::Target1 {
                name := 'Target1_linkinh_2'
            };

            INSERT test::Concept01 {
                `target` := (SELECT test::Target1
                             FILTER test::Target1.name = 'Target1_linkinh_2')
            };

            INSERT test::Target0 {
                name := 'Target0_linkinh_2'
            };

            INSERT test::Concept23 {
                `target` := (SELECT test::Target0
                             FILTER test::Target0.name = 'Target0_linkinh_2')
            };
        ''')

        with self.assertRaisesRegex(
                exceptions.InvalidPointerTargetError,
                "invalid target for link 'test::Concept01\.target': "
                "'test::Target0' \(expecting 'test::Target1'\)"):
            # Target0 is not allowed to be targeted by Concept01, since
            # Concept01 inherits from Concept1 which requires more specific
            # Target1.
            await self.con.execute('''
                INSERT test::Concept01 {
                    `target` := (
                        SELECT
                            test::Target0
                        FILTER
                            test::Target0.name = 'Target0_linkinh_2'
                    )
                };
            ''')

        schema_f = os.path.join(os.path.dirname(__file__), 'schemas',
                                'links_1_migrated.eschema')

        with open(schema_f) as f:
            schema = f.read()

        await self.con.execute(f'''
            CREATE MIGRATION test::d_links01_1 TO eschema $${schema}$$;
            COMMIT MIGRATION test::d_links01_1;
            ''')


class TestDeltaDDLGeneration(tb.DDLTestCase):
    def _assert_result(self, result, expected):
        self.assertEqual(result.strip(), textwrap.dedent(expected).strip())

    async def test_delta_ddlgen_01(self):
        result = await self.con.execute("""
            # setup delta
            #
            CREATE MIGRATION test::d1 TO eschema $$
                link name:
                    linkproperty lang to str

                concept NamedObject:
                    required link name to str:
                        linkproperty lang to str:
                            title: 'Language'
            $$;

            GET MIGRATION test::d1;
        """)

        self._assert_result(
            result[1],
            '''\
            CREATE LINK PROPERTY test::lang {
                SET is_virtual := False;
                SET readonly := False;
                SET title := 'Base link property';
            };
            CREATE LINK test::name INHERITING std::`link` {
                SET is_virtual := False;
                SET readonly := False;
            };
            ALTER LINK test::name CREATE LINK PROPERTY test::lang TO (std::str) {
                SET is_virtual := False;
                SET readonly := False;
                SET title := 'Base link property';
            };
            CREATE CONCEPT test::NamedObject INHERITING std::Object {
                SET is_virtual := False;
            };
            ALTER CONCEPT test::NamedObject {
                CREATE REQUIRED LINK test::name TO (std::str) {
                    SET is_virtual := False;
                    SET mapping := '*1';
                    SET readonly := False;
                }
                ALTER LINK test::name {
                    CREATE LINK PROPERTY std::source TO (test::NamedObject) {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link source';
                    }
                    CREATE LINK PROPERTY std::`target` TO (std::str) {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link target';
                    }
                    CREATE LINK PROPERTY test::lang TO (std::str) {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Base link property';
                    }
                }
            };
            '''
        )
