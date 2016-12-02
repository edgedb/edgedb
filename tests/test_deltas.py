##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import textwrap
import unittest
import uuid

from edgedb.client import exceptions
from edgedb.server import _testbase as tb


class TestDeltas(tb.QueryTestCase):
    async def test_delta_simple01(self):
        result = await self.con.execute("""
            # setup delta
            #
            CREATE DELTA test::d1 TO $$
                link name:
                    linkproperty lang to str

                concept NamedObject:
                    required link name to str
            $$;

            COMMIT DELTA test::d1;

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
            WHERE
                test::NamedObject.name = 'Test';

            """)

        self.assert_data_shape(result, [
            None,

            None,

            [{
                'id': uuid.UUID,
            }],

            [{
                'id': uuid.UUID,
                'name': {'@target': 'Test', '@lang': None},
            }]
        ])

    async def test_delta_link_inheritance(self):
        schema_f = os.path.join(os.path.dirname(__file__), 'schemas',
                                'links_1.eschema')

        with open(schema_f) as f:
            schema = f.read()

        await self.con.execute('''
            CREATE DELTA test::d_links01_0 TO $${schema}$$;
            COMMIT DELTA test::d_links01_0;
            '''.format(schema=schema))

        await self.con.execute('''
            INSERT test::Target1 {
                name := 'Target1_linkinh_2'
            };

            INSERT test::Concept01 {
                `target` := (SELECT test::Target1
                             WHERE test::Target1.name = 'Target1_linkinh_2')
            };

            INSERT test::Target0 {
                name := 'Target0_linkinh_2'
            };

            INSERT test::Concept23 {
                `target` := (SELECT test::Target0
                             WHERE test::Target0.name = 'Target0_linkinh_2')
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
                        WHERE
                            test::Target0.name = 'Target0_linkinh_2'
                    )
                };
            ''')

        schema_f = os.path.join(os.path.dirname(__file__), 'schemas',
                                'links_1_migrated.eschema')

        with open(schema_f) as f:
            schema = f.read()

        await self.con.execute('''
            CREATE DELTA test::d_links01_1 TO $${schema}$$;
            COMMIT DELTA test::d_links01_1;
            '''.format(schema=schema))


class TestDeltaDDLGeneration(tb.QueryTestCase):
    def _assert_result(self, result, expected):
        self.assertEqual(result.strip(), textwrap.dedent(expected).strip())

    async def test_delta_ddlgen_01(self):
        result = await self.con.execute("""
            # setup delta
            #
            CREATE DELTA test::d1 TO $$
                link name:
                    linkproperty lang to str

                concept NamedObject:
                    required link name to str:
                        linkproperty lang to str:
                            title: 'Language'
            $$;

            GET DELTA test::d1;
        """)

        self._assert_result(
            result[1],
            '''\
            CREATE LINK PROPERTY test::lang {
                SET readonly := False;
                SET title := 'Base link property';
            };
            CREATE LINK test::name INHERITING std::`link` {
                SET readonly := False;
            };
            ALTER LINK test::name CREATE LINK PROPERTY test::lang TO (std::str) {
                SET readonly := False;
                SET title := 'Base link property';
            };
            CREATE CONCEPT test::NamedObject INHERITING std::Object {
                SET is_virtual := False;
            };
            ALTER CONCEPT test::NamedObject {
                CREATE REQUIRED LINK test::name TO (std::str) {
                    SET mapping := '*1';
                    SET readonly := False;
                }
                ALTER LINK test::name {
                    CREATE LINK PROPERTY std::source TO (test::NamedObject) {
                        SET readonly := False;
                        SET title := 'Link source';
                    }
                    CREATE LINK PROPERTY std::`target` TO (std::str) {
                        SET readonly := False;
                        SET title := 'Link target';
                    }
                    CREATE LINK PROPERTY test::lang TO (std::str) {
                        SET readonly := False;
                        SET title := 'Base link property';
                    }
                }
            };
            '''
        )
