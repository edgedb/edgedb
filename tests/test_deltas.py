##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import difflib
import os.path
import textwrap
import uuid
import unittest  # NOQA

from edgedb.client import exceptions
from edgedb.server import _testbase as tb


class TestDeltas(tb.DDLTestCase):
    async def test_delta_simple01(self):
        result = await self.con.execute("""
            # setup delta
            #
            CREATE MIGRATION test::d1 TO eschema $$
                link name:
                    link property lang to str

                type NamedObject:
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

    async def test_delta_drop_01(self):
        # Check that constraints defined on scalars being dropped are
        # dropped.
        result = await self.con.execute("""
            CREATE SCALAR TYPE test::a1 EXTENDING std::str {
                CREATE CONSTRAINT std::enum(['a', 'b']) {
                    SET description := 'test_delta_drop_01_constraint';
                };
            };

            WITH MODULE schema
            SELECT Constraint {name}
            FILTER Constraint.description = 'test_delta_drop_01_constraint';

            DROP SCALAR TYPE test::a1;

            WITH MODULE schema
            SELECT Constraint {name}
            FILTER Constraint.description = 'test_delta_drop_01_constraint';
        """)

        self.assert_data_shape(result, [
            None,

            [{
                'name': 'std::enum',
            }],

            None,

            []
        ])

    async def test_delta_drop_02(self):
        # Check that links defined on types being dropped are
        # dropped.
        result = await self.con.execute("""
            CREATE TYPE test::C1 {
                CREATE LINK test::l1 TO std::str {
                    SET description := 'test_delta_drop_02_link';
                };
            };

            WITH MODULE schema
            SELECT Link {name}
            FILTER Link.description = 'test_delta_drop_02_link';

            DROP TYPE test::C1;

            WITH MODULE schema
            SELECT Link {name}
            FILTER Link.description = 'test_delta_drop_02_link';
        """)

        self.assert_data_shape(result, [
            None,

            [{
                'name': 'test::l1',
            }],

            None,

            []
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

            INSERT test::ObjectType01 {
                target := (SELECT test::Target1
                           FILTER test::Target1.name = 'Target1_linkinh_2'
                           LIMIT 1)
            };

            INSERT test::Target0 {
                name := 'Target0_linkinh_2'
            };

            INSERT test::ObjectType23 {
                target := (SELECT test::Target0
                           FILTER test::Target0.name = 'Target0_linkinh_2'
                           LIMIT 1)
            };
        ''')

        with self.assertRaisesRegex(
                exceptions.InvalidPointerTargetError,
                "invalid target for link '\(test::ObjectType01\)\.target': "
                "'test::Target0' \(expecting 'test::Target1'\)"):
            # Target0 is not allowed to be targeted by ObjectType01, since
            # ObjectType01 inherits from ObjectType1 which requires more
            # specific Target1.
            await self.con.execute('''
                INSERT test::ObjectType01 {
                    target := (
                        SELECT
                            test::Target0
                        FILTER
                            test::Target0.name = 'Target0_linkinh_2'
                        LIMIT 1
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
        result = result.strip()
        expected = textwrap.dedent(expected).strip()

        if result != expected:
            diff = '\n'.join(difflib.context_diff(
                expected.split('\n'), result.split('\n')))

            self.fail(
                f'DDL does not match the expected result.'
                f'\nEXPECTED:\n{expected}\nACTUAL:\n{result}'
                f'\nDIFF:\n{diff}')

    async def test_delta_ddlgen_01(self):
        result = await self.con.execute("""
            # setup delta
            #
            CREATE MIGRATION test::d1 TO eschema $$
                link name:
                    link property lang to str

                type NamedObject:
                    required link name to str:
                        link property lang to str:
                            title := 'Language'
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
            CREATE LINK test::name EXTENDING std::link {
                SET is_virtual := False;
                SET readonly := False;
            };
            ALTER LINK test::name CREATE LINK PROPERTY test::lang TO std::str {
                SET is_virtual := False;
                SET readonly := False;
                SET title := 'Base link property';
            };
            CREATE TYPE test::NamedObject EXTENDING std::Object {
                SET is_virtual := False;
            };
            ALTER TYPE test::NamedObject {
                CREATE REQUIRED LINK test::name TO std::str {
                    SET cardinality := '*1';
                    SET is_virtual := False;
                    SET readonly := False;
                };
                ALTER LINK test::name {
                    CREATE LINK PROPERTY std::source TO test::NamedObject {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link source';
                    };
                    CREATE LINK PROPERTY std::target TO std::str {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link target';
                    };
                    CREATE LINK PROPERTY test::lang TO std::str {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Base link property';
                    };
                };
            };
            '''
        )

    async def test_delta_ddlgen_02(self):
        result = await self.con.execute("""
            # setup delta
            #
            CREATE MIGRATION test::d2 TO eschema $$
                link a:
                    link property a_prop to array<str>

                type NamedObject:
                    required link a to array<int>
            $$;

            GET MIGRATION test::d2;
        """)

        self._assert_result(
            result[1],
            '''\
    CREATE LINK PROPERTY test::a_prop {
        SET is_virtual := False;
        SET readonly := False;
        SET title := 'Base link property';
    };
    CREATE LINK test::a EXTENDING std::link {
        SET is_virtual := False;
        SET readonly := False;
    };
    ALTER LINK test::a CREATE LINK PROPERTY test::a_prop TO array<std::str> {
        SET is_virtual := False;
        SET readonly := False;
        SET title := 'Base link property';
    };
    CREATE TYPE test::NamedObject EXTENDING std::Object {
        SET is_virtual := False;
    };
    ALTER TYPE test::NamedObject {
        CREATE REQUIRED LINK test::a TO array<std::int> {
            SET cardinality := '*1';
            SET is_virtual := False;
            SET readonly := False;
        };
        ALTER LINK test::a {
            CREATE LINK PROPERTY std::source TO test::NamedObject {
                SET is_virtual := False;
                SET readonly := False;
                SET title := 'Link source';
            };
            CREATE LINK PROPERTY std::target TO array<std::int> {
                SET is_virtual := False;
                SET readonly := False;
                SET title := 'Link target';
            };
        };
    };
            '''
        )

    async def test_delta_ddlgen_03(self):
        result = await self.con.execute("""
            # setup delta
            CREATE MIGRATION test::d3 TO eschema $$
                abstract type Foo:
                    link bar to str
                    link __typename := 'foo'
            $$;

            GET MIGRATION test::d3;
        """)

        self._assert_result(
            result[1],
            '''\
            CREATE LINK test::bar EXTENDING std::link {
                SET is_virtual := False;
                SET readonly := False;
            };
            CREATE LINK test::__typename EXTENDING std::link {
                SET is_virtual := False;
                SET readonly := False;
            };
            CREATE ABSTRACT TYPE test::Foo EXTENDING std::Object {
                SET is_virtual := False;
            };
            ALTER TYPE test::Foo {
                CREATE LINK test::bar TO std::str {
                    SET cardinality := '*1';
                    SET is_virtual := False;
                    SET readonly := False;
                };
                CREATE LINK test::__typename TO std::str {
                    SET cardinality := '*1';
                    SET computable := True;
                    SET default := SELECT
                        'foo'
                    ;
                    SET is_virtual := False;
                    SET readonly := False;
                };
                ALTER LINK test::bar {
                    CREATE LINK PROPERTY std::source TO test::Foo {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link source';
                    };
                    CREATE LINK PROPERTY std::target TO std::str {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link target';
                    };
                };
                ALTER LINK test::__typename {
                    CREATE LINK PROPERTY std::source TO test::Foo {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link source';
                    };
                    CREATE LINK PROPERTY std::target TO std::str {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link target';
                    };
                };
            };
            '''
        )

    @unittest.expectedFailure
    async def test_delta_ddlgen_04(self):
        result = await self.con.execute("""
            # setup delta
            CREATE MIGRATION test::d3 TO eschema $$
                abstract type Foo:
                    link bar to str
                    link __typename := __self__.__type__.name
            $$;

            GET MIGRATION test::d3;
        """)

        self._assert_result(
            result[1],
            '''\
            CREATE LINK test::bar EXTENDING std::link {
                SET is_virtual := False;
                SET readonly := False;
            };
            CREATE LINK test::__typename EXTENDING std::link {
                SET is_virtual := False;
                SET readonly := False;
            };
            CREATE ABSTRACT TYPE test::Foo EXTENDING std::Object {
                SET is_virtual := False;
            };
            ALTER TYPE test::Foo {
                CREATE LINK test::bar TO std::str {
                    SET cardinality := '*1';
                    SET is_virtual := False;
                    SET readonly := False;
                };
                CREATE LINK test::__typename TO std::str {
                    SET computable := True;
                    SET default := SELECT
                        __self__.__type__[IS schema::Type].name
                    ;
                    SET cardinality := '*1';
                    SET is_virtual := False;
                    SET readonly := False;
                };
                ALTER LINK test::bar {
                    CREATE LINK PROPERTY std::source TO test::Foo {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link source';
                    };
                    CREATE LINK PROPERTY std::target TO std::str {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link target';
                    };
                };
                ALTER LINK test::__typename {
                    CREATE LINK PROPERTY std::source TO test::Foo {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link source';
                    };
                    CREATE LINK PROPERTY std::target TO std::str {
                        SET is_virtual := False;
                        SET readonly := False;
                        SET title := 'Link target';
                    };
                };
            };
            '''
        )
