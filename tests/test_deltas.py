#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


import difflib
import os.path
import textwrap
import unittest  # NOQA

from edb.client import exceptions
from edb.server import _testbase as tb


class TestDeltas(tb.DDLTestCase):
    ISOLATED_METHODS = True

    async def test_delta_simple_01(self):
        result = await self.con.execute("""
            # setup delta
            #
            CREATE MIGRATION test::d1 TO eschema $$
                abstract link related:
                    property lang -> str

                type NamedObject:
                    required property name -> str
                    required link related -> NamedObject:
                        cardinality := '**'
            $$;

            COMMIT MIGRATION test::d1;

            INSERT test::NamedObject {
                name := 'Test'
            };

            INSERT test::NamedObject {
                name := 'Test 2',
                related := (SELECT DETACHED test::NamedObject
                            FILTER .name = 'Test')
            };

            SELECT
                test::NamedObject {
                    related: {
                        name,
                        @lang
                    }
                }
            FILTER
                test::NamedObject.name = 'Test 2';

            """)

        self.assert_data_shape(result, [
            None,

            None,

            [1],

            [1],

            [{
                'related': [{'name': 'Test', '@lang': None}],
            }]
        ])

    async def test_delta_drop_01(self):
        # Check that constraints defined on scalars being dropped are
        # dropped.
        result = await self.con.execute("""
            CREATE SCALAR TYPE test::a1 EXTENDING std::str {
                CREATE CONSTRAINT std::enum('a', 'b') {
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
                CREATE PROPERTY test::l1 -> std::str {
                    SET description := 'test_delta_drop_02_link';
                };
            };

            WITH MODULE schema
            SELECT Property {name}
            FILTER Property.description = 'test_delta_drop_02_link';

            DROP TYPE test::C1;

            WITH MODULE schema
            SELECT Property {name}
            FILTER Property.description = 'test_delta_drop_02_link';
        """)

        self.assert_data_shape(result, [
            None,

            [{
                'name': 'test::l1',
            }],

            None,

            []
        ])

    async def test_delta_unicode_01(self):
        result = await self.con.execute(r"""
            # setup delta
            CREATE MIGRATION test::u1 TO eschema $$
                type Пример:
                    required property номер -> int16
            $$;
            COMMIT MIGRATION test::u1;
            SET MODULE test;

            INSERT Пример {
                номер := 987
            };
            INSERT Пример {
                номер := 456
            };

            SELECT
                Пример {
                    номер
                }
            ORDER BY
                Пример.номер;
            """)

        self.assert_data_shape(result, [
            None,
            None,
            None,

            [1],
            [1],

            [{'номер': 456}, {'номер': 987}]
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
                abstract link related:
                    property lang -> str

                type NamedObject:
                    required property name -> str
                    required link related -> NamedObject:
                        inherited property lang -> str:
                            title := 'Language'
            $$;

            GET MIGRATION test::d1;
        """)

        self._assert_result(
            result[1],
            '''\
CREATE ABSTRACT PROPERTY test::lang {
    SET is_virtual := False;
    SET readonly := False;
};
CREATE ABSTRACT PROPERTY test::name {
    SET is_virtual := False;
    SET readonly := False;
};
CREATE ABSTRACT LINK test::related EXTENDING std::link {
    SET is_virtual := False;
    SET readonly := False;
};
ALTER ABSTRACT LINK test::related CREATE PROPERTY test::lang -> std::str {
    SET cardinality := '11';
    SET is_virtual := False;
    SET readonly := False;
};
CREATE TYPE test::NamedObject EXTENDING std::Object {
    SET is_virtual := False;
};
ALTER TYPE test::NamedObject {
    CREATE REQUIRED PROPERTY test::name -> std::str {
        SET cardinality := '11';
        SET is_virtual := False;
        SET readonly := False;
    };
    CREATE REQUIRED LINK test::related -> test::NamedObject {
        SET cardinality := '*1';
        SET is_virtual := False;
        SET readonly := False;
    };
    ALTER LINK test::related {
        CREATE PROPERTY std::source -> test::NamedObject {
            SET cardinality := '11';
            SET is_virtual := False;
            SET readonly := False;
        };
        CREATE PROPERTY std::target -> test::NamedObject {
            SET cardinality := '11';
            SET is_virtual := False;
            SET readonly := False;
        };
        CREATE PROPERTY test::lang -> std::str {
            SET cardinality := '11';
            SET is_virtual := False;
            SET readonly := False;
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
                type NamedObject:
                    required property a -> array<int64>
            $$;

            GET MIGRATION test::d2;
        """)

        self._assert_result(
            result[1],
            '''\
CREATE ABSTRACT PROPERTY test::a {
    SET is_virtual := False;
    SET readonly := False;
};
CREATE TYPE test::NamedObject EXTENDING std::Object {
    SET is_virtual := False;
};
ALTER TYPE test::NamedObject CREATE REQUIRED PROPERTY \
test::a -> array<std::int64> {
    SET cardinality := '11';
    SET is_virtual := False;
    SET readonly := False;
};
            '''
        )

    async def test_delta_ddlgen_03(self):
        result = await self.con.execute("""
            # setup delta
            CREATE MIGRATION test::d3 TO eschema $$
                abstract type Foo:
                    property bar -> str
                    property __typename := 'foo'
            $$;

            GET MIGRATION test::d3;
        """)

        self._assert_result(
            result[1],
            '''\
            CREATE ABSTRACT PROPERTY test::bar {
                SET is_virtual := False;
                SET readonly := False;
            };
            CREATE ABSTRACT PROPERTY test::__typename {
                SET is_virtual := False;
                SET readonly := False;
            };
            CREATE ABSTRACT TYPE test::Foo EXTENDING std::Object {
                SET is_virtual := False;
            };
            ALTER TYPE test::Foo {
                CREATE PROPERTY test::bar -> std::str {
                    SET cardinality := '11';
                    SET is_virtual := False;
                    SET readonly := False;
                };
                CREATE PROPERTY test::__typename -> std::str {
                    SET cardinality := '*1';
                    SET computable := True;
                    SET default := SELECT
                        'foo'
                    ;
                    SET is_virtual := False;
                    SET readonly := False;
                };
            };
            '''
        )

    async def test_delta_ddlgen_04(self):
        result = await self.con.execute("""
            # setup delta
            CREATE MIGRATION test::d3 TO eschema $$
                abstract type Foo:
                    property bar -> str
                    property __typename := __source__.__type__.name
            $$;

            GET MIGRATION test::d3;
        """)

        self._assert_result(
            result[1],
            '''\
            CREATE ABSTRACT PROPERTY test::bar {
                SET is_virtual := False;
                SET readonly := False;
            };
            CREATE ABSTRACT PROPERTY test::__typename {
                SET is_virtual := False;
                SET readonly := False;
            };
            CREATE ABSTRACT TYPE test::Foo EXTENDING std::Object {
                SET is_virtual := False;
            };
            ALTER TYPE test::Foo {
                CREATE PROPERTY test::bar -> std::str {
                    SET cardinality := '11';
                    SET is_virtual := False;
                    SET readonly := False;
                };
                CREATE PROPERTY test::__typename -> std::str {
                    SET cardinality := '*1';
                    SET computable := True;
                    SET default := SELECT
                        __source__.__type__[IS schema::Type].name
                    ;
                    SET is_virtual := False;
                    SET readonly := False;
                };
            };
            '''
        )
