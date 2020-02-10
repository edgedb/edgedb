#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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


class TestConstraintsSchema(tb.QueryTestCase):
    ISOLATED_METHODS = False
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'constraints.esdl')

    async def _run_link_tests(self, cases, objtype, link):
        qry = """
            INSERT {objtype} {{{{
                {link} := {{value!r}}
            }}}};
        """.format(
            objtype=objtype, link=link
        )

        for val, expected in cases:
            expr = qry.format(value=str(val))

            if expected == 'good':
                try:
                    await self.con.execute(expr)
                except Exception as ex:
                    raise AssertionError(f'{expr!r} failed') from ex
            else:
                with self.assertRaisesRegex(
                        edgedb.ConstraintViolationError, expected):
                    await self.con.execute(expr)

    async def test_constraints_scalar_length(self):
        data = {
            # max-length is 10
            (10 ** 10,
             'constraint_length must be no longer than 10 characters.'),
            (10 ** 10 - 1, 'good'),
            (10 ** 7 - 1,
             'constraint_length must be no shorter than 8 characters'),
            (10 ** 7, 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'c_length')

        data = {
            (10 ** 10,
             'constraint_length must be no longer than 10 characters.'),
            (10 ** 10 - 1, 'good'),

            (10 ** 8 - 1,
             'constraint_length_2 must be no shorter than 9 characters'),
            (10 ** 8, 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'c_length_2')

        data = {
            (10 ** 10,
             'constraint_length must be no longer than 10 characters.'),
            (10 ** 10 - 1, 'good'),
            (10 ** 9 - 1, 'c_length_3 must be no shorter than 10 characters'),
        }

        await self._run_link_tests(data, 'test::Object', 'c_length_3')

    async def test_constraints_scalar_minmax(self):
        data = {
            # max-value is "9999999989"
            (10 ** 9 - 1, "Maximum allowed value for .* is '9999999989'."),
            (10 ** 9 - 11, 'good'),

            # min-value is "99990000"
            (10 ** 8 - 10 ** 4 - 1,
             "Minimum allowed value for .* is '99990000'."),
            (10 ** 8 - 21, 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'c_minmax')

    async def test_constraints_scalar_strvalue(self):
        data = {
            # last digit is 9
            (10 ** 9 - 12, 'invalid .*'),

            # and the first is 9 too
            (10 ** 9 - 10 ** 8 - 1, 'invalid .*'),

            # and that all characters are digits
            ('99900~0009', 'invalid .*'),

            # and that first three chars are nines
            ('9900000009', 'invalid .*'),
            ('9999000009', 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'c_strvalue')

    async def test_constraints_scalar_enum_01(self):
        data = {
            ('foobar', 'must be one of:'),
            ('bar', 'good'),
            ('foo', 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'c_enum')

    async def test_constraints_scalar_enum_02(self):
        data = {
            ('foo', 'invalid'),
            ('fuz', 'good'),
            ('buz', 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'c_my_enum')

    async def test_constraints_exclusive_simple(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    INSERT test::UniqueName {
                        name := 'Test'
                    };

                    INSERT test::UniqueName {
                        name := 'Test'
                    };
                """)

    async def test_constraints_exclusive_inherited(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    INSERT test::UniqueNameInherited {
                        name := 'Test'
                    };

                    INSERT test::UniqueNameInherited {
                        name := 'Test'
                    };
                """)

    async def test_constraints_exclusive_across_ancestry(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):

                await self.con.execute("""
                    INSERT test::UniqueName {
                        name := 'exclusive_name_across'
                    };

                    INSERT test::UniqueNameInherited {
                        name := 'exclusive_name_across'
                    };
                """)

        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT test::UniqueName {
                    name := 'exclusive_name_ok'
                };

                INSERT test::UniqueNameInherited {
                    name := 'exclusive_name_inherited_ok'
                };
            """)

            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    UPDATE
                        test::UniqueNameInherited
                    FILTER
                        test::UniqueNameInherited.name =
                            'exclusive_name_inherited_ok'
                    SET {
                        name := 'exclusive_name_ok'
                    };
                """)

    async def test_constraints_exclusive_case_insensitive(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    INSERT test::UniqueName_3 {
                        name := 'TeSt'
                    };

                    INSERT test::UniqueName_3 {
                        name := 'tEsT'
                    };
                """)

    async def test_constraints_exclusive_delegation(self):
        async with self._run_and_rollback():
            # This is OK, the name exclusivity constraint is delegating
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'exclusive_name_ap'
                };

                INSERT test::AbstractConstraintParent {
                    name := 'exclusive_name_ap'
                };
            """)

            # This is OK too
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'exclusive_name_ap1'
                };

                INSERT test::AbstractConstraintPureChild {
                    name := 'exclusive_name_ap1'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT test::AbstractConstraintPureChild {
                        name := 'exclusive_name_ap2'
                    };

                    INSERT test::AbstractConstraintPureChild {
                        name := 'exclusive_name_ap2'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT test::AbstractConstraintMixedChild {
                        name := 'exclusive_name_ap2'
                    };

                    INSERT test::AbstractConstraintMixedChild {
                        name := 'exclusive_name_AP2'
                    };
                """)

        async with self._run_and_rollback():
            # This is OK, duplication is in different children
            await self.con.execute("""
                INSERT test::AbstractConstraintPureChild {
                    name := 'exclusive_name_ap3'
                };

                INSERT test::AbstractConstraintMixedChild {
                    name := 'exclusive_name_ap3'
                };
            """)

            # This is OK, the name exclusivity constraint is abstract again
            await self.con.execute("""
                INSERT test::AbstractConstraintPropagated {
                    name := 'exclusive_name_ap4'
                };

                INSERT test::AbstractConstraintPropagated {
                    name := 'exclusive_name_ap4'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Not OK, yet
                await self.con.execute("""
                    INSERT test::BecomingAbstractConstraint {
                        name := 'exclusive_name_ap5'
                    };

                    INSERT test::BecomingAbstractConstraintChild {
                        name := 'exclusive_name_ap5'
                    };
                """)

        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT test::BecomingConcreteConstraint {
                    name := 'exclusive_name_ap6'
                };

                INSERT test::BecomingConcreteConstraintChild {
                    name := 'exclusive_name_ap6'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    INSERT test::LosingAbstractConstraintParent {
                        name := 'exclusive_name_ap7'
                    };

                    INSERT test::LosingAbstractConstraintParent {
                        name := 'exclusive_name_ap7'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    INSERT test::AbstractConstraintMultipleParentsFlattening{
                        name := 'exclusive_name_ap8'
                    };

                    INSERT test::AbstractConstraintMultipleParentsFlattening{
                        name := 'exclusive_name_ap8'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # non-abstract inherited constraint
                await self.con.execute("""
                    INSERT test::AbstractInheritingNonAbstract {
                        name := 'exclusive_name_ana'
                    };

                    INSERT test::AbstractInheritingNonAbstract {
                        name := 'exclusive_name_ana'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # non-abstract inherited constraint
                await self.con.execute("""
                    INSERT test::AbstractInheritingNonAbstract {
                        name := 'exclusive_name_ana1'
                    };

                    INSERT test::AbstractInheritingNonAbstractChild {
                        name := 'exclusive_name_ana1'
                    };
                """)


class TestConstraintsSchemaMigration(tb.QueryTestCase):
    ISOLATED_METHODS = False
    SCHEMA = os.path.join(os.path.dirname(__file__),
                          'schemas', 'constraints_migration',
                          'schema.esdl')

    async def test_constraints_exclusive_migration(self):
        new_schema_f = os.path.join(os.path.dirname(__file__),
                                    'schemas', 'constraints_migration',
                                    'updated_schema.esdl')

        with open(new_schema_f) as f:
            new_schema = f.read()

        async with self.con.transaction():
            await self.con.execute(f'''
                CREATE MIGRATION d1 TO {{ module test {{ {new_schema} }} }};
                COMMIT MIGRATION d1;
            ''')

        async with self._run_and_rollback():
            # This is OK, the name exclusivity constraint is abstract
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'exclusive_name_ap'
                };

                INSERT test::AbstractConstraintParent {
                    name := 'exclusive_name_ap'
                };
            """)

            # This is OK too
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'exclusive_name_ap1'
                };

                INSERT test::AbstractConstraintPureChild {
                    name := 'exclusive_name_ap1'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT test::AbstractConstraintPureChild {
                        name := 'exclusive_name_ap2'
                    };

                    INSERT test::AbstractConstraintPureChild {
                        name := 'exclusive_name_ap2'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT test::AbstractConstraintMixedChild {
                        name := 'exclusive_name_ap2'
                    };

                    INSERT test::AbstractConstraintMixedChild {
                        name := 'exclusive_name_AP2'
                    };
                """)

        async with self._run_and_rollback():
            # This is OK, duplication is in different children
            await self.con.execute("""
                INSERT test::AbstractConstraintMixedChild {
                    name := 'exclusive_name_ap3'
                };

                INSERT test::AbstractConstraintPureChild {
                    name := 'exclusive_name_ap3'
                };
            """)

        async with self._run_and_rollback():
            # This is OK, the name exclusivity constraint is abstract again
            await self.con.execute("""
                INSERT test::AbstractConstraintPropagated {
                    name := 'exclusive_name_ap4'
                };

                INSERT test::AbstractConstraintPropagated {
                    name := 'exclusive_name_ap4'
                };
            """)

        async with self._run_and_rollback():
            # OK, former constraint was turned into an abstract constraint
            await self.con.execute("""
                INSERT test::BecomingAbstractConstraint {
                    name := 'exclusive_name_ap5'
                };

                INSERT test::BecomingAbstractConstraintChild {
                    name := 'exclusive_name_ap5'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Constraint is no longer abstract
                await self.con.execute("""
                    INSERT test::BecomingConcreteConstraint {
                        name := 'exclusive_name_ap6'
                    };

                    INSERT test::BecomingConcreteConstraintChild {
                        name := 'exclusive_name_ap6'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Constraint is no longer abstract
                await self.con.execute("""
                    INSERT test::LosingAbstractConstraintParent {
                        name := 'exclusive_name_ap6'
                    };

                    INSERT test::LosingAbstractConstraintParent {
                        name := 'exclusive_name_ap6'
                    };
                """)

        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT test::LosingAbstractConstraintParent2 {
                    name := 'exclusive_name_ap7'
                };

                INSERT test::LosingAbstractConstraintParent2 {
                    name := 'exclusive_name_ap7'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Constraint is no longer abstract
                await self.con.execute("""
                    INSERT test::AbstractConstraintMultipleParentsFlattening{
                        name := 'exclusive_name_ap8'
                    };

                    INSERT test::AbstractConstraintMultipleParentsFlattening{
                        name := 'exclusive_name_AP8'
                    };
                """)

        async with self._run_and_rollback():
            # Parent lost its concrete constraint inheritance
            await self.con.execute("""
                INSERT test::AbstractInheritingNonAbstract {
                    name := 'exclusive_name_ana'
                };

                INSERT test::AbstractInheritingNonAbstract {
                    name := 'exclusive_name_ana'
                };
            """)

        async with self._run_and_rollback():
            # Parent lost its concrete constraint inheritance
            await self.con.execute("""
                INSERT test::AbstractInheritingNonAbstract {
                    name := 'exclusive_name_ana1'
                };

                INSERT test::AbstractInheritingNonAbstractChild {
                    name := 'exclusive_name_ana1'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Child uniqueness is still enforced
                await self.con.execute("""
                    INSERT test::AbstractInheritingNonAbstractChild{
                        name := 'exclusive_name_ana2'
                    };

                    INSERT test::AbstractInheritingNonAbstractChild{
                        name := 'exclusive_name_ana2'
                    };
                """)


class TestConstraintsDDL(tb.NonIsolatedDDLTestCase):
    async def test_constraints_ddl_01(self):
        qry = """
            CREATE ABSTRACT LINK test::translated_label {
                CREATE PROPERTY lang -> std::str;
                CREATE PROPERTY prop1 -> std::str;
            };

            CREATE ABSTRACT LINK test::link_with_exclusive_property {
                CREATE PROPERTY exclusive_property -> std::str {
                    CREATE CONSTRAINT std::exclusive;
                };
            };

            CREATE ABSTRACT LINK test::link_with_exclusive_property_inherited
                EXTENDING test::link_with_exclusive_property;

            CREATE TYPE test::UniqueName {
                CREATE PROPERTY name -> std::str {
                    CREATE CONSTRAINT std::exclusive;
                };

                CREATE LINK link_with_exclusive_property -> std::Object;
            };
        """

        await self.con.execute(qry)

        # Simple exclusivity constraint on a link
        #
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    INSERT test::UniqueName {
                        name := 'Test'
                    };

                    INSERT test::UniqueName {
                        name := 'Test'
                    };
                """)

        qry = """
            CREATE TYPE test::AbstractConstraintParent {
                CREATE PROPERTY name -> std::str {
                    CREATE DELEGATED CONSTRAINT std::exclusive;
                };
            };

            CREATE TYPE test::AbstractConstraintPureChild
                EXTENDING test::AbstractConstraintParent;
        """

        await self.con.execute(qry)

        async with self._run_and_rollback():
            # This is OK, the name exclusivity constraint is abstract
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'exclusive_name_ap'
                };

                INSERT test::AbstractConstraintParent {
                    name := 'exclusive_name_ap'
                };
            """)

            # This is OK too
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'exclusive_name_ap1'
                };

                INSERT test::AbstractConstraintPureChild {
                    name := 'exclusive_name_ap1'
                };
            """)

    async def test_constraints_ddl_02(self):
        # testing the generalized constraint with 'ON (...)' clause
        qry = r"""
            CREATE ABSTRACT CONSTRAINT test::mymax1(max: std::int64)
                    ON (len(__subject__))
            {
                SET errmessage :=
                    '{__subject__} must be no longer than {max} characters.';
                USING (__subject__ <= max);
            };

            CREATE ABSTRACT CONSTRAINT test::mymax_ext1(max: std::int64)
                    ON (len(__subject__)) EXTENDING std::max_value
            {
                SET errmessage :=
                    '{__subject__} must be no longer than {max} characters.';
            };

            CREATE TYPE test::ConstraintOnTest1 {
                CREATE PROPERTY foo -> std::str {
                    CREATE CONSTRAINT test::mymax1(3);
                };

                CREATE PROPERTY bar -> std::str {
                    CREATE CONSTRAINT test::mymax_ext1(3);
                };
            };
        """

        await self.con.execute(qry)

        await self.assert_query_result(
            r'''
                SELECT schema::Constraint {
                    name,
                    args: {
                        num,
                        name,
                        kind,
                        type: {
                            name
                        },
                        typemod,
                        @value
                    }
                    FILTER .num > 0
                    ORDER BY .num ASC
                } FILTER .name = 'test::mymax_ext1' AND exists(.subject);
            ''',
            [
                {
                    "name": 'test::mymax_ext1',
                    "args": [
                        {
                            "num": 1,
                            "kind": 'POSITIONAL',
                            "name": 'max',
                            "type": {"name": 'std::int64'},
                            "@value": '3',
                            "typemod": 'SINGLETON'
                        }
                    ],
                },
            ]
        )

        await self.assert_query_result(
            r'''
                SELECT schema::Constraint {
                    name,
                    params: {
                        num,
                        name,
                        kind,
                        type: {
                            name
                        },
                        typemod
                    }
                    FILTER .num > 0
                    ORDER BY .num ASC
                } FILTER .name = 'test::mymax_ext1' AND NOT exists(.subject);
            ''',
            [
                {
                    "name": 'test::mymax_ext1',
                    "params": [
                        {
                            "num": 1,
                            "kind": 'POSITIONAL',
                            "name": 'max',
                            "type": {"name": 'std::int64'},
                            "typemod": 'SINGLETON'
                        }
                    ],
                },
            ]
        )

        # making sure the constraint was applied successfully
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'foo must be no longer than 3 characters.'):
                await self.con.execute("""
                    INSERT test::ConstraintOnTest1 {
                        foo := 'Test'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'bar must be no longer than 3 characters.'):
                await self.con.execute("""
                    INSERT test::ConstraintOnTest1 {
                        bar := 'Test'
                    };
                """)

        async with self._run_and_rollback():
            # constraint should not fail
            await self.con.execute("""
                INSERT test::ConstraintOnTest1 {
                    foo := '',
                    bar := ''
                };

                INSERT test::ConstraintOnTest1 {
                    foo := 'a',
                    bar := 'q'
                };

                INSERT test::ConstraintOnTest1 {
                    foo := 'ab',
                    bar := 'qw'
                };

                INSERT test::ConstraintOnTest1 {
                    foo := 'abc',
                    bar := 'qwe'
                };

                # a duplicate 'foo' and 'bar' just for good measure
                INSERT test::ConstraintOnTest1 {
                    foo := 'ab',
                    bar := 'qw'
                };
            """)

    async def test_constraints_ddl_03(self):
        # testing the specialized constraint with 'ON (...)' clause
        qry = r"""
            CREATE ABSTRACT CONSTRAINT test::mymax2(max: std::int64) {
                SET errmessage :=
                    '{__subject__} must be no longer than {max} characters.';
                USING (__subject__ <= max);
            };

            CREATE TYPE test::ConstraintOnTest2 {
                CREATE PROPERTY foo -> std::str {
                    CREATE CONSTRAINT test::mymax2(3) ON (len(__subject__));
                };

                CREATE PROPERTY bar -> std::str {
                    CREATE CONSTRAINT std::max_value(3) ON (len(__subject__)) {
                        SET errmessage :=
                    # XXX: once simple string concat is possible here
                    #      formatting can be saner
                    '{__subject__} must be no longer than {max} characters.';
                    };
                };
            };
        """

        await self.con.execute(qry)

        # making sure the constraint was applied successfully
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'foo must be no longer than 3 characters.'):
                await self.con.execute("""
                    INSERT test::ConstraintOnTest2 {
                        foo := 'Test'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'bar must be no longer than 3 characters.'):
                await self.con.execute("""
                    INSERT test::ConstraintOnTest2 {
                        bar := 'Test'
                    };
                """)

        async with self._run_and_rollback():
            # constraint should not fail
            await self.con.execute("""
                INSERT test::ConstraintOnTest2 {
                    foo := '',
                    bar := ''
                };

                INSERT test::ConstraintOnTest2 {
                    foo := 'a',
                    bar := 'q'
                };

                INSERT test::ConstraintOnTest2 {
                    foo := 'ab',
                    bar := 'qw'
                };

                INSERT test::ConstraintOnTest2 {
                    foo := 'abc',
                    bar := 'qwe'
                };

                # a duplicate 'foo' and 'bar' just for good measure
                INSERT test::ConstraintOnTest2 {
                    foo := 'ab',
                    bar := 'qw'
                };
            """)

    async def test_constraints_ddl_04(self):
        # testing an issue with expressions used for 'errmessage'
        qry = r"""
            CREATE ABSTRACT CONSTRAINT test::mymax3(max: std::int64) {
                SET errmessage :=
                    '{__subject__} must be no longer ' ++
                    'than {max} characters.';
                USING (__subject__ <= max);
            };

            CREATE TYPE test::ConstraintOnTest3 {
                CREATE PROPERTY foo -> std::str {
                    CREATE CONSTRAINT test::mymax3(3) ON (len(__subject__));
                };
            };
        """

        await self.con.execute(qry)

        # making sure the constraint was applied successfully
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'foo must be no longer than 3 characters.'):
                await self.con.execute("""
                    INSERT test::ConstraintOnTest3 {
                        foo := 'Test'
                    };
                """)

    async def test_constraints_ddl_05(self):
        # Test that constraint expression returns a boolean.

        await self.con.execute(r"""
            CREATE FUNCTION con05(a: int64) -> str
                USING EdgeQL $$
                    SELECT <str>a
                $$;
        """)

        # create a type with a constraint
        await self.con.execute(r"""
            CREATE TYPE test::ConstraintOnTest5 {
                CREATE REQUIRED PROPERTY foo -> int64 {
                    # Use the function in a constraint expression,
                    # s.t. it will effectively fail for any int
                    # outside 0-9 range.
                    CREATE CONSTRAINT
                        std::expression on (len(con05(__subject__)) < 2);
                }
            }
        """)

        with self.assertRaisesRegex(
                edgedb.errors.ConstraintViolationError,
                r'invalid foo'):
            await self.con.execute("""
                INSERT test::ConstraintOnTest5 {
                    foo := 42
                };
            """)

        async with self._run_and_rollback():
            # constraint should not fail
            await self.con.execute("""
                INSERT test::ConstraintOnTest5 {
                    foo := 2
                };
            """)

    async def test_constraints_ddl_06(self):
        # Test that constraint expression returns a boolean.

        await self.con.execute(r"""
            CREATE FUNCTION con06(a: int64) -> array<int64>
                USING EdgeQL $$
                    SELECT [a]
                $$;
        """)

        # create a type with a constraint
        await self.con.execute(r"""
            CREATE TYPE test::ConstraintOnTest6 {
                CREATE REQUIRED PROPERTY foo -> int64 {
                    # Use the function in a constraint expression,
                    # s.t. it will never fail.
                    CREATE CONSTRAINT
                        std::expression on (len(con06(__subject__)) < 2);
                }
            }
        """)

        async with self._run_and_rollback():
            # constraint should not fail
            await self.con.execute("""
                INSERT test::ConstraintOnTest6 {
                    foo := 42
                };
            """)

            await self.con.execute("""
                INSERT test::ConstraintOnTest6 {
                    foo := 2
                };
            """)

    async def test_constraints_ddl_function(self):
        await self.con.execute('''\
            CREATE FUNCTION test::comp_func(s: str) -> str {
                USING (
                    SELECT str_lower(s)
                );
                SET volatility := 'IMMUTABLE';
            };

            CREATE TYPE test::CompPropFunction {
                CREATE PROPERTY title -> str {
                    CREATE CONSTRAINT exclusive ON
                        (test::comp_func(__subject__));
                };
                CREATE PROPERTY comp_prop := test::comp_func(.title);
            };
        ''')

    async def test_constraints_ddl_error_02(self):
        # testing that subjectexpr cannot be overridden after it is
        # specified explicitly
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.InvalidConstraintDefinitionError,
                    r"subjectexpr is already defined for .+max_int"):
                await self.con.execute(r"""
                    CREATE ABSTRACT CONSTRAINT test::max_int(m: std::int64)
                        ON (<int64>__subject__)
                    {
                        SET errmessage :=
                      # XXX: once simple string concat is possible here
                      #      formatting can be saner
                      '{__subject__} must be no longer than {m} characters.';
                        USING (__subject__ <= m);
                    };

                    CREATE TYPE test::InvalidConstraintTest2 {
                        CREATE PROPERTY foo -> std::str {
                            CREATE CONSTRAINT test::max_int(3)
                                ON (len(__subject__));
                        };
                    };
                """)

    async def test_constraints_ddl_error_05(self):
        # Test that constraint expression returns a boolean.
        qry = """
            CREATE MIGRATION ddl_error_05 TO {
                module test {
                    type User {
                        required property login -> str {
                            constraint expression on (len(__subject__))
                        }
                    };
                };
            };
        """

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "constraint expression expected to return a bool value, "
                "got 'int64'"):
            async with self.con.transaction():
                await self.con.execute(qry)

        qry = """
            CREATE TYPE User {
                CREATE REQUIRED PROPERTY login -> str {
                    CREATE CONSTRAINT expression on (len(__subject__));
                };
            };
        """

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "constraint expression expected to return a bool value, "
                "got 'int64'"):
            await self.con.execute(qry)

        qry = """
            CREATE ABSTRACT CONSTRAINT foo {
                USING (__subject__);
            };

            CREATE TYPE User {
                CREATE REQUIRED PROPERTY login -> str {
                    CREATE CONSTRAINT foo;
                };
            };
        """

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                "constraint expression expected to return a bool value, "
                "got 'str'"):
            await self.con.execute(qry)

    async def test_constraints_ddl_error_06(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.InvalidConstraintDefinitionError,
                    r'dollar-prefixed.*cannot be used'):
                await self.con.execute(r"""
                    CREATE ABSTRACT CONSTRAINT
                    test::mymax_er_06(max: std::int64) ON (len(__subject__))
                    {
                        USING (__subject__ <= $max);
                    };

                    CREATE TYPE test::ConstraintOnTest_err_06 {
                        CREATE PROPERTY foo -> std::str {
                            CREATE CONSTRAINT test::mymax_er_06(3);
                        };
                    };
                """)
