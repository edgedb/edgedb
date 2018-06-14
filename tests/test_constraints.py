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
import unittest

from edb.server import _testbase as tb
from edb.client import exceptions


class TestConstraintsSchema(tb.QueryTestCase):
    ISOLATED_METHODS = False
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'constraints.eschema')

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
                await self.con.execute(expr)
            else:
                with self.assertRaisesRegex(
                        exceptions.ConstraintViolationError, expected):
                    await self.con.execute(expr)

    async def test_constraints_scalar_length(self):
        data = {
            # max-length is 10
            (10 ** 10, 'must be no longer than 10 characters.'),
            (10 ** 10 - 1, 'good'),
            (10 ** 7 - 1, 'must be no shorter than 8 characters'),
            (10 ** 7, 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'c_length')

        data = {
            (10 ** 10, 'must be no longer than 10 characters.'),
            (10 ** 10 - 1, 'good'),

            (10 ** 8 - 1, 'must be no shorter than 9 characters'),
            (10 ** 8, 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'c_length_2')

        data = {
            (10 ** 10, 'must be no longer than 10 characters.'),
            (10 ** 10 - 1, 'good'),
            (10 ** 9 - 1, 'must be no shorter than 10 characters'),
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
            ('foobar', 'invalid'),
            ('bar', 'good'),
            ('foo', 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'c_my_enum')

    async def test_constraints_unique_simple(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                await self.con.execute("""
                    INSERT test::UniqueName {
                        name := 'Test'
                    };

                    INSERT test::UniqueName {
                        name := 'Test'
                    };
                """)

    async def test_constraints_unique_inherited(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                await self.con.execute("""
                    INSERT test::UniqueNameInherited {
                        name := 'Test'
                    };

                    INSERT test::UniqueNameInherited {
                        name := 'Test'
                    };
                """)

    @unittest.expectedFailure
    async def test_constraints_unique_across_ancestry(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):

                await self.con.execute("""
                    INSERT test::UniqueName {
                        name := 'unique_name_across'
                    };

                    INSERT test::UniqueNameInherited {
                        name := 'unique_name_across'
                    };
                """)

        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT test::UniqueName {
                    name := 'unique_name_ok'
                };

                INSERT test::UniqueNameInherited {
                    name := 'unique_name_inherited_ok'
                };
            """)

            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # FIXME: the FILTER clause seems to filter out
                # everything, so the UPDATE is empty
                await self.con.execute("""
                    UPDATE
                        test::UniqueNameInherited
                    FILTER
                        test::UniqueNameInherited.name =
                            'unique_name_inherited_ok'
                    SET {
                        name := 'unique_name_ok'
                    };
                """)

    async def test_constraints_unique_case_insensitive(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                await self.con.execute("""
                    INSERT test::UniqueName_3 {
                        name := 'TeSt'
                    };

                    INSERT test::UniqueName_3 {
                        name := 'tEsT'
                    };
                """)

    async def test_constraints_unique_abstract(self):
        async with self._run_and_rollback():
            # This is OK, the name unique constraint is abstract
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'unique_name_ap'
                };

                INSERT test::AbstractConstraintParent {
                    name := 'unique_name_ap'
                };
            """)

            # This is OK too
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'unique_name_ap1'
                };

                INSERT test::AbstractConstraintPureChild {
                    name := 'unique_name_ap1'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT test::AbstractConstraintPureChild {
                        name := 'unique_name_ap2'
                    };

                    INSERT test::AbstractConstraintPureChild {
                        name := 'unique_name_ap2'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT test::AbstractConstraintMixedChild {
                        name := 'unique_name_ap2'
                    };

                    INSERT test::AbstractConstraintMixedChild {
                        name := 'unique_name_AP2'
                    };
                """)

        async with self._run_and_rollback():
            # This is OK, duplication is in different children
            await self.con.execute("""
                INSERT test::AbstractConstraintPureChild {
                    name := 'unique_name_ap3'
                };

                INSERT test::AbstractConstraintMixedChild {
                    name := 'unique_name_ap3'
                };
            """)

            # This is OK, the name unique constraint is abstract again
            await self.con.execute("""
                INSERT test::AbstractConstraintPropagated {
                    name := 'unique_name_ap4'
                };

                INSERT test::AbstractConstraintPropagated {
                    name := 'unique_name_ap4'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Not OK, yet
                await self.con.execute("""
                    INSERT test::BecomingAbstractConstraint {
                        name := 'unique_name_ap5'
                    };

                    INSERT test::BecomingAbstractConstraintChild {
                        name := 'unique_name_ap5'
                    };
                """)

        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT test::BecomingConcreteConstraint {
                    name := 'unique_name_ap6'
                };

                INSERT test::BecomingConcreteConstraintChild {
                    name := 'unique_name_ap6'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                await self.con.execute("""
                    INSERT test::LosingAbstractConstraintParent {
                        name := 'unique_name_ap7'
                    };

                    INSERT test::LosingAbstractConstraintParent {
                        name := 'unique_name_ap7'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                await self.con.execute("""
                    INSERT test::AbstractConstraintMultipleParentsFlattening{
                        name := 'unique_name_ap8'
                    };

                    INSERT test::AbstractConstraintMultipleParentsFlattening{
                        name := 'unique_name_ap8'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # non-abstract inherited constraint
                await self.con.execute("""
                    INSERT test::AbstractInheritingNonAbstract {
                        name := 'unique_name_ana'
                    };

                    INSERT test::AbstractInheritingNonAbstract {
                        name := 'unique_name_ana'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # non-abstract inherited constraint
                await self.con.execute("""
                    INSERT test::AbstractInheritingNonAbstract {
                        name := 'unique_name_ana1'
                    };

                    INSERT test::AbstractInheritingNonAbstractChild {
                        name := 'unique_name_ana1'
                    };
                """)


class TestConstraintsSchemaMigration(tb.QueryTestCase):
    ISOLATED_METHODS = False
    SCHEMA = os.path.join(os.path.dirname(__file__),
                          'schemas', 'constraints_migration',
                          'schema.eschema')

    async def test_constraints_unique_migration(self):
        new_schema_f = os.path.join(os.path.dirname(__file__),
                                    'schemas', 'constraints_migration',
                                    'updated_schema.eschema')

        with open(new_schema_f) as f:
            new_schema = f.read()

        await self.con.execute(f'''
            CREATE MIGRATION test::d1 TO eschema $${new_schema}$$;
            COMMIT MIGRATION test::d1;
            ''')

        async with self._run_and_rollback():
            # This is OK, the name unique constraint is abstract
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'unique_name_ap'
                };

                INSERT test::AbstractConstraintParent {
                    name := 'unique_name_ap'
                };
            """)

            # This is OK too
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'unique_name_ap1'
                };

                INSERT test::AbstractConstraintPureChild {
                    name := 'unique_name_ap1'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT test::AbstractConstraintPureChild {
                        name := 'unique_name_ap2'
                    };

                    INSERT test::AbstractConstraintPureChild {
                        name := 'unique_name_ap2'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT test::AbstractConstraintMixedChild {
                        name := 'unique_name_ap2'
                    };

                    INSERT test::AbstractConstraintMixedChild {
                        name := 'unique_name_AP2'
                    };
                """)

        async with self._run_and_rollback():
            # This is OK, duplication is in different children
            await self.con.execute("""
                INSERT test::AbstractConstraintMixedChild {
                    name := 'unique_name_ap3'
                };

                INSERT test::AbstractConstraintPureChild {
                    name := 'unique_name_ap3'
                };
            """)

        async with self._run_and_rollback():
            # This is OK, the name unique constraint is abstract again
            await self.con.execute("""
                INSERT test::AbstractConstraintPropagated {
                    name := 'unique_name_ap4'
                };

                INSERT test::AbstractConstraintPropagated {
                    name := 'unique_name_ap4'
                };
            """)

        async with self._run_and_rollback():
            # OK, former constraint was turned into an abstract constraint
            await self.con.execute("""
                INSERT test::BecomingAbstractConstraint {
                    name := 'unique_name_ap5'
                };

                INSERT test::BecomingAbstractConstraintChild {
                    name := 'unique_name_ap5'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Constraint is no longer abstract
                await self.con.execute("""
                    INSERT test::BecomingConcreteConstraint {
                        name := 'unique_name_ap6'
                    };

                    INSERT test::BecomingConcreteConstraintChild {
                        name := 'unique_name_ap6'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Constraint is no longer abstract
                await self.con.execute("""
                    INSERT test::LosingAbstractConstraintParent {
                        name := 'unique_name_ap6'
                    };

                    INSERT test::LosingAbstractConstraintParent {
                        name := 'unique_name_ap6'
                    };
                """)

        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT test::LosingAbstractConstraintParent2 {
                    name := 'unique_name_ap7'
                };

                INSERT test::LosingAbstractConstraintParent2 {
                    name := 'unique_name_ap7'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Constraint is no longer abstract
                await self.con.execute("""
                    INSERT test::AbstractConstraintMultipleParentsFlattening{
                        name := 'unique_name_ap8'
                    };

                    INSERT test::AbstractConstraintMultipleParentsFlattening{
                        name := 'unique_name_AP8'
                    };
                """)

        async with self._run_and_rollback():
            # Parent lost its concrete constraint inheritance
            await self.con.execute("""
                INSERT test::AbstractInheritingNonAbstract {
                    name := 'unique_name_ana'
                };

                INSERT test::AbstractInheritingNonAbstract {
                    name := 'unique_name_ana'
                };
            """)

        async with self._run_and_rollback():
            # Parent lost its concrete constraint inheritance
            await self.con.execute("""
                INSERT test::AbstractInheritingNonAbstract {
                    name := 'unique_name_ana1'
                };

                INSERT test::AbstractInheritingNonAbstractChild {
                    name := 'unique_name_ana1'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Child uniqueness is still enforced
                await self.con.execute("""
                    INSERT test::AbstractInheritingNonAbstractChild{
                        name := 'unique_name_ana2'
                    };

                    INSERT test::AbstractInheritingNonAbstractChild{
                        name := 'unique_name_ana2'
                    };
                """)


class TestConstraintsDDL(tb.DDLTestCase):
    async def test_constraints_ddl_01(self):
        qry = """
            CREATE ABSTRACT LINK test::translated_label {
                SET cardinality := '1*';
                CREATE PROPERTY test::lang -> std::str;
                CREATE PROPERTY test::prop1 -> std::str;
            };

            CREATE ABSTRACT LINK test::link_with_unique_property {
                CREATE PROPERTY test::unique_property -> std::str {
                    CREATE CONSTRAINT std::unique;
                };
            };

            CREATE ABSTRACT LINK test::link_with_unique_property_inherited
                EXTENDING test::link_with_unique_property;

            CREATE TYPE test::UniqueName {
                CREATE PROPERTY test::name -> std::str {
                    CREATE CONSTRAINT std::unique;
                };

                CREATE LINK test::link_with_unique_property -> std::Object;
            };
        """

        await self.con.execute(qry)

        # Simple unique constraint on a link
        #
        async with self._run_and_rollback():
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
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
                CREATE PROPERTY test::name -> std::str {
                    CREATE DELEGATED CONSTRAINT std::unique;
                };
            };

            CREATE TYPE test::AbstractConstraintPureChild
                EXTENDING test::AbstractConstraintParent;
        """

        await self.con.execute(qry)

        async with self._run_and_rollback():
            # This is OK, the name unique constraint is abstract
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'unique_name_ap'
                };

                INSERT test::AbstractConstraintParent {
                    name := 'unique_name_ap'
                };
            """)

            # This is OK too
            await self.con.execute("""
                INSERT test::AbstractConstraintParent {
                    name := 'unique_name_ap1'
                };

                INSERT test::AbstractConstraintPureChild {
                    name := 'unique_name_ap1'
                };
            """)

    async def test_constraints_ddl_02(self):
        # testing the generalized constraint with 'ON (...)' clause
        qry = r"""
            CREATE ABSTRACT CONSTRAINT test::mymax1(std::int64)
                    ON (len(__subject__))
            {
                SET errmessage :=
                    '{__subject__} must be no longer than {$0} characters.';
                SET expr := __subject__ <= $0;
            };

            CREATE ABSTRACT CONSTRAINT test::mymax_ext1(std::int64)
                    ON (len(__subject__)) EXTENDING std::max
            {
                SET errmessage :=
                    '{__subject__} must be no longer than {$0} characters.';
            };

            CREATE TYPE test::ConstraintOnTest1 {
                CREATE PROPERTY test::foo -> std::str {
                    CREATE CONSTRAINT test::mymax1(3);
                };

                CREATE PROPERTY test::bar -> std::str {
                    CREATE CONSTRAINT test::mymax_ext1(3);
                };
            };
        """

        await self.con.execute(qry)

        # making sure the constraint was applied successfully
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.ConstraintViolationError,
                    'foo must be no longer than 3 characters.'):
                await self.con.execute("""
                    INSERT test::ConstraintOnTest1 {
                        foo := 'Test'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.ConstraintViolationError,
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
            CREATE ABSTRACT CONSTRAINT test::mymax2(std::int64) {
                SET errmessage :=
                    '{__subject__} must be no longer than {$0} characters.';
                SET expr := __subject__ <= $0;
            };

            CREATE TYPE test::ConstraintOnTest2 {
                CREATE PROPERTY test::foo -> std::str {
                    CREATE CONSTRAINT test::mymax2(3) ON (len(__subject__));
                };

                CREATE PROPERTY test::bar -> std::str {
                    CREATE CONSTRAINT std::max(3) ON (len(__subject__)) {
                        SET errmessage :=
                      # XXX: once simple string concat is possible here
                      #      formatting can be saner
                      '{__subject__} must be no longer than {$0} characters.';
                    };
                };
            };
        """

        await self.con.execute(qry)

        # making sure the constraint was applied successfully
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.ConstraintViolationError,
                    'foo must be no longer than 3 characters.'):
                await self.con.execute("""
                    INSERT test::ConstraintOnTest2 {
                        foo := 'Test'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.ConstraintViolationError,
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

    @unittest.expectedFailure
    # FIXME: the test fails because errmessage is an expression that's
    #        not a simple string literal, but a concatenation of 2
    #        string literals.
    async def test_constraints_ddl_04(self):
        # testing an issue with expressions used for 'errmessage'
        qry = r"""
            CREATE ABSTRACT CONSTRAINT test::mymax3(std::int64) {
                SET errmessage :=
                    '{__subject__} must be no longer ' +
                    'than {$0} characters.';
                SET expr := __subject__ <= $0;
            };

            CREATE TYPE test::ConstraintOnTest3 {
                CREATE PROPERTY test::foo -> std::str {
                    CREATE CONSTRAINT test::mymax3(3) ON (len(__subject__));
                };
            };
        """

        await self.con.execute(qry)

        # making sure the constraint was applied successfully
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.ConstraintViolationError,
                    'foo must be no longer than 3 characters.'):
                await self.con.execute("""
                    INSERT test::ConstraintOnTest3 {
                        foo := 'Test'
                    };
                """)

    async def test_constraints_ddl_error_01(self):
        # testing various incorrect create constraint DDL commands
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subjectexpr is not a valid constraint attribute'):
                await self.con.execute("""
                    CREATE ABSTRACT CONSTRAINT test::len_fail(std::str) {
                        SET expr := __subject__ <= $0;
                        SET subjectexpr := len(__subject__);
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subject is not a valid constraint attribute'):
                await self.con.execute("""
                    CREATE ABSTRACT CONSTRAINT test::len_fail(std::str) {
                        SET expr := __subject__ <= $0;
                        # doesn't matter what subject is set to, it's illegal
                        SET subject := len(__subject__);
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subjectexpr is not a valid constraint attribute'):
                await self.con.execute("""
                    CREATE ABSTRACT CONSTRAINT test::len_fail(std::int64) {
                        SET expr := __subject__ <= $0;
                    };

                    CREATE TYPE test::InvalidConstraintTest1 {
                        CREATE PROPERTY test::foo -> std::str {
                            CREATE CONSTRAINT test::len_fail(3) {
                                SET subjectexpr := len(__subject__);
                            };
                        };
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subject is not a valid constraint attribute'):
                await self.con.execute("""
                    CREATE ABSTRACT CONSTRAINT test::len_fail(std::int64) {
                        SET expr := __subject__ <= $0;
                    };

                    CREATE TYPE test::InvalidConstraintTest1 {
                        CREATE PROPERTY test::foo -> std::str {
                            CREATE CONSTRAINT test::len_fail(3) {
                                SET subject := len(__subject__);
                            };
                        };
                    };
                """)

    async def test_constraints_ddl_error_02(self):
        # testing that subjectexpr cannot be overridden after it is
        # specified explicitly
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.InvalidConstraintDefinitionError,
                    r"subjectexpr is already defined for .+max_int"):
                await self.con.execute(r"""
                    CREATE ABSTRACT CONSTRAINT test::max_int(std::int64)
                        ON (<int64>__subject__)
                    {
                        SET errmessage :=
                      # XXX: once simple string concat is possible here
                      #      formatting can be saner
                      '{__subject__} must be no longer than {$0} characters.';
                        SET expr := __subject__ <= $0;
                    };

                    CREATE TYPE test::InvalidConstraintTest2 {
                        CREATE PROPERTY test::foo -> std::str {
                            CREATE CONSTRAINT test::max_int(3)
                                ON (len(__subject__));
                        };
                    };
                """)

    async def test_constraints_ddl_error_03(self):
        # testing various incorrect alter constraint DDL commands
        qry = """
            CREATE ABSTRACT CONSTRAINT test::foo_alter(std::any) {
                SET errmessage := 'foo';
                SET expr := __subject__ = $0;
            };

            CREATE TYPE test::ConstraintAlterTest1 {
                CREATE PROPERTY test::value -> std::int64 {
                    CREATE CONSTRAINT std::max(3);
                };
            };
        """

        await self.con.execute(qry)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subjectexpr is not a valid constraint attribute'):
                await self.con.execute("""
                    ALTER ABSTRACT CONSTRAINT test::foo_alter {
                        SET subjectexpr := len(__subject__);
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subject is not a valid constraint attribute'):
                await self.con.execute("""
                    ALTER ABSTRACT CONSTRAINT test::foo_alter {
                        SET subject := len(__subject__);
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subjectexpr is not a valid constraint attribute'):
                await self.con.execute("""
                    ALTER TYPE test::ConstraintAlterTest1 {
                        ALTER PROPERTY test::value {
                            ALTER CONSTRAINT std::max {
                                SET subjectexpr := len(__subject__);
                            };
                        };
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subject is not a valid constraint attribute'):
                await self.con.execute("""
                    ALTER TYPE test::ConstraintAlterTest1 {
                        ALTER PROPERTY test::value {
                            ALTER CONSTRAINT std::max {
                                SET subject := len(__subject__);
                            };
                        };
                    };
                """)

    async def test_constraints_ddl_error_04(self):
        # testing various incorrect DELETE constraint DDL commands
        qry = """
            CREATE ABSTRACT CONSTRAINT test::foo_drop(std::any) ON
                    (len(__subject__))
            {
                SET errmessage := 'foo';
                SET expr := __subject__ = $0;
            };

            CREATE TYPE test::ConstraintAlterTest2 {
                CREATE PROPERTY test::value -> std::int64 {
                    CREATE CONSTRAINT std::max(3) ON (__subject__ % 10);
                };
            };
        """

        await self.con.execute(qry)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subjectexpr is not a valid constraint attribute'):
                await self.con.execute("""
                    ALTER ABSTRACT CONSTRAINT test::foo_drop {
                        DROP ATTRIBUTE subjectexpr;
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subject is not a valid constraint attribute'):
                await self.con.execute("""
                    ALTER ABSTRACT CONSTRAINT test::foo_drop {
                        DROP ATTRIBUTE subject;
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subjectexpr is not a valid constraint attribute'):
                await self.con.execute("""
                    ALTER TYPE test::ConstraintAlterTest1 {
                        ALTER PROPERTY test::value {
                            ALTER CONSTRAINT std::max {
                                DROP ATTRIBUTE subjectexpr;
                            };
                        };
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    exceptions.SchemaDefinitionError,
                    'subject is not a valid constraint attribute'):
                await self.con.execute("""
                    ALTER TYPE test::ConstraintAlterTest1 {
                        ALTER PROPERTY test::value {
                            ALTER CONSTRAINT std::max {
                                DROP ATTRIBUTE subject;
                            };
                        };
                    };
                """)

    async def test_constraints_ddl_error_05(self):
        # Test that constraint expression returns a boolean.
        qry = """
            CREATE MIGRATION test::ddl_error_05 TO eschema $$
                type User:
                    required property login -> str:
                        constraint expression on (len(__subject__))

            $$;
        """

        with self.assertRaisesRegex(
                exceptions.SchemaDefinitionError,
                "constraint expression expected to return a bool value, "
                "got 'int64'"):
            await self.con.execute(qry)

        qry = """
            CREATE TYPE User {
                CREATE REQUIRED PROPERTY login -> str {
                    CREATE CONSTRAINT expression on (len(__subject__));
                };
            };
        """

        with self.assertRaisesRegex(
                exceptions.SchemaDefinitionError,
                "constraint expression expected to return a bool value, "
                "got 'int64'"):
            await self.con.execute(qry)

        qry = """
            CREATE ABSTRACT CONSTRAINT foo {
                SET expr := __subject__;
            };

            CREATE TYPE User {
                CREATE REQUIRED PROPERTY login -> str {
                    CREATE CONSTRAINT foo;
                };
            };
        """

        with self.assertRaisesRegex(
                exceptions.SchemaDefinitionError,
                "constraint expression expected to return a bool value, "
                "got 'str'"):
            await self.con.execute(qry)
