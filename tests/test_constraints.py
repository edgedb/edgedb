##
# Copyright (c) 2012-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path

from edgedb.server import _testbase as tb
from edgedb.client import exceptions


class TestConstraints(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'constraints.eschema')

    async def _run_link_tests(self, cases, concept, link):
        qry = """
            INSERT {concept} {{{{
                {link} := {{value!r}}
            }}}};
        """.format(
            concept=concept, link=link
        )

        for val, expected in cases:
            expr = qry.format(value=val)

            if expected == 'good':
                await self.con.execute(expr)
            else:
                with self.assertRaisesRegex(
                        exceptions.ConstraintViolationError, expected):
                    await self.con.execute(expr)

    async def test_constraints_atom_length(self):
        data = {
            # max-length is 10
            (10 ** 10, 'must be no longer than 10 characters.'),
            (10 ** 10 - 1, 'good'),
            (10 ** 7 - 1, 'must be no shorter than 8 characters'),
            (10 ** 7, 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'test::c_length')

        data = {
            (10 ** 10, 'must be no longer than 10 characters.'),
            (10 ** 10 - 1, 'good'),

            (10 ** 8 - 1, 'must be no shorter than 9 characters'),
            (10 ** 8, 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'test::c_length_2')

        data = {
            (10 ** 10, 'must be no longer than 10 characters.'),
            (10 ** 10 - 1, 'good'),
            (10 ** 9 - 1, 'must be no shorter than 10 characters'),
        }

        await self._run_link_tests(data, 'test::Object', 'test::c_length_3')

    async def test_constraints_atom_minmax(self):
        data = {
            # max-value is "9999999989"
            (10 ** 9 - 1, 'Maximum allowed value for .* is 9999999989.'),
            (10 ** 9 - 11, 'good'),

            # min-value is "99990000"
            (10 ** 8 - 10 ** 4 - 1,
             'Minimum allowed value for .* is 99990000.'),
            (10 ** 8 - 21, 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'test::c_minmax')

    async def test_constraints_atom_strvalue(self):
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

        await self._run_link_tests(data, 'test::Object', 'test::c_strvalue')

    async def test_constraints_atom_enum(self):
        data = {
            ('foobar', 'must be one of:'),
            ('bar', 'good'),
            ('foo', 'good'),
        }

        await self._run_link_tests(data, 'test::Object', 'test::c_enum')

    async def test_constraints_unique_simple(self):
        tx = self.con.transaction()
        try:
            await tx.start()
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                await self.con.execute("""
                    INSERT {test::UniqueName} {
                        name := 'Test'
                    };

                    INSERT {test::UniqueName} {
                        name := 'Test'
                    };
                """)
        finally:
            await tx.rollback()

    async def test_constraints_unique_inherited(self):
        tx = self.con.transaction()
        try:
            await tx.start()
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                await self.con.execute("""
                    INSERT {test::UniqueNameInherited} {
                        name := 'Test'
                    };

                    INSERT {test::UniqueNameInherited} {
                        name := 'Test'
                    };
                """)
        finally:
            await tx.rollback()

    async def test_constraints_unique_across_ancestry(self):
        tx = self.con.transaction()
        try:
            await tx.start()
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):

                await self.con.execute("""
                    INSERT {test::UniqueName} {
                        name := 'unique_name_across'
                    };

                    INSERT {test::UniqueNameInherited} {
                        name := 'unique_name_across'
                    };
                """)

        finally:
            await tx.rollback()

        tx = self.con.transaction()
        try:
            await tx.start()

            await self.con.execute("""
                INSERT {test::UniqueName} {
                    name := 'unique_name_ok'
                };

                INSERT {test::UniqueNameInherited} {
                    name := 'unique_name_inherited_ok'
                };
            """)

            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                await self.con.execute("""
                    UPDATE
                        {test::UniqueNameInherited} {
                            name := 'unique_name_ok'
                        }
                    WHERE
                        {test::UniqueNameInherited}.name =
                            'unique_name_inherited_ok';
                """)

        finally:
            await tx.rollback()

    async def test_constraints_unique_case_insensitive(self):
        tx = self.con.transaction()
        try:
            await tx.start()
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                await self.con.execute("""
                    INSERT {test::UniqueName_3} {
                        name := 'TeSt'
                    };

                    INSERT {test::UniqueName_3} {
                        name := 'tEsT'
                    };
                """)
        finally:
            await tx.rollback()

    async def test_constraints_unique_abstract(self):
        tx = self.con.transaction()
        try:
            await tx.start()
            # This is OK, the name unique constraint is abstract
            await self.con.execute("""
                INSERT {test::AbstractConstraintParent} {
                    name := 'unique_name_ap'
                };

                INSERT {test::AbstractConstraintParent} {
                    name := 'unique_name_ap'
                };
            """)

            # This is OK too
            await self.con.execute("""
                INSERT {test::AbstractConstraintParent} {
                    name := 'unique_name_ap1'
                };

                INSERT {test::AbstractConstraintPureChild} {
                    name := 'unique_name_ap1'
                };
            """)

        finally:
            await tx.rollback()

        tx = self.con.transaction()
        try:
            await tx.start()
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT {test::AbstractConstraintPureChild} {
                        name := 'unique_name_ap2'
                    };

                    INSERT {test::AbstractConstraintPureChild} {
                        name := 'unique_name_ap2'
                    };
                """)

        finally:
            await tx.rollback()

        tx = self.con.transaction()
        try:
            await tx.start()
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT {test::AbstractConstraintMixedChild} {
                        name := 'unique_name_ap2'
                    };

                    INSERT {test::AbstractConstraintMixedChild} {
                        name := 'unique_name_AP2'
                    };
                """)

        finally:
            await tx.rollback()

        tx = self.con.transaction()
        try:
            await tx.start()
            # This is OK, duplication is in different children
            await self.con.execute("""
                INSERT {test::AbstractConstraintPureChild} {
                    name := 'unique_name_ap3'
                };

                INSERT {test::AbstractConstraintMixedChild} {
                    name := 'unique_name_ap3'
                };
            """)

            # This is OK, the name unique constraint is abstract again
            await self.con.execute("""
                INSERT {test::AbstractConstraintPropagated} {
                    name := 'unique_name_ap4'
                };

                INSERT {test::AbstractConstraintPropagated} {
                    name := 'unique_name_ap4'
                };
            """)

        finally:
            await tx.rollback()

        tx = self.con.transaction()
        try:
            await tx.start()
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # Not OK, yet
                await self.con.execute("""
                    INSERT {test::BecomingAbstractConstraint} {
                        name := 'unique_name_ap5'
                    };

                    INSERT {test::BecomingAbstractConstraintChild} {
                        name := 'unique_name_ap5'
                    };
                """)

        finally:
            await tx.rollback()

        tx = self.con.transaction()
        try:
            await tx.start()
            await self.con.execute("""
                INSERT {test::BecomingConcreteConstraint} {
                    name := 'unique_name_ap6'
                };

                INSERT {test::BecomingConcreteConstraintChild} {
                    name := 'unique_name_ap6'
                };
            """)

        finally:
            await tx.rollback()

        tx = self.con.transaction()
        try:
            await tx.start()
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                await self.con.execute("""
                    INSERT {test::LosingAbstractConstraintParent} {
                        name := 'unique_name_ap7'
                    };

                    INSERT {test::LosingAbstractConstraintParent} {
                        name := 'unique_name_ap7'
                    };
                """)

        finally:
            await tx.rollback()

        tx = self.con.transaction()
        try:
            await tx.start()
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                await self.con.execute("""
                    INSERT {test::AbstractConstraintMultipleParentsFlattening}{
                        name := 'unique_name_ap8'
                    };

                    INSERT {test::AbstractConstraintMultipleParentsFlattening}{
                        name := 'unique_name_ap8'
                    };
                """)

        finally:
            await tx.rollback()

        tx = self.con.transaction()
        try:
            await tx.start()
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # non-abstract inherited constraint
                await self.con.execute("""
                    INSERT {test::AbstractInheritingNonAbstract} {
                        name := 'unique_name_ana'
                    };

                    INSERT {test::AbstractInheritingNonAbstract} {
                        name := 'unique_name_ana'
                    };
                """)

        finally:
            await tx.rollback()

        tx = self.con.transaction()
        try:
            await tx.start()
            with self.assertRaisesRegex(exceptions.ConstraintViolationError,
                                        'name violates unique constraint'):
                # non-abstract inherited constraint
                await self.con.execute("""
                    INSERT {test::AbstractInheritingNonAbstract} {
                        name := 'unique_name_ana1'
                    };

                    INSERT {test::AbstractInheritingNonAbstractChild} {
                        name := 'unique_name_ana1'
                    };
                """)

        finally:
            await tx.rollback()
