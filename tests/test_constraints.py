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

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'constraints.esdl')

    async def _run_link_tests(self, cases, objtype, link, *,
                              values_as_str=True):
        qry = f"""
            INSERT {objtype} {{{{
                {link} := {{value}}
            }}}};
        """

        for val, expected in cases:
            async with self._run_and_rollback():
                if values_as_str:
                    expr = qry.format(value=f'{str(val)!r}')
                else:
                    expr = qry.format(value=val)

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

        await self._run_link_tests(data, 'default::Object', 'c_length')

        data = {
            (10 ** 10,
             'constraint_length must be no longer than 10 characters.'),
            (10 ** 10 - 1, 'good'),

            (10 ** 8 - 1,
             'constraint_length_2 must be no shorter than 9 characters'),
            (10 ** 8, 'good'),
        }

        await self._run_link_tests(data, 'default::Object', 'c_length_2')

        data = {
            (10 ** 10,
             'constraint_length must be no longer than 10 characters.'),
            (10 ** 10 - 1, 'good'),
            (10 ** 9 - 1, 'c_length_3 must be no shorter than 10 characters'),
        }

        await self._run_link_tests(data, 'default::Object', 'c_length_3')

    async def test_constraints_scalar_minmax_01(self):
        data = {
            # max-value is "9999999989"
            (10 ** 9 - 1, "Maximum allowed value for .* is '9999999989'."),
            (10 ** 9 - 11, 'good'),

            # min-value is "99990000"
            (10 ** 8 - 10 ** 4 - 1,
             "Minimum allowed value for .* is '99990000'."),
            (10 ** 8 - 21, 'good'),
        }

        await self._run_link_tests(data, 'default::Object', 'c_minmax')

    async def test_constraints_scalar_minmax_02(self):
        data = {
            # exclusive max-value is "100"
            (1000, ".* must be less than 100."),
            (100, ".* must be less than 100."),
            (99.9999, 'good'),
            (99, 'good'),

            # exclusive min-value is "13"
            (56, 'good'),
            (13.0001, "good"),
            (13, ".* must be greater than 13."),
            (0, ".* must be greater than 13."),
        }

        await self._run_link_tests(data, 'default::Object', 'c_ex_minmax',
                                   values_as_str=False)

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

        await self._run_link_tests(data, 'default::Object', 'c_strvalue')

    async def test_constraints_scalar_enum_01(self):
        data = {
            ('foobar', 'must be one of:'),
            ('bar', 'good'),
            ('foo', 'good'),
        }

        await self._run_link_tests(data, 'default::Object', 'c_enum')

    async def test_constraints_scalar_enum_02(self):
        data = {
            ('foo', 'invalid'),
            ('fuz', 'good'),
            ('buz', 'good'),
        }

        await self._run_link_tests(data, 'default::Object', 'c_my_enum')

    async def test_constraints_exclusive_simple(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    INSERT UniqueName {
                        name := 'Test'
                    };

                    INSERT UniqueName {
                        name := 'Test'
                    };
                """)

    async def test_constraints_exclusive_inherited(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    INSERT UniqueNameInherited {
                        name := 'Test'
                    };

                    INSERT UniqueNameInherited {
                        name := 'Test'
                    };
                """)

    async def test_constraints_exclusive_across_ancestry(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):

                await self.con.execute("""
                    INSERT UniqueName {
                        name := 'exclusive_name_across'
                    };

                    INSERT UniqueNameInherited {
                        name := 'exclusive_name_across'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):

                await self.con.execute("""
                    INSERT UniqueNameInherited {
                        name := 'exclusive_name_across'
                    };

                    INSERT UniqueName {
                        name := 'exclusive_name_across'
                    };
                """)

        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT UniqueName {
                    name := 'exclusive_name_ok'
                };

                INSERT UniqueNameInherited {
                    name := 'exclusive_name_inherited_ok'
                };
            """)

            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    UPDATE
                        UniqueNameInherited
                    FILTER
                        UniqueNameInherited.name =
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
                    INSERT UniqueName_3 {
                        name := 'TeSt'
                    };

                    INSERT UniqueName_3 {
                        name := 'tEsT'
                    };
                """)

    async def test_constraints_exclusive_delegation(self):
        async with self._run_and_rollback():
            # This is OK, the name exclusivity constraint is delegating
            await self.con.execute("""
                INSERT AbstractConstraintParent {
                    name := 'exclusive_name_ap'
                };

                INSERT AbstractConstraintParent {
                    name := 'exclusive_name_ap'
                };
            """)

            # This is OK too
            await self.con.execute("""
                INSERT AbstractConstraintParent {
                    name := 'exclusive_name_ap1'
                };

                INSERT AbstractConstraintPureChild {
                    name := 'exclusive_name_ap1'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT AbstractConstraintPureChild {
                        name := 'exclusive_name_ap2'
                    };

                    INSERT AbstractConstraintPureChild {
                        name := 'exclusive_name_ap2'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT AbstractConstraintMixedChild {
                        name := 'exclusive_name_ap2'
                    };

                    INSERT AbstractConstraintMixedChild {
                        name := 'exclusive_name_AP2'
                    };
                """)

        async with self._run_and_rollback():
            # This is OK, duplication is in different children
            await self.con.execute("""
                INSERT AbstractConstraintPureChild {
                    name := 'exclusive_name_ap3'
                };

                INSERT AbstractConstraintMixedChild {
                    name := 'exclusive_name_ap3'
                };
            """)

            # This is OK, the name exclusivity constraint is abstract again
            await self.con.execute("""
                INSERT AbstractConstraintPropagated {
                    name := 'exclusive_name_ap4'
                };

                INSERT AbstractConstraintPropagated {
                    name := 'exclusive_name_ap4'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Not OK, yet
                await self.con.execute("""
                    INSERT BecomingAbstractConstraint {
                        name := 'exclusive_name_ap5'
                    };

                    INSERT BecomingAbstractConstraintChild {
                        name := 'exclusive_name_ap5'
                    };
                """)

        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT BecomingConcreteConstraint {
                    name := 'exclusive_name_ap6'
                };

                INSERT BecomingConcreteConstraintChild {
                    name := 'exclusive_name_ap6'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    INSERT LosingAbstractConstraintParent {
                        name := 'exclusive_name_ap7'
                    };

                    INSERT LosingAbstractConstraintParent {
                        name := 'exclusive_name_ap7'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                await self.con.execute("""
                    INSERT AbstractConstraintMultipleParentsFlattening{
                        name := 'exclusive_name_ap8'
                    };

                    INSERT AbstractConstraintMultipleParentsFlattening{
                        name := 'exclusive_name_ap8'
                    };
                """)

    async def test_constraints_exclusive_pair(self):
        await self.assert_query_result(
            r'''
                select {
                    single z := (
                        select Pair {x, y} filter .x = 'a' and .y = 'b')
                }
            ''',
            [
                {"z": None}
            ],
        )

    async def test_constraints_exclusive_multi_property_distinct(self):
        await self.con.execute("""
            INSERT PropertyContainer {
                tags := {"one", "two"}
            };
        """)

        # Update to same values should be fine
        await self.con.execute("""
            UPDATE PropertyContainer SET {
                tags := {"one", "two"}
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "tags violates exclusivity constraint",
        ):
            await self.con.execute("""
                INSERT PropertyContainer {
                    tags := {"one", "three"}
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "tags violates exclusivity constraint",
        ):
            await self.con.execute("""
                INSERT PropertyContainer {
                    tags := {"four", "four"}
                };
            """)

        await self.con.execute("""
            UPDATE PropertyContainer SET {
                tags := "one"
            };
        """)

    async def test_constraints_objects(self):
        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    "ObjCnstr violates exclusivity constraint"):
                await self.con.execute("""
                    INSERT ObjCnstr {
                        first_name := "foo", last_name := "bar" };

                    INSERT ObjCnstr {
                        first_name := "foo", last_name := "baz" }
            """)

        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT ObjCnstr {
                    first_name := "foo", last_name := "bar",
                    label := (INSERT Label {text := "obj_test" })
                };
            """)

            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    "ObjCnstr violates exclusivity constraint"):
                await self.con.execute("""
                    INSERT ObjCnstr {
                        first_name := "emarg", last_name := "hatch",
                        label := (SELECT Label
                                  FILTER .text = "obj_test" LIMIT 1) };
                """)

    async def test_constraints_endpoint_constraint_01(self):
        # testing (@source, @lang) on a single link
        # This constraint is pointless and can never fail
        await self.con.execute("""
            insert UniqueName {
                translated_label := ((insert Label) { @lang := 'xxx' })
            };
        """)

        await self.con.execute("""
            insert UniqueName {
                translated_label := ((insert Label) { @lang := 'xxx' })
            };
        """)

        await self.con.execute("""
            insert UniqueName {
                translated_label := ((select Label limit 1) { @lang := 'yyy' })
            };
        """)

        await self.con.execute("""
            insert UniqueName {
                translated_label := ((select Label limit 1) { @lang := 'xxx' })
            };
        """)

    async def test_constraints_endpoint_constraint_02(self):
        # testing (@source, @lang) on a multi link
        await self.con.execute("""
            insert UniqueName {
                translated_labels := ((insert Label { text := "x" }) {
                 @lang := 'x' })
            };
        """)

        await self.con.execute("""
            update UniqueName set {
                translated_labels := Label {@lang := 'x' }
            };
        """)

        # Should be fine
        await self.con.execute("""
            insert UniqueName {
                translated_labels := ((insert Label { text := "y"  }) {
                  @lang := 'x' })
            };
        """)

        await self.con.execute("""
            insert UniqueName {
                translated_labels := (Label { @lang := .text })
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'violates exclusivity constraint'
        ):
            await self.con.execute("""
                insert UniqueName {
                    translated_labels := (Label { @lang := 'x' })
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'violates exclusivity constraint'
        ):
            await self.con.execute("""
                insert UniqueNameInherited {
                    translated_labels := (Label { @lang := 'x' })
                };
            """)

    async def test_constraints_endpoint_constraint_03(self):
        # testing (@target, @lang) on a single link
        await self.con.execute("""
            insert UniqueName {
                translated_label_tgt := ((insert Label { text := "x" }) {
                 @lang := 'x' })
            };
        """)

        # Same @lang different @target
        await self.con.execute("""
            insert UniqueName {
                translated_label_tgt := ((insert Label { text := "y" }) {
                  @lang := 'x' })
            };
        """)

        # Same @target different @lang
        await self.con.execute("""
            insert UniqueNameInherited {
                translated_label_tgt := (
                  select Label { @lang := 'y' } filter .text = 'x' limit 1)
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'violates exclusivity constraint'
        ):
            await self.con.execute("""
                insert UniqueName {
                    translated_label_tgt := (
                      select Label { @lang := 'x' } filter .text = 'x' limit 1)
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'violates exclusivity constraint'
        ):
            await self.con.execute("""
                insert UniqueNameInherited {
                    translated_label_tgt := (
                      select Label { @lang := 'x' } filter .text = 'x' limit 1)
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'violates exclusivity constraint'
        ):
            await self.con.execute("""
                update UniqueName
                filter .translated_label_tgt.text = 'x'
                set { translated_label_tgt :=
                    .translated_label_tgt { @lang := '!' }
                }
            """)

        await self.con.execute("""
            update UniqueName
            filter .translated_label_tgt.text = 'x'
            set { translated_label_tgt :=
                .translated_label_tgt { @lang := @lang }
            }
        """)

    async def test_constraints_endpoint_constraint_04(self):
        # testing (@target, @lang) on a multi link
        await self.con.execute("""
            insert UniqueName {
                translated_labels_tgt := ((insert Label { text := "x" }) {
                 @lang := 'x' })
            };
        """)

        # Same @lang different @target
        await self.con.execute("""
            insert UniqueName {
                translated_labels_tgt := ((insert Label { text := "y" }) {
                  @lang := 'x' })
            };
        """)

        # Same @target different @lang
        await self.con.execute("""
            insert UniqueName {
                translated_labels_tgt := (
                  select Label { @lang := 'y' } filter .text = 'x')
            };
        """)

        await self.con.execute("""
            insert UniqueNameInherited {
                translated_labels_tgt := (
                  select Label { @lang := 'x!' })
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'violates exclusivity constraint'
        ):
            await self.con.execute("""
                insert UniqueName {
                    translated_labels_tgt := (
                      select Label { @lang := 'x' } filter .text = 'x')
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'violates exclusivity constraint'
        ):
            await self.con.execute("""
                insert UniqueNameInherited {
                    translated_labels_tgt := (
                      select Label { @lang := 'x' } filter .text = 'x')
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'violates exclusivity constraint'
        ):
            await self.con.execute("""
                insert UniqueNameInherited {
                    translated_labels_tgt := (
                      select Label { @lang := .text ++ '!' }
                      filter .text = 'x')
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'violates exclusivity constraint'
        ):
            await self.con.execute("""
                update UniqueName
                filter 'x' in .translated_labels_tgt.text
                set { translated_labels_tgt :=
                    .translated_labels_tgt { @lang := '!' }
                }
            """)

        await self.con.execute("""
            update UniqueName
            filter 'x' in .translated_labels_tgt.text
            set { translated_labels_tgt :=
                .translated_labels_tgt { @lang := @lang }
            }
        """)


class TestConstraintsSchemaMigration(tb.QueryTestCase):

    SCHEMA = os.path.join(os.path.dirname(__file__),
                          'schemas', 'constraints_migration',
                          'schema.esdl')

    async def test_constraints_exclusive_migration(self):
        new_schema_f = os.path.join(os.path.dirname(__file__),
                                    'schemas', 'constraints_migration',
                                    'updated_schema.esdl')

        with open(new_schema_f) as f:
            new_schema = f.read()

        await self.migrate(new_schema)

        async with self._run_and_rollback():
            # This is OK, the name exclusivity constraint is abstract
            await self.con.execute("""
                INSERT AbstractConstraintParent {
                    name := 'exclusive_name_ap'
                };

                INSERT AbstractConstraintParent {
                    name := 'exclusive_name_ap'
                };
            """)

            # This is OK too
            await self.con.execute("""
                INSERT AbstractConstraintParent {
                    name := 'exclusive_name_ap1'
                };

                INSERT AbstractConstraintPureChild {
                    name := 'exclusive_name_ap1'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT AbstractConstraintPureChild {
                        name := 'exclusive_name_ap2'
                    };

                    INSERT AbstractConstraintPureChild {
                        name := 'exclusive_name_ap2'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Not OK, abstract constraint materializes into a real one
                await self.con.execute("""
                    INSERT AbstractConstraintMixedChild {
                        name := 'exclusive_name_ap2'
                    };

                    INSERT AbstractConstraintMixedChild {
                        name := 'exclusive_name_AP2'
                    };
                """)

        async with self._run_and_rollback():
            # This is OK, duplication is in different children
            await self.con.execute("""
                INSERT AbstractConstraintMixedChild {
                    name := 'exclusive_name_ap3'
                };

                INSERT AbstractConstraintPureChild {
                    name := 'exclusive_name_ap3'
                };
            """)

        async with self._run_and_rollback():
            # This is OK, the name exclusivity constraint is abstract again
            await self.con.execute("""
                INSERT AbstractConstraintPropagated {
                    name := 'exclusive_name_ap4'
                };

                INSERT AbstractConstraintPropagated {
                    name := 'exclusive_name_ap4'
                };
            """)

        async with self._run_and_rollback():
            # OK, former constraint was turned into an abstract constraint
            await self.con.execute("""
                INSERT BecomingAbstractConstraint {
                    name := 'exclusive_name_ap5'
                };

                INSERT BecomingAbstractConstraintChild {
                    name := 'exclusive_name_ap5'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Constraint is no longer abstract
                await self.con.execute("""
                    INSERT BecomingConcreteConstraint {
                        name := 'exclusive_name_ap6'
                    };

                    INSERT BecomingConcreteConstraintChild {
                        name := 'exclusive_name_ap6'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Constraint is no longer abstract
                await self.con.execute("""
                    INSERT LosingAbstractConstraintParent {
                        name := 'exclusive_name_ap6'
                    };

                    INSERT LosingAbstractConstraintParent {
                        name := 'exclusive_name_ap6'
                    };
                """)

        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT LosingAbstractConstraintParent2 {
                    name := 'exclusive_name_ap7'
                };

                INSERT LosingAbstractConstraintParent2 {
                    name := 'exclusive_name_ap7'
                };
            """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'name violates exclusivity constraint'):
                # Constraint is no longer abstract
                await self.con.execute("""
                    INSERT AbstractConstraintMultipleParentsFlattening{
                        name := 'exclusive_name_ap8'
                    };

                    INSERT AbstractConstraintMultipleParentsFlattening{
                        name := 'exclusive_name_AP8'
                    };
                """)

        async with self._run_and_rollback():
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    "nope!"):
                await self.con.execute("""
                    INSERT ObjCnstr {
                        first_name := "foo", last_name := "bar" };

                    INSERT ObjCnstr {
                        first_name := "foo", last_name := "baz" }
            """)


class TestConstraintsDDL(tb.DDLTestCase):

    async def test_constraints_ddl_01(self):
        qry = """
            CREATE ABSTRACT LINK translated_label {
                CREATE PROPERTY lang -> std::str;
                CREATE PROPERTY prop1 -> std::str;
            };

            CREATE ABSTRACT LINK link_with_exclusive_property {
                CREATE PROPERTY exclusive_property -> std::str {
                    CREATE CONSTRAINT std::exclusive;
                };
            };

            CREATE ABSTRACT LINK link_with_exclusive_property_inherited
                EXTENDING link_with_exclusive_property;

            CREATE TYPE UniqueName {
                CREATE PROPERTY name -> std::str {
                    CREATE CONSTRAINT std::exclusive;
                };

                CREATE LINK link_with_exclusive_property -> std::Object;
            };
        """

        await self.con.execute(qry)

        # Simple exclusivity constraint on a link
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'name violates exclusivity constraint',
        ):
            await self.con.execute("""
                INSERT UniqueName {
                    name := 'Test'
                };

                INSERT UniqueName {
                    name := 'Test'
                };
            """)

        qry = """
            CREATE TYPE AbstractConstraintParent {
                CREATE PROPERTY name -> std::str {
                    CREATE DELEGATED CONSTRAINT std::exclusive;
                };
            };

            CREATE TYPE AbstractConstraintPureChild
                EXTENDING AbstractConstraintParent;
        """

        await self.con.execute(qry)

        # This is OK, the name exclusivity constraint is abstract
        await self.con.execute("""
            INSERT AbstractConstraintParent {
                name := 'exclusive_name_ap'
            };

            INSERT AbstractConstraintParent {
                name := 'exclusive_name_ap'
            };
        """)

        # This is OK too
        await self.con.execute("""
            INSERT AbstractConstraintParent {
                name := 'exclusive_name_ap1'
            };

            INSERT AbstractConstraintPureChild {
                name := 'exclusive_name_ap1'
            };
        """)

    async def test_constraints_ddl_02(self):
        # testing the generalized constraint with 'ON (...)' clause
        qry = r"""
            CREATE ABSTRACT CONSTRAINT mymax1(max: std::int64)
                    ON (len(__subject__))
            {
                SET errmessage :=
                    '{__subject__} must be no longer than {max} characters.';
                USING (__subject__ <= max);
            };

            CREATE ABSTRACT CONSTRAINT mymax_ext1(max: std::int64)
                    ON (len(__subject__)) EXTENDING std::max_value
            {
                SET errmessage :=
                    '{__subject__} must be no longer than {max} characters.';
            };

            CREATE TYPE ConstraintOnTest1 {
                CREATE PROPERTY foo -> std::str {
                    CREATE CONSTRAINT mymax1(3);
                };

                CREATE PROPERTY bar -> std::str {
                    CREATE CONSTRAINT mymax_ext1(3);
                };
            };
        """

        await self.con.execute(qry)

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
                        typemod,
                        @value
                    }
                    FILTER .num > 0
                    ORDER BY .num ASC
                } FILTER
                    .name = 'default::mymax_ext1'
                    AND exists(.subject);
            ''',
            [
                {
                    "name": 'default::mymax_ext1',
                    "params": [
                        {
                            "num": 1,
                            "kind": 'PositionalParam',
                            "name": 'max',
                            "type": {"name": 'std::int64'},
                            "@value": '3',
                            "typemod": 'SingletonType'
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
                } FILTER
                    .name = 'default::mymax_ext1'
                    AND NOT exists(.subject);
            ''',
            [
                {
                    "name": 'default::mymax_ext1',
                    "params": [
                        {
                            "num": 1,
                            "kind": 'PositionalParam',
                            "name": 'max',
                            "type": {"name": 'std::int64'},
                            "typemod": 'SingletonType'
                        }
                    ],
                },
            ]
        )

        # making sure the constraint was applied successfully
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'foo must be no longer than 3 characters.',
        ):
            await self.con.execute("""
                INSERT ConstraintOnTest1 {
                    foo := 'Test'
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'bar must be no longer than 3 characters.',
        ):
            await self.con.execute("""
                INSERT ConstraintOnTest1 {
                    bar := 'Test'
                };
            """)

        # constraint should not fail
        await self.con.execute("""
            INSERT ConstraintOnTest1 {
                foo := '',
                bar := ''
            };

            INSERT ConstraintOnTest1 {
                foo := 'a',
                bar := 'q'
            };

            INSERT ConstraintOnTest1 {
                foo := 'ab',
                bar := 'qw'
            };

            INSERT ConstraintOnTest1 {
                foo := 'abc',
                bar := 'qwe'
            };

            # a duplicate 'foo' and 'bar' just for good measure
            INSERT ConstraintOnTest1 {
                foo := 'ab',
                bar := 'qw'
            };
        """)

    async def test_constraints_ddl_03(self):
        # testing the specialized constraint with 'ON (...)' clause
        qry = r"""
            CREATE ABSTRACT CONSTRAINT mymax2(max: std::int64) {
                SET errmessage :=
                    '{__subject__} must be no longer than {max} characters.';
                USING (__subject__ <= max);
            };

            CREATE TYPE ConstraintOnTest2 {
                CREATE PROPERTY foo -> std::str {
                    CREATE CONSTRAINT mymax2(3) ON (len(__subject__));
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
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'foo must be no longer than 3 characters.',
        ):
            await self.con.execute("""
                INSERT ConstraintOnTest2 {
                    foo := 'Test'
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'bar must be no longer than 3 characters.',
        ):
            await self.con.execute("""
                INSERT ConstraintOnTest2 {
                    bar := 'Test'
                };
            """)

        # constraint should not fail
        await self.con.execute("""
            INSERT ConstraintOnTest2 {
                foo := '',
                bar := ''
            };

            INSERT ConstraintOnTest2 {
                foo := 'a',
                bar := 'q'
            };

            INSERT ConstraintOnTest2 {
                foo := 'ab',
                bar := 'qw'
            };

            INSERT ConstraintOnTest2 {
                foo := 'abc',
                bar := 'qwe'
            };

            # a duplicate 'foo' and 'bar' just for good measure
            INSERT ConstraintOnTest2 {
                foo := 'ab',
                bar := 'qw'
            };
        """)

    async def test_constraints_ddl_04(self):
        # testing an issue with expressions used for 'errmessage'
        qry = r"""
            CREATE ABSTRACT CONSTRAINT mymax3(max: std::int64) {
                SET errmessage :=
                    'My custom ' ++ 'message.';
                USING (__subject__ <= max);
            };

            CREATE TYPE ConstraintOnTest3 {
                CREATE PROPERTY foo -> std::str {
                    CREATE CONSTRAINT mymax3(3) ON (len(__subject__));
                };
            };
        """

        await self.con.execute(qry)

        # making sure the constraint was applied successfully
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'My custom message.',
            _details="violated constraint 'default::mymax3' on "
            "property 'foo' of "
            "object type 'default::ConstraintOnTest3'",
        ):
            await self.con.execute(
                """
                INSERT ConstraintOnTest3 {
                    foo := 'Test'
                };
                """
            )

        # testing interpolation
        await self.con.execute(r"""
            CREATE type ConstraintOnTest4_2 {
                CREATE required property email -> str {
                    CREATE constraint min_len_value(4) {
                        SET errmessage := '{"json": "{nope} {{min}} {min}"}';
                    };
                };
            };
        """)
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            '{"json": "{nope} {min} 4"}',
            _details="violated constraint 'std::min_len_value' on "
            "property 'email' of "
            "object type 'default::ConstraintOnTest4_2'",
        ):
            await self.con.execute(
                """
                INSERT ConstraintOnTest4_2 { email := '' };
                """
            )

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
            CREATE TYPE ConstraintOnTest5 {
                CREATE REQUIRED PROPERTY foo -> int64 {
                    # Use the function in a constraint expression,
                    # s.t. it will effectively fail for any int
                    # outside 0-9 range.
                    CREATE CONSTRAINT
                        std::expression on (len(con05(__subject__)) < 2);
                }
            }
        """)

        async with self.assertRaisesRegexTx(
            edgedb.errors.ConstraintViolationError,
            r'invalid foo',
        ):
            await self.con.execute("""
                INSERT ConstraintOnTest5 {
                    foo := 42
                };
            """)

        # constraint should not fail
        await self.con.execute("""
            INSERT ConstraintOnTest5 {
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
            CREATE TYPE ConstraintOnTest6 {
                CREATE REQUIRED PROPERTY foo -> int64 {
                    # Use the function in a constraint expression,
                    # s.t. it will never fail.
                    CREATE CONSTRAINT
                        std::expression on (len(con06(__subject__)) < 2);
                }
            }
        """)

        # constraint should not fail
        await self.con.execute("""
            INSERT ConstraintOnTest6 {
                foo := 42
            };
        """)

        await self.con.execute("""
            INSERT ConstraintOnTest6 {
                foo := 2
            };
        """)

    async def test_constraints_ddl_07(self):
        await self.con.execute("""
            CREATE TYPE ObjCnstr {
                CREATE PROPERTY first_name -> str;
                CREATE PROPERTY last_name -> str;
                CREATE CONSTRAINT exclusive on (__subject__.first_name);
            };
        """)

        await self.con.execute("""
            INSERT ObjCnstr { first_name := "foo", last_name := "bar" }
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "ObjCnstr violates exclusivity constraint",
        ):
            await self.con.execute("""
                INSERT ObjCnstr {
                    first_name := "foo", last_name := "baz" }
            """)

        await self.con.execute("""
            ALTER TYPE ObjCnstr {
                DROP CONSTRAINT exclusive on (__subject__.first_name);
            }
        """)

        await self.con.execute("""
            ALTER TYPE ObjCnstr {
                CREATE CONSTRAINT exclusive
                on ((__subject__.first_name, __subject__.last_name));
            }
        """)

        await self.con.execute("""
            ALTER TYPE ObjCnstr {
                ALTER CONSTRAINT exclusive
                on ((__subject__.first_name, __subject__.last_name)) {
                    SET errmessage := "nope!";
                }
            }
        """)

        # This one should work now
        await self.con.execute("""
            INSERT ObjCnstr { first_name := "foo", last_name := "baz" }
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "nope!",
        ):
            await self.con.execute("""
                INSERT ObjCnstr {
                    first_name := "foo", last_name := "bar" }
            """)

    async def test_constraints_ddl_08(self):
        await self.con.execute("""
            CREATE TYPE ObjCnstr2 {
                CREATE MULTI PROPERTY first_name -> str;
                CREATE MULTI PROPERTY last_name -> str;
                CREATE LINK foo -> Object {
                    CREATE PROPERTY p -> str;
                };
                CREATE CONSTRAINT exclusive on (__subject__.first_name);
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidConstraintDefinitionError,
            "cannot reference multiple links or properties in a "
            "constraint where at least one link or property is MULTI",
        ):
            await self.con.execute("""
                ALTER TYPE ObjCnstr2 {
                    CREATE CONSTRAINT exclusive
                    on ((__subject__.first_name, __subject__.last_name));
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "cannot use SET OF operator 'std::EXISTS' "
            "in a constraint",
        ):
            await self.con.execute("""
                ALTER TYPE ObjCnstr2 {
                    CREATE CONSTRAINT expression on (EXISTS .first_name);
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "cannot use SET OF operator 'std::EXISTS' "
            "in a constraint",
        ):
            await self.con.execute("""
                ALTER TYPE ObjCnstr2 {
                    ALTER PROPERTY first_name {
                        CREATE CONSTRAINT expression on (EXISTS __subject__);
                    }
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidConstraintDefinitionError,
            "constraints cannot contain paths with more than one hop",
        ):
            await self.con.execute("""
                ALTER TYPE ObjCnstr2 {
                    CREATE CONSTRAINT expression on (<str>.foo.id != 'lol');
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidConstraintDefinitionError,
            "link constraints may not access the link target",
        ):
            await self.con.execute("""
                ALTER TYPE ObjCnstr2 {
                    ALTER LINK foo {
                        CREATE CONSTRAINT expression on (
                            <str>__subject__.id != 'lol');
                    }
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidConstraintDefinitionError,
            "constraint expressions must be immutable",
        ):
            await self.con.execute("""
                ALTER TYPE ObjCnstr2 {
                    CREATE CONSTRAINT expression on (<str>.id != .foo@p);
                };
            """)

    async def test_constraints_ddl_09(self):
        await self.con.execute("""
            CREATE TYPE Label {
                CREATE PROPERTY text -> str;
            };
            CREATE TYPE ObjCnstr3 {
                CREATE LINK label -> Label;
                CREATE CONSTRAINT exclusive on (__subject__.label);
            };
            INSERT ObjCnstr3 {
                label := (INSERT Label {text := "obj_test" })
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "ObjCnstr3 violates exclusivity constraint",
        ):
            await self.con.execute("""
                INSERT ObjCnstr3 {
                    label := (SELECT Label
                                FILTER .text = "obj_test" LIMIT 1) };
            """)

    async def test_constraints_ddl_10(self):
        await self.con.execute(r"""
            CREATE ABSTRACT CONSTRAINT mymax5(max: std::int64) {
                USING (__subject__ <= max);
            };

            CREATE TYPE ConstraintTest10 {
                CREATE PROPERTY foo -> std::int64 {
                    CREATE CONSTRAINT mymax5(3);
                };
            };
        """)

        await self.con.execute(r"""
            ALTER ABSTRACT CONSTRAINT mymax5
            RENAME TO mymax6;
        """)

        async with self._run_and_rollback():
            with self.assertRaises(edgedb.ConstraintViolationError):
                await self.con.execute(r"""
                    INSERT ConstraintTest10 { foo := 4 }
                """)

        await self.con.execute(r"""
            CREATE MODULE foo IF NOT EXISTS;
            ALTER ABSTRACT CONSTRAINT mymax6
            RENAME TO foo::mymax2;
        """)

        await self.con.execute(r"""
            ALTER TYPE ConstraintTest10 {
                ALTER PROPERTY foo {
                    DROP CONSTRAINT foo::mymax2(3);
                }
            }
        """)
        await self.con.execute(r"""
            DROP ABSTRACT CONSTRAINT foo::mymax2;
        """)

    async def test_constraints_ddl_11(self):
        qry = r"""
            CREATE ABSTRACT CONSTRAINT mymax7(max: std::int64) {
                USING (__subject__ <= max);
            };
        """

        # Check that renaming and then recreating works
        await self.con.execute(qry)
        await self.con.execute("""
            ALTER ABSTRACT CONSTRAINT mymax7 RENAME TO mymax8;
        """)
        await self.con.execute(qry)

    async def test_constraints_ddl_12(self):
        qry = r"""
            CREATE ABSTRACT CONSTRAINT mymax9(max: std::int64) {
                USING (__subject__ <= max);
            };
        """

        # Check that deleting and then recreating works
        await self.con.execute(qry)
        await self.con.execute("""
            DROP ABSTRACT CONSTRAINT mymax9;
        """)
        await self.con.execute(qry)

    async def test_constraints_ddl_13(self):
        await self.con.execute(r"""
            CREATE ABSTRACT CONSTRAINT mymax13(max: std::int64) {
                USING (__subject__ <= max);
            };

            CREATE TYPE ConstraintTest13 {
                CREATE PROPERTY foo -> std::int64 {
                    CREATE CONSTRAINT mymax13(3);
                };
            };
        """)

        await self.con.execute(r"""
            ALTER ABSTRACT CONSTRAINT mymax13
            RENAME TO mymax13b;
        """)

        res = await self.con.query_single("""
            DESCRIBE MODULE default
        """)

        self.assertEqual(res.count("mymax13b"), 2)

    async def test_constraints_ddl_14(self):
        await self.con.execute(r"""
            CREATE ABSTRACT CONSTRAINT mymax14(max: std::int64) {
                USING (__subject__ <= max);
            };

            CREATE TYPE ConstraintTest14 {
                CREATE PROPERTY foo -> std::int64 {
                    CREATE CONSTRAINT mymax14(3);
                };
            };
        """)

        await self.con.execute(r"""
            ALTER TYPE ConstraintTest14 {
                ALTER PROPERTY foo {
                    DROP CONSTRAINT mymax14(3);
                }
            }
        """)

        await self.con.execute(r"""
            ALTER TYPE ConstraintTest14 {
                ALTER PROPERTY foo {
                    CREATE CONSTRAINT mymax14(5);
                }
            }
        """)

    async def test_constraints_ddl_15(self):
        await self.con.execute(r"""
            CREATE ABSTRACT CONSTRAINT not_bad {
                USING (__subject__ != "bad" and __subject__ != "terrible")
            };
        """)

        await self.con.execute(r"""
            CREATE TYPE Foo {
                CREATE PROPERTY foo -> str {
                    CREATE CONSTRAINT not_bad;
                }
            };
        """)

        await self.con.execute(r"""
            ALTER ABSTRACT CONSTRAINT not_bad {
                USING (__subject__ != "bad" and __subject__ != "terrible"
                       and __subject__ != "awful")
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "invalid foo",
        ):
            await self.con.execute(r"""
                INSERT Foo { foo := "awful" };
            """)

        await self.con.execute(r"""
            INSERT Foo { foo := "scow" };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "invalid foo",
        ):
            await self.con.execute(r"""
                ALTER ABSTRACT CONSTRAINT not_bad {
                    USING (__subject__ != "bad" and __subject__ != "terrible"
                           and __subject__ != "scow")
                };
            """)

    async def test_constraints_ddl_16(self):
        await self.con.execute("""
            CREATE TYPE ObjCnstr {
                CREATE PROPERTY first_name -> str;
                CREATE PROPERTY last_name -> str;
                CREATE CONSTRAINT exclusive ON (
                    (.first_name ?? "N/A", .last_name ?? "N/A")
                );
            };
        """)

        await self.con.execute("""
            INSERT ObjCnstr { first_name := "foo", last_name := "bar" }
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "ObjCnstr violates exclusivity constraint",
        ):
            await self.con.execute("""
                INSERT ObjCnstr {
                    first_name := "foo", last_name := "bar" }
            """)

        await self.con.execute("""
            INSERT ObjCnstr { first_name := "test" }
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "ObjCnstr violates exclusivity constraint",
        ):
            await self.con.execute("""
                INSERT ObjCnstr {
                    first_name := "test"
                }
            """)

        await self.con.execute("""
            INSERT ObjCnstr { last_name := "test" }
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "ObjCnstr violates exclusivity constraint",
        ):
            await self.con.execute("""
                INSERT ObjCnstr {
                    last_name := "test"
                }
            """)

    async def test_constraints_ddl_function(self):
        await self.con.execute('''\
            CREATE FUNCTION comp_func(s: str) -> str {
                USING (
                    SELECT str_lower(s)
                );
                SET volatility := 'Immutable';
            };

            CREATE TYPE CompPropFunction {
                CREATE PROPERTY title -> str {
                    CREATE CONSTRAINT exclusive ON
                        (comp_func(__subject__));
                };
                CREATE PROPERTY comp_prop := comp_func(.title);
            };
        ''')

    async def test_constraints_ddl_error_02(self):
        # testing that subjectexpr cannot be overridden after it is
        # specified explicitly
        async with self.assertRaisesRegexTx(
            edgedb.InvalidConstraintDefinitionError,
            r"subjectexpr is already defined for .+max_int",
        ):
            await self.con.execute(r"""
                CREATE ABSTRACT CONSTRAINT max_int(m: std::int64)
                    ON (<int64>__subject__)
                {
                    SET errmessage :=
                    # XXX: once simple string concat is possible here
                    #      formatting can be saner
                    '{__subject__} must be no longer than {m} characters.';
                    USING (__subject__ <= m);
                };

                CREATE TYPE InvalidConstraintTest2 {
                    CREATE PROPERTY foo -> std::str {
                        CREATE CONSTRAINT max_int(3)
                            ON (len(__subject__));
                    };
                };
            """)

    async def test_constraints_ddl_error_05(self):
        # Test that constraint expression returns a boolean.
        schema = """
            type User {
                required property login -> str {
                    constraint expression on (len(__subject__))
                }
            };
        """

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "constraint expression expected to return a bool value, "
            "got scalar type 'std::int64'",
        ):
            await self.migrate(schema)

        qry = """
            CREATE TYPE User {
                CREATE REQUIRED PROPERTY login -> str {
                    CREATE CONSTRAINT expression on (len(__subject__));
                };
            };
        """

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "constraint expression expected to return a bool value, "
            "got scalar type 'std::int64'",
        ):
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

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "constraint expression expected to return a bool value, "
            "got scalar type 'std::str'",
        ):
            await self.con.execute(qry)

        qry = """
            CREATE SCALAR TYPE wrong extending str {
                CREATE CONSTRAINT exclusive;
            };
        """

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "abstract constraint 'std::exclusive' may not "
            "be used on scalar types",
        ):
            await self.con.execute(qry)

    async def test_constraints_ddl_error_06(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidConstraintDefinitionError,
            r'dollar-prefixed.*cannot be used',
        ):
            await self.con.execute(r"""
                CREATE ABSTRACT CONSTRAINT
                mymax_er_06(max: std::int64) ON (len(__subject__))
                {
                    USING (__subject__ <= $max);
                };

                CREATE TYPE ConstraintOnTest_err_06 {
                    CREATE PROPERTY foo -> std::str {
                        CREATE CONSTRAINT mymax_er_06(3);
                    };
                };
            """)

    async def test_constraints_ddl_error_07(self):
        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r'Constant sets not allowed in singleton mode'
        ):
            await self.con.execute(r"""
                CREATE TYPE ConstraintOnTest_err_07 {
                    CREATE PROPERTY less_than_three -> std::int64 {
                        CREATE CONSTRAINT std::one_of({1,2,3});
                    };
                };
            """)

    async def test_constraints_tuple(self):
        await self.con.execute(r"""
            CREATE TYPE Transaction {
                CREATE PROPERTY credit
                    -> tuple<nest: tuple<amount: decimal, currency: str>> {
                    CREATE CONSTRAINT max_value(0)
                        ON (__subject__.nest.amount)
                };
            };
        """)
        await self.con.execute(r"""
            INSERT Transaction {
                credit := (nest := (amount := -1, currency := "usd")) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "Maximum allowed value for credit is 0.",
        ):
            await self.con.execute(r"""
                INSERT Transaction {
                    credit := (nest := (amount := 1, currency := "usd")) };
            """)

    async def test_constraints_partial_path(self):
        await self.con.execute('''\
            CREATE TYPE Vector {
                CREATE PROPERTY x -> float64;
                CREATE PROPERTY y -> float64;
                CREATE CONSTRAINT expression ON (
                    .x^2 + .y^2 < 25
                );
            };
        ''')

        await self.con.execute(r"""
            INSERT Vector { x := 3, y := 3 };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            'invalid Vector',
        ):
            await self.con.execute(r"""
                INSERT Vector { x := 4, y := 4 };
            """)

    async def test_constraints_exclusive_link_prop_01(self):
        await self.con.execute("""
            CREATE TYPE Tgt;
            CREATE TYPE Obj {
                CREATE LINK asdf -> Tgt {
                    CREATE PROPERTY what -> str;
                    CREATE CONSTRAINT exclusive ON (
                        __subject__@what ?? '??'
                    )
               }
            };
        """)

        await self.con.execute("""
            INSERT Tgt;
            INSERT Obj { asdf := assert_single((SELECT Tgt)) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "asdf violates exclusivity constraint",
        ):
            await self.con.execute("""
                INSERT Obj { asdf := assert_single((SELECT Tgt)) };
            """)

    async def test_constraints_exclusive_link_prop_02(self):
        await self.con.execute("""
            CREATE TYPE Tgt;
            CREATE TYPE Obj {
                CREATE LINK asdf -> Tgt {
                    CREATE PROPERTY what -> str {
                        CREATE CONSTRAINT exclusive ON (__subject__ ?? '??')
                    }
                }
            };
        """)

        await self.con.execute("""
            INSERT Tgt;
            INSERT Obj { asdf := assert_single((SELECT Tgt)) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "what violates exclusivity constraint",
        ):
            await self.con.execute("""
                INSERT Obj { asdf := assert_single((SELECT Tgt)) };
            """)

    async def test_constraints_exclusive_link_prop_03(self):
        await self.con.execute("""
            CREATE TYPE Tgt;
            CREATE ABSTRACT LINK asdf {
                CREATE PROPERTY what -> str {
                    CREATE CONSTRAINT exclusive ON (__subject__ ?? '??')
                }
            };
            CREATE TYPE Obj {
                CREATE LINK asdf extending asdf -> Tgt;
            };
        """)

        await self.con.execute("""
            INSERT Tgt;
            INSERT Obj { asdf := assert_single((SELECT Tgt)) };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "what violates exclusivity constraint",
        ):
            await self.con.execute("""
                INSERT Obj { asdf := assert_single((SELECT Tgt)) };
            """)

    async def test_constraints_non_strict_01(self):
        # Test constraints that use a function that is implemented
        # "non-strictly" (and so requires some special handling in the
        # compiler)
        await self.con.execute("""
            create type X {
                create property a -> array<str>;
                create property b -> array<str>;
                create constraint expression on (
                    .a ++ .b != ["foo", "bar", "baz"]);
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "invalid X",
        ):
            await self.con.execute("""
                insert X { a := ['foo'], b := ['bar', 'baz'] };
            """)

        # These should succeed, though, because the LHS is just {}
        await self.con.execute("""
            insert X { a := {}, b := ['foo', 'bar', 'baz'] };
        """)
        await self.con.execute("""
            insert X { a := ['foo', 'bar', 'baz'], b := {} };
        """)

    async def test_constraints_bad_args(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError, "Expected 0 arguments, but found 1"
        ):
            await self.con.execute(
                """
                create type X {
                    create property a -> bool;
                    create link parent -> X {
                        create constraint expression (false);
                    }
                };
            """
            )

    async def test_constraints_no_refs(self):
        await self.con.execute(
            """
            create type X {
                create property name -> str {
                    create constraint std::expression on (true);
                };
            };
        """
        )

    async def test_constraints_abstract_scalar(self):
        await self.con.execute(
            """
            create abstract scalar type posint64 extending int64 {
                create constraint min_value(0);
            };

            create scalar type limited_int64 extending posint64;

            create type X {
                create property y -> limited_int64;
            };
            """
        )
        await self.con.execute("insert X { y := 1 }")
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError, "Minimum allowed value for"
        ):
            await self.con.execute("insert X { y := -1 }")

    async def test_constraints_abstract_object_01(self):
        await self.con.execute(
            """
                create abstract type ChatBase {
                    create multi property messages: str {
                        create constraint exclusive;
                    };
                };

                create type Dialog extending ChatBase;
                create type Monolog extending ChatBase;
                insert Dialog;
                insert Monolog;
            """
        )
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "messages violates exclusivity constraint"
        ):
            await self.con.execute("""
                update ChatBase set { messages += 'hello world' };
            """)
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "messages violates exclusivity constraint"
        ):
            await self.con.execute("""
                analyze
                update ChatBase set { messages += 'hello world' };
            """)
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "messages violates exclusivity constraint"
        ):
            await self.con.execute("""
                update ChatBase set { messages := 'hello world' };
            """)

    async def test_constraints_abstract_object_02(self):
        await self.con.execute(
            """
                create abstract type ChatBase {
                    create single property messages: str {
                        create constraint exclusive;
                    };
                };

                create type Dialog extending ChatBase;
                create type Monolog extending ChatBase;
                insert Dialog;
                insert Monolog;
            """
        )
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "messages violates exclusivity constraint"
        ):
            await self.con.execute("""
                update ChatBase set { messages := 'hello world' };
            """)

    async def test_constraints_abstract_object_03(self):
        # Add one where the constraint comes from a different type
        # than the update
        await self.con.execute(
            """
                create abstract type ChatBase {
                    create single property messages: str {
                    };
                };
                create abstract type ChatBase2 {
                    create single property messages: str {
                        create constraint exclusive;
                    };
                };

                create type Dialog extending ChatBase, ChatBase2;
                create type Monolog extending ChatBase, ChatBase2;
                insert Dialog;
                insert Monolog;
            """
        )
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "messages violates exclusivity constraint"
        ):
            await self.con.execute("""
                update ChatBase set { messages := 'hello world' };
            """)

    async def test_constraints_singleton_set_ops_01(self):
        await self.con.execute(
            """
            create type X {
                create property a -> int64 {
                    create constraint expression on (
                        __subject__ in {1}
                    );
                }
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "cannot use SET OF operator 'std::IN' "
            "in a constraint"
        ):
            await self.con.execute(
                """
                create type Y {
                    create multi property a -> int64 {
                        create constraint expression on (
                            __subject__ in {1}
                        );
                    }
                };
            """)

    async def test_constraints_singleton_set_ops_02(self):
        await self.con.execute(
            """
            create type X {
                create property a -> int64 {
                    create constraint expression on (
                        __subject__ not in {1}
                    );
                }
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "cannot use SET OF operator 'std::NOT IN' "
            "in a constraint"
        ):
            await self.con.execute(
                """
                create type Y {
                    create multi property a -> int64 {
                        create constraint expression on (
                            __subject__ not in {1}
                        );
                    }
                };
            """)

    async def test_constraints_singleton_set_ops_03(self):
        await self.con.execute(
            """
            create type X {
                create property a -> int64 {
                    create constraint expression on (
                        exists(__subject__)
                    );
                }
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "cannot use SET OF operator 'std::EXISTS' "
            "in a constraint"
        ):
            await self.con.execute(
                """
                create type Y {
                    create multi property a -> int64 {
                        create constraint expression on (
                            exists(__subject__)
                        );
                    }
                };
            """)

    async def test_constraints_singleton_set_ops_04(self):
        await self.con.execute(
            """
            create type X {
                create property a -> int64 {
                    create constraint expression on (
                        __subject__ ?? 1 = 0
                    );
                }
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"cannot use SET OF operator 'std::\?\?' "
            r"in a constraint"
        ):
            await self.con.execute(
                """
                create type Y {
                    create multi property a -> int64 {
                        create constraint expression on (
                            __subject__ ?? 1 = 0
                        );
                    }
                };
            """)

    async def test_constraints_singleton_set_ops_05(self):
        await self.con.execute(
            """
            create type X {
                create property a -> tuple<bool, int64> {
                    create constraint expression on (
                        __subject__.1 < 0
                        if __subject__.0 else
                        __subject__.1 >= 0
                    );
                }
            };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            "cannot use SET OF operator 'std::IF' "
            "in a constraint"
        ):
            await self.con.execute(
                """
                create type Y {
                    create multi property a -> tuple<bool, int64> {
                        create constraint expression on (
                            __subject__.1 < 0
                            if __subject__.0 else
                            __subject__.1 >= 0
                        );
                    }
                };
            """)


class TestConstraintsInheritance(tb.DDLTestCase):

    async def _check_constraint_inheritance(
        self,
        constraint_groups: list[list[tuple[str, ...]]]
    ) -> None:
        all_constrained_entries = list(set(
            entry
            for group in constraint_groups
            for entry in group
        ))

        relatives: dict[tuple[str, ...], set[str]] = {
            entry: set()
            for entry in all_constrained_entries
        }
        for group in constraint_groups:
            for entry in group:
                relatives[entry] = relatives[entry].union(
                    other[0] for other in group
                )

        for entry in all_constrained_entries:
            for other in all_constrained_entries:
                if entry[0] == other[0]:
                    continue

                name = entry[0]
                other_name = other[0]

                if len(entry) == 2:
                    prop = entry[1]

                    if other_name in relatives[entry]:
                        async with self.assertRaisesRegexTx(
                            edgedb.ConstraintViolationError,
                            f"violates exclusivity constraint"
                        ):
                            await self.con.execute(
                                f"insert {name} {{"
                                f"    {prop} := '{other_name.lower()}'"
                                f"}};"
                            )
                    else:
                        await self.con.execute(
                            f"insert {name} {{"
                            f"    {prop} := '{other_name.lower()}'"
                            f"}};"
                        )
                        await self.con.execute(
                            f"delete {name} filter .{prop} = ("
                            f"    '{other_name.lower()}'"
                            f");"
                        )

                elif len(entry) == 4:
                    link = entry[1]
                    link_type = entry[2]
                    link_prop = entry[3]

                    if other_name in relatives[entry]:
                        async with self.assertRaisesRegexTx(
                            edgedb.ConstraintViolationError,
                            f"violates exclusivity constraint"
                        ):
                            await self.con.execute(
                                f"insert {name} {{"
                                f"    {link} := (insert {link_type}) {{"
                                f"        @{link_prop} := ("
                                f"            '{other_name.lower()}'"
                                f"        )"
                                f"    }}"
                                f"}};"
                            )
                    else:
                        await self.con.execute(
                            f"insert {name} {{"
                            f"    {link} := (insert {link_type}) {{"
                            f"        @{link_prop} := ("
                            f"            '{other_name.lower()}'"
                            f"        )"
                            f"    }}"
                            f"}};"
                        )
                        await self.con.execute(
                            f"delete {name} "
                            f"filter .{link}@{link_prop} = ("
                            f"    '{other_name.lower()}'"
                            f");"
                        )

                else:
                    raise NotImplementedError()

    async def _apply_schema_inheritance_single_object(self):
        # - single inheritance
        #   - type constraint
        await self.con.execute("""
            create type AAA {
                create required property name -> str;
                create constraint exclusive on (.name);
            };
            create type BBB extending AAA;
            create type XXX extending AAA;
            insert AAA {name := 'aaa'};
            insert BBB {name := 'bbb'};
            insert XXX {name := 'xxx'};
        """)

    async def test_constraints_inheritance_single_object_01(self):
        await self._apply_schema_inheritance_single_object()

        # Add descendant
        await self.con.execute("""
            create type CCC extending XXX;
            insert CCC {name := 'ccc'};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [('AAA', 'name'), ('BBB', 'name'), ('XXX', 'name'), ('CCC', 'name')]
        ])

    async def test_constraints_inheritance_single_object_02(self):
        await self._apply_schema_inheritance_single_object()

        # Add base
        await self.con.execute("""
            create type CCC {
                create required property name -> str;
                create constraint exclusive on (.name);
            };
            create type DDD extending CCC;
            insert CCC {name := 'ccc'};
            insert DDD {name := 'ddd'};
        """)

        await self.con.execute("""
            alter type XXX {
                extending CCC;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [('AAA', 'name'), ('BBB', 'name'), ('XXX', 'name')],
            [('CCC', 'name'), ('DDD', 'name'), ('XXX', 'name')],
        ])

    async def test_constraints_inheritance_single_object_03(self):
        await self._apply_schema_inheritance_single_object()

        # Change base
        await self.con.execute("""
            create type CCC {
                create required property name -> str;
                create constraint exclusive on (.name);
            };
            create type DDD extending CCC;
            insert CCC {name := 'ccc'};
            insert DDD {name := 'ddd'};
        """)

        await self.con.execute("""
            alter type XXX {
                drop extending AAA;
                extending CCC;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [('AAA', 'name'), ('BBB', 'name')],
            [('CCC', 'name'), ('DDD', 'name'), ('XXX', 'name')],
        ])

    async def test_constraints_inheritance_single_object_04(self):
        await self._apply_schema_inheritance_single_object()

        # Remove base
        await self.con.execute("""
            delete XXX;
            alter type XXX { drop extending AAA; };
            alter type XXX { create required property name -> str; };
            insert XXX {name := 'xxx'};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [('AAA', 'name'), ('BBB', 'name')],
            [('XXX', 'name')],
        ])

    async def _apply_schema_inheritance_mutli_object(self):
        # - multiple inheritance
        #   - abstract type constraint
        #   - type constraint
        await self.con.execute("""
            create abstract type AAA {
                create required property name -> str;
                create constraint exclusive on (.name);
            };
            create type BBB extending AAA;
            create type CCC {
                create required property name -> str;
                create constraint exclusive on (.name);
            };
            create type DDD extending CCC;
            create type XXX extending AAA, CCC;
            insert BBB {name := 'bbb'};
            insert CCC {name := 'ccc'};
            insert DDD {name := 'ddd'};
            insert XXX {name := 'xxx'};
        """)

    async def test_constraints_inheritance_multi_object_01(self):
        await self._apply_schema_inheritance_mutli_object()

        # Add descendant
        await self.con.execute("""
            create type EEE extending XXX;
            insert EEE {name := 'eee'};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('BBB', 'name'),
                ('XXX', 'name'),
                ('EEE', 'name'),
            ],
            [
                ('CCC', 'name'),
                ('DDD', 'name'),
                ('XXX', 'name'),
                ('EEE', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_object_02(self):
        await self._apply_schema_inheritance_mutli_object()

        # Add base
        await self.con.execute("""
            create type EEE {
                create required property name -> str;
                create constraint exclusive on (.name);
            };
            create type FFF extending EEE;
            insert EEE {name := 'eee'};
            insert FFF {name := 'fff'};
        """)

        await self.con.execute("""
            alter type XXX {
                extending EEE;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [('BBB', 'name'), ('XXX', 'name')],
            [('CCC', 'name'), ('DDD', 'name'), ('XXX', 'name')],
            [('EEE', 'name'), ('FFF', 'name'), ('XXX', 'name')],
        ])

    async def test_constraints_inheritance_multi_object_03(self):
        await self._apply_schema_inheritance_mutli_object()

        # Change base
        await self.con.execute("""
            create type EEE {
                create required property name -> str;
                create constraint exclusive on (.name);
            };
            create type FFF extending EEE;
            insert EEE {name := 'eee'};
            insert FFF {name := 'fff'};
        """)

        await self.con.execute("""
            alter type XXX {
                drop extending AAA;
                extending EEE;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [('BBB', 'name')],
            [('CCC', 'name'), ('DDD', 'name'), ('XXX', 'name')],
            [('EEE', 'name'), ('FFF', 'name'), ('XXX', 'name')],
        ])

    async def test_constraints_inheritance_multi_object_04(self):
        await self._apply_schema_inheritance_mutli_object()

        # Remove base
        await self.con.execute("""
            alter type XXX {
                drop extending AAA;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [('BBB', 'name')],
            [('CCC', 'name'), ('DDD', 'name'), ('XXX', 'name')],
        ])

    async def _apply_schema_inheritance_single_pointer(self):
        # - single inheritance
        #   - property
        await self.con.execute("""
            create type AAA {
                create required property name -> str {
                    create constraint exclusive;
                };
            };
            create type BBB extending AAA;
            create type XXX extending AAA;
            insert AAA {name := 'aaa'};
            insert BBB {name := 'bbb'};
            insert XXX {name := 'xxx'};
        """)

    async def test_constraints_inheritance_single_pointer_01(self):
        await self._apply_schema_inheritance_single_pointer()

        # Add descendant
        await self.con.execute("""
            create type CCC extending XXX;
            insert CCC {name := 'ccc'};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'name'),
                ('BBB', 'name'),
                ('XXX', 'name'),
                ('CCC', 'name'),
            ],
        ])

    async def test_constraints_inheritance_single_pointer_02(self):
        await self._apply_schema_inheritance_single_pointer()

        # Add base
        await self.con.execute("""
            create type CCC {
                create required property name -> str {
                    create constraint exclusive;
                };
            };
            create type DDD extending CCC;
            insert CCC {name := 'ccc'};
            insert DDD {name := 'ddd'};
        """)

        await self.con.execute("""
            alter type XXX {
                extending CCC;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'name'),
                ('BBB', 'name'),
                ('XXX', 'name'),
            ],
            [
                ('CCC', 'name'),
                ('DDD', 'name'),
                ('XXX', 'name'),
            ],
        ])

    async def test_constraints_inheritance_single_pointer_03(self):
        await self._apply_schema_inheritance_single_pointer()

        # Change base
        await self.con.execute("""
            create type CCC {
                create required property name -> str {
                    create constraint exclusive;
                };
            };
            create type DDD extending CCC;
            insert CCC {name := 'ccc'};
            insert DDD {name := 'ddd'};
        """)

        await self.con.execute("""
            alter type XXX {
                drop extending AAA;
                extending CCC;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'name'),
                ('BBB', 'name'),
            ],
            [
                ('CCC', 'name'),
                ('DDD', 'name'),
                ('XXX', 'name'),
            ],
        ])

    async def test_constraints_inheritance_single_pointer_04(self):
        await self._apply_schema_inheritance_single_pointer()

        # Remove base
        await self.con.execute("""
            delete XXX;
            alter type XXX { drop extending AAA; };
            alter type XXX {
                create required property name -> str {
                    create constraint exclusive;
                };
            };
            insert XXX {name := 'xxx'};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'name'),
                ('BBB', 'name'),
            ],
            [
                ('XXX', 'name'),
            ],
        ])

    async def _apply_schema_inheritance_multi_pointer(self):
        # - multiple inheritance
        #   - pointer constraint
        #   - pointer constraint
        await self.con.execute("""
            create type AAA {
                create required property name -> str {
                    create constraint exclusive;
                };
            };
            create type BBB extending AAA;
            create type CCC {
                create required property name -> str
                {
                    create constraint exclusive;
                };
            };
            create type DDD extending CCC;
            create type XXX extending AAA, CCC;
            insert AAA {name := 'aaa'};
            insert BBB {name := 'bbb'};
            insert CCC {name := 'ccc'};
            insert DDD {name := 'ddd'};
            insert XXX {name := 'xxx'};
        """)

    async def test_constraints_inheritance_multi_pointer_01(self):
        await self._apply_schema_inheritance_multi_pointer()

        # Add descendant
        await self.con.execute("""
            create type EEE extending XXX;
            insert EEE {name := 'eee'};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'name'),
                ('BBB', 'name'),
                ('XXX', 'name'),
                ('EEE', 'name'),
            ],
            [
                ('CCC', 'name'),
                ('DDD', 'name'),
                ('XXX', 'name'),
                ('EEE', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_pointer_02(self):
        await self._apply_schema_inheritance_multi_pointer()

        # Add base
        await self.con.execute("""
            create type EEE {
                create required property name -> str {
                    create constraint exclusive;
                };
            };
            create type FFF extending EEE;
            insert EEE {name := 'eee'};
            insert FFF {name := 'fff'};
        """)

        await self.con.execute("""
            alter type XXX {
                extending EEE;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [('AAA', 'name'), ('BBB', 'name'), ('XXX', 'name')],
            [('CCC', 'name'), ('DDD', 'name'), ('XXX', 'name')],
            [('EEE', 'name'), ('FFF', 'name'), ('XXX', 'name')],
        ])

    async def test_constraints_inheritance_multi_pointer_03(self):
        await self._apply_schema_inheritance_multi_pointer()

        # Change base
        await self.con.execute("""
            create type EEE {
                create required property name -> str {
                    create constraint exclusive;
                };
            };
            create type FFF extending EEE;
            insert EEE {name := 'eee'};
            insert FFF {name := 'fff'};
        """)

        await self.con.execute("""
            alter type XXX {
                drop extending AAA;
                extending EEE;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [('AAA', 'name'), ('BBB', 'name')],
            [('CCC', 'name'), ('DDD', 'name'), ('XXX', 'name')],
            [('EEE', 'name'), ('FFF', 'name'), ('XXX', 'name')],
        ])

    async def test_constraints_inheritance_multi_pointer_04(self):
        await self._apply_schema_inheritance_multi_pointer()

        # Remove base
        await self.con.execute("""
            alter type XXX {
                drop extending AAA;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [('AAA', 'name'), ('BBB', 'name')],
            [('CCC', 'name'), ('DDD', 'name'), ('XXX', 'name')],
        ])

    async def _apply_schema_inheritance_single_abstract_link(self):
        # - single inheritance
        #   - abstract link constraint
        await self.con.execute("""
            create type Tag;
            create abstract link PPP {
                create property name -> str;
                create constraint exclusive on (@name);
            };
            create abstract link QQQ extending PPP;
            create abstract link XXX extending PPP;
            create type AAA {
                create required link tag extending PPP -> Tag;
            };
            create type BBB {
                create required link tag extending QQQ -> Tag;
            };
            create type YYY {
                create required link tag extending XXX -> Tag;
            };
            insert AAA {tag := (insert Tag){@name := 'aaa'}};
            insert BBB {tag := (insert Tag){@name := 'bbb'}};
            insert YYY {tag := (insert Tag){@name := 'yyy'}};
        """)

    async def test_constraints_inheritance_single_abstract_link_01(self):
        await self._apply_schema_inheritance_single_abstract_link()

        # Add descendant
        await self.con.execute("""
            create abstract link RRR extending XXX;
            create type CCC {
                create required link tag extending RRR -> Tag;
            };
            insert CCC {tag := (insert Tag){@name := 'ccc'}};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
                ('CCC', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_single_abstract_link_02(self):
        await self._apply_schema_inheritance_single_abstract_link()

        # Add base
        await self.con.execute("""
            create abstract link RRR {
                create property name -> str;
                create constraint exclusive on (@name);
            };
            create abstract link SSS extending RRR;
            create type CCC {
                create required link tag extending RRR -> Tag;
            };
            create type DDD {
                create required link tag extending SSS -> Tag;
            };
            insert CCC {tag := (insert Tag){@name := 'ccc'}};
            insert DDD {tag := (insert Tag){@name := 'ddd'}};
        """)

        await self.con.execute("""
            alter abstract link XXX {
                extending RRR;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_single_abstract_link_03(self):
        await self._apply_schema_inheritance_single_abstract_link()

        # Change base
        await self.con.execute("""
            create abstract link RRR {
                create property name -> str;
                create constraint exclusive on (@name);
            };
            create abstract link SSS extending RRR;
            create type CCC {
                create required link tag extending RRR -> Tag;
            };
            create type DDD {
                create required link tag extending SSS -> Tag;
            };
            insert CCC {tag := (insert Tag){@name := 'ccc'}};
            insert DDD {tag := (insert Tag){@name := 'ddd'}};
        """)

        await self.con.execute("""
            alter abstract link XXX {
                extending RRR;
                drop extending PPP;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_single_abstract_link_04(self):
        await self._apply_schema_inheritance_single_abstract_link()

        # Remove base
        await self.con.execute("""
            delete YYY;
            alter abstract link XXX { drop extending PPP; };
            alter abstract link XXX {
                create property name -> str;
                create constraint exclusive on (@name);
            };
            insert YYY {tag := (insert Tag){@name := 'yyy'}};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
            ],
            [
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def _apply_schema_inheritance_multi_abstract_link(self):
        # - multiple inheritance
        #   - abstract link constraint
        #   - abstract link constraint
        await self.con.execute("""
            create type Tag;
            create abstract link PPP {
                create property name -> str;
                create constraint exclusive on (@name);
            };
            create abstract link QQQ extending PPP;
            create abstract link RRR {
                create property name -> str;
                create constraint exclusive on (@name);
            };
            create abstract link SSS extending RRR;
            create abstract link XXX extending PPP, RRR;
            create type AAA {
                create required link tag extending PPP -> Tag;
            };
            create type BBB {
                create required link tag extending QQQ -> Tag;
            };
            create type CCC {
                create required link tag extending RRR -> Tag;
            };
            create type DDD {
                create required link tag extending SSS -> Tag;
            };
            create type YYY {
                create required link tag extending XXX -> Tag;
            };
            insert AAA {tag := (insert Tag){@name := 'aaa'}};
            insert BBB {tag := (insert Tag){@name := 'bbb'}};
            insert CCC {tag := (insert Tag){@name := 'ccc'}};
            insert DDD {tag := (insert Tag){@name := 'ddd'}};
            insert YYY {tag := (insert Tag){@name := 'yyy'}};
        """)

    async def test_constraints_inheritance_multi_abstract_link_01(self):
        await self._apply_schema_inheritance_multi_abstract_link()

        # Add descendant
        await self.con.execute("""
            create abstract link TTT extending XXX;
            create type EEE {
                create required link tag extending TTT -> Tag;
            };
            insert EEE {tag := (insert Tag){@name := 'eee'}};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
                ('EEE', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
                ('EEE', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_abstract_link_02(self):
        await self._apply_schema_inheritance_multi_abstract_link()

        # Add base
        await self.con.execute("""
            create abstract link TTT {
                create property name -> str;
                create constraint exclusive on (@name);
            };
            create abstract link UUU extending TTT;
            create type EEE {
                create required link tag extending TTT -> Tag;
            };
            create type FFF {
                create required link tag extending UUU -> Tag;
            };
            insert EEE {tag := (insert Tag){@name := 'eee'}};
            insert FFF {tag := (insert Tag){@name := 'fff'}};
        """)

        await self.con.execute("""
            alter abstract link XXX {
                extending TTT;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
            [
                ('EEE', 'tag', 'Tag', 'name'),
                ('FFF', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_abstract_link_03(self):
        await self._apply_schema_inheritance_multi_abstract_link()

        # Change base
        await self.con.execute("""
            create abstract link TTT {
                create property name -> str;
                create constraint exclusive on (@name);
            };
            create abstract link UUU extending TTT;
            create type EEE {
                create required link tag extending TTT -> Tag;
            };
            create type FFF {
                create required link tag extending UUU -> Tag;
            };
            insert EEE {tag := (insert Tag){@name := 'eee'}};
            insert FFF {tag := (insert Tag){@name := 'fff'}};
        """)

        await self.con.execute("""
            alter abstract link XXX {
                extending TTT;
                drop extending PPP;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
            [
                ('EEE', 'tag', 'Tag', 'name'),
                ('FFF', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_abstract_link_04(self):
        await self._apply_schema_inheritance_multi_abstract_link()

        # Remove base
        await self.con.execute("""
            alter abstract link XXX {
                drop extending PPP;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def _apply_schema_inheritance_multi_mixed_link(self):
        # - multiple inheritance
        #   - abstract link constraint
        #   - abstract type link constraint
        await self.con.execute("""
            create type Tag;
            create type Tag2;
            create abstract link PPP {
                create property name -> str;
                create constraint exclusive on (@name);
            };
            create abstract link QQQ extending PPP;
            create type AAA {
                create required link tag extending PPP -> Tag;
            };
            create type BBB {
                create required link tag2 extending QQQ -> Tag2;
            };
            create abstract type CCC {
                create required link tag -> Tag {
                    create property name -> str;
                    create constraint exclusive on (@name);
                };
            };
            create type DDD extending CCC;
            create type XXX extending AAA, CCC;
            insert AAA {tag := (insert Tag){@name := 'aaa'}};
            insert BBB {tag2 := (insert Tag2){@name := 'bbb'}};
            insert DDD {tag := (insert Tag){@name := 'ddd'}};
            insert XXX {tag := (insert Tag){@name := 'xxx'}};
        """)

    async def test_constraints_inheritance_multi_mixed_link_01(self):
        await self._apply_schema_inheritance_multi_mixed_link()

        # Add descendant
        await self.con.execute("""
            create type EEE extending XXX;
            insert EEE {tag := (insert Tag){@name := 'eee'}};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag2', 'Tag2', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
                ('EEE', 'tag', 'Tag', 'name'),
            ],
            [
                ('DDD', 'tag', 'Tag', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
                ('EEE', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_mixed_link_02(self):
        await self._apply_schema_inheritance_multi_mixed_link()

        # Add base
        await self.con.execute("""
            create abstract link RRR {
                create property name -> str;
                create constraint exclusive on (@name);
            };
            create abstract link SSS extending RRR;
            create type EEE {
                create required link tag extending RRR -> Tag;
            };
            create type FFF {
                create required link tag2 extending SSS -> Tag2;
            };
            insert EEE {tag := (insert Tag){@name := 'eee'}};
            insert FFF {tag2 := (insert Tag2){@name := 'fff'}};
        """)

        await self.con.execute("""
            alter type XXX {
                extending EEE;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag2', 'Tag2', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
            [
                ('DDD', 'tag', 'Tag', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
            [
                ('EEE', 'tag', 'Tag', 'name'),
                ('FFF', 'tag2', 'Tag2', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_mixed_link_03(self):
        await self._apply_schema_inheritance_multi_mixed_link()

        # Change base
        await self.con.execute("""
            create abstract link TTT {
                create property name -> str;
                create constraint exclusive on (@name);
            };
            create abstract link UUU extending TTT;
            create type EEE {
                create required link tag extending TTT -> Tag;
            };
            create type FFF {
                create required link tag2 extending UUU -> Tag2;
            };
            insert EEE {tag := (insert Tag){@name := 'eee'}};
            insert FFF {tag2 := (insert Tag2){@name := 'fff'}};
        """)

        await self.con.execute("""
            alter type XXX {
                extending EEE;
                drop extending AAA;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag2', 'Tag2', 'name'),
            ],
            [
                ('DDD', 'tag', 'Tag', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
            [
                ('EEE', 'tag', 'Tag', 'name'),
                ('FFF', 'tag2', 'Tag2', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_mixed_link_04(self):
        await self._apply_schema_inheritance_multi_mixed_link()

        # Remove base
        await self.con.execute("""
            alter type XXX {
                drop extending AAA;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag2', 'Tag2', 'name'),
            ],
            [
                ('DDD', 'tag', 'Tag', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
        ])

    async def _apply_schema_inheritance_single_abstract_link_prop(self):
        # - single inheritance
        #   - abstract link constraint
        await self.con.execute("""
            create type Tag;
            create abstract link PPP {
                create property name -> str {
                    create constraint exclusive;
                };
            };
            create abstract link QQQ extending PPP;
            create abstract link XXX extending PPP;
            create type AAA {
                create required link tag extending PPP -> Tag;
            };
            create type BBB {
                create required link tag extending QQQ -> Tag;
            };
            create type YYY {
                create required link tag extending XXX -> Tag;
            };
            insert AAA {tag := (insert Tag){@name := 'aaa'}};
            insert BBB {tag := (insert Tag){@name := 'bbb'}};
            insert YYY {tag := (insert Tag){@name := 'yyy'}};
        """)

    async def test_constraints_inheritance_single_abstract_link_prop_01(self):
        await self._apply_schema_inheritance_single_abstract_link_prop()

        # Add descendant
        await self.con.execute("""
            create abstract link RRR extending XXX;
            create type CCC {
                create required link tag extending RRR -> Tag;
            };
            insert CCC {tag := (insert Tag){@name := 'ccc'}};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
                ('CCC', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_single_abstract_link_prop_02(self):
        await self._apply_schema_inheritance_single_abstract_link_prop()

        # Add base
        await self.con.execute("""
            create abstract link RRR {
                create property name -> str {
                    create constraint exclusive;
                };
            };
            create abstract link SSS extending RRR;
            create type CCC {
                create required link tag extending RRR -> Tag;
            };
            create type DDD {
                create required link tag extending SSS -> Tag;
            };
            insert CCC {tag := (insert Tag){@name := 'ccc'}};
            insert DDD {tag := (insert Tag){@name := 'ddd'}};
        """)

        await self.con.execute("""
            alter abstract link XXX {
                extending RRR;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_single_abstract_link_prop_03(self):
        await self._apply_schema_inheritance_single_abstract_link_prop()

        # Change base
        await self.con.execute("""
            create abstract link RRR {
                create property name -> str {
                    create constraint exclusive;
                };
            };
            create abstract link SSS extending RRR;
            create type CCC {
                create required link tag extending RRR -> Tag;
            };
            create type DDD {
                create required link tag extending SSS -> Tag;
            };
            insert CCC {tag := (insert Tag){@name := 'ccc'}};
            insert DDD {tag := (insert Tag){@name := 'ddd'}};
        """)

        await self.con.execute("""
            alter abstract link XXX {
                extending RRR;
                drop extending PPP;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_single_abstract_link_prop_04(self):
        await self._apply_schema_inheritance_single_abstract_link_prop()

        # Remove base
        await self.con.execute("""
            delete YYY;
            alter abstract link XXX { drop extending PPP; };
            alter abstract link XXX {
                create property name -> str {
                    create constraint exclusive;
                };
            };
            insert YYY {tag := (insert Tag){@name := 'yyy'}};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
            ],
            [
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def _apply_schema_inheritance_multi_abstract_link_prop(self):
        # - multiple inheritance
        #   - abstract link constraint
        #   - abstract link constraint
        await self.con.execute("""
            create type Tag;
            create abstract link PPP {
                create property name -> str {
                    create constraint exclusive;
                };
            };
            create abstract link QQQ extending PPP;
            create abstract link RRR {
                create property name -> str {
                    create constraint exclusive;
                };
            };
            create abstract link SSS extending RRR;
            create abstract link XXX extending PPP, RRR;
            create type AAA {
                create required link tag extending PPP -> Tag;
            };
            create type BBB {
                create required link tag extending QQQ -> Tag;
            };
            create type CCC {
                create required link tag extending RRR -> Tag;
            };
            create type DDD {
                create required link tag extending SSS -> Tag;
            };
            create type YYY {
                create required link tag extending XXX -> Tag;
            };
            insert AAA {tag := (insert Tag){@name := 'aaa'}};
            insert BBB {tag := (insert Tag){@name := 'bbb'}};
            insert CCC {tag := (insert Tag){@name := 'ccc'}};
            insert DDD {tag := (insert Tag){@name := 'ddd'}};
            insert YYY {tag := (insert Tag){@name := 'yyy'}};
        """)

    async def test_constraints_inheritance_multi_abstract_link_prop_01(self):
        await self._apply_schema_inheritance_multi_abstract_link_prop()

        # Add descendant
        await self.con.execute("""
            create abstract link TTT extending XXX;
            create type EEE {
                create required link tag extending TTT -> Tag;
            };
            insert EEE {tag := (insert Tag){@name := 'eee'}};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
                ('EEE', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
                ('EEE', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_abstract_link_prop_02(self):
        await self._apply_schema_inheritance_multi_abstract_link_prop()

        # Add base
        await self.con.execute("""
            create abstract link TTT {
                create property name -> str {
                    create constraint exclusive;
                };
            };
            create abstract link UUU extending TTT;
            create type EEE {
                create required link tag extending TTT -> Tag;
            };
            create type FFF {
                create required link tag extending UUU -> Tag;
            };
            insert EEE {tag := (insert Tag){@name := 'eee'}};
            insert FFF {tag := (insert Tag){@name := 'fff'}};
        """)

        await self.con.execute("""
            alter abstract link XXX {
                extending TTT;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
            [
                ('EEE', 'tag', 'Tag', 'name'),
                ('FFF', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_abstract_link_prop_03(self):
        await self._apply_schema_inheritance_multi_abstract_link_prop()

        # Change base
        await self.con.execute("""
            create abstract link TTT {
                create property name -> str {
                    create constraint exclusive;
                };
            };
            create abstract link UUU extending TTT;
            create type EEE {
                create required link tag extending TTT -> Tag;
            };
            create type FFF {
                create required link tag extending UUU -> Tag;
            };
            insert EEE {tag := (insert Tag){@name := 'eee'}};
            insert FFF {tag := (insert Tag){@name := 'fff'}};
        """)

        await self.con.execute("""
            alter abstract link XXX {
                extending TTT;
                drop extending PPP;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
            [
                ('EEE', 'tag', 'Tag', 'name'),
                ('FFF', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_abstract_link_prop_04(self):
        await self._apply_schema_inheritance_multi_abstract_link_prop()

        # Remove base
        await self.con.execute("""
            alter abstract link XXX {
                drop extending PPP;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag', 'Tag', 'name'),
            ],
            [
                ('CCC', 'tag', 'Tag', 'name'),
                ('DDD', 'tag', 'Tag', 'name'),
                ('YYY', 'tag', 'Tag', 'name'),
            ],
        ])

    async def _apply_schema_inheritance_multi_mixed_link_prop(self):
        # - multiple inheritance
        #   - abstract link property constraint
        #   - abstract type link property constraint
        await self.con.execute("""
            create type Tag;
            create type Tag2;
            create abstract link PPP {
                create property name -> str {
                    create constraint exclusive;
                };
            };
            create abstract link QQQ extending PPP;
            create type AAA {
                create required link tag extending PPP -> Tag;
            };
            create type BBB {
                create required link tag2 extending QQQ -> Tag2;
            };
            create abstract type CCC {
                create required link tag -> Tag {
                    create property name -> str {
                        create constraint exclusive;
                    };
                };
            };
            create type DDD extending CCC;
            create type XXX extending AAA, CCC;
            insert AAA {tag := (insert Tag){@name := 'aaa'}};
            insert BBB {tag2 := (insert Tag2){@name := 'bbb'}};
            insert DDD {tag := (insert Tag){@name := 'ddd'}};
            insert XXX {tag := (insert Tag){@name := 'xxx'}};
        """)

    async def test_constraints_inheritance_multi_mixed_link_prop_01(self):
        await self._apply_schema_inheritance_multi_mixed_link_prop()

        # Add descendant
        await self.con.execute("""
            create type EEE extending XXX;
            insert EEE {tag := (insert Tag){@name := 'eee'}};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag2', 'Tag2', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
                ('EEE', 'tag', 'Tag', 'name'),
            ],
            [
                ('DDD', 'tag', 'Tag', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
                ('EEE', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_mixed_link_prop_02(self):
        await self._apply_schema_inheritance_multi_mixed_link_prop()

        # Add base
        await self.con.execute("""
            create abstract link RRR {
                create property name -> str {
                    create constraint exclusive;
                };
            };
            create abstract link SSS extending RRR;
            create type EEE {
                create required link tag extending RRR -> Tag;
            };
            create type FFF {
                create required link tag2 extending SSS -> Tag2;
            };
            insert EEE {tag := (insert Tag){@name := 'eee'}};
            insert FFF {tag2 := (insert Tag2){@name := 'fff'}};
        """)

        await self.con.execute("""
            alter type XXX {
                extending EEE;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag2', 'Tag2', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
            [
                ('DDD', 'tag', 'Tag', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
            [
                ('EEE', 'tag', 'Tag', 'name'),
                ('FFF', 'tag2', 'Tag2', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_mixed_link_prop_03(self):
        await self._apply_schema_inheritance_multi_mixed_link_prop()

        # Change base
        await self.con.execute("""
            create abstract link TTT {
                create property name -> str {
                    create constraint exclusive;
                };
            };
            create abstract link UUU extending TTT;
            create type EEE {
                create required link tag extending TTT -> Tag;
            };
            create type FFF {
                create required link tag2 extending UUU -> Tag2;
            };
            insert EEE {tag := (insert Tag){@name := 'eee'}};
            insert FFF {tag2 := (insert Tag2){@name := 'fff'}};
        """)

        await self.con.execute("""
            alter type XXX {
                extending EEE;
                drop extending AAA;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag2', 'Tag2', 'name'),
            ],
            [
                ('DDD', 'tag', 'Tag', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
            [
                ('EEE', 'tag', 'Tag', 'name'),
                ('FFF', 'tag2', 'Tag2', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_multi_mixed_link_prop_04(self):
        await self._apply_schema_inheritance_multi_mixed_link_prop()

        # Remove base
        await self.con.execute("""
            alter type XXX {
                drop extending AAA;
            }
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [
                ('AAA', 'tag', 'Tag', 'name'),
                ('BBB', 'tag2', 'Tag2', 'name'),
            ],
            [
                ('DDD', 'tag', 'Tag', 'name'),
                ('XXX', 'tag', 'Tag', 'name'),
            ],
        ])

    async def test_constraints_inheritance_abstract_constraint_01(self):
        # Abstract constraints do not share their exclusiveness with descendants
        await self.con.execute("""
            create abstract constraint PPP extending exclusive;
            create abstract constraint QQQ extending PPP;
            create abstract constraint RRR extending PPP;
            create type AAA {
                create required property name -> str
                {
                    create constraint PPP;
                };
            };
            create type BBB {
                create required property name -> str
                {
                    create constraint QQQ;
                };
            };
            create type CCC {
                create required property name -> str
                {
                    create constraint RRR;
                };
            };
            create type DDD extending CCC;
            insert AAA {name := 'aaa'};
            insert BBB {name := 'bbb'};
            insert CCC {name := 'ccc'};
            insert DDD {name := 'ddd'};
        """)

        # Check constraints
        await self._check_constraint_inheritance([
            [('AAA', 'name')],
            [('BBB', 'name')],
            [('CCC', 'name'), ('DDD', 'name')],
        ])
