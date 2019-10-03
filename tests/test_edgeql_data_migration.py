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


import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLDataMigration(tb.DDLTestCase):
    """Test that migrations preserve data under certain circumstances.

    Renaming, changing constraints, increasing cardinality should not
    destroy data.

    The test cases here use the same migrations as
    `test_migrations_equivalence`, therefore the test numbers should
    match for easy reference, even if it means skipping some.
    """

    _counter = 0

    @property
    def migration_name(self):
        self._counter += 1
        return f'm{self._counter}'

    async def _migrate(self, migration):
        async with self.con.transaction():
            mname = self.migration_name
            await self.con.execute(f"""
                CREATE MIGRATION {mname} TO {{
                    {migration}
                }};
                COMMIT MIGRATION {mname};
            """)

    async def test_edgeql_migration_01(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate("""
            type Base;
        """)
        await self.con.execute("""
            INSERT Base;
        """)

        # Try altering the schema to a state inconsistent with current
        # data.
        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r"missing value for required property test::Base.name"):
            await self._migrate("""
                type Base {
                    required property name -> str;
                }
            """)
        # Migration without making the property required.
        await self._migrate("""
            type Base {
                property name -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name
                };
            """,
            [{
                'name': None,
            }],
        )

        await self.con.execute("""
            UPDATE
                Base
            SET {
                name := 'base_01'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name
                };
            """,
            [{
                'name': 'base_01',
            }],
        )

        # Inherit from the Base, making name required.
        await self._migrate("""
            type Base {
                property name -> str;
            }

            type Derived extending Base {
                inherited required property name -> str;
            }
        """)
        await self.con.execute("""
            INSERT Derived {
                name := 'derived_01'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base.name;
            """,
            {'base_01', 'derived_01'},
        )

    async def test_edgeql_migration_02(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base {
                inherited required property foo -> str;
            }
        """)
        await self.con.execute("""
            INSERT Base {
                foo := 'base_02',
            };
            INSERT Derived {
                foo := 'derived_02',
            };
        """)

        await self._migrate(r"""
            type Base {
                # rename 'foo'
                property foo2 -> str;
            }

            type Derived extending Base {
                inherited required property foo2 -> str;
            }
        """)

        # the data still persists
        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    foo2,
                } ORDER BY .foo2;
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo2': 'base_02',
            }, {
                '__type__': {'name': 'test::Derived'},
                'foo2': 'derived_02',
            }],
        )

    async def test_edgeql_migration_03(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base {
                inherited required property foo -> str;
            }
        """)
        await self.con.execute("""
            INSERT Base {
                foo := 'base_03',
            };
            INSERT Derived {
                foo := 'derived_03',
            };
        """)

        await self._migrate(r"""
            type Base;
                # drop 'foo'

            type Derived extending Base {
                # completely different property
                property foo2 -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    [IS Derived].foo2,
                } ORDER BY .foo2;
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo2': None,
            }, {
                '__type__': {'name': 'test::Derived'},
                'foo2': None,
            }],
        )

    async def test_edgeql_migration_04(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base;

            type Further extending Derived {
                inherited required property foo -> str;
            }
        """)
        await self.con.execute("""
            INSERT Base {
                foo := 'base_04',
            };
            INSERT Derived {
                foo := 'derived_04',
            };
            INSERT Further {
                foo := 'further_04',
            };
        """)

        await self._migrate(r"""
            type Base;
                # drop 'foo'

            type Derived extending Base;

            type Further extending Derived {
                # completely different property
                property foo2 -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    [IS Further].foo2,
                } ORDER BY .foo2;
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo2': None,
            }, {
                '__type__': {'name': 'test::Derived'},
                'foo2': None,
            }, {
                '__type__': {'name': 'test::Further'},
                'foo2': None,
            }],
        )

    async def test_edgeql_migration_06(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property foo -> int64;
            }

            type Derived extending Base {
                inherited required property foo -> int64;
            }
        """)
        await self.con.execute("""
            INSERT Base {
                foo := 6,
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    foo,
                };
            """,
            [{
                '__type__': {'name': 'test::Base'},
                # the value was correctly inserted
                'foo': 6,
            }],
        )

        await self._migrate(r"""
            type Base {
                # change property type (can't preserve value)
                property foo -> str;
            }

            type Derived extending Base {
                inherited required property foo -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    foo,
                };
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo': '6',
            }],
        )

    async def test_edgeql_migration_07(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link bar -> Child;
            }
        """)
        res = await self.con.fetchall(r"""
            SELECT (
                INSERT Base {
                    bar := (INSERT Child),
                }
            ) {
                bar: {id}
            }
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                required link bar -> Child {
                    # add a constraint
                    constraint exclusive;
                }
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {id},
                };
            """,
            [{
                'bar': {'id': res[0].bar.id},
            }],
        )

    async def test_edgeql_migration_08(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            INSERT Base {
                foo := 'very_long_test_str_base_08',
            };
        """)

        # Try altering the schema to a state inconsistent with current
        # data.
        new_state = r"""
            type Base {
                required property foo -> str {
                    # add a constraint
                    constraint max_len_value(10);
                }
            }
        """
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r"foo must be no longer than 10 characters"):
            await self._migrate(new_state)

        # Fix the data.
        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := 'base_08',
            };
        """)

        # Migrate to same state as before now that the data is fixed.
        await self._migrate(new_state)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': 'base_08',
            }],
        )

    async def test_edgeql_migration_09(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            scalar type constraint_length extending str {
                constraint max_len_value(10);
            }
            type Base {
                property foo -> constraint_length;
            }
        """)
        await self.con.execute(r"""
            INSERT Base {
                foo := 'b09',
            };
        """)

        # Try altering the schema to a state inconsistent with current
        # data.
        new_state = r"""
            scalar type constraint_length extending str {
                constraint max_len_value(10);
                # add a constraint
                constraint min_len_value(5);
            }
            type Base {
                property foo -> constraint_length;
            }
        """
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'Existing test::Base\.foo values violate the new constraint'):
            await self._migrate(new_state)

        # Fix the data.
        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := 'base_09',
            };
        """)

        # Migrate to same state as before now that the data is fixed.
        await self._migrate(new_state)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': 'base_09',
            }],
        )

    async def test_edgeql_migration_11(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            INSERT Base {
                foo := 'base_11',
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                # change property to link with same name
                link foo -> Child {
                    # add a constraint
                    constraint exclusive;
                }
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': None,
            }],
        )

    async def test_edgeql_migration_12(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                property foo -> str {
                    constraint exclusive;
                }

                link bar -> Child {
                    constraint exclusive;
                }
            }
        """)
        data = await self.con.fetchall(r"""
            SELECT (
                INSERT Base {
                    foo := 'base_12',
                    bar := (INSERT Child)
                })
            {
                foo,
                bar: {id}
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                # drop constraints
                property foo -> str;
                link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                    bar: {id}
                };
            """,
            [{
                'foo': 'base_12',
                'bar': {'id': data[0].bar.id}
            }],
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: column Derived~2.bar does
        not exist

        A problem arises when a link was inherited, but then needs to
        be moved towards the derived types in the inheritance
        hierarchy. This is the opposite of factoring out common link.

        See also `test_edgeql_migration_27`
    ''')
    async def test_edgeql_migration_13(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link bar -> Child;
            }

            type Derived extending Base {
                inherited required link bar -> Child;
            }
        """)
        data = await self.con.fetchall(r"""
            SELECT (
                INSERT Derived {
                    bar := (INSERT Child)
                })
            {
                bar: {id}
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base;
                # drop 'bar'

            type Derived extending Base {
                # no longer inherit link 'bar'
                link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    bar: {id}
                };
            """,
            [{
                'bar': {'id': data[0].bar.id}
            }],
        )

    async def test_edgeql_migration_14(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base;

            type Derived extending Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            INSERT Derived {
                foo := 'derived_14',
            };
        """)

        await self._migrate(r"""
            type Base {
                # move the property earlier in the inheritance
                property foo -> str;
            }

            type Derived extending Base {
                inherited required property foo -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    foo,
                };
            """,
            [{
                'foo': 'derived_14',
            }],
        )

    async def test_edgeql_migration_16(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base;

            type Derived extending Base {
                link bar -> Child;
            }
        """)
        data = await self.con.fetchall(r"""
            SELECT (
                INSERT Derived {
                    bar := (INSERT Child),
                }
            ) {
                bar: {id}
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                # move the link earlier in the inheritance
                link bar -> Child;
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    bar,
                };
            """,
            [{
                'bar': {'id': data[0].bar.id},
            }],
        )

        await self._migrate(r"""
            type Child;

            type Base {
                link bar -> Child;
            }

            type Derived extending Base {
                # also make the link 'required'
                inherited required link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    bar,
                };
            """,
            [{
                'bar': {'id': data[0].bar.id},
            }],
        )

    async def test_edgeql_migration_18(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property name := 'computable'
            }
        """)
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self._migrate(r"""
            type Base {
                # change a property from a computable to regular with a default
                property name -> str {
                    default := 'something'
                }
            }
        """)

        # Insert a new object, this one should have a new default name.
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name,
                } ORDER BY .name EMPTY LAST;
            """,
            [{
                'name': 'something',
            }, {
                'name': None,
            }],
        )

    async def test_edgeql_migration_19(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property name -> str
            }
        """)
        await self.con.execute(r"""
            INSERT Base {
                name := 'base_19'
            };
        """)

        await self._migrate(r"""
            type Base {
                # change a regular property to a computable
                property name := 'computable'
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name,
                };
            """,
            [{
                'name': 'computable',
            }],
        )

    async def test_edgeql_migration_21(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            INSERT Base {
                foo := 'base_21'
            };
        """)

        await self._migrate(r"""
            type Base {
                property foo -> str;
                # add a property
                property bar -> int64;
            }
        """)

        await self.con.execute(r"""
            UPDATE Base
            SET {
                bar := 21
            };
        """)
        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                    bar
                };
            """,
            [{
                'foo': 'base_21',
                'bar': 21,
            }],
        )

        await self._migrate(r"""
            type Base {
                # make the old property into a computable
                property foo := <str>__source__.bar;
                property bar -> int64;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                    bar
                };
            """,
            [{
                'foo': '21',
                'bar': 21,
            }],
        )

    async def test_edgeql_migration_22(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            INSERT Base {
                foo := 'base_22'
            };
        """)

        await self._migrate(r"""
            # rename the type, although this test doesn't ensure that
            # renaming actually took place
            type NewBase {
                property foo -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT NewBase {
                    foo,
                };
            """,
            [{
                'foo': 'base_22',
            }],
        )

        await self._migrate(r"""
            type NewBase {
                property foo -> str;
                # add a property
                property bar -> int64;
            }
        """)

        await self.con.execute(r"""
            UPDATE NewBase
            SET {
                bar := 22
            };
        """)
        await self.assert_query_result(
            r"""
                SELECT NewBase {
                    foo,
                    bar
                };
            """,
            [{
                'foo': 'base_22',
                'bar': 22,
            }],
        )

        await self._migrate(r"""
            type NewBase {
                # drop 'foo'
                property bar -> int64;
            }

            # add a view to emulate the original
            view Base := (
                SELECT NewBase {
                    foo := <str>.bar
                }
            );
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': '22',
            }],
        )

    @test.xfail('''
        edgedb.errors.InvalidReferenceError: reference to a
        non-existent schema item 9a576552-d980-11e9-ab34-4b202b90ea53
        in schema <Schema gen:4923 at 0x7f074da5b198>

        Note that the `test_migrations_equivalence_23` works fine.
    ''')
    async def test_edgeql_migration_23(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child {
                property foo -> str;
            }

            type Base {
                link bar -> Child;
            }

            view View01 := (
                SELECT Base {
                    child_foo := .bar.foo
                }
            );
        """)
        await self.con.execute(r"""
            INSERT Base {
                bar := (
                    INSERT Child {
                        foo := 'child_23'
                    }
                )
            };
        """)

        await self._migrate(r"""
            type Child {
                property foo -> str;
            }

            # exchange a type for a view
            view Base := (
                SELECT Child {
                    # bar is the same as the root object
                    bar := Child
                }
            );

            view View01 := (
                # now this view refers to another view
                SELECT Base {
                    child_foo := .bar.foo
                }
            );
        """)

        await self.assert_query_result(
            r"""
                SELECT View01 {
                    child_foo,
                };
            """,
            [{
                'child_foo': 'child_23',
            }],
        )

    async def test_edgeql_migration_24(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link bar -> Child;
            }
        """)
        data = await self.con.fetchall(r"""
            SELECT (
                INSERT Base {
                    bar := (INSERT Child)
                }
            ) {
                bar: {id}
            }
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                # increase link cardinality
                multi link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {id},
                };
            """,
            [{
                'bar': [{'id': data[0].bar.id}],
            }],
        )

    async def test_edgeql_migration_25(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                multi link bar -> Child;
            }
        """)
        data = await self.con.fetchall(r"""
            SELECT (
                INSERT Base {
                    bar := (INSERT Child)
                }
            ) {
                bar: {id}
            }
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                # reduce link cardinality
                link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {id},
                };
            """,
            [{
                'bar': {'id': data[0].bar[0].id},
            }],
        )

        await self._migrate(r"""
            type Child;

            type Base {
                link bar -> Child {
                    # further restrict the link
                    constraint exclusive
                }
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {id},
                };
            """,
            [{
                'bar': {'id': data[0].bar[0].id},
            }],
        )

    async def test_edgeql_migration_26(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Parent {
                link bar -> Child;
            }
        """)
        await self.con.execute(r"""
            INSERT Parent {
                bar := (INSERT Child)
            };
        """)

        await self._migrate(r"""
            type Child;

            type Parent {
                link bar -> Child;
            }

            # derive a type
            type DerivedParent extending Parent;
        """)

        await self.assert_query_result(
            r"""
                SELECT Parent {
                    type := .__type__.name,
                    bar_type := .bar.__type__.name
                };
            """,
            [{
                'type': 'test::Parent',
                'bar_type': 'test::Child',
            }],
        )

        await self._migrate(r"""
            type Child;

            type DerivedChild extending Child;

            type Parent {
                link bar -> Child;
            }

            # derive a type with a more restrictive link
            type DerivedParent extending Parent {
                inherited link bar -> DerivedChild;
            }
        """)

        await self.con.execute(r"""
            INSERT DerivedParent {
                bar := (INSERT DerivedChild)
            }
        """)
        await self.assert_query_result(
            r"""
                SELECT Parent {
                    type := .__type__.name,
                    bar_type := .bar.__type__.name
                } ORDER BY .bar_type;
            """,
            [{
                'type': 'test::Parent',
                'bar_type': 'test::Child',
            }, {
                'type': 'test::DerivedParent',
                'bar_type': 'test::DerivedChild',
            }],
        )

    async def test_edgeql_migration_27(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            abstract type Named {
                property name -> str;
            }

            type Foo extending Named;
            type Bar extending Named;
        """)
        await self.con.execute(r"""
            INSERT Foo {
                name := 'foo_27',
            };
            INSERT Bar {
                name := 'bar_27',
            };
        """)

        await self._migrate(r"""
            abstract type Named {
                property name -> str;
            }

            # the types stop extending named, but retain the property
            # 'name'
            type Foo {
                property name -> str;
            };

            type Bar {
                property name -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_27',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.name;
            """,
            [
                'bar_27',
            ],
        )

        await self._migrate(r"""
            abstract type Named {
                property name -> str;
            }

            type Foo {
                property name -> str;
            };

            type Bar {
                # rename 'name' to 'title'
                property title -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_27',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.title;
            """,
            [
                'bar_27',
            ],
        )

    async def test_edgeql_migration_29(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child {
                property foo -> str;
            }

            view Base := (
                SELECT Child {
                    bar := .foo
                }
            );
        """)
        await self.con.execute(r"""
            INSERT Child {
                foo := 'child_29',
            };
        """)

        await self._migrate(r"""
            # drop everything
        """)

    async def test_edgeql_migration_30(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Foo {
                property name -> str;
            };

            type Bar {
                property title -> str;
            };
        """)
        await self.con.execute(r"""
            INSERT Foo {
                name := 'foo_30',
            };
            INSERT Bar {
                title := 'bar_30',
            };
        """)

        await self._migrate(r"""
            type Foo {
                property name -> str;
            };

            type Bar {
                # rename 'title' to 'name'
                property name -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_30',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.name;
            """,
            [
                'bar_30',
            ],
        )

        await self._migrate(r"""
            # both types have a name, so the name prop is factored out
            # into a more basic type.
            abstract type Named {
                property name -> str;
            }

            type Foo extending Named;
            type Bar extending Named;
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_30',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.name;
            """,
            [
                'bar_30',
            ],
        )

    async def test_edgeql_migration_31(self):
        # Issue 727.
        #
        # Starting with the sample schema (from frontpage) migrate to
        # a schema with only type User.
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            # This is an abstract object containing
            # text.
            abstract type Text {
              required property body -> str {
                # Maximum length of text is 10000
                # characters.
                constraint max_len_value(10000);
              }
            }

            type User {
              required property name -> str;
            }

            abstract type Owned {
              # By default links are optional.
              required link owner -> User;
            }

            # UniquelyNamed is a an abstract type that
            # enforces name uniqueness across all
            # instances of its subtype.
            abstract type UniquelyNamed {
              required property name -> str {
                delegated constraint exclusive;
              }
            }

            type Status extending UniquelyNamed;

            type Priority extending UniquelyNamed;

            # LogEntry is an Owned and a Text,
            # so it will have all of their links
            # and properties, in particular, the
            # "owner" link and the "body" property.
            type LogEntry extending Owned, Text {
              required property spent_time -> int64;
            }

            type Comment extending Text, Owned {
              required link issue -> Issue;
              link parent -> Comment;
            }
            # issue_num_t is defined as a concrete
            # sequence type, used to generate
            # sequential issue numbers.
            scalar type issue_num_t extending sequence;

            type Issue extending Owned, Text {
              required property title -> str;

              required property number -> issue_num_t {
                # The number values are automatically
                # generated, and are not supposed to be
                # directly writable.
                readonly := true;
              }

              property time_estimate -> int64;

              property start_date -> datetime {
                # The default value of start_date will be a
                # result of the EdgeQL expression above.
                default := (SELECT datetime_current());
              }

              property due_date -> datetime;

              required link status -> Status;

              link priority -> Priority;

              # The watchers link is mapped to User
              # type in many-to-many relation.
              multi link watchers -> User;

              multi link time_spent_log -> LogEntry {
                # Exclusive multi-link represents
                # a one-to-many relation.
                constraint exclusive;
              }

              multi link related_to -> Issue;
            }
        """)
        await self.con.execute(r"""
            INSERT Status {
                name := 'Open'
            };
            INSERT Status {
                name := 'Closed'
            };

            INSERT User {
                name := 'cosmophile'
            };
        """)

        await self._migrate(r"""
            type User {
              required property name -> str;
            }
        """)

        # there's only the User left
        await self.assert_query_result(
            r"""
                SELECT User.name;
            """,
            [
                'cosmophile',
            ],
        )

    async def test_edgeql_migration_32(self):
        # Issue 727.
        #
        # Starting with a small schema migrate to remove its elements.
        await self.con.execute("""
            SET MODULE test;
        """)

        # There are non-zero default Objects existing in a fresh blank
        # database because of placeholder objects used for GraphQL.
        start_objects = await self.con.fetchone(r"""
            SELECT count(Object);
        """)

        await self._migrate(r"""
            type LogEntry {
              required property spent_time -> int64;
            }
            type Issue {
              multi link time_spent_log -> LogEntry {
                constraint exclusive;
              }
            }
        """)
        await self.con.execute(r"""
            INSERT LogEntry {
                spent_time := 100
            };

            INSERT Issue {
                time_spent_log := LogEntry
            };
        """)

        await self._migrate(r"""
            type LogEntry {
              required property spent_time -> int64;
            }
        """)

        # there's only the LogEntry left
        await self.assert_query_result(
            r"""
                SELECT LogEntry.spent_time;
            """,
            [
                100,
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT count(Object);
            """,
            [
                start_objects + 1,
            ],
        )

        await self._migrate(r"""
            # empty schema
        """)

        # no more additional objects
        await self.assert_query_result(
            r"""
                SELECT count(Object);
            """,
            [
                start_objects,
            ],
        )

    async def test_edgeql_migration_33(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link foo -> Child;
            }
        """)
        await self.con.execute(r"""
            INSERT Child;
            INSERT Base {
                foo := (SELECT Child LIMIT 1)
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                'foo': {
                    '__type__': {'name': 'test::Child'},
                }
            }],
        )

        await self._migrate(r"""
            type Child;
            type Child2;

            type Base {
                # change link type
                link foo -> Child2;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                # the link is empty because the target was changed
                'foo': None
            }],
        )

        await self.con.execute(r"""
            INSERT Child2;

            UPDATE Base
            SET {
                foo := (SELECT Child2 LIMIT 1)
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                'foo': {
                    '__type__': {'name': 'test::Child2'},
                }
            }],
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: column Base~2.foo does not exist

        The migration succeeds, but the propertry 'foo' can't be selected.
    ''')
    async def test_edgeql_migration_34(self):
        # this is the reverse of test_edgeql_migration_11
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    constraint exclusive;
                }
            }
        """)
        await self.con.execute(r"""
            INSERT Child;
            INSERT Base {
                foo := (SELECT Child LIMIT 1)
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                'foo': {
                    '__type__': {'name': 'test::Child'},
                }
            }],
        )

        await self._migrate(r"""
            type Base {
                # change link to property with same name
                property foo -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo
                };
            """,
            [{
                # the property is empty now
                'foo': None
            }],
        )

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := 'base_foo_34'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo
                };
            """,
            [{
                'foo': 'base_foo_34'
            }],
        )

    async def test_edgeql_migration_35(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                link foo := (
                    SELECT Child FILTER .name = 'computable_35'
                )
            }
        """)
        await self.con.execute(r"""
            INSERT Child {
                name := 'computable_35'
            };
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': [{
                    'name': 'computable_35',
                }]
            }]
        )

        await self._migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                # change a link from a computable to regular
                multi link foo -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': []
            }]
        )

        # Make sure that the new 'foo' can be updated.
        await self.con.execute(r"""
            INSERT Child {
                name := 'child_35'
            };
            UPDATE Base
            SET {
                foo := (
                    SELECT Child FILTER .name = 'child_35'
                )
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': [{
                    'name': 'child_35'
                }]
            }]
        )

    async def test_edgeql_migration_36(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                multi link foo -> Child;
            }
        """)
        await self.con.execute(r"""
            INSERT Child {
                name := 'computable_36'
            };
            INSERT Child {
                name := 'child_36'
            };
            INSERT Base {
                foo := (
                    SELECT Child FILTER .name = 'child_36'
                )
            };
        """)

        await self._migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                # change a regular link to a computable
                link foo := (
                    SELECT Child FILTER .name = 'computable_36'
                )
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': [{
                    'name': 'computable_36'
                }]
            }]
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: relation
        "edgedb_b395491c-e402-11e9-89e9-61d04b39f30a.
        b5441cbe-e402-11e9-847b-433bfe78aa8d"
        does not exist

        The second migration fails.
    ''')
    async def test_edgeql_migration_37(self):
        # testing schema views
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base;

            view BaseView := (
                SELECT Base {
                    foo := 'base_view_37'
                }
            )
        """)
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo
                };
            """,
            [{
                'foo': 'base_view_37'
            }]
        )

        await self._migrate(r"""
            type Base;

            view BaseView := (
                SELECT Base {
                    # "rename" a computable, since the value is given and
                    # not stored, this is no different from dropping
                    # original and creating a new property
                    foo2 := 'base_view_37'
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo2
                };
            """,
            [{
                'foo2': 'base_view_37'
            }]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"object type 'test::Base' has no link or property 'foo'"):
            await self.con.execute(r"""
                SELECT BaseView {
                    foo
                };
            """)

    async def test_edgeql_migration_38(self):
        # testing schema views
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base;

            view BaseView := (
                SELECT Base {
                    foo := 'base_view_38'
                }
            )
        """)
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo
                };
            """,
            [{
                'foo': 'base_view_38'
            }]
        )

        await self._migrate(r"""
            type Base;

            view BaseView := (
                SELECT Base {
                    # keep the name, but change the type
                    foo := 38
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo
                };
            """,
            [{
                'foo': 38
            }]
        )

    async def test_edgeql_migration_39(self):
        # testing schema views
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            view BaseView := (
                SELECT Base {
                    foo := (SELECT Foo FILTER .name = 'base_view_39')
                }
            )
        """)
        await self.con.execute(r"""
            INSERT Base;
            INSERT Foo {name := 'base_view_39'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo: {
                        name
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_view_39'
                }]
            }]
        )

        await self._migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            view BaseView := (
                SELECT Base {
                    # "rename" a computable, since the value is given and
                    # not stored, this is no different from dropping
                    # original and creating a new multi-link
                    foo2 := (SELECT Foo FILTER .name = 'base_view_39')
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo2: {
                        name
                    }
                };
            """,
            [{
                'foo2': [{
                    'name': 'base_view_39'
                }]
            }]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"object type 'test::Base' has no link or property 'foo'"):
            await self.con.execute(r"""
                SELECT BaseView {
                    foo: {
                        name
                    }
                };
            """)

    async def test_edgeql_migration_40(self):
        # testing schema views
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            type Bar {
                property name -> str
            }

            view BaseView := (
                SELECT Base {
                    foo := (SELECT Foo FILTER .name = 'foo_40')
                }
            )
        """)
        await self.con.execute(r"""
            INSERT Base;
            INSERT Foo {name := 'foo_40'};
            INSERT Bar {name := 'bar_40'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo: {
                        name
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'foo_40'
                }]
            }]
        )

        await self._migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            type Bar {
                property name -> str
            }

            view BaseView := (
                SELECT Base {
                    # keep the name, but change the type
                    foo := (SELECT Bar FILTER .name = 'bar_40')
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo: {
                        name
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'bar_40'
                }]
            }]
        )

    @test.xfail('''
        The error appears to be the same as for test_migrations_equivalence_41
    ''')
    async def test_edgeql_migration_41(self):
        # testing schema views
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            view BaseView := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            @bar := 'foo_bar_view_41'
                        }
                        FILTER .name = 'base_view_41'
                    )
                }
            )
        """)
        await self.con.execute(r"""
            INSERT Base;
            INSERT Foo {name := 'base_view_41'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo: {
                        name,
                        @bar
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_view_41',
                    '@bar': 'foo_bar_view_41',
                }]
            }]
        )

        await self._migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            view BaseView := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            # "rename" a computable link property, since
                            # the value is given and not stored, this is
                            # no different from dropping original and
                            # creating a new multi-link
                            @baz := 'foo_bar_view_41'
                        }
                        FILTER .name = 'base_view_41'
                    )
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo: {
                        name,
                        @baz
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_view_41',
                    '@baz': 'foo_bar_view_41'
                }]
            }]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"link 'fuu' has no property 'bar'"):
            await self.con.execute(r"""
                SELECT BaseView {
                    foo: {
                        name,
                        @bar
                    }
                };
            """)

    @test.xfail('''
        The error appears to be the same as for test_migrations_equivalence_42
    ''')
    async def test_edgeql_migration_42(self):
        # testing schema views
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            view BaseView := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            @bar := 'foo_bar_view_42'
                        }
                        FILTER .name = 'base_view_42'
                    )
                }
            )
        """)
        await self.con.execute(r"""
            INSERT Base;
            INSERT Foo {name := 'base_view_42'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo: {
                        name,
                        @bar
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_view_42',
                    '@bar': 'foo_bar_view_42',
                }]
            }]
        )

        await self._migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            view BaseView := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            # keep the name, but change the type
                            @bar := 42
                        }
                        FILTER .name = 'base_view_42'
                    )
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseView {
                    foo: {
                        name,
                        @bar
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_view_42',
                    '@bar': 42,
                }]
            }]
        )

    async def test_edgeql_migration_function_01(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello01(a: int64) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello01(1);""",
            ['hello1'],
        )

        # add an extra parameter with a default (so it can be omitted
        # in principle)
        await self._migrate(r"""
            function hello01(a: int64, b: int64=42) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>(a + b)
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello01(1);""",
            ['hello43'],
        )

        await self.assert_query_result(
            r"""SELECT hello01(1, 2);""",
            ['hello3'],
        )

    async def test_edgeql_migration_function_02(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello02(a: int64) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello02(1);""",
            ['hello1'],
        )

        # add an extra parameter with a default (so it can be omitted
        # in principle)
        await self._migrate(r"""
            function hello02(a: int64, b: OPTIONAL int64=42) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>(a + b)
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello02(1);""",
            ['hello43'],
        )

        await self.assert_query_result(
            r"""SELECT hello02(1, 2);""",
            ['hello3'],
        )

    async def test_edgeql_migration_function_03(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello03(a: int64) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello03(1);""",
            ['hello1'],
        )

        # add an extra parameter with a default (so it can be omitted
        # in principle)
        await self._migrate(r"""
            function hello03(a: int64, NAMED ONLY b: int64=42) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>(a + b)
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello03(1);""",
            ['hello43'],
        )

        await self.assert_query_result(
            r"""SELECT hello03(1, b := 2);""",
            ['hello3'],
        )

    async def test_edgeql_migration_function_04(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello04(a: int64) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello04(1);""",
            ['hello1'],
        )

        # same parameters, different return type
        await self._migrate(r"""
            function hello04(a: int64) -> int64
                from edgeql $$
                    SELECT -a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello04(1);""",
            [-1],
        )

    async def test_edgeql_migration_function_05(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello05(a: int64) -> str
                from edgeql $$
                    SELECT <str>a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello05(1);""",
            ['1'],
        )

        # same parameters, different return type (array)
        await self._migrate(r"""
            function hello05(a: int64) -> array<int64>
                from edgeql $$
                    SELECT [a]
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello05(1);""",
            [[1]],
        )

    @test.xfail('''
        It should be possible to change the underlying function (to a
        compatible one) of a default value without explicitly dropping
        the default first.

        edgedb.errors.InternalServerError: cannot drop function
        "edgedb_06261450-db74-11e9-9e9a-9520733a1c54".hello06(bigint)
        because other objects depend on it
    ''')
    async def test_edgeql_migration_function_06(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello06(a: int64) -> str
                from edgeql $$
                    SELECT <str>a
                $$;

            type Base {
                property foo -> int64 {
                    # use the function in default value computation
                    default := len(hello06(2) ++ hello06(123))
                }
            }
        """)
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self._migrate(r"""
            function hello06(a: int64) -> array<int64>
                from edgeql $$
                    SELECT [a]
                $$;

            type Base {
                property foo -> int64 {
                    # use the function in default value computation
                    default := len(hello06(2) ++ hello06(123))
                }
            }
        """)
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {4, 2},
        )

    async def test_edgeql_migration_function_07(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello07(a: int64) -> str
                from edgeql $$
                    SELECT <str>a
                $$;

            type Base {
                # use the function in computable value
                property foo := len(hello07(2) ++ hello07(123))
            }
        """)
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self._migrate(r"""
            function hello07(a: int64) -> array<int64>
                from edgeql $$
                    SELECT [a]
                $$;

            type Base {
                # use the function in computable value
                property foo := len(hello07(2) ++ hello07(123))
            }
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {2},
        )

    async def test_edgeql_migration_function_08(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello08(a: int64) -> str
                from edgeql $$
                    SELECT <str>a
                $$;

            # use the function in a view directly
            view foo := len(hello08(2) ++ hello08(123));
        """)

        await self.assert_query_result(
            r"""SELECT foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self._migrate(r"""
            function hello08(a: int64) -> array<int64>
                from edgeql $$
                    SELECT [a]
                $$;

            # use the function in a view directly
            view foo := len(hello08(2) ++ hello08(123));
        """)

        await self.assert_query_result(
            r"""SELECT foo;""",
            {2},
        )

    async def test_edgeql_migration_function_09(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello09(a: int64) -> str
                from edgeql $$
                    SELECT <str>a
                $$;

            type Base;

            # use the function in a view directly
            view BaseView := (
                SELECT Base {
                    foo := len(hello09(2) ++ hello09(123))
                }
            );
        """)
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT BaseView.foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self._migrate(r"""
            function hello09(a: int64) -> array<int64>
                from edgeql $$
                    SELECT [a]
                $$;

            type Base;

            # use the function in a view directly
            view BaseView := (
                SELECT Base {
                    foo := len(hello09(2) ++ hello09(123))
                }
            );
        """)

        await self.assert_query_result(
            r"""SELECT BaseView.foo;""",
            {2},
        )

    @test.xfail('''
        See `test_migrations_equivalence_function_10` first.
    ''')
    async def test_edgeql_migration_function_10(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello10(a: int64) -> str
                from edgeql $$
                    SELECT <str>a
                $$;

            type Base {
                required property foo -> int64 {
                    # use the function in a constraint expression
                    constraint expression on (len(hello10(__subject__)) < 2)
                }
            }
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid foo'):
            await self.con.execute(r"""
                INSERT Base {foo := 42};
            """)

        # same parameters, different return type (array)
        await self._migrate(r"""
            function hello10(a: int64) -> array<int64>
                from edgeql $$
                    SELECT [a]
                $$;

            type Base {
                required property foo -> int64 {
                    # use the function in a constraint expression
                    constraint expression on (len(hello10(__subject__)) < 2)
                }
            }
        """)

        # no problem with the constraint now
        await self.con.execute(r"""
            INSERT Base {foo := 42};
        """)

    async def test_edgeql_migration_function_11(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello11(a: int64) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello11(1);""",
            ['hello1'],
        )

        await self._migrate(r"""
            # replace the function with a new one by the same name
            function hello11(a: str) -> str
                from edgeql $$
                    SELECT 'hello' ++ a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello11(' world');""",
            ['hello world'],
        )

        # make sure that the old one is gone
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not find a function variant hello11'):
            await self.con.execute(
                r"""SELECT hello11(1);"""
            )

    @test.xfail('''
        edgedb.errors.QueryError: could not find a function variant hello12

        After the migration only one version of the function exists,
        instead of two.
    ''')
    async def test_edgeql_migration_function_12(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            function hello12(a: int64) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;
        """)

        await self.assert_query_result(
            r"""SELECT hello12(1);""",
            ['hello1'],
        )

        await self._migrate(r"""
            function hello12(a: int64) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;

            # make the function polymorphic
            function hello12(a: str) -> str
                from edgeql $$
                    SELECT 'hello' ++ a
                $$;
        """)

        await self.assert_query_result(
            r"""SELECT hello12(' world');""",
            ['hello world'],
        )

        # make sure that the old one still works
        await self.assert_query_result(
            r"""SELECT hello12(1);""",
            ['hello1'],
        )

    @test.xfail('''
        edgedb.errors.QueryError: could not find a function variant hello13

        The first migration ostensibly succeeds, but there's only one
        version of the function instead of two.
    ''')
    async def test_edgeql_migration_function_13(self):
        # this is the inverse of test_edgeql_migration_function_12
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            # start with a polymorphic function
            function hello13(a: int64) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;

            function hello13(a: str) -> str
                from edgeql $$
                    SELECT 'hello' ++ a
                $$;
        """)

        await self.assert_query_result(
            r"""SELECT hello13(' world');""",
            ['hello world'],
        )
        await self.assert_query_result(
            r"""SELECT hello13(1);""",
            ['hello1'],
        )

        await self._migrate(r"""
            # remove one of the 2 versions
            function hello13(a: int64) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;
        """)

        await self.assert_query_result(
            r"""SELECT hello13(1);""",
            ['hello1'],
        )

        # make sure that the other one is gone
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not find a function variant hello13'):
            await self.con.execute(
                r"""SELECT hello11(' world');"""
            )

    async def test_edgeql_migration_linkprops_01(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link foo -> Child;
            };
        """)
        await self.con.execute(r"""
            INSERT Base {
                foo := (INSERT Child)
            };
        """)

        # Migration adding a link property.
        await self._migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> str
                }
            };
        """)

        # actually record a link property
        await self.con.execute(r"""
            UPDATE
                Base
            SET {
                foo: {
                    @bar := 'lp01'
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': {'@bar': 'lp01'}}],
        )

    async def test_edgeql_migration_linkprops_02(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> str
                }
            };
        """)
        await self.con.execute(r"""
            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo: {@bar := 'lp02'},
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    # change the link property name
                    property bar2 -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar2 }
                };
            """,
            [{'foo': {'@bar2': 'lp02'}}],
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: column "bar" of relation
        "f24ff0f4-db8f-11e9-a887-356e9f41deb7" does not exist
    ''')
    async def test_edgeql_migration_linkprops_03(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> int64
                }
            };
        """)
        await self.con.execute(r"""
            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo: {@bar := 3},
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    # change the link property type
                    property bar -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': {'@bar2': '3'}}],
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: duplicate key value
        violates unique constraint
        "68d4c708-db91-11e9-9b69-4fe8032d0_source_target_ptr_item_id_key"
    ''')
    async def test_edgeql_migration_linkprops_04(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> str
                }
            };
        """)
        await self.con.execute(r"""
            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo: {@bar := 'lp04'},
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                # change the link cardinality
                multi link foo -> Child {
                    property bar -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': [{'@bar': 'lp04'}]}],
        )

    async def test_edgeql_migration_linkprops_05(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                multi link foo -> Child {
                    property bar -> str
                }
            };
        """)
        await self.con.execute(r"""
            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo: {@bar := 'lp05'},
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                # change the link cardinality
                link foo -> Child {
                    property bar -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': {'@bar': 'lp05'}}],
        )

    async def test_edgeql_migration_linkprops_06(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link child -> Child {
                    property foo -> str;
                }
            };
        """)
        await self.con.execute(r"""
            INSERT Base {child := (INSERT Child)};
            UPDATE Base
            SET {
                child: {
                    @foo := 'lp06',
                },
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                link child -> Child {
                    property foo -> str;
                    # add another link prop
                    property bar -> int64;
                }
            };
        """)
        # update the existing data with a new link prop 'bar'
        await self.con.execute(r"""
            UPDATE Base
            SET {
                child: {
                    @bar := 111,
                },
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                        @bar
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp06',
                    '@bar': 111
                }
            }],
        )

    async def test_edgeql_migration_linkprops_07(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                inherited link child -> Child {
                    property foo -> str
                }
            };
        """)
        await self.con.execute(r"""
            INSERT Derived {child := (INSERT Child)};
            UPDATE Derived
            SET {
                child: {
                    @foo := 'lp07',
                },
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                # move the link property earlier in the inheritance tree
                link child -> Child {
                    property foo -> str
                }
            };

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp07',
                }
            }],
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: relation
        "edgedb_f1a94eb6-dbf2-11e9-a3fb-214b780369d9.f20ba958-dbf2-11e9-8884-5764a7d0627a"
        does not exist

        See `test_edgeql_insert_derived_02` first.
    ''')
    async def test_edgeql_migration_linkprops_08(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link child -> Child {
                    property foo -> str
                }
            };

            type Derived extending Base;
        """)
        await self.con.execute(r"""
            INSERT Derived {child := (INSERT Child)};
        """)
        await self.con.execute(r"""
            UPDATE Derived
            SET {
                child: {
                    @foo := 'lp08',
                },
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                inherited link child -> Child {
                    # move the link property later in the inheritance tree
                    property foo -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp08',
                }
            }],
        )

    async def test_edgeql_migration_linkprops_09(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                inherited link child -> Child {
                    property foo -> str
                }
            };
        """)
        await self.con.execute(r"""
            INSERT Derived {child := (INSERT Child)};
            UPDATE Derived
            SET {
                child: {
                    @foo := 'lp09',
                },
            };
        """)

        await self._migrate(r"""
            type Child;

            # factor out link property all the way to an abstract link
            abstract link base_child {
                property foo -> str;
            }

            type Base {
                link child extending base_child -> Child;
            };

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp09',
                }
            }],
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: relation
        "edgedb_510fbc78-dbf5-11e9-a939-c1e7446a8fdd.543b5dfe-dbf5-11e9-b1c7-a3ddec71fbde"
        does not exist

        See `test_edgeql_insert_derived_02` first.
    ''')
    async def test_edgeql_migration_linkprops_10(self):
        # reverse of the test_edgeql_migration_linkprops_09 refactoring
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Child;

            abstract link base_child {
                property foo -> str;
            }

            type Base {
                link child extending base_child -> Child;
            };

            type Derived extending Base;
        """)
        await self.con.execute(r"""
            INSERT Derived {child := (INSERT Child)};
            UPDATE Derived
            SET {
                child: {
                    @foo := 'lp10',
                },
            };
        """)

        await self._migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                inherited link child -> Child {
                    # move the link property later in the inheritance tree
                    property foo -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp10',
                }
            }],
        )

    async def test_edgeql_migration_linkprops_11(self):
        # merging a link with the same properties
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Thing;

            type Owner {
                link item -> Thing {
                    property foo -> str;
                }
            };

            type Renter {
                link item -> Thing {
                    property foo -> str;
                }
            };
        """)
        await self.con.execute(r"""
            INSERT Owner {item := (INSERT Thing)};
            UPDATE Owner
            SET {
                item: {
                    @foo := 'owner_lp11',
                },
            };

            INSERT Renter {item := (INSERT Thing)};
            UPDATE Renter
            SET {
                item: {
                    @foo := 'renter_lp11',
                },
            };
        """)

        await self._migrate(r"""
            type Thing;

            type Base {
                link item -> Thing {
                    property foo -> str;
                }
            };

            type Owner extending Base;

            type Renter extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Owner {
                    item: {
                        @foo,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': 'owner_lp11',
                }
            }],
        )

        await self.assert_query_result(
            r"""
                SELECT Renter {
                    item: {
                        @foo,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': 'renter_lp11',
                }
            }],
        )

    @test.xfail('''
        This appears to fail to find the link property 'foo' or 'bar'
        when applying the delta of the second migration.
    ''')
    async def test_edgeql_migration_linkprops_12(self):
        # merging a link with different properties
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Thing;

            type Owner {
                link item -> Thing {
                    property foo -> str;
                }
            };

            type Renter {
                link item -> Thing {
                    property bar -> str;
                }
            };
        """)
        await self.con.execute(r"""
            INSERT Owner {item := (INSERT Thing)};
            UPDATE Owner
            SET {
                item: {
                    @foo := 'owner_lp11',
                },
            };

            INSERT Renter {item := (INSERT Thing)};
            UPDATE Renter
            SET {
                item: {
                    @bar := 'renter_lp11',
                },
            };
        """)

        await self._migrate(r"""
            type Thing;

            type Base {
                link item -> Thing {
                    property foo -> str;
                    property bar -> str;
                }
            };

            type Owner extending Base;

            type Renter extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Owner {
                    item: {
                        @foo,
                        @bar,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': 'owner_lp11',
                    '@bar': None,
                }
            }],
        )

        await self.assert_query_result(
            r"""
                SELECT Renter {
                    item: {
                        @foo,
                        @bar,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': None,
                    '@bar': 'renter_lp11',
                }
            }],
        )

    async def test_edgeql_migration_annotation_01(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [],
            }],
        )

        await self._migrate(r"""
            type Base {
                # add a title annotation
                annotation title := 'Base description 01'
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 01'
                }],
            }],
        )

        await self._migrate(r"""
            # add inheritable and non-inheritable annotations
            abstract annotation foo_anno;
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 01';
                annotation foo_anno := 'Base foo_anno 01';
                annotation bar_anno := 'Base bar_anno 01';
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 01'
                }, {
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 01'
                }, {
                    'name': 'test::foo_anno',
                    '@value': 'Base foo_anno 01'
                }],
            }],
        )

        await self._migrate(r"""
            abstract annotation foo_anno;
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 01';
                annotation foo_anno := 'Base foo_anno 01';
                annotation bar_anno := 'Base bar_anno 01';
            }

            # extend Base
            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 01'
                }, {
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 01'
                }, {
                    'name': 'test::foo_anno',
                    '@value': 'Base foo_anno 01'
                }],
            }, {
                'name': 'test::Derived',
                'annotations': [{
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 01'
                }],
            }],
        )

    async def test_edgeql_migration_annotation_02(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [],
            }],
        )

        await self._migrate(r"""
            abstract annotation foo_anno;

            type Base {
                annotation title := 'Base description 02';
                annotation foo_anno := 'Base foo_anno 02';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 02'
                }, {
                    'name': 'test::foo_anno',
                    '@value': 'Base foo_anno 02'
                }],
            }, {
                'name': 'test::Derived',
                # annotation not inherited
                'annotations': [],
            }],
        )

        await self._migrate(r"""
            # remove foo_anno
            type Base {
                annotation title := 'Base description 02';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 02'
                }],
            }, {
                'name': 'test::Derived',
                'annotations': [],
            }],
        )

    async def test_edgeql_migration_annotation_03(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [],
            }],
        )

        await self._migrate(r"""
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 03';
                annotation bar_anno := 'Base bar_anno 03';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 03'
                }, {
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 03'
                }],
            }, {
                'name': 'test::Derived',
                # annotation inherited
                'annotations': [{
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 03'
                }],
            }],
        )

        await self._migrate(r"""
            # remove bar_anno
            type Base {
                annotation title := 'Base description 03';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 03'
                }],
            }, {
                'name': 'test::Derived',
                'annotations': [],
            }],
        )

    @test.xfail('''
        Fails on the last migration that attempts to rename the
        property being indexed.
    ''')
    async def test_edgeql_migration_index_01(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property name -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

        await self._migrate(r"""
            type Base {
                property name -> str;
                # an index
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

        await self._migrate(r"""
            type Base {
                # rename the indexed property
                property title -> str;
                index on (.title);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.title'
                }]
            }],
        )

    async def test_edgeql_migration_index_02(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property name -> str;
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

        await self._migrate(r"""
            type Base {
                property name -> str;
                # remove the index
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

    async def test_edgeql_migration_index_03(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property name -> int64;
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

        await self._migrate(r"""
            type Base {
                property name -> int64;
                # an index
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

        await self._migrate(r"""
            type Base {
                # change the indexed property type
                property name -> str;
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

    async def test_edgeql_migration_index_04(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self._migrate(r"""
            type Base {
                property first_name -> str;
                property last_name -> str;
                property name := .first_name ++ ' ' ++ .last_name;
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

        await self._migrate(r"""
            type Base {
                property first_name -> str;
                property last_name -> str;
                property name := .first_name ++ ' ' ++ .last_name;
                # an index on a computable
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )
