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


from edb import errors

from edb.common import markup
from edb.testbase import lang as tb

from edb import edgeql
from edb.edgeql import qltypes

from edb.schema import delta as s_delta
from edb.schema import ddl as s_ddl
from edb.schema import links as s_links
from edb.schema import objtypes as s_objtypes

from edb.tools import test


class TestSchema(tb.BaseSchemaLoadTest):
    def test_schema_inherited_01(self):
        """
            type UniqueName {
                property name -> str {
                    constraint exclusive
                }
            };
            type UniqueName_2 extending UniqueName {
                inherited property name -> str {
                    constraint exclusive
                }
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "'name'.*must be declared using the `inherited` keyword",
                  position=214)
    def test_schema_inherited_02(self):
        """
            type UniqueName {
                property name -> str {
                    constraint exclusive
                }
            };

            type UniqueName_2 extending UniqueName {
                property name -> str {
                    constraint exclusive
                }
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "'name'.*cannot be declared `inherited`",
                  position=47)
    def test_schema_inherited_03(self):
        """
            type UniqueName {
                inherited property name -> str
            };
        """

    @tb.must_fail(errors.InvalidLinkTargetError,
                  'invalid link target, expected object type, got ScalarType',
                  position=55)
    def test_schema_bad_link_01(self):
        """
            type Object {
                link foo -> str
            };
        """

    @tb.must_fail(errors.InvalidLinkTargetError,
                  'invalid link target, expected object type, got ScalarType',
                  position=55)
    def test_schema_bad_link_02(self):
        """
            type Object {
                link foo := 1 + 1
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  'link or property name length exceeds the maximum.*',
                  position=43)
    def test_schema_bad_link_03(self):
        """
            type Object {
                link f123456789_123456789_123456789_123456789_123456789\
_123456789_123456789_123456789 -> Object
            };
        """

    @tb.must_fail(errors.InvalidPropertyTargetError,
                  "invalid property type: expected a scalar type, "
                  "or a scalar collection, got 'test::Object'",
                  position=59)
    def test_schema_bad_prop_01(self):
        """
            type Object {
                property foo -> Object
            };
        """

    @tb.must_fail(errors.InvalidPropertyTargetError,
                  "invalid property type: expected a scalar type, "
                  "or a scalar collection, got 'test::Object'",
                  position=59)
    def test_schema_bad_prop_02(self):
        """
            type Object {
                property foo := (SELECT Object)
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  'link or property name length exceeds the maximum.*',
                  position=43)
    def test_schema_bad_prop_03(self):
        """
            type Object {
                property f123456789_123456789_123456789_123456789_123456789\
_123456789_123456789_123456789 -> str
            };
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  "type 'int' does not exist",
                  position=59,
                  hint='did you mean one of these: int16, int32, int64?')
    def test_schema_bad_type_01(self):
        """
            type Object {
                property foo -> int
            };
        """

    def test_schema_computable_cardinality_inference_01(self):
        schema = self.load_schema("""
            type Object {
                property foo -> str;
                property bar -> str;
                property foo_plus_bar := __source__.foo ++ __source__.bar;
            };
        """)

        obj = schema.get('test::Object')
        self.assertEqual(
            obj.getptr(schema, 'foo_plus_bar').get_cardinality(schema),
            qltypes.Cardinality.ONE)

    def test_schema_computable_cardinality_inference_02(self):
        schema = self.load_schema("""
            type Object {
                multi property foo -> str;
                property bar -> str;
                property foo_plus_bar := __source__.foo ++ __source__.bar;
            };
        """)

        obj = schema.get('test::Object')
        self.assertEqual(
            obj.getptr(schema, 'foo_plus_bar').get_cardinality(schema),
            qltypes.Cardinality.MANY)

    def test_schema_refs_01(self):
        schema = self.load_schema("""
            type Object1;
            type Object2 {
                link foo -> Object1
            };
            type Object3 extending Object1;
            type Object4 extending Object1;
            type Object5 {
                link bar -> Object2
            };
            type Object6 extending Object4;
        """)

        Obj1 = schema.get('test::Object1')
        Obj2 = schema.get('test::Object2')
        Obj3 = schema.get('test::Object3')
        Obj4 = schema.get('test::Object4')
        Obj5 = schema.get('test::Object5')
        Obj6 = schema.get('test::Object6')
        obj1_id = Obj1.getptr(schema, 'id')
        obj1_type = Obj1.getptr(schema, '__type__')
        obj1_type_source = obj1_type.getptr(schema, 'source')
        obj2_type = Obj2.getptr(schema, '__type__')
        foo = Obj2.getptr(schema, 'foo')
        foo_target = foo.getptr(schema, 'target')
        bar = Obj5.getptr(schema, 'bar')

        self.assertEqual(
            schema.get_referrers(Obj1),
            frozenset({
                foo,         # Object 1 is a Object2.foo target
                foo_target,  # and also a target of its @target property
                Obj3,        # It is also in Object3's bases and ancestors
                Obj4,        # Likewise for Object4
                Obj6,        # Object6 through its ancestors
                obj1_id,     # Inherited id property
                obj1_type,   # Inherited __type__ link
                obj1_type_source,  # and its @source property
            })
        )

        self.assertEqual(
            schema.get_referrers(Obj1, scls_type=s_objtypes.ObjectType),
            {
                Obj3,        # It is also in Object3's bases and ancestors
                Obj4,        # Likewise for Object4
                Obj6,        # Object6 through its ancestors
            }
        )

        self.assertEqual(
            schema.get_referrers(Obj2, scls_type=s_links.Link),
            {
                foo,        # Obj2 is foo's source
                bar,        # Obj2 is bar's target
                obj2_type,  # Iherited Obj2.__type__ link
            }
        )

        self.assertEqual(
            schema.get_referrers(Obj2, scls_type=s_links.Link,
                                 field_name='target'),
            {
                bar,        # Obj2 is bar's target
            }
        )

        schema = self.run_ddl(schema, '''
            ALTER TYPE test::Object4 DROP EXTENDING test::Object1;
        ''')

        self.assertEqual(
            schema.get_referrers(Obj1),
            frozenset({
                foo,         # Object 1 is a Object2.foo target
                foo_target,  # and also a target of its @target property
                Obj3,        # It is also in Object3's bases and ancestors
                obj1_id,     # Inherited id property
                obj1_type,   # Inherited __type__ link
                obj1_type_source,  # and its @source property
            })
        )

        schema = self.run_ddl(schema, '''
            ALTER TYPE test::Object3 DROP EXTENDING test::Object1;
        ''')

        self.assertEqual(
            schema.get_referrers(Obj1),
            frozenset({
                foo,         # Object 1 is a Object2.foo target
                foo_target,  # and also a target of its @target property
                obj1_id,     # Inherited id property
                obj1_type,   # Inherited __type__ link
                obj1_type_source,  # and its @source property
            })
        )

        schema = self.run_ddl(schema, '''
            CREATE FUNCTION
            test::my_contains(arr: array<anytype>, val: anytype) -> bool {
                FROM edgeql $$
                    SELECT contains(arr, val);
                $$;
            };

            CREATE ABSTRACT CONSTRAINT
            test::my_one_of(one_of: array<anytype>) {
                SET expr := (
                    WITH foo := test::Object1
                    SELECT test::my_contains(one_of, __subject__)
                );
            };

            CREATE SCALAR TYPE test::my_scalar_t extending str {
                CREATE CONSTRAINT test::my_one_of(['foo', 'bar']);
            };
        ''')

        my_scalar_t = schema.get('test::my_scalar_t')
        abstr_constr = schema.get('test::my_one_of')
        constr = my_scalar_t.get_constraints(schema).objects(schema)[0]
        my_contains = schema.get_functions('test::my_contains')[0]
        self.assertEqual(
            schema.get_referrers(my_contains),
            frozenset({
                constr,
            })
        )

        self.assertEqual(
            schema.get_referrers(Obj1),
            frozenset({
                foo,           # Object 1 is a Object2.foo target
                foo_target,    # and also a target of its @target property
                abstr_constr,  # abstract constraint my_one_of
                constr,        # concrete constraint in my_scalar_t
                obj1_id,       # Inherited id property
                obj1_type,     # Inherited __type__ link
                obj1_type_source,  # and its @source property
            })
        )

    def test_schema_refs_02(self):
        schema = self.load_schema("""
            type Object1 {
                property num -> int64;
            };
            type Object2 {
                required property num -> int64 {
                    default := (
                        SELECT Object1.num + 1
                        ORDER BY Object1.num DESC
                        LIMIT 1
                    )
                }
            };
        """)

        Obj1 = schema.get('test::Object1')
        obj1_num = Obj1.getptr(schema, 'num')

        Obj2 = schema.get('test::Object2')
        obj2_num = Obj2.getptr(schema, 'num')

        self.assertEqual(
            schema.get_referrers(obj1_num),
            frozenset({
                Obj1,
                obj2_num,
            })
        )

    def test_schema_refs_03(self):
        schema = self.load_schema("""
            type Object1 {
                property num -> int64;
            };
            type Object2 {
                required property num -> int64 {
                    default := (
                        SELECT Object1.num LIMIT 1
                    )
                }
            };
        """)

        Obj1 = schema.get('test::Object1')
        obj1_num = Obj1.getptr(schema, 'num')

        Obj2 = schema.get('test::Object2')
        obj2_num = Obj2.getptr(schema, 'num')

        self.assertEqual(
            schema.get_referrers(obj1_num),
            frozenset({
                Obj1,
                obj2_num,
            })
        )

    def test_schema_annotation_inheritance(self):
        schema = self.load_schema("""
            abstract annotation noninh;
            abstract inheritable annotation inh;

            type Object1 {
                annotation noninh := 'bar';
                annotation inh := 'inherit me';
            };

            type Object2 extending Object1;
        """)

        Object1 = schema.get('test::Object1')
        Object2 = schema.get('test::Object2')

        self.assertEqual(Object1.get_annotation(schema, 'test::noninh'), 'bar')
        # Attributes are non-inheritable by default
        self.assertIsNone(Object2.get_annotation(schema, 'test::noninh'))

        self.assertEqual(
            Object1.get_annotation(schema, 'test::inh'), 'inherit me')
        self.assertEqual(
            Object2.get_annotation(schema, 'test::inh'), 'inherit me')

    def test_schema_object_verbosename(self):
        schema = self.load_schema("""
            abstract inheritable annotation attr;
            abstract link lnk_1;
            abstract property prop_1;

            type Object1 {
                annotation attr := 'inherit me';
                property foo -> std::str {
                    annotation attr := 'propprop';
                    constraint max_len_value(10)
                }

                link bar -> Object {
                    constraint exclusive;
                    annotation attr := 'bbb';
                    property bar_prop -> std::str {
                        annotation attr := 'aaa';
                        constraint max_len_value(10);
                    }
                }
            };
        """)

        schema = self.run_ddl(schema, '''
            CREATE FUNCTION test::foo (a: int64) -> int64
            FROM EdgeQL $$ SELECT a; $$;
        ''')

        self.assertEqual(
            schema.get('test::attr').get_verbosename(schema),
            "abstract annotation 'test::attr'",
        )

        self.assertEqual(
            schema.get('test::lnk_1').get_verbosename(schema),
            "abstract link 'test::lnk_1'",
        )

        self.assertEqual(
            schema.get('test::prop_1').get_verbosename(schema),
            "abstract property 'test::prop_1'",
        )

        self.assertEqual(
            schema.get('std::max_len_value').get_verbosename(schema),
            "abstract constraint 'std::max_len_value'",
        )

        fn = list(schema.get_functions('std::json_typeof'))[0]
        self.assertEqual(
            fn.get_verbosename(schema),
            'function std::json_typeof(json: std::json)',
        )

        fn_param = fn.get_params(schema).get_by_name(schema, 'json')
        self.assertEqual(
            fn_param.get_verbosename(schema, with_parent=True),
            "parameter 'json' of function std::json_typeof(json: std::json)",
        )

        op = list(schema.get_operators('std::AND'))[0]
        self.assertEqual(
            op.get_verbosename(schema),
            'operator "std::bool AND std::bool"',
        )

        obj = schema.get('test::Object1')

        self.assertEqual(
            obj.get_verbosename(schema),
            "object type 'test::Object1'",
        )

        self.assertEqual(
            obj.get_annotations(schema).get(
                schema, 'test::attr').get_verbosename(
                    schema, with_parent=True),
            "annotation 'test::attr' of object type 'test::Object1'",
        )

        foo_prop = obj.get_pointers(schema).get(schema, 'foo')
        self.assertEqual(
            foo_prop.get_verbosename(schema, with_parent=True),
            "property 'foo' of object type 'test::Object1'",
        )

        self.assertEqual(
            foo_prop.get_annotations(schema).get(
                schema, 'test::attr').get_verbosename(
                    schema, with_parent=True),
            "annotation 'test::attr' of property 'foo' of "
            "object type 'test::Object1'",
        )

        self.assertEqual(
            next(iter(foo_prop.get_constraints(
                schema).objects(schema))).get_verbosename(
                    schema, with_parent=True),
            "constraint 'std::max_len_value' of property 'foo' of "
            "object type 'test::Object1'",
        )

        bar_link = obj.get_pointers(schema).get(schema, 'bar')
        self.assertEqual(
            bar_link.get_verbosename(schema, with_parent=True),
            "link 'bar' of object type 'test::Object1'",
        )

        bar_link_prop = bar_link.get_pointers(schema).get(schema, 'bar_prop')
        self.assertEqual(
            bar_link_prop.get_annotations(schema).get(
                schema, 'test::attr').get_verbosename(
                    schema, with_parent=True),
            "annotation 'test::attr' of property 'bar_prop' of "
            "link 'bar' of object type 'test::Object1'",
        )

        self.assertEqual(
            next(iter(bar_link_prop.get_constraints(
                schema).objects(schema))).get_verbosename(
                    schema, with_parent=True),
            "constraint 'std::max_len_value' of property 'bar_prop' of "
            "link 'bar' of object type 'test::Object1'",
        )


class TestGetMigration(tb.BaseSchemaLoadTest):
    """Test migration deparse consistency.

    This tests that schemas produced by `COMMIT MIGRATION foo` and
    by deparsed DDL via `GET MIGRATION foo` are identical.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.std_schema = tb._load_std_schema()
        cls.schema = cls.run_ddl(cls.schema, 'CREATE MODULE default;')

    def _assert_migration_consistency(self, schema_text):

        migration_text = f'''
            CREATE MIGRATION m TO {{
                {schema_text}
            }};
        '''

        migration_ql = edgeql.parse_block(migration_text)

        migration_cmd = s_ddl.cmd_from_ddl(
            migration_ql[0],
            schema=self.schema,
            modaliases={
                None: 'default'
            },
        )

        migration_cmd = s_ddl.compile_migration(
            migration_cmd,
            self.std_schema,
            self.schema,
        )

        context = s_delta.CommandContext()
        schema, migration = migration_cmd.apply(self.schema, context)

        ddl_plan = s_delta.DeltaRoot(canonical=True)
        ddl_plan.update(migration.get_commands(schema))

        baseline_schema, _ = ddl_plan.apply(schema, context)

        ddl_text = s_ddl.ddl_text_from_delta(schema, migration)

        try:
            test_schema = self.run_ddl(schema, ddl_text)
        except errors.EdgeDBError as e:
            self.fail(markup.dumps(e))

        diff = s_ddl.delta_schemas(baseline_schema, test_schema)

        if list(diff.get_subcommands()):
            self.fail(
                f'unexpected difference in schema produced by\n'
                f'COMMIT MIGRATION and DDL obtained from GET MIGRATION:\n'
                f'{markup.dumps(diff)}\n'
                f'DDL text was:\n{ddl_text}'
            )

    def _assert_migration_equivalence(self, migrations):
        # Compare 2 schemas obtained by multiple-step migration to a
        # single-step migration.

        # Validate that the final schema state has consistent migration.
        self._assert_migration_consistency(migrations[-1])

        # Generate a base schema with 'test' module already created to
        # avoid having two different instances of 'test' module in
        # different evolution branches.
        base_schema = self.load_schema('')

        # Jump to final schema state in a single migration.
        single_migration = self.run_ddl(base_schema, f'''
            CREATE MIGRATION m TO {{
                {migrations[-1]}
            }};
            COMMIT MIGRATION m;
        ''', 'test')

        # Evolve a schema in a series of migrations.
        for i, state in enumerate(migrations):
            multi_migration = self.run_ddl(base_schema, f'''
                CREATE MIGRATION m{i} TO {{
                    {state}
                }};
                COMMIT MIGRATION m{i};
            ''', 'test')

        diff = s_ddl.delta_schemas(single_migration, multi_migration)

        if list(diff.get_subcommands()):
            self.fail(
                f'unexpected difference in schema produced by\n'
                f'alternative migration paths:\n'
                f'{markup.dumps(diff)}\n'
            )

    def test_get_migration_01(self):

        schema = '''
            abstract inheritable annotation my_anno;

            abstract type Named {
                property name -> str {
                    annotation title := 'Name';
                    delegated constraint exclusive {
                        annotation title := 'uniquely named';
                    }
                }
            }

            type User extending Named {
                required multi link friends -> User {
                    annotation my_anno := 'foo';
                }
            };

            abstract link special;
            abstract property annotated_name {
                annotation title := 'Name';
            }

            type SpecialUser extending User {
                inherited property name extending annotated_name -> str;
                inherited link friends extending special -> SpecialUser;
            };
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_02(self):
        schema = '''
            abstract type Named {
                property name -> str {
                    delegated constraint exclusive;
                }
            }

            abstract type User extending Named {
                inherited required property name -> str {
                    delegated constraint exclusive;
                }
            };

            type SpecialUser extending User;
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_03(self):
        schema = '''
            abstract type Named {
                property name -> str {
                    delegated constraint exclusive;
                }
            }

            type Ingredient extending Named {
                property vegetarian -> bool {
                    default := false;
                }
            }

            scalar type unit extending enum<'ml', 'g', 'oz'>;

            type Recipe extending Named {
                multi link ingredients -> Ingredient {
                    property quantity -> decimal {
                        annotation title := 'ingredient quantity';
                    };
                    property unit -> unit;
                }
            }

            view VegRecipes := (
                SELECT Recipe
                FILTER all(.ingredients.vegetarian)
            );

            function get_ingredients(
                recipe: Recipe
            ) -> tuple<name: str, quantity: decimal> {
                from edgeql $$
                    SELECT (
                        name := recipe.ingredients.name,
                        quantity := recipe.ingredients.quantity,
                    );
                $$
            }
        '''

        self._assert_migration_consistency(schema)

    def test_migrations_equivalence_01(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            type Base {
                property name -> str;
            }
        """, r"""
            type Base {
                property name -> str;
            }

            type Derived extending Base {
                inherited required property name -> str;
            }
        """])

    def test_migrations_equivalence_02(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base {
                inherited required property foo -> str;
            }
        """, r"""
            type Base {
                # rename 'foo'
                property foo2 -> str;
            }

            type Derived extending Base {
                inherited required property foo2 -> str;
            }
        """])

    def test_migrations_equivalence_03(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base {
                inherited required property foo -> str;
            }
        """, r"""
            type Base;
                # drop 'foo'

            type Derived extending Base {
                # completely different property
                property foo2 -> str;
            }
        """])

    def test_migrations_equivalence_04(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base;

            type Further extending Derived {
                inherited required property foo -> str;
            }
        """, r"""
            type Base;
                # drop 'foo'

            type Derived extending Base;

            type Further extending Derived {
                # completely different property
                property foo2 -> str;
            };
        """])

    def test_migrations_equivalence_05(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base {
                inherited required property foo -> str;
            }
        """, r"""
            type Base;
                # drop foo

            type Derived extending Base {
                # no longer inherited property 'foo'
                property foo -> str;
            }
        """])

    def test_migrations_equivalence_06(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> int64;
            }

            type Derived extending Base {
                inherited required property foo -> int64;
            }
        """, r"""
            type Base {
                # change property type
                property foo -> str;
            }

            type Derived extending Base {
                inherited required property foo -> str;
            }
        """])

    def test_migrations_equivalence_07(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base {
                link bar -> Child;
            }
        """, r"""
            type Child;

            type Base {
                required link bar -> Child {
                    # add a constraint
                    constraint exclusive;
                }
            }
        """])

    def test_migrations_equivalence_08(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> str;
            }
        """, r"""
            type Base {
                required property foo -> str {
                    # add a constraint
                    constraint max_len_value(10);
                }
            }
        """])

    def test_migrations_equivalence_09(self):
        self._assert_migration_equivalence([r"""
            scalar type constraint_length extending str {
                constraint max_len_value(10);
            }
        """, r"""
            scalar type constraint_length extending str {
                constraint max_len_value(10);
                # add a constraint
                constraint min_len_value(5);
            }
        """])

    def test_migrations_equivalence_10(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> str;
            }
        """, r"""
            type Child;

            type Base {
                # change property to link with same name
                link foo -> Child;
            }
        """])

    def test_migrations_equivalence_11(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> str;
            }
        """, r"""
            type Child;

            type Base {
                # change property to link with same name
                link foo -> Child {
                    # add a constraint
                    constraint exclusive;
                }
            }
        """])

    def test_migrations_equivalence_12(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base {
                property foo -> str {
                    constraint exclusive;
                }

                link bar -> Child {
                    constraint exclusive;
                }
            }
        """, r"""
            type Child;

            type Base {
                # drop constraints
                property foo -> str;
                link bar -> Child;
            }
        """])

    def test_migrations_equivalence_13(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base {
                link bar -> Child;
            }

            type Derived extending Base {
                inherited required link bar -> Child;
            }
        """, r"""
            type Child;

            type Base;
                # drop 'bar'

            type Derived extending Base {
                # no longer inherit link 'bar'
                link bar -> Child;
            }
        """])

    def test_migrations_equivalence_14(self):
        self._assert_migration_equivalence([r"""
            type Base;

            type Derived extending Base {
                property foo -> str;
            }
        """, r"""
            type Base {
                # move the property earlier in the inheritance
                property foo -> str;
            }

            type Derived extending Base {
                inherited required property foo -> str;
            }
        """])

    def test_migrations_equivalence_15(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base;

            type Derived extending Base {
                link bar -> Child;
            }
        """, r"""
            type Child;

            type Base {
                # move the link earlier in the inheritance
                link bar -> Child;
            }

            type Derived extending Base;
        """])

    def test_migrations_equivalence_16(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base;

            type Derived extending Base {
                link bar -> Child;
            }
        """, r"""
            type Child;

            type Base {
                # move the link earlier in the inheritance
                link bar -> Child;
            }

            type Derived extending Base;
        """, r"""
            type Child;

            type Base {
                link bar -> Child;
            }

            type Derived extending Base {
                # also make the link 'required'
                inherited required link bar -> Child;
            }
        """])

    def test_migrations_equivalence_17(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property name := 'computable'
            }
        """, r"""
            type Base {
                # change a property from a computable to regular
                property name -> str
            }
        """])

    def test_migrations_equivalence_18(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property name := 'something'
            }
        """, r"""
            type Base {
                # change a property from a computable to regular with a default
                property name -> str {
                    default := 'something'
                }
            }
        """])

    def test_migrations_equivalence_19(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property name -> str
            }
        """, r"""
            type Base {
                # change a regular property to a computable
                property name := 'computable'
            }
        """])

    def test_migrations_equivalence_20(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property name -> str {
                    default := 'something'
                }
            }
        """, r"""
            type Base {
                # change a regular property to a computable
                property name := 'something'
            }
        """])

    def test_migrations_equivalence_21(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> str;
            }
        """, r"""
            type Base {
                property foo -> str;
                # add a property
                property bar -> int64;
            }
        """, r"""
            type Base {
                # make the old property into a computable
                property foo := <str>__source__.bar;
                property bar -> int64;
            }
        """])

    def test_migrations_equivalence_22(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> str;
            }
        """, r"""
            # rename the type, although this test doesn't ensure that
            # renaming actually took place
            type NewBase {
                property foo -> str;
            }
        """, r"""
            type NewBase {
                property foo -> str;
                # add a property
                property bar -> int64;
            }
        """, r"""
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
        """])

    def test_migrations_equivalence_23(self):
        self._assert_migration_equivalence([r"""
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
        """, r"""
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
        """])

    def test_migrations_equivalence_24(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base {
                link bar -> Child;
            }
        """, r"""
            type Child;

            type Base {
                # increase link cardinality
                multi link bar -> Child;
            }
        """])

    def test_migrations_equivalence_25(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base {
                multi link bar -> Child;
            }
        """, r"""
            type Child;

            type Base {
                # reduce link cardinality
                link bar -> Child;
            }
        """, r"""
            type Child;

            type Base {
                link bar -> Child {
                    # further restrict the link
                    constraint exclusive
                }
            }
        """])

    def test_migrations_equivalence_26(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Parent {
                link bar -> Child;
            }
        """, r"""
            type Child;

            type Parent {
                link bar -> Child;
            }

            # derive a type
            type DerivedParent extending Parent;
        """, r"""
            type Child;

            type DerivedChild extending Child;

            type Parent {
                link bar -> Child;
            }

            # derive a type with a more restrictive link
            type DerivedParent extending Parent {
                inherited link bar -> DerivedChild;
            }
        """])

    def test_migrations_equivalence_27(self):
        self._assert_migration_equivalence([r"""
            abstract type Named {
                property name -> str;
            }

            type Foo extending Named;
            type Bar extending Named;
        """, r"""
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
        """, r"""
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
        """])

    def test_migrations_equivalence_28(self):
        self._assert_migration_equivalence([r"""
            type Child {
                property foo -> str;
            }
        """, r"""
            # drop everything
        """])

    def test_migrations_equivalence_29(self):
        self._assert_migration_equivalence([r"""
            type Child {
                property foo -> str;
            }

            view Base := (
                SELECT Child {
                    bar := .foo
                }
            );
        """, r"""
            # drop everything
        """])

    def test_migrations_equivalence_30(self):
        # This is the inverse of the test_migrations_equivalence_27
        # scenario. We're trying to merge and refactor common
        # property.
        self._assert_migration_equivalence([r"""
            type Foo {
                property name -> str;
            };

            type Bar {
                property title -> str;
            };
        """, r"""
            type Foo {
                property name -> str;
            };

            type Bar {
                # rename 'title' to 'name'
                property name -> str;
            };
        """, r"""
            # both types have a name, so the name prop is factored out
            # into a more basic type.
            abstract type Named {
                property name -> str;
            }

            type Foo extending Named;
            type Bar extending Named;
        """])

    def test_migrations_equivalence_31(self):
        # Issue 727.
        #
        # Starting with the sample schema (from frontpage) migrate to
        # a schema with only type User.
        self._assert_migration_equivalence([r"""
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
        """, r"""
            type User {
              required property name -> str;
            }
        """])

    def test_migrations_equivalence_32(self):
        # Issue 727.
        #
        # Starting with a small schema migrate to remove its elements.
        self._assert_migration_equivalence([r"""
            type LogEntry {
              required property spent_time -> int64;
            }
            type Issue {
              multi link time_spent_log -> LogEntry {
                constraint exclusive;
              }
            }
        """, r"""
            type LogEntry {
              required property spent_time -> int64;
            }
        """, r"""
            # empty schema
        """])

    def test_migrations_equivalence_function_01(self):
        self._assert_migration_equivalence([r"""
            function hello01(a: int64) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """, r"""
            function hello01(a: int64, b: int64=42) -> str
                from edgeql $$
                    SELECT 'hello' ++ <str>(a + b)
                $$
        """])

    def test_migrations_equivalence_function_06(self):
        self._assert_migration_equivalence([r"""
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
        """, r"""
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
        """])

    @test.xfail('''
        edb.errors.InvalidConstraintDefinitionError: std::expression
        constraint expression expected to return a bool value, got
        'int64'
    ''')
    def test_migrations_equivalence_function_10(self):
        self._assert_migration_equivalence([r"""
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
        """, r"""
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
        """])

    def test_migrations_equivalence_linkprops_03(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> int64
                }
            };
        """, r"""
            type Child;

            type Base {
                link foo -> Child {
                    # change the link property type
                    property bar -> str
                }
            };
        """])

    def test_migrations_equivalence_linkprops_07(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                inherited link child -> Child {
                    property foo -> str
                }
            };
        """, r"""
            type Child;

            type Base {
                # move the link property earlier in the inheritance tree
                link child -> Child {
                    property foo -> str
                }
            };

            type Derived extending Base;
        """])

    def test_migrations_equivalence_linkprops_08(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base {
                link child -> Child {
                    property foo -> str
                }
            };

            type Derived extending Base;
        """, r"""
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
        """])

    def test_migrations_equivalence_linkprops_09(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                inherited link child -> Child {
                    property foo -> str
                }
            };
        """, r"""
            type Child;

            # factor out link property all the way to an abstract link
            abstract link base_child {
                property foo -> str;
            }

            type Base {
                link child extending base_child -> Child;
            };

            type Derived extending Base;
        """])

    def test_migrations_equivalence_linkprops_10(self):
        self._assert_migration_equivalence([r"""
            type Child;

            abstract link base_child {
                property foo -> str;
            }

            type Base {
                link child extending base_child -> Child;
            };

            type Derived extending Base;
        """, r"""
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
        """])

    def test_migrations_equivalence_linkprops_11(self):
        self._assert_migration_equivalence([r"""
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
        """, r"""
            type Thing;

            type Base {
                link item -> Thing {
                    property foo -> str;
                }
            };

            type Owner extending Base;

            type Renter extending Base;
        """])

    def test_migrations_equivalence_linkprops_12(self):
        self._assert_migration_equivalence([r"""
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
        """, r"""
            type Thing;

            type Base {
                link item -> Thing {
                    property foo -> str;
                    property bar -> str;
                }
            };

            type Owner extending Base;

            type Renter extending Base;
        """])
