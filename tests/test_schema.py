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
