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


import re

from edb import errors

from edb.common import markup
from edb.testbase import lang as tb

from edb import edgeql
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import parser as qlparser
from edb.edgeql import qltypes

from edb.schema import delta as s_delta
from edb.schema import ddl as s_ddl
from edb.schema import links as s_links
from edb.schema import objtypes as s_objtypes

from edb.tools import test


class TestSchema(tb.BaseSchemaLoadTest):
    def test_schema_overloaded_01(self):
        """
            type UniqueName {
                property name -> str {
                    constraint exclusive
                }
            };
            type UniqueName_2 extending UniqueName {
                overloaded property name -> str {
                    constraint exclusive
                }
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "'name'.*must be declared using the `overloaded` keyword",
                  position=228)
    def test_schema_overloaded_02(self):
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
                  "'name'.*cannot be declared `overloaded`",
                  position=61)
    def test_schema_overloaded_03(self):
        """
            type UniqueName {
                overloaded property name -> str
            };
        """

    @tb.must_fail(errors.InvalidLinkTargetError,
                  'invalid link target, expected object type, got scalar type',
                  position=69)
    def test_schema_bad_link_01(self):
        """
            type Object {
                link foo -> str
            };
        """

    @tb.must_fail(errors.InvalidLinkTargetError,
                  'invalid link target, expected object type, got scalar type',
                  position=69)
    def test_schema_bad_link_02(self):
        """
            type Object {
                link foo := 1 + 1
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  'link or property name length exceeds the maximum.*',
                  position=57)
    def test_schema_bad_link_03(self):
        """
            type Object {
                link f123456789_123456789_123456789_123456789_123456789\
_123456789_123456789_123456789 -> Object
            };
        """

    @tb.must_fail(errors.InvalidPropertyTargetError,
                  "invalid property type: expected a scalar type, "
                  "or a scalar collection, got object type 'test::Object'",
                  position=73)
    def test_schema_bad_prop_01(self):
        """
            type Object {
                property foo -> Object
            };
        """

    @tb.must_fail(errors.InvalidPropertyTargetError,
                  "invalid property type: expected a scalar type, "
                  "or a scalar collection, got object type 'test::Object'",
                  position=73)
    def test_schema_bad_prop_02(self):
        """
            type Object {
                property foo := (SELECT Object)
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  'link or property name length exceeds the maximum.*',
                  position=57)
    def test_schema_bad_prop_03(self):
        """
            type Object {
                property f123456789_123456789_123456789_123456789_123456789\
_123456789_123456789_123456789 -> str
            };
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  "type 'int' does not exist",
                  position=73,
                  hint='did you mean one of these: int16, int32, int64?')
    def test_schema_bad_type_01(self):
        """
            type Object {
                property foo -> int
            };
        """

    @tb.must_fail(errors.InvalidPropertyTargetError,
                  "expected a scalar type, or a scalar collection, "
                  "got collection 'array<test::Foo>'",
                  position=94)
    def test_schema_bad_type_02(self):
        """
            type Foo;

            type Base {
                property foo -> array<Foo>;
            }
        """

    @tb.must_fail(errors.InvalidPropertyTargetError,
                  "expected a scalar type, or a scalar collection, "
                  "got collection 'tuple<test::Foo>'",
                  position=94)
    def test_schema_bad_type_03(self):
        """
            type Foo;

            type Base {
                property foo -> tuple<Foo>;
            }
        """

    @tb.must_fail(errors.InvalidPropertyTargetError,
                  "expected a scalar type, or a scalar collection, "
                  "got collection 'tuple<std::str, array<test::Foo>>'",
                  position=94)
    def test_schema_bad_type_04(self):
        """
            type Foo;

            type Base {
                property foo -> tuple<str, array<Foo>>;
            }
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
                USING (
                    SELECT contains(arr, val)
                );
            };

            CREATE ABSTRACT CONSTRAINT
            test::my_one_of(one_of: array<anytype>) {
                USING (
                    SELECT (
                        test::my_contains(one_of, __subject__),
                        test::Object1,
                    ).0
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

    def test_schema_annotation_inheritance_01(self):
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

    def test_schema_annotation_inheritance_02(self):
        schema = tb._load_std_schema()

        schema = self.run_ddl(schema, '''
            CREATE MODULE default;
            CREATE TYPE default::Base;
            CREATE TYPE default::Derived EXTENDING default::Base;
            CREATE ABSTRACT INHERITABLE ANNOTATION default::inh_anno;
            CREATE ABSTRACT ANNOTATION default::noinh_anno;
            ALTER TYPE default::Base
                CREATE ANNOTATION default::noinh_anno := 'foo';
            ALTER TYPE default::Base
                CREATE ANNOTATION default::inh_anno := 'bar';
        ''')

        inh_anno = schema.get('default::inh_anno')
        der = schema.get('default::Derived')
        annos = der.get_annotations(schema)
        anno = annos.get(schema, 'default::inh_anno')
        self.assertEqual(anno.get_annotation(schema), inh_anno)

        no_anno = annos.get(schema, 'default::noinh_anno', default=None)
        self.assertIsNone(no_anno)

    def test_schema_constraint_inheritance_01(self):
        schema = tb._load_std_schema()

        schema = self.run_ddl(schema, r'''
            CREATE MODULE default;
            CREATE ABSTRACT TYPE default::Named;
            CREATE TYPE default::User EXTENDING default::Named;
            ALTER TYPE default::Named CREATE SINGLE PROPERTY name -> std::str;
            # unusual ordering of constraint definition
            ALTER TYPE default::Named
                ALTER PROPERTY name
                    CREATE DELEGATED CONSTRAINT exclusive;
            ALTER TYPE default::User
                ALTER PROPERTY name
                    ALTER CONSTRAINT exclusive {
                        SET DELEGATED;
                    };
        ''')

        User = schema.get('default::User')
        name_prop = User.getptr(schema, 'name')
        constr = name_prop.get_constraints(schema).objects(schema)[0]
        base_names = constr.get_bases(schema).names(schema)
        self.assertEqual(len(base_names), 1)
        self.assertTrue(base_names[0].startswith(
            'default::std|exclusive@@default|__|name@@default|Named@'))

    def test_schema_constraint_inheritance_02(self):
        schema = tb._load_std_schema()

        schema = self.run_ddl(schema, r'''
            CREATE MODULE default;
            CREATE ABSTRACT TYPE default::Named;
            CREATE TYPE default::User EXTENDING default::Named;
            ALTER TYPE default::Named CREATE SINGLE PROPERTY name -> std::str;
            # unusual ordering of constraint definition
            ALTER TYPE default::User
                ALTER PROPERTY name
                    CREATE DELEGATED CONSTRAINT exclusive;
            ALTER TYPE default::Named
                ALTER PROPERTY name
                    CREATE DELEGATED CONSTRAINT exclusive;
        ''')

        User = schema.get('default::User')
        name_prop = User.getptr(schema, 'name')
        constr = name_prop.get_constraints(schema).objects(schema)[0]
        base_names = constr.get_bases(schema).names(schema)
        self.assertEqual(len(base_names), 1)
        self.assertTrue(base_names[0].startswith(
            'default::std|exclusive@@default|__|name@@default|Named@'))

    def test_schema_constraint_inheritance_03(self):
        schema = tb._load_std_schema()

        schema = self.run_ddl(schema, r'''
            CREATE MODULE default;
            CREATE ABSTRACT TYPE default::Named {
                CREATE SINGLE PROPERTY name -> std::str;
            };
            ALTER TYPE default::Named {
                ALTER PROPERTY name {
                    CREATE DELEGATED CONSTRAINT std::exclusive;
                };
            };
            CREATE TYPE default::Recipe EXTENDING default::Named;
            CREATE ALIAS default::VegRecipes := (
                SELECT default::Recipe
                FILTER .name ILIKE 'veg%'
            );
        ''')

        VegRecipes = schema.get('default::VegRecipes')
        name_prop = VegRecipes.getptr(schema, 'name')
        constr = name_prop.get_constraints(schema).objects(schema)
        self.assertEqual(
            len(constr), 0,
            'there should be no constraints on alias links or properties',
        )

    def test_schema_constraint_inheritance_04(self):
        schema = tb._load_std_schema()

        schema = self.run_ddl(schema, r'''
            CREATE MODULE default;
            CREATE ABSTRACT TYPE default::Named {
                CREATE SINGLE PROPERTY name -> std::str;
            };
            CREATE TYPE default::Recipe EXTENDING default::Named;
            CREATE ALIAS default::VegRecipes := (
                SELECT default::Recipe
                FILTER .name ILIKE 'veg%'
            );
            ALTER TYPE default::Named {
                ALTER PROPERTY name {
                    CREATE DELEGATED CONSTRAINT std::exclusive;
                };
            };
        ''')

        VegRecipes = schema.get('default::VegRecipes')
        name_prop = VegRecipes.getptr(schema, 'name')
        constr = name_prop.get_constraints(schema).objects(schema)

        self.assertEqual(
            len(constr), 0,
            'there should be no constraints on alias links or properties',
        )

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
            USING EdgeQL $$ SELECT a; $$;
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

    def test_schema_advanced_types(self):
        schema = self.load_schema("""
            type D;
            abstract type F {
                property f -> int64;
                link d -> D {
                    property f_d_prop -> str;
                }
            }
            type T1 {
                property n -> str;
                link d -> D {
                    property t1_d_prop -> str;
                }
            };
            type T2 extending F {
                property n -> str;
            };
            type T3;

            type A {
                link t -> T1 | T2;
                link t2 := .t[IS T2];
                link tf := .t[IS F];
            }
        """)

        A = schema.get('test::A')
        T2 = schema.get('test::T2')
        F = schema.get('test::F')
        A_t = A.getptr(schema, 't')
        A_t2 = A.getptr(schema, 't2')
        A_tf_link = A.getptr(schema, 'tf')
        A_tf = A_tf_link.get_target(schema)

        # Check that ((T1 | T2) & F) has properties from both parts
        # of the intersection.
        self.assertIsNotNone(A_tf.getptr(schema, 'n'))
        self.assertIsNotNone(A_tf.getptr(schema, 'f'))

        # Ditto for link properties defined on a common link.
        tfd = A_tf.getptr(schema, 'd')
        tfd.getptr(schema, 'f_d_prop')
        tfd.getptr(schema, 't1_d_prop')

        self.assertTrue(
            A_t2.get_target(schema).issubclass(
                schema,
                A_t.get_target(schema)
            )
        )

        self.assertTrue(
            A_tf.issubclass(
                schema,
                T2,
            )
        )

        self.assertTrue(
            A_tf.issubclass(
                schema,
                F,
            )
        )

    def test_schema_ancestor_propagation_on_sdl_migration(self):
        schema = self.load_schema("""
            type A;
            type B extending A;
            type C extending B;
        """)

        Object = schema.get('std::Object')
        A = schema.get('test::A')
        B = schema.get('test::B')
        C = schema.get('test::C')
        std_link = schema.get('std::link')
        Object__type__ = Object.getptr(schema, '__type__')
        A__type__ = A.getptr(schema, '__type__')
        B__type__ = B.getptr(schema, '__type__')
        C__type__ = C.getptr(schema, '__type__')
        self.assertEqual(
            C__type__.get_ancestors(schema).objects(schema),
            (
                B__type__,
                A__type__,
                Object__type__,
                std_link,
            )
        )

        schema = self.run_ddl(schema, """
            CREATE MIGRATION ancestor_propagation TO {
                module test {
                    type A;
                    type B;
                    type C extending B;
                }
            };
            COMMIT MIGRATION ancestor_propagation;
        """)

        self.assertEqual(
            C__type__.get_ancestors(schema).objects(schema),
            (
                B__type__,
                Object__type__,
                std_link,
            )
        )

    def test_schema_correct_ancestors_on_explicit_derive_ref(self):
        schema = self.load_schema("""
            type A {
                property name -> str;
            }
            type B extending A;
        """)

        std_prop = schema.get('std::property')
        B = schema.get('test::B')
        B_name = B.getptr(schema, 'name')
        schema, derived = std_prop.derive_ref(
            schema,
            B,
            schema.get('std::str'),
            name=B_name.get_name(schema),
            inheritance_merge=False,
            mark_derived=True,
        )

        self.assertEqual(
            derived.get_ancestors(schema).objects(schema),
            (
                std_prop,
            )
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

    def _assert_migration_consistency(self, schema_text, multi_module=False):

        if multi_module:
            migration_text = f'''
                CREATE MIGRATION m TO {{
                    {schema_text}
                }};
            '''
        else:
            migration_text = f'''
                CREATE MIGRATION m TO {{
                    module default {{
                        {schema_text}
                    }}
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
        schema = migration_cmd.apply(self.schema, context)
        migration = migration_cmd.scls

        ddl_plan = migration.get_delta(schema)
        baseline_schema = ddl_plan.apply(schema, context)
        ddl_text = s_ddl.ddl_text_from_delta(baseline_schema, ddl_plan)

        try:
            test_schema = self.run_ddl(schema, ddl_text)
        except errors.EdgeDBError as e:
            self.fail(markup.dumps(e))

        diff = s_ddl.delta_schemas(test_schema, baseline_schema)
        if list(diff.get_subcommands()):
            self.fail(
                f'unexpected difference in schema produced by\n'
                f'COMMIT MIGRATION and DDL obtained from GET MIGRATION:\n'
                f'{markup.dumps(diff)}\n'
                f'DDL text was:\n{ddl_text}'
            )

        # Now, dump the final schema into DDL and SDL and see if
        # reapplying those representations produces in the same
        # schema. This tests the codepath used by DESCRIBE command as
        # well and validates that DESCRIBE is producing valid grammar.
        ddl_text = s_ddl.ddl_text_from_schema(baseline_schema)
        sdl_text = s_ddl.sdl_text_from_schema(baseline_schema)

        try:
            ddl_schema = self.run_ddl(self.std_schema, ddl_text)
            sdl_schema = self.run_ddl(
                self.std_schema,
                f'''
                CREATE MIGRATION m TO {{ {sdl_text} }};
                COMMIT MIGRATION m;
                ''',
            )
        except errors.EdgeDBError as e:
            self.fail(markup.dumps(e))

        diff = s_ddl.delta_schemas(ddl_schema, baseline_schema)

        if list(diff.get_subcommands()):
            self.fail(
                f'unexpected difference in schema produced by\n'
                f'COMMIT MIGRATION and DDL obtained from dumping the schema:\n'
                f'{markup.dumps(diff)}\n'
                f'DDL text was:\n{ddl_text}'
            )

        diff = s_ddl.delta_schemas(sdl_schema, baseline_schema)

        if list(diff.get_subcommands()):
            self.fail(
                f'unexpected difference in schema produced by\n'
                f'COMMIT MIGRATION and SDL obtained from dumping the schema:\n'
                f'{markup.dumps(diff)}\n'
                f'SDL text was:\n{sdl_text}'
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

        # Evolve a schema in a series of migrations.
        multi_migration = base_schema
        for i, state in enumerate(migrations):
            mig_text = f'''
                CREATE MIGRATION m{i} TO {{
                    module default {{
                        {state}
                    }}
                }};
                COMMIT MIGRATION m{i};
            '''

            # Jump to the current schema state directly from base.
            cur_state = self.run_ddl(base_schema, mig_text, 'test')
            # Perform incremental migration.
            multi_migration = self.run_ddl(multi_migration, mig_text, 'test')

            diff = s_ddl.delta_schemas(multi_migration, cur_state)

            if list(diff.get_subcommands()):
                self.fail(
                    f'unexpected difference in schema produced by\n'
                    f'alternative migration paths on step {i}:\n'
                    f'{markup.dumps(diff)}\n'
                )

    def test_get_migration_01(self):
        schema = r'''
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

            type SpecialUser extending User {
                overloaded property name extending annotated_name -> str;
                overloaded link friends extending special -> SpecialUser;
            };

            abstract link special;
            abstract property annotated_name {
                annotation title := 'Name';
            }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_02(self):
        schema = r'''
            abstract type Named {
                property name -> str {
                    # legal, albeit superfluous std
                    delegated constraint std::exclusive;
                }
            }

            abstract type User extending Named {
                overloaded required property name -> str {
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

            function get_ingredients(
                recipe: Recipe
            ) -> tuple<name: str, quantity: decimal> {
                using (
                    SELECT (
                        name := recipe.ingredients.name,
                        quantity := recipe.ingredients.quantity,
                    )
                )
            }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_04(self):
        # validate that we can trace partial paths
        schema = r'''
            alias X := (SELECT Foo{num := .bar});

            type Foo {
                property bar -> int64;
            };
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_05(self):
        # validate that we can trace partial paths
        schema = r'''
            alias X := (SELECT Foo FILTER .bar > 2);

            type Foo {
                property bar -> int64;
            };
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_06(self):
        # validate that we can trace INTROSPECT
        schema = r'''
            alias X := (SELECT INTROSPECT Foo);

            type Foo;
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_07(self):
        # validate that we can trace DELETE
        schema = r'''
            type Bar {
                property data -> str;
            }

            type Foo {
                required property bar -> str {
                    # if bar is not specified, grab it from Bar and
                    # delete the object
                    default := (DELETE Bar LIMIT 1).data
                }
            };
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_08(self):
        schema = r'''
            type Bar {
                property data -> str {
                    constraint min_value(10) on (len(<str>__subject__))
                }
            }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_09(self):
        schema = r'''
            type Foo;
            type Spam {
                link foo -> Foo;
                property name -> str;
            };
            type Ham extending Spam {
                overloaded link foo {
                    constraint exclusive;
                };
                overloaded property name {
                    constraint exclusive;
                };
            };
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_10(self):
        schema = r'''
            # The two types declared are mutually dependent.
            type Foo {
                link bar -> Bar;
            };

            type Bar {
                link foo -> Foo;
            };
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_11(self):
        schema = r'''
            # The two types declared are mutually dependent.
            type Foo {
                link bar -> Bar {
                    default := (
                        SELECT Bar FILTER .name > 'a'
                        LIMIT 1
                    );
                };
                property name -> str;
            };

            type Bar {
                link foo -> Foo {
                    default := (
                        SELECT Foo FILTER .name < 'z'
                        LIMIT 1
                    );
                };
                property name -> str;
            };
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_12(self):
        schema = r'''
            # The function declaration appears earlier in the document
            # than the declaration for the argument type, which should
            # not matter.
            function get_name(obj: Foo) -> str
                using (SELECT obj.name);

            type Foo {
                required property name -> str;
            };
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_13(self):
        # validate that we can trace alias declared before type
        schema = r'''
            alias X := (SELECT Foo.name);

            type Foo {
                property name -> str;
            }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_14(self):
        # validate that we can trace alias with DETACHED expr declared
        # before type
        schema = r'''
            alias X := (DETACHED Foo.name);

            type Foo {
                property name -> str;
            }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_15(self):
        schema = r'''
            type Foo {
                property bar -> int64;
                annotation title := 'Foo';
            };
        '''

        self._assert_migration_consistency(schema)

    @test.xfail('''
        AssertionError: unexpected difference in schema produced by
        COMMIT MIGRATION and DDL obtained from GET MIGRATION
        <DeltaRoot source_context=None, canonical=True> (
            <AlterObjectType classname=default::X ...> (
                <CreateAnnotationValue
                 classname=default::std|title@@default|X ...>
        ...

        DDL text was:
        CREATE TYPE default::Foo {
            CREATE SINGLE PROPERTY bar -> std::int64;
        };
        CREATE ALIAS default::X {
            USING (WITH
                MODULE default
            SELECT
                Foo
            );
            CREATE ANNOTATION std::title := 'A Foo alias';
        };
    ''')
    def test_get_migration_16(self):
        schema = r'''
            type Foo {
                property bar -> int64;
            };

            alias X {
                using (SELECT Foo);
                annotation title := 'A Foo alias';
            }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_17(self):
        # Test abstract and concrete constraints order of declaration.
        schema = r'''
        type Foo {
            property color -> str {
                constraint my_one_of(['red', 'green', 'blue']);
            }
        }

        abstract constraint my_one_of(one_of: array<anytype>) {
            using (contains(one_of, __subject__));
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_18(self):
        # Test abstract and concrete constraints order of declaration.
        schema = r'''
        type Foo {
            property color -> constraint_my_enum;
        }

        scalar type constraint_my_enum extending str {
           constraint my_one_of(['red', 'green', 'blue']);
        }

        abstract constraint my_one_of(one_of: array<anytype>) {
            using (contains(one_of, __subject__));
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_19(self):
        # Test abstract and concrete annotations order of declaration.
        schema = r'''
        type Foo {
            property name -> str;
            annotation my_anno := 'Foo';
        }

        abstract annotation my_anno;
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_20(self):
        # Test abstract and concrete annotations order of declaration.
        schema = r'''
        type Foo {
            property name -> str {
                annotation my_anno := 'Foo';
            }
        }

        abstract annotation my_anno;
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_21(self):
        # Test index and function order of definition.
        schema = r'''
        type Foo {
            # an index defined before property & function
            index on (idx(.bar));
            property bar -> int64;
        }

        function idx(num: int64) -> bool {
            using (SELECT (num % 2) = 0);
            volatility := 'IMMUTABLE';
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_22(self):
        # Test prop default and function order of definition.
        schema = r'''
        type Foo {
            property name -> str {
                default := name_def();
            };
        }

        function name_def() -> str {
            using (SELECT 'some_name' ++ <str>uuid_generate_v1mc());
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_23(self):
        # Test prop default and function order of definition. The
        # function happens to be shadowing a "std" function. We expect
        # that the function `default::to_upper` will actually be used.
        schema = r'''
        type Foo {
            property name -> str {
                default := str_upper('some_name');
            };
        }

        function str_upper(val: str) -> str {
            using (SELECT '^^' ++ str_upper(val) ++ '^^');
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_24(self):
        # Test constraint and computable using a function defined in
        # the same SDL.
        schema = r'''
        type Tagged {
            property tag := make_tag(.title);
            required property title -> str {
                constraint exclusive on (make_tag(__subject__))
            }
        }

        function make_tag(s: str) -> str {
            using (
                select str_lower(
                    re_replace( r' ', r'-',
                        re_replace( r'[^(\w|\s)]', r'', s, flags := 'g'),
                    flags := 'g')
                )
            );
            volatility := 'IMMUTABLE';  # needed for the constraint
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_25(self):
        # Test dependency tracking across distant ancestors.
        schema = r'''
        # declaring SpecialUser before User and Named
        type SpecialUser extending User {
            overloaded property name -> str {
                annotation title := 'Name';
            }
        };

        type User extending Named;

        abstract type Named {
            property name -> str;
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_26(self):
        # Test index issues.
        schema = r'''
        type Dictionary {
            required property name -> str;
            index on (__subject__.name);
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_27(self):
        # Test index issues.
        schema = r'''
        abstract link translated_label {
            property lang -> str;
            property prop1 -> str;
        }

        type Label {
            property text -> str;
        }

        type UniqueName {
            link translated_label extending translated_label -> Label {
                constraint exclusive on
                    ((__subject__@source, __subject__@lang));
                constraint exclusive on
                    (__subject__@prop1);
            }
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_28(self):
        # Test standard library dependencies that aren't specifically 'std'.
        schema = r'''
        type Foo {
            required property date -> cal::local_date;
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_29(self):
        # Test dependency due to a long path (more than 1 step).
        schema = r'''
        alias View01 := (
            # now this alias refers to another alias
            SELECT Base {
                child_foo := .bar.foo
            }
        );

        # exchange a type for a alias
        alias Base := (
            SELECT Child {
                # bar is the same as the root object
                bar := Child
            }
        );

        type Child {
            property foo -> str;
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_30(self):
        # Test annotated function SDL.
        schema = r'''
        function idx(num: int64) -> bool {
            using (SELECT (num % 2) = 0);
            volatility := 'IMMUTABLE';
            annotation title := 'func anno';
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_31(self):
        # Test "on target delete".
        schema = r'''
        type Foo {
            link link0 -> Object {
                on target delete restrict;
            };
            link link1 -> Object {
                on target delete delete source;
            };
            link link2 -> Object {
                on target delete allow;
            };
            link link3 -> Object {
                on target delete deferred restrict;
            };
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_32(self):
        # Test migration of index dependent on two links.
        # Issue #1181
        schema = r'''
        type Author {
            required property name -> str;
        }

        type Comment {
            required property body -> str;
        }

        type CommentRating {
            required link author -> Author;
            required link comment -> Comment;

            index on ((__subject__.author, __subject__.comment));
        }
        '''

        self._assert_migration_consistency(schema)

    def test_get_migration_multi_module_01(self):
        schema = r'''
            # The two declared types declared are from different
            # modules and have linear dependency.
            module default {
                type Foo extending other::Bar {
                    property foo -> str;
                };
            }

            module other {
                type Bar {
                    property bar -> str;
                };
            }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_get_migration_multi_module_02(self):
        schema = r'''
            # The two types declared are mutually dependent and are from
            # different modules.
            type default::Foo {
                link bar -> other::Bar;
            };

            type other::Bar {
                link foo -> default::Foo;
            };
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_get_migration_multi_module_03(self):
        # Test abstract and concrete constraints order of declaration,
        # when the components are spread across different modules.
        schema = r'''
        type default::Foo {
            property color -> scal_mod::constraint_my_enum;
        }

        scalar type scal_mod::constraint_my_enum extending str {
           constraint cons_mod::my_one_of(['red', 'green', 'blue']);
        }

        abstract constraint cons_mod::my_one_of(one_of: array<anytype>) {
            using (contains(one_of, __subject__));
        }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_get_migration_multi_module_04(self):
        # View and type from different modules
        schema = r'''
            alias default::X := (SELECT other::Foo.name);

            type other::Foo {
                property name -> str;
            }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_get_migration_multi_module_05(self):
        # View and type from different modules
        schema = r'''
            alias default::X := (SELECT other::Foo FILTER .name > 'a');

            type other::Foo {
                property name -> str;
            }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_get_migration_multi_module_06(self):
        # Type and annotation from different modules.
        schema = r'''
        type default::Foo {
            property name -> str;
            annotation other::my_anno := 'Foo';
        }

        abstract annotation other::my_anno;
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_get_migration_multi_module_07(self):
        # Type and annotation from different modules.
        schema = r'''
        type default::Foo {
            property name -> str {
                annotation other::my_anno := 'Foo';
            }
        }

        abstract annotation other::my_anno;
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_get_migration_multi_module_08(self):
        schema = r'''
        # The function declaration appears in a different module
        # from the type.
        function default::get_name(val: other::foo_t) -> str
            using (SELECT val[0]);

        scalar type other::foo_t extending str {
            constraint min_len_value(3);
        }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_get_migration_multi_module_09(self):
        schema = r'''
        type default::Foo {
            property bar -> int64;
            # an index
            index on (other::idx(.bar));
        }

        function other::idx(num: int64) -> bool {
            using (SELECT (num % 2) = 0);
            volatility := 'IMMUTABLE';
        }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_get_migration_multi_module_10(self):
        # Test prop default and function order of definition.
        schema = r'''
        type default::Foo {
            property name -> str {
                default := other::name_def();
            };
        }

        function other::name_def() -> str {
            using (SELECT 'some_name' ++ <str>uuid_generate_v1mc());
        }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_get_migration_multi_module_11(self):
        # Test prop default and function order of definition.
        schema = r'''
        type default::Foo {
            property name -> str {
                # use WITH instead of fully-qualified name
                default := (WITH MODULE other SELECT name_def());
            };
        }

        function other::name_def() -> str {
            using (SELECT 'some_name' ++ <str>uuid_generate_v1mc());
        }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_get_migration_multi_module_12(self):
        # Test prop default and function order of definition.
        schema = r'''
        type default::Foo {
            property name -> str {
                # use WITH instead of fully-qualified name
                default := (
                    WITH mod AS MODULE other
                    SELECT mod::name_def()
                );
            };
        }

        function other::name_def() -> str {
            using (SELECT 'some_name' ++ <str>uuid_generate_v1mc());
        }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

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
                overloaded required property name -> str;
            }
        """])

    def test_migrations_equivalence_02(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base {
                overloaded required property foo -> str;
            }
        """, r"""
            type Base {
                # rename 'foo'
                property foo2 -> str;
            }

            type Derived extending Base {
                overloaded required property foo2 -> str;
            }
        """])

    def test_migrations_equivalence_03(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base {
                overloaded required property foo -> str;
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
                overloaded required property foo -> str;
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
                overloaded required property foo -> str;
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
                overloaded required property foo -> int64;
            }
        """, r"""
            type Base {
                # change property type
                property foo -> str;
            }

            type Derived extending Base {
                overloaded required property foo -> str;
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
                overloaded required link bar -> Child;
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
                overloaded required property foo -> str;
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
                overloaded required link bar -> Child;
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

            # add an alias to emulate the original
            alias Base := (
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

            alias Alias01 := (
                SELECT Base {
                    child_foo := .bar.foo
                }
            );
        """, r"""
            type Child {
                property foo -> str;
            }

            # exchange a type for an alias
            alias Base := (
                SELECT Child {
                    # bar is the same as the root object
                    bar := Child
                }
            );

            alias Alias01 := (
                # now this alias refers to another alias
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
                overloaded link bar -> DerivedChild;
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

            alias Base := (
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

    def test_migrations_equivalence_33(self):
        self._assert_migration_equivalence([r"""
            type Child;

            type Base {
                link foo -> Child;
            }
        """, r"""
            type Child;
            type Child2;

            type Base {
                # change link type
                link foo -> Child2;
            }
        """])

    def test_migrations_equivalence_34(self):
        # this is the reverse of test_migrations_equivalence_11
        self._assert_migration_equivalence([r"""
            type Child;

            type Base {
                link foo -> Child {
                    constraint exclusive;
                }
            }
        """, r"""
            type Base {
                # change link to property with same name
                property foo -> str;
            }
        """])

    def test_migrations_equivalence_35(self):
        self._assert_migration_equivalence([r"""
            type Child {
                required property name -> str;
            }

            type Base {
                link foo := (
                    SELECT Child FILTER .name = 'computable_35'
                )
            }
        """, r"""
            type Child {
                required property name -> str;
            }

            type Base {
                # change a link from a computable to regular
                multi link foo -> Child;
            }
        """])

    def test_migrations_equivalence_36(self):
        self._assert_migration_equivalence([r"""
            type Child {
                required property name -> str;
            }

            type Base {
                multi link foo -> Child;
            }
        """, r"""
            type Child {
                required property name -> str;
            }

            type Base {
                # change a regular link to a computable
                link foo := (
                    SELECT Child FILTER .name = 'computable_36'
                )
            }
        """])

    def test_migrations_equivalence_37(self):
        # testing schema aliases
        self._assert_migration_equivalence([r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    foo := 'base_alias_37'
                }
            )
        """, r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    # "rename" a computable, since the value is given and
                    # not stored, this is no different from dropping
                    # original and creating a new property
                    foo2 := 'base_alias_37'
                }
            )
        """])

    def test_migrations_equivalence_38(self):
        # testing schema aliases
        self._assert_migration_equivalence([r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    foo := 'base_alias_38'
                }
            )
        """, r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    # keep the name, but change the type
                    foo := 38
                }
            )
        """])

    def test_migrations_equivalence_39(self):
        # testing schema aliases
        self._assert_migration_equivalence([r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (SELECT Foo FILTER .name = 'base_alias_39')
                }
            )
        """, r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    # "rename" a computable, since the value is given and
                    # not stored, this is no different from dropping
                    # original and creating a new multi-link
                    foo2 := (SELECT Foo FILTER .name = 'base_alias_39')
                }
            )
        """])

    def test_migrations_equivalence_40(self):
        # testing schema aliases
        self._assert_migration_equivalence([r"""
            type Base;

            type Foo {
                property name -> str
            }

            type Bar {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (SELECT Foo FILTER .name = 'foo_40')
                }
            )
        """, r"""
            type Base;

            type Foo {
                property name -> str
            }

            type Bar {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    # keep the name, but change the type
                    foo := (SELECT Bar FILTER .name = 'bar_40')
                }
            )
        """])

    def test_migrations_equivalence_41(self):
        # testing schema aliases
        self._assert_migration_equivalence([r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            @bar := 'foo_bar_alias_41'
                        }
                        FILTER .name = 'base_alias_41'
                    )
                }
            )
        """, r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            # "rename" a computable link property, since
                            # the value is given and not stored, this is
                            # no different from dropping original and
                            # creating a new multi-link
                            @baz := 'foo_bar_alias_41'
                        }
                        FILTER .name = 'base_alias_41'
                    )
                }
            )
        """])

    def test_migrations_equivalence_42(self):
        # testing schema aliases
        self._assert_migration_equivalence([r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            @bar := 'foo_bar_alias_42'
                        }
                        FILTER .name = 'base_alias_42'
                    )
                }
            )
        """, r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            # keep the name, but change the type
                            @bar := 42
                        }
                        FILTER .name = 'base_alias_42'
                    )
                }
            )
        """])

    def test_migrations_equivalence_function_01(self):
        self._assert_migration_equivalence([r"""
            function hello01(a: int64) -> str
                using (
                    SELECT 'hello' ++ <str>a
                )
        """, r"""
            function hello01(a: int64, b: int64=42) -> str
                using (
                    SELECT 'hello' ++ <str>(a + b)
                )
        """])

    def test_migrations_equivalence_function_06(self):
        self._assert_migration_equivalence([r"""
            function hello06(a: int64) -> str
                using edgeql $$
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
                using (
                    SELECT [a]
                );

            type Base {
                property foo -> int64 {
                    # use the function in default value computation
                    default := len(hello06(2) ++ hello06(123))
                }
            }
        """])

    def test_migrations_equivalence_function_10(self):
        self._assert_migration_equivalence([r"""
            function hello10(a: int64) -> str
                using edgeql $$
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
                using (
                    SELECT [a]
                );

            type Base {
                required property foo -> int64 {
                    # use the function in a constraint expression
                    constraint expression on (len(hello10(__subject__)) < 2)
                }
            }
        """])

    def test_migrations_equivalence_function_11(self):
        self._assert_migration_equivalence([r"""
            function hello11(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """, r"""
            # replace the function with a new one by the same name
            function hello11(a: str) -> str
                using (
                    SELECT 'hello' ++ a
                )
        """])

    def test_migrations_equivalence_function_12(self):
        self._assert_migration_equivalence([r"""
            function hello12(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;
        """, r"""
            function hello12(a: int64) -> str
                using (
                    SELECT 'hello' ++ <str>a
                );

            # make the function polymorphic
            function hello12(a: str) -> str
                using (
                    SELECT 'hello' ++ a
                );
        """])

    def test_migrations_equivalence_function_13(self):
        # this is the inverse of test_migrations_equivalence_function_12
        self._assert_migration_equivalence([r"""
            # start with a polymorphic function
            function hello13(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;

            function hello13(a: str) -> str
                using edgeql $$
                    SELECT 'hello' ++ a
                $$;
        """, r"""
            # remove one of the 2 versions
            function hello13(a: int64) -> str
                using (
                    SELECT 'hello' ++ <str>a
                );
        """])

    def test_migrations_equivalence_function_14(self):
        self._assert_migration_equivalence([r"""
            function hello14(a: str, b: str) -> str
                using (
                    SELECT a ++ b
                )
        """, r"""
            # Replace the function with a new one by the same name,
            # but working with arrays.
            function hello14(a: array<str>, b: array<str>) -> array<str>
                using (
                    SELECT a ++ b
                )
        """])

    def test_migrations_equivalence_function_15(self):
        self._assert_migration_equivalence([r"""
            function hello15(a: str, b: str) -> str
                using (
                    SELECT a ++ b
                )
        """, r"""
            # Replace the function with a new one by the same name,
            # but working with arrays.
            function hello15(a: tuple<str, str>) -> str
                using (
                    SELECT a.0 ++ a.1
                )
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
                overloaded link child -> Child {
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
                overloaded link child -> Child {
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
                overloaded link child -> Child {
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
                overloaded link child -> Child {
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

    def test_migrations_equivalence_annotation_01(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            type Base {
                # add a title annotation
                annotation title := 'Base description 01'
            }
        """, r"""
            # add inheritable and non-inheritable annotations
            abstract annotation foo_anno;
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 01';
                annotation foo_anno := 'Base foo_anno 01';
                annotation bar_anno := 'Base bar_anno 01';
            }
        """, r"""
            abstract annotation foo_anno;
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 01';
                annotation foo_anno := 'Base foo_anno 01';
                annotation bar_anno := 'Base bar_anno 01';
            }

            # extend Base
            type Derived extending Base;
        """])

    def test_migrations_equivalence_annotation_02(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            abstract annotation foo_anno;

            type Base {
                annotation title := 'Base description 02';
                annotation foo_anno := 'Base foo_anno 02';
            }

            type Derived extending Base;
        """, r"""
            # remove foo_anno
            type Base {
                annotation title := 'Base description 02';
            }

            type Derived extending Base;
        """])

    def test_migrations_equivalence_annotation_03(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 03';
                annotation bar_anno := 'Base bar_anno 03';
            }

            type Derived extending Base;
        """, r"""
            # remove bar_anno
            type Base {
                annotation title := 'Base description 03';
            }

            type Derived extending Base;
        """])

    @test.xfail('''
        Fails on the last migration that attempts to rename the
        property being indexed.

        This is an example of a general problem that any renaming
        needs to be done in such a way so that the existing
        expressions are still valid.
    ''')
    def test_migrations_equivalence_index_01(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property name -> str;
            }
        """, r"""
            type Base {
                property name -> str;
                # an index
                index on (.name);
            }
        """, r"""
            type Base {
                # rename the indexed property
                property title -> str;
                index on (.title);
            }
        """])

    def test_migrations_equivalence_index_02(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property name -> str;
                index on (.name);
            }
        """, r"""
            type Base {
                property name -> str;
                # remove the index
            }
        """])

    def test_migrations_equivalence_index_03(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property name -> int64;
            }
        """, r"""
            type Base {
                property name -> int64;
                # an index
                index on (.name);
            }
        """, r"""
            type Base {
                # change the indexed property type
                property name -> str;
                index on (.name);
            }
        """])

    def test_migrations_equivalence_index_04(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property first_name -> str;
                property last_name -> str;
                property name := .first_name ++ ' ' ++ .last_name;
            }
        """, r"""
            type Base {
                property first_name -> str;
                property last_name -> str;
                property name := .first_name ++ ' ' ++ .last_name;
                # an index on a computable
                index on (.name);
            }
        """])

    # NOTE: array<str>, array<int16>, array<json> already exist in std
    # schema, so it's better to use array<float32> or some other
    # non-typical scalars in tests as a way of testing a collection
    # that would actually be created/dropped.
    def test_migrations_equivalence_collections_01(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            type Base {
                property foo -> array<float32>;
            }
        """])

    def test_migrations_equivalence_collections_02(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            type Base {
                property foo -> tuple<str, int32>;
            }
        """])

    def test_migrations_equivalence_collections_03(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            type Base {
                # nested collection
                property foo -> tuple<str, int32, array<float32>>;
            }
        """])

    def test_migrations_equivalence_collections_04(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            type Base {
                property foo -> tuple<a: str, b: int32>;
            }
        """])

    def test_migrations_equivalence_collections_05(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> float32;
            }
        """, r"""
            type Base {
                # convert property type to array
                property foo -> array<float32>;
            }
        """])

    def test_migrations_equivalence_collections_06(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> array<int32>;
            }
        """, r"""
            type Base {
                # change the array type (old type is castable into new)
                property foo -> array<float32>;
            }
        """])

    def test_migrations_equivalence_collections_07(self):
        self._assert_migration_equivalence([r"""
            type Base {
                # convert property type to tuple
                property foo -> tuple<str, int32>;
            }
        """, r"""
            type Base {
                # convert property type to a bigger tuple
                property foo -> tuple<str, int32, int32>;
            }
        """])

    def test_migrations_equivalence_collections_08(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> tuple<int32, int32>;
            }
        """, r"""
            type Base {
                # convert property type to a tuple with different (but
                # cast-compatible) element types
                property foo -> tuple<str, int32>;
            }
        """])

    def test_migrations_equivalence_collections_09(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> tuple<str, int32>;
            }
        """, r"""
            type Base {
                # convert property type from unnamed to named tuple
                property foo -> tuple<a: str, b: int32>;
            }
        """])

    def test_migrations_equivalence_collections_10(self):
        # This is trying to validate that the error message is
        # sensible. There was a bug that caused an unhelpful error
        # message to appear due to incomplete dependency resolution
        # and incorrect DDL sorting for this migration.
        with self.assertRaisesRegex(
                errors.InvalidPropertyTargetError,
                "expected a scalar type, or a scalar collection, "
                "got collection 'array<default::Foo>'"):
            self._assert_migration_equivalence([r"""
                type Base;

                type Foo;
            """, r"""
                type Base {
                    property foo -> array<Foo>;
                }

                type Foo;
            """])

    def test_migrations_equivalence_collections_11(self):
        # This is trying to validate that the error message is
        # sensible. There was a bug that caused an unhelpful error
        # message to appear due to incomplete dependency resolution
        # and incorrect DDL sorting for this migration.
        with self.assertRaisesRegex(
                errors.InvalidPropertyTargetError,
                "expected a scalar type, or a scalar collection, "
                "got collection 'tuple<std::str, default::Foo>'"):

            self._assert_migration_equivalence([r"""
                type Base;

                type Foo;
            """, r"""
                type Base {
                    property foo -> tuple<str, Foo>;
                }

                type Foo;
            """])

    def test_migrations_equivalence_collections_12(self):
        # This is trying to validate that the error message is
        # sensible. There was a bug that caused an unhelpful error
        # message to appear due to incomplete dependency resolution
        # and incorrect DDL sorting for this migration.
        with self.assertRaisesRegex(
                errors.InvalidPropertyTargetError,
                "expected a scalar type, or a scalar collection, "
                "got collection 'array<default::Foo>'"):

            self._assert_migration_equivalence([r"""
            type Base {
                property foo -> array<Foo>;
            }

            type Foo;
        """, r"""
            type Base {
                property foo -> array<Foo>;
                # nested collection
                property bar -> tuple<str, array<Foo>>;
            }

            type Foo;
        """])

    def test_migrations_equivalence_collections_13(self):
        # schema aliases & collection test
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> float32;
            };

            # aliases that don't have arrays
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """, r"""
            type Base {
                property foo -> float32;
            };

            # "same" aliases that now have arrays
            alias BaseAlias := Base { bar := [Base.foo] };
            alias CollAlias := [Base.foo];
        """])

    def test_migrations_equivalence_collections_14(self):
        # schema aliases & collection test
        self._assert_migration_equivalence([r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # aliases that don't have tuples
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """, r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # "same" aliases that now have tuples
            alias BaseAlias := Base { bar := (Base.name, Base.foo) };
            alias CollAlias := (Base.name, Base.foo);
        """])

    def test_migrations_equivalence_collections_15(self):
        # schema aliases & collection test
        self._assert_migration_equivalence([r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # aliases that don't have nested collections
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """, r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # "same" aliases that now have nested collections
            alias BaseAlias := Base {
                bar := (Base.name, Base.number, [Base.foo])
            };
            alias CollAlias := (Base.name, Base.number, [Base.foo]);
        """])

    def test_migrations_equivalence_collections_16(self):
        # schema aliases & collection test
        self._assert_migration_equivalence([r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # aliases that don't have named tuples
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """, r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # "same" aliases that now have named tuples
            alias BaseAlias := Base {
                bar := (a := Base.name, b := Base.foo)
            };
            alias CollAlias := (a := Base.name, b := Base.foo);
        """])

    def test_migrations_equivalence_collections_17(self):
        # schema aliases & collection test
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> float32;
                property bar -> int32;
            };

            # aliases with array<int32>
            alias BaseAlias := Base { data := [Base.bar] };
            alias CollAlias := [Base.bar];
        """, r"""
            type Base {
                property foo -> float32;
                property bar -> int32;
            };

            # aliases with array<flaot32>
            alias BaseAlias := Base { data := [Base.foo] };
            alias CollAlias := [Base.foo];
        """])

    def test_migrations_equivalence_collections_18(self):
        # schema aliases & collection test
        self._assert_migration_equivalence([r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # aliases with tuple<str, int32>
            alias BaseAlias := Base {
                data := (Base.name, Base.number)
            };
            alias CollAlias := (Base.name, Base.number);
        """, r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # aliases with tuple<str, int32, float32>
            alias BaseAlias := Base {
                data := (Base.name, Base.number, Base.foo)
            };
            alias CollAlias := (Base.name, Base.number, Base.foo);
        """])

    def test_migrations_equivalence_collections_20(self):
        # schema aliases & collection test
        self._assert_migration_equivalence([r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # aliases with tuple<str, int32>
            alias BaseAlias := Base {
                data := (Base.name, Base.number)
            };
            alias CollAlias := (Base.name, Base.number);
        """, r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # aliases with tuple<str, float32>
            alias BaseAlias := Base {
                data := (Base.name, Base.foo)
            };
            alias CollAlias := (Base.name, Base.foo);
        """])

    def test_migrations_equivalence_collections_21(self):
        # schema aliases & collection test
        self._assert_migration_equivalence([r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # aliases with tuple<str, float32>
            alias BaseAlias := Base {
                data := (Base.name, Base.foo)
            };
            alias CollAlias := (Base.name, Base.foo);
        """, r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # aliases with named tuple<a: str, b: float32>
            alias BaseAlias := Base {
                data := (a := Base.name, b := Base.foo)
            };
            alias CollAlias := (a := Base.name, b := Base.foo);
        """])


class TestDescribe(tb.BaseSchemaLoadTest):
    """Test the DESCRIBE command."""

    re_filter = re.compile(r'[\s]+|(,(?=\s*[})]))')
    uuid_re = re.compile(
        r'[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}'
        r'-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}'
    )

    def _assert_describe(
        self,
        schema_text,
        *tests
    ):
        schema = self.load_schema(schema_text)

        tests = [iter(tests)] * 2

        for stmt_text, expected_output in zip(*tests):
            qltree = qlparser.parse(stmt_text, {None: 'test'})
            stmt = qlcompiler.compile_ast_to_ir(
                qltree,
                schema,
                modaliases={None: 'test'},
            )

            output = self.uuid_re.sub(
                '@SID@',
                stmt.expr.expr.result.expr.value,
            )

            if isinstance(expected_output, list):
                for variant in expected_output:
                    try:
                        self.assert_equal(variant, output)
                    except AssertionError:
                        pass
                    else:
                        return

                expected_output = expected_output[0]

            self.assert_equal(
                expected_output,
                output,
                message=f'query: {stmt_text!r}')

    def test_describe_01(self):
        self._assert_describe(
            """
            type Foo;
            abstract annotation anno;
            scalar type int_t extending int64 {
                annotation anno := 'ext int';
                constraint max_value(15);
            }

            abstract link f {
                property p -> int_t {
                    annotation anno := 'annotated link property';
                    constraint max_value(10);
                }
            }

            type Parent {
                multi property name -> str;
                index on (.name);
            }

            type Parent2 {
                link foo -> Foo;
            }

            type Child extending Parent, Parent2 {
                annotation anno := 'annotated';

                overloaded link foo extending f -> Foo {
                    constraint exclusive {
                        annotation anno := 'annotated constraint';
                    }
                    annotation anno := 'annotated link';
                }
            }
            """,

            'DESCRIBE TYPE Child AS SDL',

            """
            type test::Child extending test::Parent, test::Parent2 {
                annotation test::anno := 'annotated';
                overloaded link foo extending test::f -> test::Foo {
                    annotation test::anno := 'annotated link';
                    constraint std::exclusive {
                        annotation test::anno := 'annotated constraint';
                    };
                };
            };
            """,

            'DESCRIBE TYPE Child AS TEXT VERBOSE',

            """
            type test::Child extending test::Parent, test::Parent2 {
                annotation test::anno := 'annotated';
                index on (.name);
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                overloaded single link foo extending test::f -> test::Foo {
                    annotation test::anno := 'annotated link';
                    constraint std::exclusive {
                        annotation test::anno := 'annotated constraint';
                    };
                    single property p -> test::int_t {
                        constraint std::max_value(10);
                    };
                };
                required single property id -> std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
                multi property name -> std::str;
            };
            """,

            'DESCRIBE TYPE Child AS TEXT',

            """
            type test::Child extending test::Parent, test::Parent2 {
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                overloaded single link foo extending test::f -> test::Foo {
                    single property p -> test::int_t;
                };
                required single property id -> std::uuid {
                    readonly := true;
                };
                multi property name -> std::str;
            };
            """,

            'DESCRIBE OBJECT int_t AS TEXT',

            """
            scalar type test::int_t extending std::int64;
            """,

            'DESCRIBE OBJECT int_t AS TEXT VERBOSE',

            """
            scalar type test::int_t extending std::int64 {
                annotation test::anno := 'ext int';
                constraint std::max_value(15);
            };
            """,

            'DESCRIBE OBJECT array_agg AS TEXT',

            """
            function std::array_agg(s: SET OF anytype) ->  array<anytype> {
                volatility := 'IMMUTABLE';
                using sql;
            };
            """,

            'DESCRIBE FUNCTION stdgraphql::short_name AS SDL',

            r"""
            function stdgraphql::short_name(name: std::str) -> std::str {
                volatility := 'IMMUTABLE';
                using (
                    SELECT (
                        name[5:] IF name LIKE 'std::%' ELSE
                        name[9:] IF name LIKE 'default::%' ELSE
                        re_replace(r'(.+?)::(.+$)', r'\1__\2', name)
                    ) ++ '_Type'
                )
            ;};
            """,
        )

    def test_describe_02(self):
        self._assert_describe(
            """
            type Foo;
            type Bar;
            type Spam {
                link foobar -> Foo | Bar
            }
            """,

            'DESCRIBE TYPE Spam AS SDL',

            # The order of components in UNION is not defined,
            # so we provide two possibilities of output.
            [
                """
                type test::Spam {
                    single link foobar -> (test::Foo | test::Bar);
                };
                """,
                """
                type test::Spam {
                    single link foobar -> (test::Bar | test::Foo);
                };
                """,
            ]
        )

    def test_describe_03(self):
        self._assert_describe(
            """
            scalar type custom_str_t extending str {
                constraint regexp('[A-Z]+');
            }
            """,

            'DESCRIBE SCHEMA',

            """
            CREATE MODULE test IF NOT EXISTS;

            CREATE SCALAR TYPE test::custom_str_t EXTENDING std::str {
                CREATE CONSTRAINT std::regexp('[A-Z]+');
            };
            """
        )

    def test_describe_04(self):
        self._assert_describe(
            """
            abstract constraint my_one_of(one_of: array<anytype>) {
                using (contains(one_of, __subject__));
            }
            """,

            'DESCRIBE SCHEMA',

            """
            CREATE MODULE test IF NOT EXISTS;

            CREATE ABSTRACT CONSTRAINT test::my_one_of(one_of: array<anytype>){
                SET orig_expr := 'contains(one_of, __subject__)';
                USING (WITH
                    MODULE test
                SELECT
                    contains(one_of, __subject__)
                );
            };
            """
        )

    def test_describe_05(self):
        self._assert_describe(
            """
            type Foo {
                required single property middle_name -> std::str {
                    default := 'abc';
                    readonly := true;
                };
            }

            type Bar extending Foo;
            """,

            'DESCRIBE TYPE Foo AS TEXT',

            """
            type test::Foo {
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                required single property id -> std::uuid {
                    readonly := true;
                };
                required single property middle_name -> std::str {
                    default := 'abc';
                    readonly := true;
                };
            };
            """,

            'DESCRIBE TYPE Foo AS TEXT VERBOSE',

            """
            type test::Foo {
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                required single property id -> std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
                required single property middle_name -> std::str {
                    default := 'abc';
                    readonly := true;
                };
            };
            """,

            'DESCRIBE TYPE Bar AS TEXT',

            """
            type test::Bar extending test::Foo {
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                required single property id -> std::uuid {

                    readonly := true;
                };
                required single property middle_name -> std::str {
                    default := 'abc';
                    readonly := true;
                };
            };
            """,

            'DESCRIBE TYPE Foo AS SDL',

            '''
            type test::Foo {
                required single property middle_name -> std::str {
                    default := 'abc';
                    readonly := true;
                };
            };
            ''',

            'DESCRIBE TYPE Bar AS SDL',

            'type test::Bar extending test::Foo;'
        )

    def test_describe_06(self):
        self._assert_describe(
            """
            abstract type HasImage {
                # just a URL to the image
                required property image -> str;
                index on (__subject__.image);
            }


            type User extending HasImage {
                property name -> str;
            }
            """,

            'DESCRIBE TYPE User AS TEXT VERBOSE',

            """
            type test::User extending test::HasImage {
                index on (__subject__.image);
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                required single property id -> std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
                required single property image -> std::str;
                single property name -> std::str;
            };
            """,

            'DESCRIBE TYPE User AS TEXT',

            """
            type test::User extending test::HasImage {
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                required single property id -> std::uuid {
                    readonly := true;
                };
                required single property image -> std::str;
                single property name -> std::str;
            };
            """,

            'DESCRIBE TYPE User AS SDL',

            '''
            type test::User extending test::HasImage {
                single property name -> std::str;
            };
            ''',

            'DESCRIBE TYPE HasImage AS TEXT VERBOSE',

            '''
            abstract type test::HasImage {
                index on (__subject__.image);
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                required single property id -> std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
                required single property image -> std::str;
            };
            ''',

            'DESCRIBE TYPE HasImage AS TEXT',

            '''
            abstract type test::HasImage {
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                required single property id -> std::uuid {
                    readonly := true;
                };
                required single property image -> std::str;
            };
            ''',

            'DESCRIBE TYPE HasImage AS SDL',

            '''
            abstract type test::HasImage {
                index on (WITH
                    MODULE test
                SELECT
                    __subject__.image
                ) {
                    orig_expr := '__subject__.image';
                };
                required single property image -> std::str;
            };
            ''',

            'DESCRIBE TYPE HasImage AS DDL',

            '''
            CREATE ABSTRACT TYPE test::HasImage {
                CREATE REQUIRED SINGLE PROPERTY image -> std::str;
                CREATE INDEX ON (WITH
                    MODULE test
                SELECT
                    __subject__.image
                ) {
                    SET orig_expr := '__subject__.image';
                };
            };
            '''
        )

    def test_describe_07(self):
        self._assert_describe(
            """
            scalar type constraint_enum extending str {
                constraint one_of('foo', 'bar');
            }

            abstract constraint my_one_of(one_of: array<anytype>) {
                using (contains(one_of, __subject__));
            }

            scalar type constraint_my_enum extending str {
                constraint my_one_of(['fuz', 'buz']);
            }

            abstract link translated_label {
                property lang -> str;
                property prop1 -> str;
            }

            type Label {
                property text -> str;
            }

            type UniqueName {
                link translated_label extending translated_label -> Label {
                    constraint exclusive on (
                        (__subject__@source, __subject__@lang)
                    );
                    constraint exclusive on (__subject__@prop1);
                }

            }
            """,

            'DESCRIBE OBJECT constraint_my_enum AS TEXT VERBOSE',

            """
            scalar type test::constraint_my_enum extending std::str {
                constraint test::my_one_of(['fuz', 'buz']);
            };
            """,

            'DESCRIBE OBJECT my_one_of AS DDL',

            '''
            CREATE ABSTRACT CONSTRAINT test::my_one_of(one_of: array<anytype>)
            {
                SET orig_expr := 'contains(one_of, __subject__)';
                USING (WITH
                    MODULE test
                SELECT
                    contains(one_of, __subject__)
                );
            };
            ''',

            'DESCRIBE OBJECT UniqueName AS SDL',

            '''
            type test::UniqueName {
                single link translated_label extending test::translated_label
                        -> test::Label {
                    constraint std::exclusive on (WITH
                        MODULE test
                    SELECT
                        __subject__@prop1
                    ) {
                        orig_subjectexpr := '__subject__@prop1';
                    };
                    constraint std::exclusive on (WITH
                        MODULE test
                    SELECT
                        (__subject__@source, __subject__@lang)
                    ) {
                        orig_subjectexpr :=
                            '(__subject__@source, __subject__@lang)';
                    };
                };
            };
            ''',

            'DESCRIBE OBJECT UniqueName AS TEXT',

            '''
            type test::UniqueName {
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                single link translated_label extending test::translated_label
                    -> test::Label
                {
                    single property lang -> std::str;
                    single property prop1 -> std::str;
                };
                required single property id -> std::uuid {
                    readonly := true;
                };
            };
            ''',

            'DESCRIBE OBJECT UniqueName AS TEXT VERBOSE',

            '''
            type test::UniqueName {
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                single link translated_label extending test::translated_label
                    -> test::Label
                {
                    constraint std::exclusive on (__subject__@prop1);
                    constraint std::exclusive on (
                        (__subject__@source, __subject__@lang));
                    single property lang -> std::str;
                    single property prop1 -> std::str;
                };
                required single property id -> std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
            };
            ''',

            'DESCRIBE OBJECT std::max_len_value AS DDL',

            '''
            CREATE ABSTRACT CONSTRAINT std::max_len_value(max: std::int64)
                EXTENDING std::max_value, std::len_value
            {
                SET errmessage := '{__subject__} must be no longer
                                   than {max} characters.';
            };
            ''',

            'DESCRIBE OBJECT std::len_value AS SDL',

            '''
            abstract constraint std::len_value on (len(<std::str>__subject__))
            {
                errmessage := 'invalid {__subject__}';
                orig_subjectexpr := 'len(<std::str>__subject__)';
            };
            ''',

            'DESCRIBE OBJECT std::len_value AS TEXT',

            '''
            abstract constraint std::len_value on (len(<std::str>__subject__))
            {
                errmessage := 'invalid {__subject__}';
            };
            ''',

            'DESCRIBE OBJECT std::len_value AS TEXT VERBOSE',

            '''
            abstract constraint std::len_value on (len(<std::str>__subject__))
            {
                errmessage := 'invalid {__subject__}';
            };
            '''
        )

    def test_describe_08(self):
        self._assert_describe(
            """
            type Foo {
                property bar -> str {
                    readonly := False;
                }
            };
            """,

            'DESCRIBE TYPE Foo',

            """
            CREATE TYPE test::Foo {
                CREATE SINGLE PROPERTY bar -> std::str {
                    SET readonly := false;
                };
            };
            """,
            'DESCRIBE TYPE Foo AS SDL',

            """
            type test::Foo {
                single property bar -> std::str {
                    readonly := false;
                };
            };
            """,
        )

    def test_describe_alias_01(self):
        self._assert_describe(
            """
            type Foo {
                property name -> str;
            };

            alias Bar := (SELECT Foo {name, calc := 1});
            """,

            'DESCRIBE SCHEMA',

            """
            CREATE MODULE test IF NOT EXISTS;
            CREATE TYPE test::Foo {
                CREATE SINGLE PROPERTY name -> std::str;
            };
            CREATE ALIAS test::Bar :=
                (WITH
                    MODULE test
                SELECT
                    Foo {
                        name,
                        calc := 1
                    }
                );
            """
        )

    def test_describe_alias_02(self):
        self._assert_describe(
            """
            type Foo {
                property name -> str;
            };

            alias Bar {
                using (SELECT Foo {name, calc := 1});
                annotation title := 'bar alias';
            };
            """,

            'DESCRIBE SCHEMA',

            """
            CREATE MODULE test IF NOT EXISTS;
            CREATE TYPE test::Foo {
                CREATE SINGLE PROPERTY name -> std::str;
            };
            CREATE ALIAS test::Bar {
                USING (WITH
                    MODULE test
                SELECT
                    Foo {
                        name,
                        calc := 1
                    }
                );
                CREATE ANNOTATION std::title := 'bar alias';
            };
            """
        )

    def test_describe_alias_03(self):
        self._assert_describe(
            """
            alias scalar_alias := {1, 2, 3};
            """,

            'DESCRIBE SCHEMA',

            """
            CREATE MODULE test IF NOT EXISTS;
            CREATE ALIAS test::scalar_alias :=
                (WITH
                    MODULE test
                SELECT
                    {1, 2, 3}
                );
            """
        )

    def test_describe_alias_04(self):
        self._assert_describe(
            """
            alias tuple_alias := (1, 2, 3);
            alias array_alias := [1, 2, 3];
            """,

            'DESCRIBE SCHEMA',

            """
            CREATE MODULE test IF NOT EXISTS;
            CREATE ALIAS test::array_alias :=
                ([1, 2, 3]);
            CREATE ALIAS test::tuple_alias :=
                (WITH
                    MODULE test
                SELECT
                    (1, 2, 3)
                );
            """
        )

    def test_describe_computable_01(self):
        self._assert_describe(
            """
            type Foo {
                property compprop := 'foo';
                link complink := (SELECT Foo LIMIT 1);
                property annotated_compprop -> str {
                    using ('foo');
                    annotation title := 'compprop';
                };
                link annotated_link -> Foo {
                    using (SELECT Foo LIMIT 1);
                    annotation title := 'complink';
                };
            };
            """,

            'DESCRIBE SCHEMA',

            """
            CREATE MODULE test IF NOT EXISTS;
            CREATE TYPE test::Foo {
                CREATE SINGLE PROPERTY annotated_compprop {
                    USING ('foo');
                    CREATE ANNOTATION std::title := 'compprop';
                };
                CREATE SINGLE LINK annotated_link {
                    USING (WITH
                        MODULE test
                    SELECT
                        Foo
                    LIMIT
                        1
                    );
                    CREATE ANNOTATION std::title := 'complink';
                };
                CREATE SINGLE LINK complink := (WITH
                    MODULE test
                SELECT
                    Foo
                LIMIT
                    1
                );
                CREATE SINGLE PROPERTY compprop := ('foo');
            };
            """
        )

    def test_describe_computable_02(self):
        self._assert_describe(
            """
            type Foo {
                property compprop := 'foo';
                link complink := (SELECT Foo LIMIT 1);
                property annotated_compprop -> str {
                    using ('foo');
                    annotation title := 'compprop';
                };
                link annotated_link -> Foo {
                    using (SELECT Foo LIMIT 1);
                    annotation title := 'complink';
                };
            };
            """,

            'DESCRIBE TYPE test::Foo',

            """
            CREATE TYPE test::Foo {
                CREATE SINGLE PROPERTY annotated_compprop {
                    USING ('foo');
                    CREATE ANNOTATION std::title := 'compprop';
                };
                CREATE SINGLE LINK annotated_link {
                    USING (WITH
                        MODULE test
                    SELECT
                        Foo
                    LIMIT
                        1
                    );
                    CREATE ANNOTATION std::title := 'complink';
                };
                CREATE SINGLE LINK complink := (WITH
                    MODULE test
                SELECT
                    Foo
                LIMIT
                    1
                );
                CREATE SINGLE PROPERTY compprop := ('foo');
            };
            """
        )

    def test_describe_builtins_01(self):
        self._assert_describe(
            """
            """,

            'DESCRIBE TYPE schema::ObjectType',

            # the links order is non-deterministic
            """
            CREATE TYPE schema::ObjectType
            EXTENDING schema::InheritingObject,
                      schema::ConsistencySubject,
                      schema::AnnotationSubject,
                      schema::Type,
                      schema::Source
            {
                CREATE MULTI LINK intersection_of -> schema::ObjectType;
                CREATE MULTI LINK union_of -> schema::ObjectType;
                CREATE SINGLE PROPERTY is_compound_type := (
                    (EXISTS (.union_of) OR EXISTS (.intersection_of))
                );
                CREATE MULTI LINK links := (
                    .pointers[IS schema::Link]
                );
                CREATE MULTI LINK properties := (
                    .pointers[IS schema::Property]
                );
            };
            """,

            'DESCRIBE TYPE schema::ObjectType AS SDL',

            """
            type schema::ObjectType extending
                    schema::InheritingObject,
                    schema::ConsistencySubject,
                    schema::AnnotationSubject,
                    schema::Type,
                    schema::Source
            {
                multi link intersection_of -> schema::ObjectType;
                multi link links := (.pointers[IS schema::Link]);
                multi link properties := (.pointers[IS schema::Property]);
                multi link union_of -> schema::ObjectType;
                single property is_compound_type := (
                    (EXISTS (.union_of) OR EXISTS (.intersection_of))
                );
            };
            """,
        )

    def test_describe_bad_01(self):
        with self.assertRaisesRegex(
            errors.InvalidReferenceError,
            "schema item 'std::Tuple' does not exist",
        ):
            self._assert_describe(
                """
                """,

                'DESCRIBE OBJECT std::Tuple',

                '',
            )

    def test_describe_on_target_delete_01(self):
        # Test "on target delete".
        self._assert_describe(
            """
            type Foo {
                link bar -> Object {
                    on target delete allow;
                };
            }
            """,

            'DESCRIBE TYPE Foo',

            """
            CREATE TYPE test::Foo {
                CREATE SINGLE LINK bar -> std::Object {
                    ON TARGET DELETE ALLOW;
                };
            };
            """,

            'DESCRIBE TYPE Foo AS SDL',

            """
            type test::Foo {
                single link bar -> std::Object {
                    on target delete  allow;
                };
            };
            """,

            'DESCRIBE TYPE Foo AS TEXT',

            """
            type test::Foo {
                required single link __type__ -> schema::Type {
                    readonly := true;
                };
                single link bar -> std::Object {
                    on target delete  allow;
                };
                required single property id -> std::uuid {
                    readonly := true;
                };
            };
            """,
        )
