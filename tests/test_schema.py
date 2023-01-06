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


from __future__ import annotations
from typing import *

import re

from edb import errors

from edb.common import markup

from edb.edgeql import compiler as qlcompiler
from edb.edgeql import parser as qlparser
from edb.edgeql import qltypes

from edb.schema import ddl as s_ddl
from edb.schema import links as s_links
from edb.schema import name as s_name
from edb.schema import objtypes as s_objtypes

from edb.testbase import lang as tb
from edb.tools import test

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


class TestSchema(tb.BaseSchemaLoadTest):
    DEFAULT_MODULE = 'test'

    def test_schema_overloaded_prop_01(self):
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
    def test_schema_overloaded_prop_02(self):
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
    def test_schema_overloaded_prop_03(self):
        """
            type UniqueName {
                overloaded property name -> str
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "it is illegal for the computed property 'val' "
                  "of object type 'test::UniqueName_2' to overload "
                  "an existing property")
    def test_schema_overloaded_prop_04(self):
        """
            type UniqueName {
                property val -> str {
                    constraint exclusive;
                }
            };
            type UniqueName_2 extending UniqueName {
                overloaded property val -> str {
                    using ('bad');
                }
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "it is illegal for the computed property 'val' "
                  "of object type 'test::UniqueName_2' to overload "
                  "an existing property")
    def test_schema_overloaded_prop_05(self):
        """
            type UniqueName {
                property val := 'ok';
            };
            type UniqueName_2 extending UniqueName {
                # This doesn't appear to be a computable property, but
                # it is due to inheritance.
                overloaded property val -> str {
                    constraint exclusive;
                }
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "it is illegal for the property 'val' of object "
                  "type 'test::UniqueName_3' to extend both a computed "
                  "and a non-computed property")
    def test_schema_overloaded_prop_06(self):
        """
            type UniqueName {
                property val := 'ok';
            };
            type UniqueName_2 {
                property val -> str;
            };
            type UniqueName_3 extending UniqueName, UniqueName_2;
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "it is illegal for the property 'val' of object "
                  "type 'test::UniqueName_3' to extend more than one "
                  "computed property")
    def test_schema_overloaded_prop_07(self):
        """
            type UniqueName {
                property val := 'ok';
            };
            type UniqueName_2 {
                property val := 'ok';
            };
            # It's illegal to extend 2 computable properties even if
            # the expression is the same for them.
            type UniqueName_3 extending UniqueName, UniqueName_2;
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "it is illegal for the property 'val' of object "
                  "type 'test::UniqueName_4' to extend both a computed "
                  "and a non-computed property")
    def test_schema_overloaded_prop_08(self):
        """
            type UniqueName {
                property val -> str;
            };
            type UniqueName_2 {
                property val := 'ok';
            };
            type UniqueName_3 extending UniqueName_2;
            type UniqueName_4 extending UniqueName, UniqueName_3;
        """

    @tb.must_fail(errors.SchemaError,
                  "it is illegal to create a type union that causes "
                  "a computed property 'val' to mix with other "
                  "versions of the same property 'val'")
    def test_schema_overloaded_prop_09(self):
        # Overloading implicitly via a type UNION.
        """
            type UniqueName {
                property val -> str;
            };
            type UniqueName_2 {
                property val := 'ok';
            };
            alias Combo := {UniqueName, UniqueName_2};
        """

    @tb.must_fail(errors.SchemaError,
                  "it is illegal to create a type union that causes "
                  "a computed property 'val' to mix with other "
                  "versions of the same property 'val'")
    def test_schema_overloaded_prop_10(self):
        # Overloading implicitly via a type UNION.
        """
            type UniqueName {
                property val -> str;
            };
            type UniqueName_2 {
                property val := 'ok';
            };
            type Combo {
               multi link comp := {UniqueName, UniqueName_2};
            }
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "it is illegal for the computed link 'foo' "
                  "of object type 'test::UniqueName_2' to overload "
                  "an existing link")
    def test_schema_overloaded_link_01(self):
        """
            type Foo;
            type UniqueName {
                link foo -> Foo;
            };
            type UniqueName_2 extending UniqueName {
                overloaded link foo -> Foo {
                    using (SELECT Foo LIMIT 1);
                }
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "it is illegal for the computed link 'foo' "
                  "of object type 'test::UniqueName_2' to overload "
                  "an existing link")
    def test_schema_overloaded_link_02(self):
        """
            type Foo;
            type UniqueName {
                link foo := (SELECT Foo LIMIT 1);
            };
            type UniqueName_2 extending UniqueName {
                # This doesn't appear to be a computable link, but
                # it is due to inheritance.
                overloaded link foo -> Foo {
                    constraint exclusive;
                }
            };
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "it is illegal for the link 'foo' of object "
                  "type 'test::UniqueName_3' to extend both a computed "
                  "and a non-computed link")
    def test_schema_overloaded_link_03(self):
        """
            type Foo;
            type UniqueName {
                link foo := (SELECT Foo LIMIT 1);
            };
            type UniqueName_2 {
                link foo -> Foo;
            };
            type UniqueName_3 extending UniqueName, UniqueName_2;
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "it is illegal for the link 'foo' of object "
                  "type 'test::UniqueName_3' to extend more than one "
                  "computed link")
    def test_schema_overloaded_link_04(self):
        """
            type Foo;
            type UniqueName {
                link foo := (SELECT Foo LIMIT 1);
            };
            type UniqueName_2 {
                link foo := (SELECT Foo LIMIT 1);
            };
            # It's illegal to extend 2 computable links even if
            # the expression is the same for them.
            type UniqueName_3 extending UniqueName, UniqueName_2;
        """

    @tb.must_fail(
        errors.InvalidLinkTargetError,
        "invalid link target type, expected object type, "
        "got scalar type 'std::str'",
        position=69,
    )
    def test_schema_bad_link_01(self):
        """
            type Object {
                link foo -> str
            };
        """

    @tb.must_fail(
        errors.InvalidLinkTargetError,
        "invalid link target type, expected object type, "
        "got scalar type 'std::int64'",
        position=69,
    )
    def test_schema_bad_link_02(self):
        """
            type Object {
                link foo := 1 + 1
            };
        """

    @tb.must_fail(
        errors.InvalidLinkTargetError,
        "object type 'std::FreeObject' is not a valid link target",
        position=69,
    )
    def test_schema_bad_link_03(self):
        """
            type Object {
                link foo -> FreeObject
            };
        """

    @tb.must_fail(
        errors.InvalidLinkTargetError,
        "invalid link target type, expected object type, "
        "got scalar type 'std::str'",
    )
    def test_schema_bad_link_04(self):
        """
            type Object {
                property foo -> str;
                link bar := .foo;
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

    @tb.must_fail(errors.InvalidReferenceError,
                  "property 'test::bar' does not exist",
                  position=59)
    def test_schema_bad_prop_04(self):
        """
            abstract property foo extending bar;
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  "link 'test::bar' does not exist",
                  position=55)
    def test_schema_bad_prop_05(self):
        """
            abstract link foo extending bar;
        """

    @tb.must_fail(errors.InvalidDefinitionError,
                  "'test::foo' is defined recursively")
    def test_schema_bad_prop_06(self):
        """
            abstract link foo extending foo;
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  "object type or alias 'std::str' does not exist")
    def test_schema_bad_prop_07(self):
        """
            type Person {
                required property name := str {
                    # empty block
                }
            }
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  "type 'test::int' does not exist",
                  position=73)
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

    @tb.must_fail(errors.InvalidReferenceError,
                  "type 'test::Bar' does not exist",
                  position=46)
    def test_schema_bad_type_05(self):
        """
            type Foo extending Bar;
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  "type 'test::Bar' does not exist",
                  position=66)
    def test_schema_bad_type_06(self):
        """
            type Foo {
                link val -> Bar;
            };
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  "annotation 'test::bar' does not exist",
                  position=54)
    def test_schema_bad_type_07(self):
        """
            type Foo {
                annotation bar := 'Bogus';
            };
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  "constraint 'test::bogus' does not exist",
                  position=96)
    def test_schema_bad_type_08(self):
        """
            type Foo {
                property val -> str {
                    constraint bogus(5);
                }
            };
        """

    @tb.must_fail(errors.InvalidDefinitionError,
                  "'test::Foo' is defined recursively")
    def test_schema_bad_type_09(self):
        """
            type Foo extending Foo;
        """

    @tb.must_fail(errors.InvalidDefinitionError,
                  "'test::Foo0' is defined recursively")
    def test_schema_bad_type_10(self):
        """
            type Foo0 extending Foo1;
            type Foo1 extending Foo2;
            type Foo2 extending Foo3;
            type Foo3 extending Foo0;
        """

    @tb.must_fail(errors.UnsupportedFeatureError,
                  "unsupported type intersection in schema")
    def test_schema_bad_type_11(self):
        """
            type Foo;
            type Bar;
            type Spam {
                multi link foobar := Foo[IS Bar];
            }
        """

    @tb.must_fail(errors.SchemaError,
                  "invalid type: pseudotype 'anytype' is a generic type")
    def test_schema_bad_type_12(self):
        """
            type Foo {
                property val -> anytype;
            }
        """

    @tb.must_fail(errors.SchemaError,
                  "invalid type: pseudotype 'anytype' is a generic type")
    def test_schema_bad_type_13(self):
        """
            type Foo {
                link val -> anytype;
            }
        """

    @tb.must_fail(errors.SchemaError,
                  "invalid type: pseudotype 'anytuple' is a generic type")
    def test_schema_bad_type_14(self):
        """
            type Foo {
                property val -> anytuple;
            }
        """

    @tb.must_fail(errors.SchemaError,
                  "scalar type must have a concrete base type",
                  position=27)
    def test_schema_bad_type_15(self):
        """
            scalar type Foo;
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
        foo_bar = obj.getptr(schema, s_name.UnqualName('foo_plus_bar'))
        self.assertEqual(
            foo_bar.get_cardinality(schema),
            qltypes.SchemaCardinality.One,
        )

    def test_schema_computable_cardinality_inference_02(self):
        schema = self.load_schema("""
            type Object {
                multi property foo -> str;
                property bar -> str;
                multi property foo_plus_bar :=
                    __source__.foo ++ __source__.bar;
            };
        """)

        obj = schema.get('test::Object')
        foo_bar = obj.getptr(schema, s_name.UnqualName('foo_plus_bar'))
        self.assertEqual(
            foo_bar.get_cardinality(schema),
            qltypes.SchemaCardinality.Many,
        )

    @tb.must_fail(errors.SchemaDefinitionError,
                  "possibly more than one element returned by an expression "
                  "for the computed link 'ham' of object type 'test::Spam' "
                  "explicitly declared as 'single'",
                  line=5, col=36)
    def test_schema_computable_cardinality_inference_03(self):
        """
            type Spam {
                required property title -> str;
                multi link spams -> Spam;
                single link ham := .spams;
            }
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "possibly more than one element returned by an expression "
                  "for the computed property 'hams' of object type "
                  "'test::Spam' explicitly declared as 'single'",
                  line=5, col=41)
    def test_schema_computable_cardinality_inference_04(self):
        """
            type Spam {
                required property title -> str;
                multi link spams -> Spam;
                single property hams := .spams.title;
            }
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "possibly an empty set returned by an expression for "
                  "the computed property 'hams' of object type "
                  "'test::Spam' explicitly declared as 'required'",
                  line=5, col=43)
    def test_schema_computable_cardinality_inference_05(self):
        """
            type Spam {
                required property title -> str;
                multi link spams -> Spam;
                required property hams := .spams.title;
            }
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "cannot make property 'title' of object type 'test::B' "
                  "optional: its parent property 'title' of object type "
                  "'test::A' is defined as required",
                  line=7, col=17)
    def test_schema_optionality_consistency_check_01(self):
        """
            type A {
                required property title -> str;
            }

            type B extending A {
                overloaded optional property title -> str;
            }
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "cannot make property 'title' of object type 'test::C' "
                  "optional: its parent property 'title' of object type "
                  "'test::B' is defined as required",
                  line=11, col=17)
    def test_schema_optionality_consistency_check_02(self):
        """
            type A {
                optional property title -> str;
            }

            type B {
                required property title -> str;
            }

            type C extending A, B {
                overloaded optional property title -> str;
            }
        """

    @tb.must_fail(
        errors.SchemaDefinitionError,
        "the type inferred from the expression of the computed property "
        "'title' of object type 'test::A' is scalar type 'std::int64', "
        "which does not match the explicitly specified scalar type 'std::str'",
        line=3, col=35)
    def test_schema_target_consistency_check_01(self):
        """
            type A {
                property title -> str {
                    using (1)
                }
            }
        """

    @tb.must_fail(
        errors.SchemaDefinitionError,
        "the type inferred from the expression of the computed property "
        "'title' of object type 'test::A' is collection "
        "'tuple<std::int64, std::int64>', which does not match the explicitly "
        "specified collection 'tuple<std::str, std::str>'",
        line=3, col=35)
    def test_schema_target_consistency_check_02(self):
        """
            type A {
                property title -> tuple<str, str> {
                    using ((1, 2))
                }
            }
        """

    @tb.must_fail(
        errors.SchemaDefinitionError,
        "the type inferred from the expression of the computed link "
        "'foo' of object type 'test::C' is object type 'test::B', "
        "which does not match the explicitly specified object type 'test::A'",
        line=6, col=29)
    def test_schema_target_consistency_check_03(self):
        """
            type A;
            type B;

            type C {
                link foo -> A {
                    using (SELECT B)
                }
            }
        """

    @tb.must_fail(
        errors.InvalidReferenceError,
        "'test::X' exists, but is a scalar type, not an object type",
        line=3, col=30)
    def test_schema_wrong_type_01(self):
        """
            scalar type X extending enum<TEST>;
            type Y extending X {}
        """

    @tb.must_fail(
        errors.InvalidReferenceError,
        "'test::X' exists, but is an object type, not a constraint",
        line=3, col=22)
    def test_schema_wrong_type_02(self):
        """
            type X;
            type Y { constraint X; }
        """

    @tb.must_fail(errors.InvalidDefinitionError,
                  "object 'test::T' was already declared")
    def test_schema_duplicate_def_01(self):
        """
            type T;
            type T;
        """

    @tb.must_fail(
        errors.InvalidDefinitionError,
        "property 'foo' of object type 'test::T' was already declared",
    )
    def test_schema_duplicate_def_02(self):
        """
            type T {
                property foo -> str;
                property foo -> int64;
            };
        """

    @tb.must_fail(
        errors.InvalidDefinitionError,
        "access policy 'foo' of object type 'test::T' was already declared",
    )
    def test_schema_duplicate_def_03(self):
        """
            type T {
                access policy foo allow all using (true);
                access policy foo allow all using (true);
            };
        """

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
        obj1_id = Obj1.getptr(schema, s_name.UnqualName('id'))
        obj1_type = Obj1.getptr(schema, s_name.UnqualName('__type__'))
        obj1_type_source = obj1_type.getptr(
            schema, s_name.UnqualName('source'))
        obj2_type = Obj2.getptr(schema, s_name.UnqualName('__type__'))
        foo = Obj2.getptr(schema, s_name.UnqualName('foo'))
        foo_target = foo.getptr(schema, s_name.UnqualName('target'))
        bar = Obj5.getptr(schema, s_name.UnqualName('bar'))

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
                SET volatility := 'Immutable';
                USING (
                    SELECT contains(arr, val)
                );
            };

            CREATE ABSTRACT CONSTRAINT
            test::my_one_of(one_of: array<anytype>) {
                USING (
                    SELECT (
                        test::my_contains(one_of, __subject__),
                    ).0
                );
            };

            CREATE SCALAR TYPE test::my_scalar_t extending str {
                CREATE CONSTRAINT test::my_one_of(['foo', 'bar']);
            };
        ''')

        my_scalar_t = schema.get('test::my_scalar_t')
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
        obj1_num = Obj1.getptr(schema, s_name.UnqualName('num'))

        Obj2 = schema.get('test::Object2')
        obj2_num = Obj2.getptr(schema, s_name.UnqualName('num'))

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
        obj1_num = Obj1.getptr(schema, s_name.UnqualName('num'))

        Obj2 = schema.get('test::Object2')
        obj2_num = Obj2.getptr(schema, s_name.UnqualName('num'))

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

        self.assertEqual(
            Object1.get_annotation(schema, s_name.QualName('test', 'noninh')),
            'bar',
        )
        # Attributes are non-inheritable by default
        self.assertIsNone(
            Object2.get_annotation(schema, s_name.QualName('test', 'noninh')),
        )

        self.assertEqual(
            Object1.get_annotation(schema, s_name.QualName('test', 'inh')),
            'inherit me',
        )
        self.assertEqual(
            Object2.get_annotation(schema, s_name.QualName('test', 'inh')),
            'inherit me',
        )

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
        anno = annos.get(schema, s_name.QualName('default', 'inh_anno'))
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
        name_prop = User.getptr(schema, s_name.UnqualName('name'))
        constr = name_prop.get_constraints(schema).objects(schema)[0]
        base_names = constr.get_bases(schema).names(schema)
        self.assertEqual(len(base_names), 1)
        self.assertTrue(str(base_names[0]).startswith(
            'default::std|exclusive@default|__||name&default||Named@'))

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
        name_prop = User.getptr(schema, s_name.UnqualName('name'))
        constr = name_prop.get_constraints(schema).objects(schema)[0]
        base_names = constr.get_bases(schema).names(schema)
        self.assertEqual(len(base_names), 1)
        self.assertTrue(str(base_names[0]).startswith(
            'default::std|exclusive@default|__||name&default||Named@'))

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
        name_prop = VegRecipes.getptr(schema, s_name.UnqualName('name'))
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
        name_prop = VegRecipes.getptr(schema, s_name.UnqualName('name'))
        constr = name_prop.get_constraints(schema).objects(schema)

        self.assertEqual(
            len(constr), 0,
            'there should be no constraints on alias links or properties',
        )

    @tb.must_fail(errors.InvalidConstraintDefinitionError,
                  "abstract constraint 'test::aaa' must define parameters "
                  "to reflect parameters of the abstract constraint "
                  "'std::max_len_value' it extends",
                  line=2, col=13)
    def test_schema_constraint_inheritance_05(self):
        """
            abstract constraint aaa extending std::max_len_value;
        """

    @tb.must_fail(errors.InvalidConstraintDefinitionError,
                  "abstract constraint 'test::zzz' extends multiple "
                  "constraints with parameters",
                  line=8, col=13)
    def test_schema_constraint_inheritance_06(self):
        """
            abstract constraint aaa(max: std::int64)
                extending std::max_len_value;

            abstract constraint bbb(min: std::int64)
                extending std::min_len_value;

            abstract constraint zzz(max: std::int64)
                extending aaa, bbb;
        """

    @tb.must_fail(errors.InvalidConstraintDefinitionError,
                  "abstract constraint 'test::zzz' has fewer parameters "
                  "than the abstract constraint 'test::aaa' it extends",
                  line=5, col=13)
    def test_schema_constraint_inheritance_07(self):
        """
            abstract constraint aaa(max: std::int64, foo: str)
                extending std::max_len_value;

            abstract constraint zzz(max: std::int64)
                extending aaa;
        """

    def test_schema_constraint_inheritance_08(self):
        """
            abstract constraint aaa(max: std::int64)
                extending std::max_len_value;

            abstract constraint zzz(max: std::int64)
                extending aaa;
        """

    @tb.must_fail(errors.InvalidConstraintDefinitionError,
                  "abstract constraint 'test::zzz' has fewer parameters "
                  "than the abstract constraint 'test::aaa' it extends",
                  line=5, col=13)
    def test_schema_constraint_inheritance_09(self):
        """
            abstract constraint aaa(max: std::int64, foo: str)
                extending std::max_len_value;

            abstract constraint zzz(max: std::str)
                extending aaa;
        """

    @tb.must_fail(errors.InvalidConstraintDefinitionError,
                  "the 'min' parameter of the abstract constraint 'test::zzz' "
                  "must be renamed to 'max' to match the signature of the "
                  "base abstract constraint 'test::aaa'",
                  line=5, col=13)
    def test_schema_constraint_inheritance_10(self):
        """
            abstract constraint aaa(max: std::int64)
                extending std::max_len_value;

            abstract constraint zzz(min: std::str)
                extending aaa;
        """

    @tb.must_fail(errors.InvalidConstraintDefinitionError,
                  "the 'max' parameter of the abstract constraint 'test::zzz' "
                  "has type of std::str that is not implicitly castable to "
                  "the corresponding parameter of the abstract constraint "
                  "'test::aaa' with type std::int64",
                  line=5, col=13)
    def test_schema_constraint_inheritance_11(self):
        """
            abstract constraint aaa(max: std::int64)
                extending std::max_len_value;

            abstract constraint zzz(max: std::str)
                extending aaa;
        """

    @tb.must_fail(errors.InvalidConstraintDefinitionError,
                  "the 'max' parameter of the abstract constraint 'test::zzz' "
                  "cannot be of generic type because the corresponding "
                  "parameter of the abstract constraint 'test::aaa' it "
                  "extends has a concrete type",
                  line=5, col=13)
    def test_schema_constraint_inheritance_12(self):
        """
            abstract constraint aaa(max: std::int64)
                extending std::max_len_value;

            abstract constraint zzz(max: anyscalar)
                extending aaa;
        """

    @tb.must_fail(errors.InvalidConstraintDefinitionError,
                  "cannot redefine constraint 'std::exclusive'"
                  " of property 'name' of object type 'test::B' as delegated:"
                  " it is defined as non-delegated in property 'name'"
                  " of object type 'test::A'",
                  line=10, col=21)
    def test_schema_constraint_inheritance_13(self):
        """
            type A {
                property name -> str {
                    constraint exclusive;
                }
            }

            type B extending A {
                overloaded property name -> str {
                    delegated constraint exclusive;
                }
            }
        """

    def test_schema_property_cardinality_alter_01(self):
        schema = self.load_schema('''
            type Foo {
                single property foo1 -> str;
                single property bar1 := .foo1 ++ '!';

                single property foo2 -> str;
                property bar2 := .foo2 ++ '!';
            }
        ''')

        with self.assertRaisesRegex(
            errors.SchemaDefinitionError,
            "cannot convert property 'foo1' of object type 'test::Foo' to "
            "'multi' cardinality because this affects expression of "
            "property 'bar1' of object type 'test::Foo'"
        ):
            self.run_ddl(schema, '''
                ALTER TYPE Foo ALTER PROPERTY foo1 SET MULTI
            ''', default_module='test')

        # Altering foo2 is OK, because the computable bar2 was declared
        # without explicit cardinality.
        self.run_ddl(schema, '''
            ALTER TYPE Foo ALTER PROPERTY foo2 SET MULTI
        ''', default_module='test')

    def test_schema_property_cardinality_alter_02(self):
        schema = self.load_schema('''
            type Foo {
                single property foo1 -> str;
            }

            type Bar extending Foo;
        ''')

        with self.assertRaisesRegex(
            errors.SchemaDefinitionError,
            "cannot redefine the cardinality of property 'foo1' of "
            "object type 'test::Bar'.*"
        ):
            self.run_ddl(schema, '''
                ALTER TYPE Bar ALTER PROPERTY foo1 SET MULTI
            ''', default_module='test')

    def test_schema_property_cardinality_alter_03(self):
        schema = self.load_schema('''
            type Foo {
                single property foo1 -> str;
            }

            type Bar extending Foo;
        ''')

        schema = self.run_ddl(schema, '''
            ALTER TYPE Foo ALTER PROPERTY foo1 SET MULTI
        ''', default_module='test')

        Bar = schema.get('test::Bar', type=s_objtypes.ObjectType)
        foo1 = Bar.getptr(schema, s_name.UnqualName('foo1'))
        self.assertEqual(str(foo1.get_cardinality(schema)), 'Many')

    def test_schema_ref_diamond_inheritance(self):
        schema = tb._load_std_schema()

        schema = self.run_ddl(schema, '''
            CREATE MODULE default;
            CREATE TYPE default::A;
            CREATE TYPE default::B EXTENDING A;
            CREATE TYPE default::C EXTENDING A, B;
        ''')

        orig_get_children = type(schema).get_children

        def stable_get_children(self, scls):
            children = orig_get_children(self, scls)
            return list(sorted(children, key=lambda obj: obj.get_name(schema)))

        type(schema).get_children = stable_get_children

        try:
            schema = self.run_ddl(schema, '''
                ALTER TYPE default::A CREATE PROPERTY foo -> str;
            ''')
        finally:
            type(schema).get_children = orig_get_children

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

                index on (.foo)
            };
        """)

        schema = self.run_ddl(schema, '''
            CREATE FUNCTION test::foo (a: int64) -> int64
            USING ( SELECT a );
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
            "function 'std::json_typeof(json: std::json)'",
        )

        fn_param = fn.get_params(schema).get_by_name(schema, 'json')
        self.assertEqual(
            fn_param.get_verbosename(schema, with_parent=True),
            "parameter 'json' of function 'std::json_typeof(json: std::json)'",
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
                schema, s_name.QualName('test', 'attr')).get_verbosename(
                    schema, with_parent=True),
            "annotation 'test::attr' of object type 'test::Object1'",
        )

        foo_prop = obj.get_pointers(schema).get(
            schema, s_name.UnqualName('foo'))
        self.assertEqual(
            foo_prop.get_verbosename(schema, with_parent=True),
            "property 'foo' of object type 'test::Object1'",
        )

        self.assertEqual(
            foo_prop.get_annotations(schema).get(
                schema, s_name.QualName('test', 'attr')).get_verbosename(
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

        bar_link = obj.get_pointers(schema).get(
            schema, s_name.UnqualName('bar'))
        self.assertEqual(
            bar_link.get_verbosename(schema, with_parent=True),
            "link 'bar' of object type 'test::Object1'",
        )

        bar_link_prop = bar_link.get_pointers(schema).get(
            schema, s_name.UnqualName('bar_prop'))
        self.assertEqual(
            bar_link_prop.get_annotations(schema).get(
                schema, s_name.QualName('test', 'attr')).get_verbosename(
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

        self.assertEqual(
            next(iter(obj.get_indexes(
                schema).objects(schema))).get_verbosename(
                    schema, with_parent=True),
            "index 'foo_7770702d' of object type 'test::Object1'",
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
        A_t = A.getptr(schema, s_name.UnqualName('t'))
        A_t2 = A.getptr(schema, s_name.UnqualName('t2'))
        A_tf_link = A.getptr(schema, s_name.UnqualName('tf'))
        A_tf = A_tf_link.get_target(schema)

        # Check that ((T1 | T2) & F) has properties from both parts
        # of the intersection.
        A_tf.getptr(schema, s_name.UnqualName('n'))
        A_tf.getptr(schema, s_name.UnqualName('f'))

        # Ditto for link properties defined on a common link.
        tfd = A_tf.getptr(schema, s_name.UnqualName('d'))
        tfd.getptr(schema, s_name.UnqualName('f_d_prop'))

        # t1_d_prop is only present in T1, and so wouldn't be in T1 | T2
        self.assertIsNone(tfd.maybe_get_ptr(schema, 't1_d_prop'))

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

        BaseObject = schema.get('std::BaseObject')
        Object = schema.get('std::Object')
        A = schema.get('test::A')
        B = schema.get('test::B')
        C = schema.get('test::C')
        std_link = schema.get('std::link')
        BaseObject__type__ = BaseObject.getptr(
            schema, s_name.UnqualName('__type__'))
        Object__type__ = Object.getptr(
            schema, s_name.UnqualName('__type__'))
        A__type__ = A.getptr(schema, s_name.UnqualName('__type__'))
        B__type__ = B.getptr(schema, s_name.UnqualName('__type__'))
        C__type__ = C.getptr(schema, s_name.UnqualName('__type__'))
        self.assertEqual(
            C__type__.get_ancestors(schema).objects(schema),
            (
                B__type__,
                A__type__,
                Object__type__,
                BaseObject__type__,
                std_link,
            )
        )

        schema = self.run_ddl(schema, """
            START MIGRATION TO {
                module test {
                    type A;
                    type B;
                    type C extending B;
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

        self.assertEqual(
            C__type__.get_ancestors(schema).objects(schema),
            (
                B__type__,
                Object__type__,
                BaseObject__type__,
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
        B_name = B.getptr(schema, s_name.UnqualName('name'))
        schema, derived = std_prop.derive_ref(
            schema,
            B,
            target=schema.get('std::str'),
            name=B_name.get_name(schema),
            inheritance_merge=False,
            mark_derived=True,
            transient=True,
        )

        self.assertEqual(
            derived.get_ancestors(schema).objects(schema),
            (
                std_prop,
            )
        )

    def test_schema_ast_contects_01(self):
        schema = self.load_schema("")
        schema = self.run_ddl(schema, """
            create type test::Foo {
                create property asdf := 1 + 2 + 3
            };
        """)

        obj = schema.get('test::Foo')
        asdf = obj.getptr(schema, s_name.UnqualName('asdf'))
        expr_ast = asdf.get_expr(schema).qlast
        self.assertEqual(
            expr_ast.context.name,
            f'<{asdf.id} expr>'
        )

        schema = self.run_ddl(schema, """
            alter type test::Foo {
                create property x -> str { set default := "test" };
            }
        """)
        x = obj.getptr(schema, s_name.UnqualName('x'))
        default_ast = x.get_default(schema).qlast
        self.assertEqual(
            default_ast.context.name,
            f'<{x.id} default>'
        )

    @tb.must_fail(errors.InvalidReferenceError,
                  "cannot follow backlink 'bar'",
                  line=4, col=27)
    def test_schema_backlink_01(self):
        """
            type Bar {
                link foo -> Foo;
                link f := .<bar[IS Foo];
            }

            type Foo {
                link bar := .<foo[IS Bar];
            }
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  "cannot follow backlink 'bar'",
                  line=4, col=27)
    def test_schema_backlink_02(self):
        """
            type Bar {
                link foo -> Foo;
                link f := .<bar;
            }

            type Foo {
                link bar := .<foo;
            }
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  "cannot follow backlink 'bar'",
                  line=3, col=22)
    def test_schema_backlink_03(self):
        """
            alias B := Bar {
                f := .<bar[IS Foo]
            };

            type Bar {
                link foo -> Foo;
            }

            type Foo {
                link bar := .<foo[IS Bar];
            }
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  "cannot follow backlink 'bar'",
                  line=3, col=20)
    def test_schema_backlink_04(self):
        """
            function foo() -> uuid using (
                Bar.<bar[IS Foo].id
            );

            type Bar {
                link foo -> Foo;
            }

            type Foo {
                link bar := .<foo[IS Bar];
            }
        """

    def test_schema_recursive_01(self):
        schema = self.load_schema(r'''
            type Foo {
                link next -> Foo;
                property val := 1;
            }
        ''', modname='default')

        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            "property 'val' of object type 'default::Foo' "
            "is defined recursively"
        ):
            self.run_ddl(schema, '''
                ALTER TYPE default::Foo
                    ALTER PROPERTY val USING (1 + (.next.val ?? 0));
            ''')

    @test.xfail('''
        The error is not raised.
    ''')
    def test_schema_recursive_02(self):
        schema = self.load_schema(r'''
            type Foo {
                link next -> Bar;
                property val := 1;
            }

            type Bar {
                link next -> Foo;
                property val := 1;
            }
        ''', modname='default')

        self.run_ddl(schema, '''
            ALTER TYPE default::Foo
                ALTER PROPERTY val USING (1 + (.next.val ?? 0));
        ''')

        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            "definition dependency cycle between "
            "property 'val' of object type 'default::Bar' and "
            "property 'val' of object type 'default::Foo'"
        ):
            self.run_ddl(schema, '''
                ALTER TYPE default::Bar
                    ALTER PROPERTY val USING (1 + (.next.val ?? 0));
                    # ALTER PROPERTY val USING ('bad');
            ''')

    def test_schema_recursive_03(self):
        schema = self.load_schema(r'''
            function foo(v: int64) -> int64 using (
                1 + v
            );
        ''', modname='default')

        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            r"function 'default::foo\(v:.+int64\)' "
            r"is defined recursively"
        ):
            self.run_ddl(schema, '''
                ALTER FUNCTION foo(v: int64) USING (
                    0 IF v < 0 ELSE 1 + foo(v -1)
                );
            ''')

        # Make sure unrelated functions with similar prefix
        # aren't matched erroneously (#3115)
        schema = self.load_schema(r'''
            function len_strings(words: str) -> int64 using (
                len(words)
            );
        ''', modname='default')

    @test.xerror('''
        RecursionError: maximum recursion depth exceeded in comparison

        This happens while processing '_alter_finalize'.
    ''')
    def test_schema_recursive_04(self):
        schema = self.load_schema(r'''
            function foo(v: int64) -> int64 using (
                1 + v
            );

            function bar(v: int64) -> int64 using (
                0 IF v < 0 ELSE 1 + foo(v -1)
            );
        ''', modname='default')

        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            r"definition dependency cycle between "
            r"function 'default::bar\(v:.+int64\)' and "
            r"function 'default::foo\(v:.+int64\)'"
        ):
            self.run_ddl(schema, '''
                ALTER FUNCTION foo(v: int64) USING (
                    0 IF v < 0 ELSE 1 + bar(v -1)
                );
            ''')

    def test_schema_recursive_05(self):
        schema = self.load_schema(r'''
            type Foo {
                property val := foo(1);
            }

            function foo(v: int64) -> int64 using (
                1 + v
            );
        ''', modname='default')

        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            r"definition dependency cycle between "
            r"function 'default::foo\(v:.+int64\)' and "
            r"property 'val' of object type 'default::Foo'"
        ):
            self.run_ddl(schema, r'''
                ALTER FUNCTION foo(v: int64) USING (
                    # This is very broken now
                    assert_exists(1 + (SELECT Foo LIMIT 1).val)
                );
            ''')

    def test_schema_recursive_06(self):
        schema = self.load_schema(r'''
            type Foo {
                property val := 1;
            }

            function foo(v: int64) -> optional int64 using (
                1 + (SELECT Foo LIMIT 1).val
            );
        ''', modname='default')

        with self.assertRaisesRegex(
            errors.SchemaDefinitionError,
            r"cannot alter property 'val' of object type 'default::Foo' "
            r"because this affects body expression of "
            r"function 'default::foo\(v:.+int64\)'"
        ):
            self.run_ddl(schema, r'''
                ALTER TYPE Foo {
                    # This is very broken now
                    ALTER PROPERTY val USING (foo(1));
                };
            ''')

    @test.xerror('''
        ...File
          "/home/victor/dev/magicstack/edgedb/edb/edgeql/compiler/stmtctx.py",
          line 588, in declare_view_from_schema
            assert view_expr is not None
        AssertionError
    ''')
    def test_schema_recursive_07(self):
        schema = self.load_schema(r'''
            type Foo {
                property val -> int64;
            }

            alias FooAlias0 := Foo {
                comp := .val + (SELECT FooAlias1 LIMIT 1).comp
            };

            alias FooAlias1 := Foo {
                comp := .val + 1
            };
        ''', modname='default')

        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            "definition dependency cycle between "
            "alias 'default::FooAlias1' and alias 'default::FooAlias0'"
        ):
            self.run_ddl(schema, r'''
                ALTER ALIAS FooAlias1 USING (
                    Foo {
                      comp := .val + (SELECT FooAlias0 LIMIT 1).comp
                    }
                );
            ''')

    @test.xerror('''
        RecursionError: maximum recursion depth exceeded in comparison
    ''')
    def test_schema_recursive_08(self):
        schema = self.load_schema(r'''
            type Foo;

            type Bar extending Foo;
        ''', modname='default')

        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            "'default::Foo' is defined recursively"
        ):
            self.run_ddl(schema, r'''
                ALTER TYPE Foo EXTENDING Bar;
            ''')

    def test_schema_with_block_01(self):
        self.load_schema(r'''
            function test(foo: str, bar: str) -> str {
                using (
                    with
                        f := foo,
                        b := bar,
                        r := f ++ b,
                    select r
                )
            }
        ''', modname='default')

    def test_schema_with_block_02(self):
        self.load_schema(r'''
            alias a := 1;

            function test(foo: int64, bar: int64) -> int64 {
                using (
                    with
                        aa := a,
                        def as module default,
                        aa2 := def::a
                    select aa + aa2 + foo + bar
                )
            }
        ''', modname='default')

    def test_schema_computed_01(self):
        # Issue #3499
        """
            type Version {
                multi link fields -> Field {
                    property position -> int32 {
                        default := 0;
                    }
                }
                multi link sections :=
                    (select .ordered_fields filter .type = 'section');
                multi link ordered_fields :=
                    (select .fields order by @position);
            }

            type Field {
                required property type -> str;
                multi link versions := .<fields[is Version];
            }
        """

    def test_schema_computed_02(self):
        # Issue #3499
        """
            type Version {
                multi link fields -> Field {
                    property position -> int32 {
                        default := 0;
                    }
                }
                multi link ordered_fields :=
                    (select .fields order by @position);
                multi link sections :=
                    (select .ordered_fields filter .type = 'section');
            }

            type Field {
                required property type -> str;
                multi link versions := .<fields[is Version];
            }
        """

    def test_schema_computed_03(self):
        # Issue #3499
        """
            type Version {
                multi link ordered_fields :=
                    (select .fields order by @position);
                multi link sections :=
                    (select .ordered_fields filter .type = 'section');
                multi link fields -> Field {
                    property position -> int32 {
                        default := 0;
                    }
                }
            }

            type Field {
                required property type -> str;
                multi link versions := .<fields[is Version];
            }
        """

    def test_schema_computed_04(self):
        """
            type User {
                required property name -> str;

                multi link likedPosts := .<author[is PostLike].post;
            }

            type Post {
                required property content -> str;
            }

            abstract type ALike {
                required link author -> User;
            }

            type PostLike extending ALike {
                required link post -> Post;
            }
        """

    def test_schema_computed_05(self):
        """
            type User {
                required property name -> str;

                property val_e := {'alice', 'billie'} except .name;
                property val_i := {'alice', 'billie'} intersect .name;
            }
        """

    def test_schema_alias_01(self):
        """
            type User {
                required property name -> str;
            }

            alias val_e := {'alice', 'billie'} except User.name;
            alias val_i := {'alice', 'billie'} intersect User.name;
        """


class TestGetMigration(tb.BaseSchemaLoadTest):
    """Test migration deparse consistency.

    This tests that schemas produced by `COMMIT MIGRATION foo` and
    by deparsed DDL via `GET MIGRATION foo` are identical.
    """

    std_schema: s_schema.Schema

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.std_schema = tb._load_std_schema()

    def _assert_migration_consistency(
        self,
        schema_text: str,
        multi_module: bool = False,
    ) -> s_schema.Schema:
        if multi_module:
            migration_text = f'''
                START MIGRATION TO {{
                    {schema_text}
                }};
                POPULATE MIGRATION;
                COMMIT MIGRATION;
            '''
        else:
            migration_text = f'''
                START MIGRATION TO {{
                    module default {{
                        {schema_text}
                    }}
                }};
                POPULATE MIGRATION;
                COMMIT MIGRATION;
            '''

        baseline_schema = self.run_ddl(self.schema, migration_text)
        migration = baseline_schema.get_last_migration()
        assert migration is not None

        ddl_text = migration.get_script(baseline_schema)

        try:
            test_schema = self.run_ddl(self.schema, ddl_text)
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
                START MIGRATION TO {{ {sdl_text} }};
                POPULATE MIGRATION;
                COMMIT MIGRATION;
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

        return baseline_schema

    def _assert_migration_equivalence(self, migrations):
        # Compare 2 schemas obtained by multiple-step migration to a
        # single-step migration.

        # Always finish up by migrating to an empty schema
        if migrations[-1].strip():
            migrations = migrations + ['']

        # Generate a base schema with 'test' module already created to
        # avoid having two different instances of 'test' module in
        # different evolution branches.
        base_schema = self.load_schema('')

        # Evolve a schema in a series of migrations.
        multi_migration = base_schema
        for i, state in enumerate(migrations):
            mig_text = f'''
                START MIGRATION TO {{
                    module default {{
                        {state}
                    }}
                }};
                DESCRIBE CURRENT MIGRATION AS JSON;
                POPULATE MIGRATION;
                COMMIT MIGRATION;
            '''

            # Jump to the current schema state directly from base.
            cur_state = self._assert_migration_consistency(state)

            # Perform incremental migration.
            multi_migration = self.run_ddl(multi_migration, mig_text, 'test')

            diff = s_ddl.delta_schemas(multi_migration, cur_state)

            note = ('' if i + 1 < len(migrations)
                    else ' (migrating to empty schema)')

            if list(diff.get_subcommands()):
                self.fail(
                    f'unexpected difference in schema produced by '
                    f'incremental migration on step {i + 1}{note}:\n'
                    f'{markup.dumps(diff)}\n'
                )

    def test_schema_get_migration_01(self):
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

    def test_schema_get_migration_02(self):
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

    def test_schema_get_migration_03(self):
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

            scalar type unit extending enum<ml, g, oz>;

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
            ) -> set of tuple<name: str, quantity: decimal> {
                using (
                    SELECT (
                        name := recipe.ingredients.name,
                        quantity := recipe.ingredients@quantity,
                    )
                )
            }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_04(self):
        # validate that we can trace partial paths
        schema = r'''
            alias X := (SELECT Foo{num := .bar});

            type Foo {
                property bar -> int64;
            };
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_05(self):
        # validate that we can trace partial paths
        schema = r'''
            alias X := (SELECT Foo FILTER .bar > 2);

            type Foo {
                property bar -> int64;
            };
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_06(self):
        # validate that we can trace INTROSPECT
        schema = r'''
            alias X := (SELECT INTROSPECT Foo);

            type Foo;
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_07(self):
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

    def test_schema_get_migration_08(self):
        schema = r'''
            type Bar {
                property data -> str {
                    constraint min_value(10) on (len(<str>__subject__))
                }
            }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_09(self):
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

    def test_schema_get_migration_10(self):
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

    def test_schema_get_migration_11(self):
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

    def test_schema_get_migration_12(self):
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

    def test_schema_get_migration_13(self):
        # validate that we can trace alias declared before type
        schema = r'''
            alias X := (SELECT Foo.name);

            type Foo {
                property name -> str;
            }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_14(self):
        # validate that we can trace alias with DETACHED expr declared
        # before type
        schema = r'''
            alias X := (DETACHED Foo.name);

            type Foo {
                property name -> str;
            }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_15(self):
        schema = r'''
            type Foo {
                property bar -> int64;
                annotation title := 'Foo';
            };
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_16(self):
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

    def test_schema_get_migration_17(self):
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

    def test_schema_get_migration_18(self):
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

    def test_schema_get_migration_19(self):
        # Test abstract and concrete annotations order of declaration.
        schema = r'''
        type Foo {
            property name -> str;
            annotation my_anno := 'Foo';
        }

        abstract annotation my_anno;
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_20(self):
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

    def test_schema_get_migration_21(self):
        # Test index and function order of definition.
        schema = r'''
        type Foo {
            # an index defined before property & function
            index on (idx(.bar));
            property bar -> int64;
        }

        function idx(num: int64) -> bool {
            using (SELECT (num % 2) = 0);
            volatility := 'Immutable';
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_22(self):
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

    def test_schema_get_migration_23(self):
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
            using (SELECT '^^' ++ std::str_upper(val) ++ '^^');
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_24(self):
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
            volatility := 'Immutable';  # needed for the constraint
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_25(self):
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

    def test_schema_get_migration_26(self):
        # Test index issues.
        schema = r'''
        type Dictionary {
            required property name -> str;
            index on (__subject__.name);
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_27(self):
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

    def test_schema_get_migration_28(self):
        # Test standard library dependencies that aren't specifically 'std'.
        schema = r'''
        type Foo {
            required property date -> cal::local_date;
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_29(self):
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

    def test_schema_get_migration_30(self):
        # Test annotated function SDL.
        schema = r'''
        function idx(num: int64) -> bool {
            using (SELECT (num % 2) = 0);
            volatility := 'Immutable';
            annotation title := 'func anno';
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_31(self):
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

    def test_schema_get_migration_32(self):
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

    def test_schema_get_migration_33(self):
        # Make sure that Course is defined before the schedule
        # computable is defined.
        #
        # Issue #1383.
        schema = r'''
        type Class extending HasAvailability {
            multi link schedule :=
                .<class[IS Course].scheduledAt;
        }
        abstract type HasAvailability {
            multi link availableAt -> TimeSpot;
        }
        abstract type HasSchedule {
            multi link scheduledAt -> TimeSpot;
        }
        type Course extending HasSchedule{
            required property name -> str;
            link class -> Class;
        }
        type TimeSpot;
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_34(self):
        # Make sure that awkward order of function definitions doesn't
        # affect the migraiton.
        #
        # Issue #1649.
        schema = r'''
        function b() -> int64 {
            using EdgeQL $$
                SELECT a()
            $$
        }
        function a() -> int64 {
            using EdgeQL $$
                SELECT 1
            $$
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_35(self):
        schema = r'''
            function bar() -> optional str using(
                SELECT <str>Object.id LIMIT 1);
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_36(self):
        # Make sure that awkward order of computables and types
        # extending each other doesn't affect the migraiton.
        #
        # Issue #1941.
        schema = r'''
        type Owner {
            multi link professional_skills := .<user[IS Pencil];
        };
        abstract type Thing {
            required link user -> Owner;
        };
        type Pencil extending Thing;
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_37(self):
        # Make sure that awkward order of computables and types
        # extending each other doesn't affect the migraiton.
        #
        # Issue #1941.
        schema = r'''
        type Owner {
            multi link professional_skills := .<user[IS Pencil];
        };
        abstract type Thing {
            required link user -> Owner;
        };
        type Pencil extending Thing;
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_38(self):
        # Make sure that awkward order of computables and types
        # extending each other doesn't affect the migraiton.
        #
        # Issue #1941.
        schema = r'''
        type Owner {
            multi link professional_skills := .<user[IS Thing][IS Pencil];
        };
        abstract type Thing {
            required link user -> Owner;
        };
        type Pencil extending Thing;
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_39(self):
        # Make sure that awkward order of computables and types
        # extending each other doesn't affect the migraiton.
        #
        # Issue #1941.
        schema = r'''
        type Owner {
            multi link professional_skills := .<user[IS Thing][IS Pencil];
        };
        type Color {
        };
        abstract type Thing {
            required link user -> Owner;
            required link color -> Color;
            required property enabled -> bool;
        };
        type Pencil extending Thing {
            constraint exclusive on ((.user, .color));
        };
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_40(self):
        # Make sure that awkward order of computables and types
        # extending each other doesn't affect the migraiton.
        #
        # Issue #1941.
        schema = r'''
        type Owner {
            multi property notes := .<user[IS Pencil]@note;
        };
        abstract type Thing {
            required link user extending base_user -> Owner;
        };
        type Pencil extending Thing;
        abstract link base_user {
            property note -> str;
        };
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_41(self):
        schema = r'''
        type Base {
            property firstname -> str {
                constraint max_len_value(10);
                # Test that it's illegal to restate the constraint,
                # just like in DDL.
                constraint max_len_value(10);
            }
        }
        '''

        with self.assertRaisesRegex(
                errors.InvalidDefinitionError,
                r'constraint .+ already declared'):
            self._assert_migration_consistency(schema)

    def test_schema_get_migration_42(self):
        schema = r'''
        type Base {
            property firstname -> str {
                constraint max_len_value(10);
            }
        }

        type Derived extending Base {
            overloaded property firstname -> str {
                # Same constraint, but stricter.
                constraint max_len_value(5);
            }
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_43(self):
        schema = r'''
        type Base {
            property firstname -> str {
                constraint max_len_value(10);
            }
        }

        type Derived extending Base {
            overloaded property firstname -> str {
                # Test that it's legal to restate the constraint when
                # overloading.
                constraint max_len_value(10);
            }
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_44(self):
        schema = r'''
        type Foo {
            property val -> str;
            property comp := count((
                # Use an alias in WITH block in a computable
                WITH x := .val
                # Use an alias in SELECT in a computable
                SELECT y := Bar FILTER x = y.val
            ))
        }

        type Bar {
            property val -> str;
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_45(self):
        # Need to make sure that usage of ad-hoc aliases doesn't mask
        # the real error.
        with self.assertRaisesRegex(
            errors.SchemaDefinitionError,
            "mutations are invalid in computed property 'comp'"
        ):
            schema = r'''
            type Foo {
                property val -> str;
                property comp := count((
                    # Use an alias in WITH block in a computable
                    WITH x := .val
                    UPDATE Bar FILTER x = Bar.val
                    SET {
                        val := 'foo'
                    }
                ))
            }

            type Bar {
                property val -> str;
            }
            '''

            self._assert_migration_consistency(schema)

    def test_schema_get_migration_46(self):
        # Need to make sure that usage of ad-hoc aliases doesn't mask
        # the real error.
        with self.assertRaisesRegex(
            errors.SchemaDefinitionError,
            "mutations are invalid in computed property 'comp'"
        ):
            schema = r'''
            type Foo {
                property val -> str;
                property comp := count((
                    # Use an alias in WITH block in a computable
                    WITH x := .val
                    DELETE Bar FILTER x = Bar.val
                ))
            }

            type Bar {
                property val -> str;
            }
            '''

            self._assert_migration_consistency(schema)

    def test_schema_get_migration_47(self):
        schema = r'''
        type Bar {
            property val -> str;
        }

        alias Foo := (
            # Use an alias in WITH block in a computable
            WITH x := Bar.val ++ 'q'
            # Use an alias in SELECT in a computable
            SELECT y := Bar FILTER x = y.val
        );
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_48(self):
        schema = r'''
        type Bar {
            property val -> str;
        }

        function foo() -> int64 using (
            count((
                # Use an alias in WITH block in a computable
                WITH x := Bar.val ++ 'q'
                # Use an alias in SELECT in a computable
                SELECT y := Bar FILTER x = y.val
            ))
        );
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_49(self):
        schema = r'''
        type Base {
            property val := 'ok';
        }

        type Gen1 extending Base;
        type Gen2 extending Gen1;
        type Gen3 extending Gen2 {
            property derived_val := .val ++ '!';
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_50(self):
        schema = r'''
        type Base {
            property val := 'ok';
        }

        type Gen1Lin1 extending Base;
        type Gen2Lin1 extending Gen1Lin1;

        type Gen1Lin2 extending Base;
        type Gen2Lin2 extending Gen1Lin2;

        # Diamond inheritance for the .val
        type Gen3 extending Gen2Lin1, Gen2Lin2 {
            property aliased_val3 := .val;
        }
        # Alias of an alias.
        type Gen4 extending Gen3 {
            property aliased_val4 := .aliased_val3;
        }
        # Derive a computable from alias.
        type Gen5 extending Gen4 {
            property derived_val := .aliased_val4 ++ '!';
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_51(self):
        schema = r'''
        abstract type Version {
            required link entity -> Versioned;
        }

        abstract type Barks;
        abstract type Versioned;

        type DogVersion extending Version, Barks {
            overloaded required link entity -> Dog;
        }

        type Dog extending Versioned {
            multi link versions := (SELECT Dog.<entity[IS DogVersion]);
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_52(self):
        schema = r'''
        abstract link friendship {
            property strength -> float64;
            index on (@strength);
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_53(self):
        schema = r'''
            scalar type ClipType extending enum<Test1, Test2>;
            scalar type RecordingType extending enum<Test3, Test4>;

            type LiveClass {
              link _clips := .<_class[is Clip];
              link _recordings := .<_class[is Recording];
            }

            type Clip {
              required property _type -> ClipType;
              link _class -> LiveClass;
            }

            type Recording {
              required property _type -> RecordingType;
              link _class -> LiveClass;
            }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_multi_module_01(self):
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

    def test_schema_get_migration_multi_module_02(self):
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

    def test_schema_get_migration_multi_module_03(self):
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

    def test_schema_get_migration_multi_module_04(self):
        # View and type from different modules
        schema = r'''
            alias default::X := (SELECT other::Foo.name);

            type other::Foo {
                property name -> str;
            }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_schema_get_migration_multi_module_05(self):
        # View and type from different modules
        schema = r'''
            alias default::X := (SELECT other::Foo FILTER .name > 'a');

            type other::Foo {
                property name -> str;
            }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_schema_get_migration_multi_module_06(self):
        # Type and annotation from different modules.
        schema = r'''
        type default::Foo {
            property name -> str;
            annotation other::my_anno := 'Foo';
        }

        abstract annotation other::my_anno;
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_schema_get_migration_multi_module_07(self):
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

    def test_schema_get_migration_multi_module_08(self):
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

    def test_schema_get_migration_multi_module_09(self):
        schema = r'''
        type default::Foo {
            property bar -> int64;
            # an index
            index on (other::idx(.bar));
        }

        function other::idx(num: int64) -> bool {
            using (SELECT (num % 2) = 0);
            volatility := 'Immutable';
        }
        '''

        self._assert_migration_consistency(schema, multi_module=True)

    def test_schema_get_migration_multi_module_10(self):
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

    def test_schema_get_migration_multi_module_11(self):
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

    def test_schema_get_migration_multi_module_12(self):
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

    def test_schema_get_migration_default_ptrs_01(self):
        schema = r'''
        type Foo {
            property name {
                using (1);
                annotation title := "foo";
            };
            link everything {
                using (Object);
                annotation title := "bar";
            };
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_union_ptrs_01(self):
        schema = r'''
        abstract type Entity {
            link parent -> Entity;
        };
        type BaseCourse extending Entity {}
        type Unit extending Entity {
             overloaded link parent -> Unit | BaseCourse;
        }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_except_01(self):
        schema = r'''
        type ExceptTest {
            constraint exclusive on (.name) except (.deleted);
            required property name -> str;
            property deleted -> bool;
        };
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_except_02(self):
        schema = r'''
        type ExceptTest {
            index on (.name) except (.deleted);
            required property name -> str;
            property deleted -> bool;
        };
        '''

        self._assert_migration_consistency(schema)

    def test_schema_access_policy_parens_01(self):
        schema = r'''
        type Foo {
            access policy test
              when (with y := 1, select y = 1)
              allow all
              using (with x := 1, select x = 1)
        };
        '''

        self._assert_migration_consistency(schema)

    def test_schema_access_policy_parens_02(self):
        schema = r'''
        type Foo {
            access policy test
              when ((with y := 1, select y = 1))
              allow all
              using ((with x := 1, select x = 1))
        };
        '''

        self._assert_migration_consistency(schema)

    def test_schema_migrations_equivalence_01(self):
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

    def test_schema_migrations_equivalence_02(self):
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

    def test_schema_migrations_equivalence_03(self):
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

    def test_schema_migrations_equivalence_04(self):
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

    def test_schema_migrations_equivalence_05(self):
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

    def test_schema_migrations_equivalence_06(self):
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
                property foo -> int32;
            }

            type Derived extending Base {
                overloaded required property foo -> int32;
            }
        """])

    def test_schema_migrations_equivalence_07(self):
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

    def test_schema_migrations_equivalence_08(self):
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

    def test_schema_migrations_equivalence_09(self):
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

    def test_schema_migrations_equivalence_10(self):
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

    def test_schema_migrations_equivalence_11(self):
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

    def test_schema_migrations_equivalence_12(self):
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

    def test_schema_migrations_equivalence_13(self):
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

    def test_schema_migrations_equivalence_14(self):
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

    def test_schema_migrations_equivalence_15(self):
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

    def test_schema_migrations_equivalence_16(self):
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

    def test_schema_migrations_equivalence_17(self):
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

    def test_schema_migrations_equivalence_18(self):
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

    def test_schema_migrations_equivalence_19(self):
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

    def test_schema_migrations_equivalence_20(self):
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

    def test_schema_migrations_equivalence_21(self):
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

    def test_schema_migrations_equivalence_22(self):
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

    @test.xfail('''
        This wants to transmute an object type into an alias. It
        produces DDL, but the DDL doesn't really make any sense. We
        are going to probably need to add DDL syntax to accomplish
        this.
    ''')
    def test_schema_migrations_equivalence_23(self):
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

    def test_schema_migrations_equivalence_24(self):
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

    def test_schema_migrations_equivalence_26(self):
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
            type GenericChild;

            type Child extending GenericChild;

            type GenericParent {
                link bar -> GenericChild;
            }

            type Parent extending GenericParent {
                overloaded link bar -> Child;
            }

        """])

    def test_schema_migrations_equivalence_27(self):
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

    def test_schema_migrations_equivalence_28(self):
        self._assert_migration_equivalence([r"""
            type Child {
                property foo -> str;
            }
        """, r"""
            # drop everything
        """])

    def test_schema_migrations_equivalence_29(self):
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

    def test_schema_migrations_equivalence_30(self):
        # This is the inverse of the test_schema_migrations_equivalence_27
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

    def test_schema_migrations_equivalence_31(self):
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

    def test_schema_migrations_equivalence_32(self):
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

    def test_schema_migrations_equivalence_34(self):
        # this is the reverse of test_schema_migrations_equivalence_11
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

    def test_schema_migrations_equivalence_35(self):
        self._assert_migration_equivalence([r"""
            type Child {
                required property name -> str;
            }

            type Base {
                multi link foo := (
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
        """, r"""
        """])

    def test_schema_migrations_equivalence_36(self):
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
                multi link foo := (
                    SELECT Child FILTER .name = 'computable_36'
                )
            }
        """])

    def test_schema_migrations_equivalence_37(self):
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

    def test_schema_migrations_equivalence_38(self):
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

    def test_schema_migrations_equivalence_39(self):
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

    def test_schema_migrations_equivalence_40(self):
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

    def test_schema_migrations_equivalence_41(self):
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

    def test_schema_migrations_equivalence_42(self):
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

    def test_schema_migrations_equivalence_43(self):
        # change a prop used in a computable
        self._assert_migration_equivalence([r"""
            type Foo {
                property val -> int64;
                property comp := .val + 2;
            };
        """, r"""
            type Foo {
                property val -> float64;
                property comp := .val + 2;
            };
        """])

    def test_schema_migrations_equivalence_44(self):
        # change a link used in a computable
        self._assert_migration_equivalence([r"""
            type Action {
                required link user -> User;
            };
            type Post extending Action;
            type User {
                multi link actions := .<user[IS Post];
            };

            alias UserAlias := User {
                action_ids := .actions.id
            };
        """, r"""
            type Action {
                required link user -> User;
            };
            type Post extending Action;
            type User {
                multi link actions := .<user[IS Action];
            };

            alias UserAlias := User {
                action_ids := .actions.id
            };
        """])

    def test_schema_migrations_equivalence_45(self):
        # change a link used in a computable
        self._assert_migration_equivalence([r"""
            type Action {
                required link user -> User;
            };
            type Post extending Action;
            type User {
                multi link actions := .<user[IS Post];
            };

            alias UserAlias := User {
                action_ids := .actions.id
            };
        """, r"""
            type Action {
                required link user -> User;
            };
            type Post extending Action;
            type User {
                multi link actions := .<user[IS Post];
                # Similar to previous test, but with an intermediate
                # step. Separating addition of a new computable and
                # then swapping the old one for the new one.
                #
                # Basically, we model a situation where some kind of
                # link "actions" has to exist throughout the entire
                # process (part of some interface that cannot be
                # easily changed maybe).
                multi link new_actions := .<user[IS Action];
            };

            alias UserAlias := User {
                action_ids := .actions.id,
                new_action_ids := .new_actions.id,
            };
        """, r"""
            type Action {
                required link user -> User;
            };
            type Post extending Action;
            type User {
                multi link actions := .<user[IS Action];
            };

            alias UserAlias := User {
                action_ids := .actions.id
            };
        """])

    def test_schema_migrations_equivalence_46(self):
        # change a link used in a computable
        self._assert_migration_equivalence([r"""
            type Action;
            type Post extending Action {
                required link user -> User;
            };
            type User {
                multi link actions := .<user[IS Post]
            };

            alias UserAlias := User {
                action_ids := .actions.id
            };
        """, r"""
            type Action {
                required link user -> User;
            };
            type Post extending Action;
            type User {
                multi link actions := .<user[IS Action]
            };

            alias UserAlias := User {
                action_ids := .actions.id
            };
        """])

    def test_schema_migrations_equivalence_47(self):
        # change a link used in a computable
        self._assert_migration_equivalence([r"""
            type Action {
                required link user -> User;
            };
            type Post extending Action;
            type User {
                multi link actions := .<user[IS Action]
            };
        """, r"""
            type Action;
            type Post extending Action {
                required link user -> User;
            };
            type User {
                multi link actions := .<user[IS Post]
            };
        """])

    def test_schema_migrations_equivalence_48(self):
        # change a link used in a computable
        self._assert_migration_equivalence([r"""
            type Action {
                required property name -> str;
            };
            type Post {
                required property stamp -> datetime;
                required link user -> User;
            };
            type User {
                multi link owned := .<user[IS Post]
            };
        """, r"""
            type Action {
                required property name -> str;
                required link user -> User;
            };
            type Post {
                required property stamp -> datetime;
            };
            type User {
                multi link owned := .<user[IS Action]
            };
        """])

    def test_schema_migrations_equivalence_49(self):
        self._assert_migration_equivalence([r"""
            type Foo {
                link bars := .<foo[IS Bar];
                link spam := .<foo[IS Spam];
            };

            type Bar {
                link foo -> Foo;
            };

            type Spam {
                link foo -> Foo;
            };
        """, r"""
            type Foo {
                link spam := .<foo[IS Spam];
            };

            type Spam {
                link foo -> Foo;
            };
        """])

    def test_schema_migrations_equivalence_50(self):
        self._assert_migration_equivalence([r"""
            type User {
                required property name -> str {
                    constraint exclusive;
                };
                index on (__subject__.name);
            };
        """, r"""
            type User extending Named;

            abstract type Named {
                required property name -> str {
                    constraint exclusive;
                };
                index on (__subject__.name);
            };
        """])

    def test_schema_migrations_equivalence_51(self):
        self._assert_migration_equivalence([r"""
            abstract type Text;
            abstract type Owned;
            type Comment extending Text, Owned;
        """, r"""
        """])

    def test_schema_migrations_equivalence_52(self):
        self._assert_migration_equivalence([r"""
            scalar type Slug extending str;
            type User {
                required property name -> str;
            };
        """, r"""
            scalar type Slug extending str;
            abstract type Named {
                required property name -> Slug;
            };
            type User extending Named;
        """])

    def test_schema_migrations_equivalence_53(self):
        self._assert_migration_equivalence([r"""
            scalar type Slug extending str;
            abstract type Named {
                required property name -> Slug;
            };
            type User {
                required property name -> str;
            };
        """, r"""
            scalar type Slug extending str;
            abstract type Named {
                required property name -> Slug;
            };
            type User extending Named {
                property foo -> str;
            }
        """])

    def test_schema_migrations_equivalence_54(self):
        self._assert_migration_equivalence([r"""
            type User {
                  required property name -> str;
                  index on (__subject__.name);
            };
        """, r"""
            scalar type Slug extending str;
            abstract type Named {
                required property name -> Slug;
                index on (__subject__.name);
            };
            type User extending Named;
        """])

    def test_schema_migrations_equivalence_55(self):
        self._assert_migration_equivalence([r"""
            type User {
                  required property name -> str;
                  property asdf := .name ++ "!";
            };
        """, r"""
            scalar type Slug extending str;
            abstract type Named {
                required property name -> Slug;
                index on (__subject__.name);
            };
            type User extending Named {
                  property asdf := .name ++ "!";
            }
        """])

    def test_schema_migrations_equivalence_56a(self):
        self._assert_migration_equivalence([r"""
            type User {
                required property name -> str;
            };

            alias TwoUsers := (
                select User {
                    initial := .name[0],
                } order by .name limit 2
            );
        """])

    def test_schema_migrations_equivalence_56b(self):
        self._assert_migration_equivalence([r"""
            type User {
                required property name -> str;
            };

            global TwoUsers := (
                select User {
                    initial := .name[0],
                } order by .name limit 2
            );
        """])

    def test_schema_migrations_equivalence_57a(self):
        self._assert_migration_equivalence([r"""
            type User {
                required property name -> str;
            };

            alias TwoUsers := (
                select User {
                    initial := .name[0],
                } order by .name limit 2
            );
        """, r"""
            type User {
                required property name -> str;
            };

            alias TwoUsers := (User);
        """])

    def test_schema_migrations_equivalence_57b(self):
        self._assert_migration_equivalence([r"""
            type User {
                required property name -> str;
            };

            global TwoUsers := (
                select User {
                    initial := .name[0],
                } order by .name limit 2
            );
        """, r"""
            type User {
                required property name -> str;
            };

            global TwoUsers := (User);
        """])

    def test_schema_migrations_equivalence_58(self):
        self._assert_migration_equivalence([r"""
            abstract type C {
                link x -> E {
                    constraint exclusive;
                }
            }

            abstract type A;

            type B extending A;

            type D extending C, A;

            type E extending A {
                link y := assert_single(.<`x`[IS C]);
            }
        """, r"""
        """])

    def test_schema_migrations_equivalence_59(self):
        self._assert_migration_equivalence([r"""
            type User {
                required property name -> str;
                index pg::spgist on (.name);
            };
        """, r"""
            type User {
                required property name -> str;
                index pg::spgist on (.name) {
                    annotation description := 'test';
                };
            };
        """, r"""
            type User {
                required property name -> str;
            };
        """])

    def test_schema_migrations_equivalence_60(self):
        self._assert_migration_equivalence([r"""
            type User {
                required property name -> str;
            };
        """, r"""
            type User {
                required property name -> str;
                index pg::spgist on (.name);
            };
        """, r"""
            type User {
                required property name -> str;
                index pg::spgist on (.name) {
                    annotation description := 'test';
                };
            };
        """])

    def test_schema_migrations_equivalence_compound_01(self):
        # Check that union types can be referenced in computables
        # Bug #2002.
        self._assert_migration_equivalence([r"""
            type Type1;
            type Type2;
            type Type3 {
                link l1 -> (Type1 | Type2);
                link l2 := (SELECT .l1);
            };
        """, r"""
            type Type11;  # Rename
            type Type2;
            type Type3 {
                link l1 -> (Type11 | Type2);
                link l2 := (SELECT .l1);
            };
        """, r"""
            type Type11;
            type Type2;
            type TypeS;
            type Type3 {
                link l1 -> (Type11 | Type2 | TypeS);  # Expand union
                link l2 := (SELECT .l1);
            };
        """, r"""
        """])

    def test_schema_migrations_equivalence_function_01(self):
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

    def test_schema_migrations_equivalence_function_04(self):
        self._assert_migration_equivalence([r"""
            function foo() -> str USING ('foo');
        """, r"""
            function foo() -> str USING ('bar');
        """])

    def test_schema_migrations_equivalence_function_06(self):
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

    def test_schema_migrations_equivalence_function_10(self):
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

    def test_schema_migrations_equivalence_function_11(self):
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

    def test_schema_migrations_equivalence_function_12(self):
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

    def test_schema_migrations_equivalence_function_13(self):
        # this is the inverse of test_schema_migrations_equivalence_function_12
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

    def test_schema_migrations_equivalence_function_14(self):
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

    def test_schema_migrations_equivalence_function_15(self):
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

    def test_schema_migrations_equivalence_function_16(self):
        # change prop type without changing the affected function.
        self._assert_migration_equivalence([r"""
            type Foo {
                property bar -> array<int64>;
            };

            function hello16() -> optional int64
                using (
                    SELECT len((SELECT Foo LIMIT 1).bar)
                )
        """, r"""
            type Foo {
                property bar -> array<float64>;
            };

            function hello16() -> optional int64
                using (
                    SELECT len((SELECT Foo LIMIT 1).bar)
                )
        """])

    def test_schema_migrations_equivalence_function_17(self):
        # change prop type without changing the affected function.
        self._assert_migration_equivalence([r"""
            type Foo {
                property bar -> array<int64>;
            };

            type Bar;

            function hello17() -> optional Bar
                using (
                    SELECT Bar
                    OFFSET len((SELECT Foo.bar LIMIT 1)) ?? 0
                    LIMIT 1
                )
        """, r"""
            type Foo {
                property bar -> array<float64>;
            };

            type Bar;

            function hello17() -> optional Bar
                using (
                    SELECT Bar
                    OFFSET len((SELECT Foo.bar LIMIT 1)) ?? 0
                    LIMIT 1
                )
        """])

    def test_schema_migrations_equivalence_function_18(self):
        self._assert_migration_equivalence([r"""
            function a() -> float64 {
                using (
                    SELECT random()
                )
            }
        """, r"""
            function a() -> float64 {
                volatility := "volatile";
                using (
                    SELECT random()
                )
            }
        """])

    def test_schema_migrations_equivalence_function_19(self):
        self._assert_migration_equivalence([r"""
            function a() -> float64 {
                volatility := "volatile";
                using (
                    SELECT random()
                )
            }
        """, r"""
            function a() -> float64 {
                using (
                    SELECT random()
                )
            }
        """])

    def test_schema_migrations_equivalence_function_20(self):
        self._assert_migration_equivalence([r"""
            function a() -> float64 {
                volatility := "volatile";
                using (
                    SELECT 1.0
                )
            }
        """, r"""
            function a() -> float64 {
                using (
                    SELECT 1.0
                )
            }
        """])

    def test_schema_migrations_equivalence_function_21(self):
        self._assert_migration_equivalence([r"""
            function foo(variadic s: str) -> str using ("!");
        """, r"""
            function foo(variadic s: str) -> str using ("?");
        """])

    def test_schema_migrations_equivalence_recursive_01(self):
        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            "property 'val' of object type 'default::Foo' "
            "is defined recursively"
        ):
            self._assert_migration_equivalence([r"""
                type Foo {
                    link next -> Foo;
                    property val := 1;
                }
            """, r"""
                type Foo {
                    link next -> Foo;
                    property val := 1 + (.next.val ?? 0);
                }
            """])

    def test_schema_migrations_equivalence_recursive_02(self):
        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            "definition dependency cycle between "
            "property 'val' of object type 'default::Bar' and "
            "property 'val' of object type 'default::Foo'"
        ):
            self._assert_migration_equivalence([r"""
                type Foo {
                    link next -> Bar;
                    property val := 1;
                }

                type Bar {
                    link next -> Foo;
                    property val := 1;
                }
            """, r"""
                type Foo {
                    link next -> Bar;
                    property val := 1 + (.next.val ?? 0);
                }

                type Bar {
                    link next -> Foo;
                    property val := 1;
                }
            """, r"""
                type Foo {
                    link next -> Bar;
                    property val := 1 + (.next.val ?? 0);
                }

                type Bar {
                    link next -> Foo;
                    property val := 1 + (.next.val ?? 0);
                }
            """])

    def test_schema_migrations_equivalence_recursive_03(self):
        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            r"function 'default::foo\(v: int64\)' "
            r"is defined recursively"
        ):
            self._assert_migration_equivalence([r"""
                function foo(v: int64) -> int64 using (
                    1 + v
                );
            """, r"""
                function foo(v: int64) -> int64 using (
                    0 IF v < 0 ELSE 1 + foo(v -1)
                );
            """])

    def test_schema_migrations_equivalence_recursive_04(self):
        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            r"definition dependency cycle between "
            r"function 'default::bar\(v: int64\)' and "
            r"function 'default::foo\(v: int64\)'"
        ):
            self._assert_migration_equivalence([r"""
                function foo(v: int64) -> int64 using (
                    1 + v
                );

                function bar(v: int64) -> int64 using (
                    0 IF v < 0 ELSE 1 + foo(v -1)
                );
            """, r"""
                function foo(v: int64) -> int64 using (
                    0 IF v < 0 ELSE 1 + bar(v -1)
                );

                function bar(v: int64) -> int64 using (
                    0 IF v < 0 ELSE 1 + foo(v -1)
                );
            """])

    def test_schema_migrations_equivalence_recursive_05(self):
        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            r"definition dependency cycle between "
            r"function 'default::foo\(v: int64\)' and "
            r"property 'val' of object type 'default::Foo'"
        ):
            self._assert_migration_equivalence([r"""
                type Foo {
                    property val := foo(1);
                }

                function foo(v: int64) -> int64 using (
                    1 + v
                );
            """, r"""
                type Foo {
                    property val := foo(1);
                }

                function foo(v: int64) -> int64 using (
                    # This is very broken now
                    1 + (SELECT Foo LIMIT 1).val
                );
            """])

    def test_schema_migrations_equivalence_recursive_06(self):
        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            r"definition dependency cycle between "
            r"function 'default::foo\(v: int64\)' and "
            r"property 'val' of object type 'default::Foo'"
        ):
            self._assert_migration_equivalence([r"""
                type Foo {
                    property val := 1;
                }

                function foo(v: int64) -> optional int64 using (
                    1 + (SELECT Foo LIMIT 1).val
                );
            """, r"""
                type Foo {
                    # This is very broken now
                    property val := foo(1);
                }

                function foo(v: int64) -> optional int64 using (
                    1 + (SELECT Foo LIMIT 1).val
                );
            """])

    def test_schema_migrations_equivalence_recursive_07(self):
        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            "definition dependency cycle between "
            "alias 'default::FooAlias1' and alias 'default::FooAlias0'"
        ):
            self._assert_migration_equivalence([r"""
                type Foo {
                    property val -> int64;
                }

                alias FooAlias0 := Foo {
                    comp := .val + (SELECT FooAlias1 LIMIT 1).comp
                };

                alias FooAlias1 := Foo {
                    comp := .val + 1
                };
            """, r"""
                type Foo {
                    property val -> int64;
                }

                alias FooAlias0 := Foo {
                    comp := .val + (SELECT FooAlias1 LIMIT 1).comp
                };

                alias FooAlias1 := Foo {
                    comp := .val + (SELECT FooAlias0 LIMIT 1).comp
                };
            """])

    def test_schema_migrations_equivalence_recursive_08(self):
        with self.assertRaisesRegex(
            errors.InvalidDefinitionError,
            "'default::Foo' is defined recursively"
        ):
            self._assert_migration_equivalence([r"""
                type Foo;

                type Bar extending Foo;
            """, r"""
                type Foo extending Bar;

                type Bar extending Foo;
            """])

    def test_schema_migrations_equivalence_computed_01(self):
        self._assert_migration_equivalence([r"""
            type Foo {
                property x := 10;
            };
        """, r"""
            type Foo {
                single property x := 10;
            };
        """, r"""
            type Foo {
                multi property x := 10;
            };
        """])

    def test_schema_migrations_equivalence_computed_02(self):
        self._assert_migration_equivalence([r"""
            type Foo {
                single property x := 10;
            };
        """, r"""
            type Foo {
                property x := 10;
            };
        """, r"""
            type Foo {
                multi property x := 10;
            };
        """])

    def test_schema_migrations_equivalence_linkprops_03(self):
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
                    property bar -> int32
                }
            };
        """])

    def test_schema_migrations_equivalence_linkprops_07(self):
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

    def test_schema_migrations_equivalence_linkprops_08(self):
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
        """, r"""
        """])

    def test_schema_migrations_equivalence_linkprops_09(self):
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

    def test_schema_migrations_equivalence_linkprops_10(self):
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
        """, r"""
        """])

    def test_schema_migrations_equivalence_linkprops_11(self):
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

    def test_schema_migrations_equivalence_linkprops_12(self):
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

    def test_schema_migrations_equivalence_linkprops_13(self):
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
                link child -> Child {
                    property foo -> str
                }
            };

            type Derived extending Base;
        """, r"""
        """])

    def test_schema_migrations_equivalence_linkprops_14(self):
        self._assert_migration_equivalence([r"""
            abstract link link_with_value {
                single property value -> int64;
                index on (__subject__@value);
            }
            type Tgt;
            type Foo {
                link l1 extending link_with_value -> Tgt;
                link l2 -> Tgt {
                    property value -> int64;
                    index on (__subject__@value);
                    index on ((__subject__@target, __subject__@value));
                };
            };
        """, r"""
            abstract link link_with_value {
                single property value -> int64;
                index on (__subject__@value);
                index on ((__subject__@target, __subject__@value));
            }
            type Tgt;
            type Foo {
                link l1 extending link_with_value -> Tgt;
                link l2 -> Tgt {
                    property value -> int64;
                    index on (__subject__@value) {
                        annotation title := "value!";
                    }
                };
            };
        """])

    def test_schema_migrations_equivalence_annotation_01(self):
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

    def test_schema_migrations_equivalence_annotation_02(self):
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

    def test_schema_migrations_equivalence_annotation_03(self):
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

    def test_schema_migrations_equivalence_annotation_04(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            abstract inheritable annotation bar_anno;

            type Base {
                annotation bar_anno := 'Base bar_anno 04';
            }

            type Derived extending Base;
        """, r"""
            # rename bar_anno -> foo_anno
            abstract inheritable annotation foo_anno;

            type Base {
                annotation foo_anno := 'Base bar_anno 04';
            }

            type Derived extending Base;
        """])

    def test_schema_migrations_equivalence_annotation_05(self):
        self._assert_migration_equivalence([r"""
            abstract inheritable annotation my_anno;

            type Base {
                property my_prop -> str {
                    annotation my_anno := 'Base my_anno 05';
                }
            }

            type Derived extending Base {
                overloaded property my_prop -> str {
                    annotation my_anno := 'Derived my_anno 05';
                }
            }
        """, r"""
            # rename annotated & inherited property
            abstract inheritable annotation my_anno;

            type Base {
                property renamed_prop -> str {
                    annotation my_anno := 'Base my_anno 05';
                }
            }

            type Derived extending Base {
                overloaded property renamed_prop -> str {
                    annotation my_anno := 'Derived my_anno 05';
                }
            }
        """, r"""
        """])

    def test_schema_migrations_equivalence_annotation_06(self):
        self._assert_migration_equivalence([r"""
            abstract inheritable annotation my_anno;

            type Base {
                link my_link -> Object {
                    annotation my_anno := 'Base my_anno 06';
                }
            }

            type Derived extending Base {
                overloaded link my_link -> Object {
                    annotation my_anno := 'Derived my_anno 06';
                }
            }
        """, r"""
            # rename annotated & inherited link
            abstract inheritable annotation my_anno;

            type Base {
                link renamed_link -> Object {
                    annotation my_anno := 'Base my_anno 06';
                }
            }

            type Derived extending Base {
                overloaded link renamed_link -> Object {
                    annotation my_anno := 'Derived my_anno 06';
                }
            }
        """, r"""
        """])

    def test_schema_migrations_equivalence_annotation_07(self):
        self._assert_migration_equivalence([r"""
            abstract inheritable annotation my_anno;

            type Base {
                link my_link -> Object {
                    annotation my_anno := 'Base my_anno 06';
                }
            }

            type Derived extending Base {
                overloaded link my_link -> Object {
                    annotation my_anno := 'Derived my_anno 06';
                }
            }
        """, r"""
            abstract inheritable annotation my_anno;

            type Base {
                link my_link -> Object {
                    annotation my_anno := 'Base my_anno 06';
                }
            }

            type Derived extending Base;
        """, r"""
        """])

    def test_schema_migrations_equivalence_annotation_08(self):
        self._assert_migration_equivalence([r"""
            abstract annotation ann1;
            type T {
                annotation ann1 := 'test!';
            };
        """, r"""
            abstract annotation ann2;
            type T {
                annotation ann2 := 'test?';
            };
        """])

    def test_schema_migrations_equivalence_index_01(self):
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

    def test_schema_migrations_equivalence_index_02(self):
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

    def test_schema_migrations_equivalence_index_03(self):
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
                property name -> int32;
                index on (.name);
            }
        """])

    def test_schema_migrations_equivalence_index_04(self):
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
                index fts::textsearch(language := 'english') on (.name);
            }
        """])

    def test_schema_migrations_equivalence_index_05(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property first_name -> str;
                index on (.first_name);
            }
        """, r"""
            type Base {
                property first_name -> str;
                index on (.first_name) {
                    # add annotation
                    annotation title := 'index on first name';
                }
            }
        """, r"""
            type Base {
                property first_name -> str;
                # drop index
            }
        """])

    def test_schema_migrations_equivalence_constraint_01(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property first_name -> str {
                    constraint max_len_value(10)
                }
            }
        """, r"""
            type Base {
                property first_name -> str {
                    constraint max_len_value(10) {
                        # add annotation
                        annotation title := 'constraint on first name';
                    }
                }
            }
        """, r"""
            type Base {
                property first_name -> str;
                # drop constraint
            }
        """])

    def test_schema_migrations_equivalence_constraint_02(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property firstname -> str {
                    constraint max_len_value(10)
                }
            }

            type Derived extending Base;
        """, r"""
            # rename constrained & inherited property
            type Base {
                property first_name -> str {
                    constraint max_len_value(10)
                }
            }

            type Derived extending Base;
        """])

    def test_schema_migrations_equivalence_constraint_03(self):
        self._assert_migration_equivalence([r"""
            abstract constraint Lol { using (__subject__ < 10) };
            type Foo {
                property x -> int64 {
                    constraint Lol;
                }
            }
            type Bar extending Foo;

        """, r"""
            abstract constraint Lolol { using (__subject__ < 10) };
            type Foo {
                property x -> int64 {
                    constraint Lolol;
                }
            }
            type Bar extending Foo;
        """])

    def test_schema_migrations_equivalence_constraint_04(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property firstname -> str {
                    constraint max_len_value(10);
                }
            }

            type Derived extending Base {
                overloaded property firstname -> str {
                    # add another constraint to make the prop overloaded
                    constraint min_len_value(5);
                }
            }
        """, r"""
            # rename constrained & inherited property
            type Base {
                property first_name -> str {
                    constraint max_len_value(10);
                }
            }

            type Derived extending Base {
                overloaded property first_name -> str {
                    constraint min_len_value(5);
                }
            }
        """, r"""
        """])

    def test_schema_migrations_equivalence_constraint_05(self):
        self._assert_migration_equivalence([r"""
            abstract constraint not_bad {
                using (__subject__ != "bad" and __subject__ != "terrible")
            }

            type Foo {
                property x -> str {
                    constraint not_bad;
                }
            }
            type Bar extending Foo;
        """, r"""
            abstract constraint not_bad {
                using (__subject__ != "bad" and __subject__ != "awful")
            }

            type Foo {
                property x -> str {
                    constraint not_bad;
                }
            }
            type Bar extending Foo;
        """])

    def test_schema_migrations_equivalence_constraint_06(self):
        self._assert_migration_equivalence([r"""
            type Cell {
                link right -> Cell;
                # `left` is inferred to be multi
                link left := .<right[IS Cell];
            }
        """, r"""
            type Cell {
                link right -> Cell {
                    # Add the constraint to make it 1-1
                    constraint exclusive;
                }
                # This should now be inferred as single
                link left := .<right[IS Cell];
            }
        """])

    def test_schema_migrations_equivalence_constraint_07(self):
        self._assert_migration_equivalence([r"""
            type Cell {
                link right -> Cell;
                # `left` is inferred to be multi
                link left := .<right[IS Cell];
            }
        """, r"""
            type Cell {
                link right -> Cell {
                    # Add the constraint to make it 1-1
                    constraint exclusive;
                }
                # Explicitly single link
                single link left := .<right[IS Cell];
            }
        """])

    def test_schema_migrations_equivalence_constraint_08(self):
        self._assert_migration_equivalence([r"""
            type Cell {
                link right -> Cell;
                multi link left := .<right[IS Cell];
            }
        """, r"""
            type Cell {
                link right -> Cell {
                    # Add the constraint to make it 1-1
                    constraint exclusive;
                }
                multi link left := .<right[IS Cell];
            }
        """, r"""
            type Cell {
                link right -> Cell {
                    constraint exclusive;
                }
                # Now switch to a single link
                single link left := .<right[IS Cell];
            }
        """])

    def test_schema_migrations_equivalence_constraint_09(self):
        self._assert_migration_equivalence([r"""
            type Cell {
                link right -> Cell {
                    # Add the constraint to make it 1-1
                    constraint exclusive;
                }
                # Explicitly single link
                single link left := .<right[IS Cell];
            }
        """, r"""
            type Cell {
                link right -> Cell;
                # Explicitly multi link
                multi link left := .<right[IS Cell];
            }
        """])

    def test_schema_migrations_equivalence_constraint_10(self):
        self._assert_migration_equivalence([r"""
            type Cell {
                link right -> Cell {
                    # Add the constraint to make it 1-1
                    constraint exclusive;
                }
                link left := .<right[IS Cell];
            }
        """, r"""
            type Cell {
                link right -> Cell;
                link left := .<right[IS Cell];
            }
        """])

    def test_schema_migrations_equivalence_policies_01(self):
        self._assert_migration_equivalence([r"""
            type X {
                required property x -> str;
                access policy test
                    allow all using (.x not like '%redacted%');
            };
        """, r"""
            type X {
                required property x -> str;
                access policy asdf
                    allow all using (.x not like '%redacted%');
            };
        """, r"""
            type X {
                required property x -> str;
                access policy asdf
                    when (true)
                    allow all using (.x not like '%redacted%');
            };
        """])

    def test_schema_migrations_equivalence_policies_02(self):
        self._assert_migration_equivalence([r"""
            type Foo {
                access policy asdf
                allow all using ((select Bar filter .name = 'X').b ?? false);
            }
            type Bar {
                required property name -> str;
                property b -> bool;
                   constraint exclusive on (.name);
            };
        """])

    def test_schema_migrations_equivalence_policies_03(self):
        self._assert_migration_equivalence([r"""
            type Foo {
                access policy asdf
                allow all using (true);
            }
        """, """
            type Foo {
                access policy asdf
                allow all;
            }
        """])

    def test_schema_migrations_equivalence_globals_01(self):
        self._assert_migration_equivalence([r"""
            global foo -> str;
        """, r"""
            required global foo -> str {
                default := "test";
            }
        """, r"""
            required global foo -> int64 {
                default := 0 + 1;
            }
        """])

    def test_schema_migrations_equivalence_globals_02(self):
        self._assert_migration_equivalence([r"""
            global foo -> str;
        """, r"""
            global foo -> str {
                default := "test";
            }
        """, r"""
            global foo := "test";
        """, r"""
            global foo := 10;
        """, r"""
            global bar := 10;
        """, r"""
            required global bar := 10;
        """, r"""
            required multi global bar := 10;
        """, r"""
            global bar -> str;
        """])

    def test_schema_migrations_equivalence_globals_03(self):
        self._assert_migration_equivalence([r"""
            global foo := 20;
        """, r"""
            alias foo := 20;
        """, r"""
            global foo := 20;
        """])

    def test_schema_migrations_equivalence_globals_04(self):
        self._assert_migration_equivalence([r"""
            global foo -> str
        """, r"""
            global foo := 20;
        """, r"""
            global foo -> int64;
        """])

    def test_schema_migrations_equivalence_globals_05(self):
        self._assert_migration_equivalence([r"""
            global cur_username -> str;
            global cur_user := (
                select User filter .username = global cur_username);

            type User {
              required property username -> str {
                constraint exclusive;
              }
            }
        """])

    def test_schema_migrations_equivalence_globals_use_01(self):
        self._assert_migration_equivalence([r"""
            global current -> uuid;
            type Foo {
                 property name -> str;
            };
            alias CurFoo := (select Foo filter .id = global current)
        """, r"""
            global current_foo -> uuid;
            type Foo {
                 property name -> str;
            };
            alias CurFoo := (select Foo filter .id = global current_foo)
        """])

    # NOTE: array<str>, array<int16>, array<json> already exist in std
    # schema, so it's better to use array<float32> or some other
    # non-typical scalars in tests as a way of testing a collection
    # that would actually be created/dropped.
    def test_schema_migrations_equivalence_collections_01(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            type Base {
                property foo -> array<float32>;
            }
        """])

    def test_schema_migrations_equivalence_collections_02(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            type Base {
                property foo -> tuple<str, int32>;
            }
        """])

    def test_schema_migrations_equivalence_collections_03(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            type Base {
                # nested collection
                property foo -> tuple<str, int32, array<float32>>;
            }
        """])

    def test_schema_migrations_equivalence_collections_04(self):
        self._assert_migration_equivalence([r"""
            type Base;
        """, r"""
            type Base {
                property foo -> tuple<a: str, b: int32>;
            }
        """])

    def test_schema_migrations_equivalence_collections_06(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> array<int32>;
            }
        """, r"""
            type Base {
                # change the array type (old type is castable into new)
                property foo -> array<float64>;
            }
        """])

    def test_schema_migrations_equivalence_collections_08(self):
        self._assert_migration_equivalence([r"""
            type Base {
                property foo -> tuple<int32, int32>;
            }
        """, r"""
            type Base {
                # convert property type to a tuple with different (but
                # cast-compatible) element types
                property foo -> tuple<int64, int32>;
            }
        """])

    def test_schema_migrations_equivalence_collections_09(self):
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

    def test_schema_migrations_equivalence_collections_10(self):
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

    def test_schema_migrations_equivalence_collections_11(self):
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

    def test_schema_migrations_equivalence_collections_12(self):
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

    def test_schema_migrations_equivalence_collections_13(self):
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

    def test_schema_migrations_equivalence_collections_14(self):
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

    def test_schema_migrations_equivalence_collections_15(self):
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
        """, r"""
        """])

    def test_schema_migrations_equivalence_collections_16(self):
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

    def test_schema_migrations_equivalence_collections_17(self):
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

            # aliases with array<float32>
            alias BaseAlias := Base { data := [Base.foo] };
            alias CollAlias := [Base.foo];
        """])

    def test_schema_migrations_equivalence_collections_18(self):
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

    def test_schema_migrations_equivalence_collections_20(self):
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

    def test_schema_migrations_equivalence_collections_21(self):
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

    def test_schema_migrations_equivalence_collections_22(self):
        # change prop type without changing the affected expression.
        self._assert_migration_equivalence([r"""
            type Foo {
                property bar -> array<int64>;
            };

            type Bar {
                property val -> int64 {
                    default := len((SELECT Foo LIMIT 1).bar)
                };
            };
        """, r"""
            type Foo {
                property bar -> array<float64>;
            };

            type Bar {
                property val -> int64 {
                    default := len((SELECT Foo LIMIT 1).bar)
                };
            };
        """])

    def test_schema_migrations_equivalence_collections_23(self):
        self._assert_migration_equivalence([r"""
            scalar type MyScalar extending str;

            type User {
                required property tup -> tuple<x:MyScalar>;
            };
        """, r"""
            scalar type MyScalar extending str;

            type User {
                required property tup -> tuple<x:str>;
            };
        """])

    def test_schema_migrations_equivalence_collections_24(self):
        self._assert_migration_equivalence([r"""
            scalar type MyScalar extending str;

            type User {
                required property tup -> tuple<x:MyScalar>;
            };
        """, r"""
            scalar type MyScalarRenamed extending str;

            type User {
                required property tup -> tuple<x:MyScalarRenamed>;
            };
        """])

    def test_schema_migrations_equivalence_collections_25(self):
        self._assert_migration_equivalence([r"""
            scalar type MyScalar extending str;

            type User {
                required property arr -> array<MyScalar>;
            };
        """, r"""
            scalar type MyScalarRenamed extending str;

            type User {
                required property tup -> array<MyScalarRenamed>;
            };
        """])

    def test_schema_migrations_equivalence_collections_26(self):
        self._assert_migration_equivalence([r"""
            scalar type MyScalar extending str;
            scalar type MyScalar2 extending int64;

            type User {
                required property tup ->
                    tuple<
                        a: tuple<x:MyScalar>,
                        b: MyScalar,
                        c: array<MyScalar2>,
                        d: tuple<array<MyScalar2>>,
                    >;
            };
        """, r"""
            scalar type MyScalarRenamed extending str;
            scalar type MyScalar2Renamed extending int64;

            type User {
                required property tup ->
                    tuple<
                        a: tuple<x:MyScalarRenamed>,
                        b: MyScalarRenamed,
                        c: array<MyScalar2Renamed>,
                        d: tuple<array<MyScalar2Renamed>>,
                    >;
            };
        """, r"""
        """])

    def test_schema_migrations_equivalence_collections_27(self):
        self._assert_migration_equivalence([r"""
        """, r"""
            scalar type MyScalar2Renamed extending int64;

            type User {
                required property tup ->
                    tuple<
                        c: array<MyScalar2Renamed>,
                        d: array<MyScalar2Renamed>,
                    >;
            };
        """, r"""
        """])

    def test_schema_migrations_equivalence_rename_refs_01(self):
        self._assert_migration_equivalence([r"""
            type Note {
                required property remark -> str;
                constraint exclusive on (__subject__.remark);
            };
        """, r"""
            type Note {
                required property note -> str;
                constraint exclusive on (__subject__.note);
            };
        """])

    def test_schema_migrations_equivalence_rename_refs_02(self):
        self._assert_migration_equivalence([r"""
            type Note {
                required property remark -> str;
            };

            type User {
                property x -> str {
                    default := (SELECT Note.remark LIMIT 1)
                }
            };
        """, r"""
            type Note {
                required property note -> str;
            };

            type User {
                property x -> str {
                    default := (SELECT Note.note LIMIT 1)
                }
            };
        """])

    def test_schema_migrations_equivalence_rename_refs_03(self):
        self._assert_migration_equivalence([r"""
            type Remark {
                required property note -> str;
            };

            function foo(x: Remark) -> str using ( SELECT x.note );
        """, r"""
            type Note {
                required property note -> str;
            };

            function foo(x: Note) -> str using ( SELECT x.note );
        """])

    def test_schema_migrations_equivalence_rename_refs_04(self):
        self._assert_migration_equivalence([r"""
            type Note {
                required property note -> str;
                index on (.note);
            };
        """, r"""
            type Note {
                required property remark -> str;
                index on (.remark);
            };
        """])

    def test_schema_migrations_equivalence_rename_refs_05(self):
        self._assert_migration_equivalence([r"""
            type Note {
                required property note -> str;
                property foo := .note ++ "!";
            };
        """, r"""
            type Remark {
                required property remark -> str;
                property foo := .remark ++ "!";
            };
        """])

    def test_schema_migrations_equivalence_rename_refs_06(self):
        self._assert_migration_equivalence([r"""
            type Note {
                required property note -> str;
            };
            alias Alias1 := Note;
            alias Alias2 := (SELECT Note.note);
            alias Alias3 := Note { command := .note ++ "!" };
        """, r"""
            type Remark {
                required property remark -> str;
            };
            alias Alias1 := Remark;
            alias Alias2 := (SELECT Remark.remark);
            alias Alias3 := Remark { command := .remark ++ "!" };
        """])

    def test_schema_migrations_equivalence_rename_refs_07(self):
        self._assert_migration_equivalence([r"""
            type Obj1 {
                 required property id1 -> str;
                 required property id2 -> str;
                 property exclusive_hack {
                     using ((.id1, .id2));
                     constraint exclusive;
                 };
             }
        """, r"""
            type Obj2 {
                 required property id1 -> str;
                 required property id2 -> str;
                 property exclusive_hack {
                     using ((.id1, .id2));
                     constraint exclusive;
                 };
             }
        """])

    def test_schema_migrations_equivalence_rename_alias_01(self):
        self._assert_migration_equivalence([r"""
            type Note {
                required property note -> str;
            };
            alias Alias1 := Note;
            alias Alias2 := (SELECT Note.note);
            alias Alias3 := Note { command := .note ++ "!" };

            alias Foo := Note {
                a := Alias1
            };
        """, r"""
            type Note {
                required property note -> str;
            };
            alias Blias1 := Note;
            alias Blias2 := (SELECT Note.note);
            alias Blias3 := Note { command := .note ++ "!" };

            alias Foo := Note {
                a := Blias1
            };
        """])

    @test.xerror('''
        Trips a SchemaError in the initial migration accessing a missing type

        The type produces from the default is a view type not in the schema
    ''')
    def test_schema_migrations_equivalence_rename_alias_02(self):
        self._assert_migration_equivalence([r"""
            type Note {
                required property note -> str;
            };
            alias Alias2 := (SELECT Note.note);

            type Foo {
                multi property b -> str {
                    default := (SELECT Alias2 LIMIT 1);
                }
            };
        """, r"""
            type Note {
                required property note -> str;
            };
            alias Blias2 := (SELECT Note.note);

            type Foo {
                multi property b -> str {
                    default := (SELECT Blias2 LIMIT 1);
                }
            };
        """])

    def test_schema_migrations_equivalence_rename_annot_01(self):
        self._assert_migration_equivalence([r"""
            abstract annotation foo;

            type Object1 {
                annotation foo := 'bar';
            };
        """, r"""
            abstract annotation bar;

            type Object1 {
                annotation bar := 'bar';
            };
        """])

    def test_schema_migrations_equivalence_rename_type_01(self):
        self._assert_migration_equivalence([r"""
            type Foo;
            type Baz {
                link a -> Foo;
            }
        """, r"""
            type Bar;
            type Baz {
                link a -> Bar;
            }
        """])

    def test_schema_migrations_equivalence_rename_type_02(self):
        self._assert_migration_equivalence([r"""
            type Note {
                property note -> str;
            }
            type Subtype extending Note;
            type Link {
                link a -> Note;
            }
            type Uses {
                required property x -> str {
                    default := (SELECT Note.note LIMIT 1)
                }
            };
            type ComputeLink {
                property foo -> str;
                multi link x := (
                    SELECT Note FILTER Note.note = ComputeLink.foo);
            };
            alias Alias := Note;
        """, r"""
            type Remark {
                property note -> str;
            }
            type Subtype extending Remark;
            type Link {
                link a -> Remark;
            }
            type Uses {
                required property x -> str {
                    default := (SELECT Remark.note LIMIT 1)
                }
            };
            type ComputeLink {
                property foo -> str;
                multi link x := (
                    SELECT Remark FILTER Remark.note = ComputeLink.foo);
            };
            alias Alias := Remark;
        """])

    def test_schema_migrations_equivalence_rename_type_03(self):
        self._assert_migration_equivalence([r"""
            type Note {
                property note -> str;
            }
        """, r"""
            type Remark {
                property note -> str;
            }
            type Subtype extending Remark;
            type Link {
                link a -> Remark;
            }
            type Uses {
                required property x -> str {
                    default := (SELECT Remark.note LIMIT 1)
                }
            };
            type ComputeLink {
                property foo -> str;
                multi link x := (
                    SELECT Remark FILTER Remark.note = ComputeLink.foo);
            };
            alias Alias := Remark;
        """])

    def test_schema_migrations_equivalence_rename_enum_01(self):
        self._assert_migration_equivalence([r"""
            scalar type foo extending enum<'foo', 'bar'>;
            type Baz {
                property a -> foo;
            }
        """, r"""
            scalar type bar extending enum<'foo', 'bar'>;
            type Baz {
                property a -> bar;
            }
        """])

    def test_schema_migrations_equivalence_rename_scalar_01(self):
        self._assert_migration_equivalence([r"""
            scalar type foo extending str;
            type Baz {
                property a -> foo;
            }
        """, r"""
            scalar type bar extending str;
            type Baz {
                property a -> bar;
            }
        """])

    def test_schema_migrations_equivalence_rename_abs_constraint_01(self):
        self._assert_migration_equivalence([r"""
            abstract constraint greater_or_equal(val: int64) {
                using (SELECT __subject__ >= val);
            };
            type Note {
                required property note -> int64 {
                    constraint greater_or_equal(10);
                }
            };
        """, r"""
            abstract constraint not_less(val: int64) {
                using (SELECT __subject__ >= val);
            };
            type Note {
                required property note -> int64 {
                    constraint not_less(10);
                }
            };
        """])

    def test_schema_migrations_equivalence_rename_abs_ptr_01(self):
        self._assert_migration_equivalence([r"""
            abstract link abs_link {
                property prop -> int64;
            };

            type LinkedObj;
            type RenameObj {
                multi link link EXTENDING abs_link
                    -> LinkedObj;
            };
        """, r"""
            abstract link new_abs_link {
                property prop -> int64;
            };

            type LinkedObj;
            type RenameObj {
                multi link link EXTENDING new_abs_link
                    -> LinkedObj;
            };
        """])

    def test_schema_migrations_equivalence_rename_abs_ptr_02(self):
        self._assert_migration_equivalence([r"""
            abstract property abs_prop {
                annotation title := "lol";
            };

            type RenameObj {
                property prop EXTENDING abs_prop -> str;
            };
        """, r"""
            abstract property new_abs_prop {
                annotation title := "lol";
            };

            type RenameObj {
                property prop EXTENDING new_abs_prop -> str;
            };
        """])

    def test_schema_migrations_drop_parent_01(self):
        self._assert_migration_equivalence([r"""
            type Parent {
                property name -> str {
                    constraint exclusive;
                }
            }
            type Child extending Parent {
                overloaded property name -> str;
            };
        """, r"""
            type Parent {
                property name -> str {
                    constraint exclusive;
                }
            }
            type Child {
                property name -> str;
            };
        """])

    def test_schema_migrations_drop_parent_02(self):
        self._assert_migration_equivalence([r"""
            type Parent {
                property name -> str {
                    constraint exclusive;
                }
            }
            type Child extending Parent;
        """, r"""
            type Parent {
                property name -> str {
                    constraint exclusive;
                }
            }
            type Child {
                property name -> str;
            }
        """])

    def test_schema_migrations_drop_parent_03(self):
        self._assert_migration_equivalence([r"""
            type Parent {
                property name -> str {
                    delegated constraint exclusive;
                }
            }
            type Child extending Parent;
        """, r"""
            type Parent {
                property name -> str {
                    delegated constraint exclusive;
                }
            }
            type Child {
                property name -> str {
                    constraint exclusive;
                }
            }
        """])

    def test_schema_migrations_drop_parent_04(self):
        self._assert_migration_equivalence([r"""
            type Parent {
                link foo -> Object {
                    property x -> str;
                }
            }
            type Child extending Parent;
        """, r"""
            type Parent {
                link foo -> Object {
                    property x -> str;
                }
            }
            type Child {
                link foo -> Object {
                    property x -> str;
                }
            }
        """])

    def test_schema_migrations_drop_parent_05(self):
        self._assert_migration_equivalence([r"""
            type Parent {
                property x -> str;
                index on (.x);
            }
            type Child extending Parent;
        """, r"""
            type Parent {
                property x -> str;
                index on (.x);
            }
            type Child;
        """])

    def test_schema_migrations_drop_parent_06(self):
        self._assert_migration_equivalence([r"""
            type Parent {
                property x -> str;
                constraint expression on (.x != "YOLO");
            }
            type Child extending Parent;
        """, r"""
            type Parent {
                property x -> str;
                constraint expression on (.x != "YOLO");
            }
            type Child;
        """])

    def test_schema_migrations_drop_parent_07(self):
        self._assert_migration_equivalence([r"""
            type Parent {
                property x -> str;
                property z := .x ++ "!";
            }
            type Child extending Parent;
        """, r"""
            type Parent {
                property x -> str;
                property z := .x ++ "!";
            }
            type Child;
        """])

    def test_schema_migrations_drop_owned_default_01(self):
        self._assert_migration_equivalence([
            r"""
                type Foo;
                type Post {
                    required property createdAt -> str {
                        default := "asdf";
                        constraint expression on (__subject__ != "!");
                    }
                    link whatever -> Foo {
                        default := (SELECT Foo LIMIT 1);
                    }
                }
            """,
            r"""
                type Foo;
                abstract type Event {
                    required property createdAt -> str {
                        default := "asdf";
                        constraint expression on (__subject__ != "!");
                    }
                    link whatever -> Foo {
                        default := (SELECT Foo LIMIT 1);
                    }
                }

                type Post extending Event;
            """,
            r"""
                type Foo;
                type Post {
                    required property createdAt -> str {
                        default := "asdf";
                        constraint expression on (__subject__ != "!");
                    }
                    link whatever -> Foo {
                        default := (SELECT Foo LIMIT 1);
                    }
                }
            """
        ])

    def test_schema_migrations_computed_optionality_01(self):
        self._assert_migration_equivalence([r"""
            abstract type Removable {
                optional single property removed := EXISTS(
                    .<element[IS Tombstone]
                );
            };
            type Topic extending Removable {
                multi link defs := .<topic[IS Definition];
            };
            alias VisibleTopic := (
                SELECT Topic {
                    defs := (
                        SELECT .<topic[IS Definition] FILTER NOT .removed
                    ),
                }
                FILTER NOT .removed
            );
            type Definition extending Removable {
                required link topic -> Topic;
            };
            type Tombstone {
                required link element -> Removable {
                    constraint exclusive;
                }
            };
        """, r"""
            abstract type Removable {
                property removed := EXISTS(.<element[IS Tombstone]);
            };
            type Topic extending Removable {
                multi link defs := .<topic[IS Definition];
            };
            alias VisibleTopic := (
                SELECT Topic {
                    defs := (
                        SELECT .<topic[IS Definition] FILTER NOT .removed
                    ),
                }
                FILTER NOT .removed
            );
            type Definition extending Removable {
                required link topic -> Topic;
            };
            type Tombstone {
                required link element -> Removable {
                    constraint exclusive;
                }
            };
        """])

    def test_schema_migrations_extend_enum_01(self):
        self._assert_migration_equivalence([r"""
            scalar type foo extending enum<Foo, Bar>;
        """, r"""
            scalar type foo extending enum<Foo, Bar, Baz>;
        """])

    def test_schema_to_empty_01(self):
        self._assert_migration_equivalence([r"""
            type A {
                property name -> str;
            }
            type B {
                property name -> str;
            }
            type C extending A, B {
            }
        """])

    def test_schema_migrations_union_01(self):
        with self.assertRaisesRegex(
            errors.QueryError,
            "it is illegal to create a type union that causes a "
            "computed property 'deleted' to mix with other versions of the "
            "same property 'deleted'"
        ):
            self._assert_migration_equivalence([r"""
                type Category {
                    required property title -> str;
                    required property deleted :=
                        EXISTS(.<element[IS DeletionRecord]);
                };
                type Article {
                    required property title -> str;
                    required property deleted :=
                        EXISTS(.<element[IS DeletionRecord]);
                };
                type DeletionRecord {
                    link element -> Article | Category;
                }
            """])

    def test_schema_migrations_drop_depended_on_parent_01(self):
        self._assert_migration_equivalence([r"""
            type Person2 {
                required single property first -> str;
            }

            type Person2a extending Person2 {
                constraint exclusive on (__subject__.first);
            }
        """, r"""
        """])

    def test_schema_migrations_drop_depended_on_parent_02(self):
        self._assert_migration_equivalence([r"""
            type Person2;
            type Person2a extending Person2;
        """, r"""
        """])

    def test_schema_migrations_drop_depended_on_parent_03(self):
        self._assert_migration_equivalence([r"""
            type Person2 {
                required single property first -> str;
            };
            type Person2a extending Person2;
        """, r"""
            type Person2a;
        """])

    def test_schema_migrations_drop_from_one_parent_01(self):
        self._assert_migration_equivalence([r"""
            abstract type Text { property x -> str { constraint exclusive } }
            abstract type Owned { property x -> str { constraint exclusive } }
            type Comment extending Text, Owned;
        """, r"""
            abstract type Text { }
            abstract type Owned { property x -> str { constraint exclusive } }
            type Comment extending Text, Owned;
        """])

    def test_schema_migrations_drop_from_one_parent_02(self):
        self._assert_migration_equivalence([r"""
            abstract type Text { property x -> str { constraint exclusive } }
            abstract type Owned { property x -> str { constraint exclusive } }
            type Comment extending Text, Owned;
        """, r"""
            abstract type Text { property x -> str }
            abstract type Owned { property x -> str { constraint exclusive } }
            type Comment extending Text, Owned;
        """])

    def test_schema_migrations_expression_ref_01(self):
        self._assert_migration_equivalence([
            r"""
                type Article {
                    required property deleted_a := (
                        EXISTS (.<element[IS DeletionRecord]));
                };
                type Category {
                    required property deleted_c := (
                        EXISTS (.<element[IS DeletionRecord]));
                };
                type DeletionRecord {
                    required link element -> (Article | Category) {
                        on target delete delete source;
                        constraint std::exclusive;
                    };
                };
            """,
            r"""
                abstract type Removable {
                    property deleted := EXISTS(.<element[IS DeletionRecord]);
                }
                type Article extending Removable;
                type Category extending Removable;
                type DeletionRecord {
                    required link element -> Removable {
                        on target delete delete source;
                        constraint std::exclusive;
                    };
                };
            """
        ])

    def test_schema_migrations_on_target_delete_01(self):
        self._assert_migration_equivalence([
            r"""
                type User {
                    multi link workspaces -> Workspace {
                        property title -> str;
                        on target delete allow;
                    }
                }

                type Workspace {
                    multi link users := .<workspaces[is User];
                }
            """,
            r"""
                type User {
                    multi link workspaces := .<users[is Workspace];
                }

                type Workspace {
                    multi link users -> User {
                        property title -> str;
                        on target delete allow;
                    }
                }
            """
        ])

    def test_schema_migrations_on_source_delete_01(self):
        self._assert_migration_equivalence([
            r"""
                type User {
                    multi link workspaces -> Workspace {
                        property title -> str;
                        on source delete delete target;
                    }
                }

                type Workspace {
                    multi link users := .<workspaces[is User];
                }
            """,
            r"""
                type User {
                    multi link workspaces -> Workspace {
                        property title -> str;
                        on source delete allow;
                    }
                }

                type Workspace {
                    multi link users := .<workspaces[is User];
                }
            """,
            r"""
                type User {
                    multi link workspaces -> Workspace {
                        property title -> str;
                    }
                }

                type Workspace {
                    multi link users := .<workspaces[is User];
                }
            """
        ])

    def test_schema_migrations_rename_with_stuff_01(self):
        self._assert_migration_equivalence([
            r"""
                type Base {
                        property x -> str;
                        property xbang := .x ++ "!";
                }

                type NamedObject extending Base {
                        required property foo -> str;
                }
            """,
            r"""
                type Base {
                        property x -> str;
                        property xbang := .x ++ "!";
                }

                type ReNamedObject extending Base {
                        required property foo -> str;
                }
            """
        ])

    def test_schema_migrations_rename_with_stuff_02(self):
        self._assert_migration_equivalence([
            r"""
                type Base {
                        property x -> str;
                        index on (.x);
                }

                type NamedObject extending Base {
                        required property foo -> str;
                }
            """,
            r"""
                type Base {
                        property x -> str;
                        index on (.x);
                }

                type ReNamedObject extending Base {
                        required property foo -> str;
                }
            """
        ])

    def test_schema_migrations_rename_with_stuff_03(self):
        self._assert_migration_equivalence([
            r"""
                type Base {
                        property x -> str;
                        property z -> str {
                            constraint expression on (__subject__ != "lol");
                        };
                }

                type NamedObject extending Base {
                        required property foo -> str;
                }
            """,
            r"""
                type Base {
                        property x -> str;
                        property z -> str {
                            constraint expression on (__subject__ != "lol");
                        };
                }

                type ReNamedObject extending Base {
                        required property foo -> str;
                }
            """
        ])

    def test_schema_migrations_rename_with_stuff_04(self):
        self._assert_migration_equivalence([
            r"""
                type Base {
                        property x -> str;
                        constraint expression on ((.x != "lol"));
                }

                type NamedObject extending Base {
                        required property foo -> str;
                }
            """,
            r"""
                type Base {
                        property x -> str;
                        constraint expression on ((.x != "lol"));
                }

                type ReNamedObject extending Base {
                        required property foo -> str;
                }
            """
        ])

    def test_schema_migrations_except_01(self):
        self._assert_migration_equivalence([
            r"""
                type ExceptTest {
                    required property name -> str;
                    property deleted -> bool;
                };
            """,
            r"""
                type ExceptTest {
                    required property name -> str;
                    property deleted -> bool;
                    constraint exclusive on (.name) except (.deleted);
                    index on (.name) except (.deleted);
                };
            """,
        ])

    def test_schema_migrations_half_diamonds_00(self):
        self._assert_migration_equivalence([
            r"""
                abstract type A;

                abstract type B {
                    link z -> G {
                        constraint exclusive;
                    }
                }

                abstract type C extending B {
                    link y -> H {
                        constraint exclusive;
                    }
                }

                abstract type D;

                type E extending D;

                type F extending C, B, D;

                type G extending D, A {
                    link x := assert_single(.<z[IS B]);
                }

                type H extending B, D, A {
                    link w := assert_single(.<y[IS C]);
                }
            """,
            r"""
            """,
        ])

    def test_schema_migrations_half_diamonds_01(self):
        self._assert_migration_equivalence([
            r"""
                abstract type A;
                abstract type B {
                    link z -> A;
                };
                abstract type C extending B;
                abstract type D;
                type F extending C, B;
            """,
            r"""
            """,
        ])

    def test_schema_migrations_half_diamonds_02(self):
        self._assert_migration_equivalence([
            r"""
                abstract type A;
                abstract type B {
                    link z -> A;
                };
                abstract type C extending B;
                abstract type C2 extending B;
                abstract type D;
                type F extending C, C2, B;
            """,
            r"""
            """,
        ])

    def test_schema_migrations_half_diamonds_03(self):
        self._assert_migration_equivalence([
            r"""
                abstract type A;
                abstract type B {
                    link z -> A;
                };
                abstract type C extending B;
                abstract type C2 extending C;
                abstract type D;
                type F extending C, C2, B;
            """,
            r"""
            """,
        ])

    def test_schema_migrations_half_diamonds_04(self):
        self._assert_migration_equivalence([
            r"""
                abstract type A;
                abstract type B {
                    link z -> A;
                };
                abstract type C extending B;
                abstract type C2 extending C;
                abstract type D;
                type F extending C2, B;
            """,
            r"""
            """,
        ])

    def test_schema_migrations_half_diamonds_05(self):
        self._assert_migration_equivalence([
            r"""
                abstract type A;
                abstract type B {
                    link z -> A;
                };
                abstract type C extending B;
                abstract type C2 extending C;
                abstract type D;
                type F extending C, C2, B;
                type F2 extending C, C2, B, F;
            """,
            r"""
            """,
        ])


class TestDescribe(tb.BaseSchemaLoadTest):
    """Test the DESCRIBE command."""

    DEFAULT_MODULE = 'test'

    re_filter = re.compile(r'[\s]+|(,(?=\s*[})]))')
    maxDiff = 10000

    def _assert_describe(
        self,
        schema_text,
        *tests,
        as_ddl=False,
        default_module='test',
        explicit_modules=False,
    ):
        if as_ddl:
            schema = tb._load_std_schema()
            schema = self.run_ddl(schema, schema_text, default_module)
        elif explicit_modules:
            sdl_schema = qlparser.parse_sdl(schema_text)
            schema = tb._load_std_schema()
            schema = s_ddl.apply_sdl(
                sdl_schema,
                base_schema=schema,
                current_schema=schema,
            )
        else:
            schema = self.load_schema(schema_text, modname=default_module)

        tests = [iter(tests)] * 2

        for stmt_text, expected_output in zip(*tests):
            qltree = qlparser.parse(stmt_text, {None: 'test'})
            stmt = qlcompiler.compile_ast_to_ir(
                qltree,
                schema,
                options=qlcompiler.CompilerOptions(
                    modaliases={None: 'test'},
                ),
            )

            output = stmt.expr.expr.result.expr.value
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

    def test_schema_describe_01(self):
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
            }

            type Parent2 {
                link foo -> Foo;
                index on (.foo);
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
                index on (.foo);
                required single link __type__ -> schema::ObjectType {
                    readonly := true;
                };
                overloaded single link foo extending test::f -> test::Foo {
                    annotation test::anno := 'annotated link';
                    constraint std::exclusive {
                        annotation test::anno := 'annotated constraint';
                    };
                    optional single property p -> test::int_t {
                        constraint std::max_value(10);
                    };
                };
                required single property id -> std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
                optional multi property name -> std::str;
            };
            """,

            'DESCRIBE TYPE Child AS TEXT',

            """
            type test::Child extending test::Parent, test::Parent2 {
                required single link __type__ -> schema::ObjectType {
                    readonly := true;
                };
                overloaded single link foo extending test::f -> test::Foo {
                    optional single property p -> test::int_t;
                };
                required single property id -> std::uuid {
                    readonly := true;
                };
                optional multi property name -> std::str;
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
                volatility := 'Immutable';
                annotation std::description := 'Return the array made from all
                    of the input set elements.';
                using sql function 'array_agg';
            };
            """,

            'DESCRIBE FUNCTION sys::get_version AS SDL',

            r"""
            function sys::get_version() -> tuple<major: std::int64,
                                                 minor: std::int64,
                                                 stage: sys::VersionStage,
                                                 stage_no: std::int64,
                                                 local: array<std::str>>
            {
                volatility := 'Stable';
                annotation std::description :=
                    'Return the server version as a tuple.';
                using (SELECT <tuple<
                    major: std::int64,
                    minor: std::int64,
                    stage: sys::VersionStage,
                    stage_no: std::int64,
                    local: array<std::str>>>sys::__version_internal()
                )
            ;};
            """,
        )

    def test_schema_describe_02(self):
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
                    link foobar -> (test::Foo | test::Bar);
                };
                """,
                """
                type test::Spam {
                    link foobar -> (test::Bar | test::Foo);
                };
                """,
            ]
        )

    def test_schema_describe_03(self):
        self._assert_describe(
            """
            scalar type custom_str_t extending str {
                constraint regexp('[A-Z]+');
            }
            """,

            'DESCRIBE MODULE test',

            """
            CREATE SCALAR TYPE test::custom_str_t EXTENDING std::str {
                CREATE CONSTRAINT std::regexp('[A-Z]+');
            };
            """
        )

    def test_schema_describe_04(self):
        self._assert_describe(
            """
            abstract constraint my_one_of(one_of: array<anytype>) {
                using (contains(one_of, __subject__));
            }
            """,

            'DESCRIBE MODULE test',

            """
            CREATE ABSTRACT CONSTRAINT test::my_one_of(one_of: array<anytype>){
                USING (std::contains(one_of, __subject__));
            };
            """
        )

    def test_schema_describe_05(self):
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
                required single link __type__ -> schema::ObjectType {
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
                required single link __type__ -> schema::ObjectType {
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
                required single link __type__ -> schema::ObjectType {
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

    def test_schema_describe_06(self):
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
                required single link __type__ -> schema::ObjectType {
                    readonly := true;
                };
                required single property id -> std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
                required single property image -> std::str;
                optional single property name -> std::str;
            };
            """,

            'DESCRIBE TYPE User AS TEXT',

            """
            type test::User extending test::HasImage {
                required single link __type__ -> schema::ObjectType {
                    readonly := true;
                };
                required single property id -> std::uuid {
                    readonly := true;
                };
                required single property image -> std::str;
                optional single property name -> std::str;
            };
            """,

            'DESCRIBE TYPE User AS SDL',

            '''
            type test::User extending test::HasImage {
                property name -> std::str;
            };
            ''',

            'DESCRIBE TYPE HasImage AS TEXT VERBOSE',

            '''
            abstract type test::HasImage {
                index on (__subject__.image);
                required single link __type__ -> schema::ObjectType {
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
                required single link __type__ -> schema::ObjectType {
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
                index on (__subject__.image);
                required property image -> std::str;
            };
            ''',

            'DESCRIBE TYPE HasImage AS DDL',

            '''
            CREATE ABSTRACT TYPE test::HasImage {
                CREATE REQUIRED PROPERTY image -> std::str;
                CREATE INDEX ON (__subject__.image);
            };
            '''
        )

    def test_schema_describe_07(self):
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
                USING (std::contains(one_of, __subject__));
            };
            ''',

            'DESCRIBE OBJECT UniqueName AS SDL',

            '''
            type test::UniqueName {
                link translated_label extending test::translated_label
                        -> test::Label {
                    constraint std::exclusive on (__subject__@prop1);
                    constraint std::exclusive on (
                        (__subject__@source, __subject__@lang)
                    );
                };
            };
            ''',

            'DESCRIBE OBJECT UniqueName AS TEXT',

            '''
            type test::UniqueName {
                required single link __type__ -> schema::ObjectType {
                    readonly := true;
                };
                optional single link translated_label
                extending test::translated_label
                    -> test::Label
                {
                    optional single property lang -> std::str;
                    optional single property prop1 -> std::str;
                };
                required single property id -> std::uuid {
                    readonly := true;
                };
            };
            ''',

            'DESCRIBE OBJECT UniqueName AS TEXT VERBOSE',

            '''
            type test::UniqueName {
                required single link __type__ -> schema::ObjectType {
                    readonly := true;
                };
                optional single link translated_label
                extending test::translated_label
                    -> test::Label
                {
                    constraint std::exclusive on (__subject__@prop1);
                    constraint std::exclusive on (
                        (__subject__@source, __subject__@lang));
                    optional single property lang -> std::str;
                    optional single property prop1 -> std::str;
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
                CREATE ANNOTATION std::description := 'Specifies the maximum
                    length of subject string representation.';
            };
            ''',

            'DESCRIBE OBJECT std::len_value AS SDL',

            '''
            abstract constraint std::len_value
            on (std::len(<std::str>__subject__))
            {
                errmessage := 'invalid {__subject__}';
            };
            ''',

            'DESCRIBE OBJECT std::len_value AS TEXT',

            '''
            abstract constraint std::len_value
            on (std::len(<std::str>__subject__))
            {
                errmessage := 'invalid {__subject__}';
            };
            ''',

            'DESCRIBE OBJECT std::len_value AS TEXT VERBOSE',

            '''
            abstract constraint std::len_value
            on (std::len(<std::str>__subject__))
            {
                errmessage := 'invalid {__subject__}';
            };
            '''
        )

    def test_schema_describe_08(self):
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
                CREATE PROPERTY bar -> std::str {
                    SET readonly := false;
                };
            };
            """,
            'DESCRIBE TYPE Foo AS SDL',

            """
            type test::Foo {
                property bar -> std::str {
                    readonly := false;
                };
            };
            """,
        )

    def test_schema_describe_09(self):
        # Test normalization of with block. The default module should
        # be inlined into the explicit fully-qualified name.
        self._assert_describe(
            r'''
            abstract constraint my_constr0(val: int64) {
                using (
                    WITH MODULE math
                    SELECT abs(__subject__ + val) > 2
                );
            };

            abstract constraint my_constr1(val: int64) {
                using (
                    WITH m AS MODULE math
                    SELECT m::abs(__subject__ + val) > 2
                );
            };

            abstract constraint my_constr2(val: int64) {
                using (
                    WITH
                        MODULE math,
                        x := __subject__ + val
                    SELECT abs(x) > 2
                );
            };
            ''',

            'DESCRIBE CONSTRAINT my_constr0 AS SDL',

            '''
            abstract constraint test::my_constr0(val: std::int64) {
                using (SELECT
                    (math::abs((__subject__ + val)) > 2)
                );
            };
            ''',

            'DESCRIBE CONSTRAINT my_constr1 AS SDL',

            '''
            abstract constraint test::my_constr1(val: std::int64) {
                using (WITH m AS MODULE math
                    SELECT
                        (m::abs((__subject__ + val)) > 2)
                );
            };
            ''',

            'DESCRIBE CONSTRAINT my_constr2 AS SDL',

            '''
            abstract constraint test::my_constr2(val: std::int64) {
                using (WITH
                        x :=
                            (__subject__ + val)
                    SELECT
                        (math::abs(x) > 2)
                    );
                };
            ''',
        )

    def test_schema_describe_10(self):
        # Test normalization of unusual defaults: query expressions.
        # Note that these defaults may not necessarily be practical,
        # but are used to test expression normalization in various
        # contexts.
        self._assert_describe(
            r'''
            type Foo {
                required property val -> int64;
            }

            type Bar0 {
                link insert_foo -> Foo {
                    # insert a new Foo if not supplied
                    default := (
                        INSERT Foo {
                            val := -1
                        }
                    )
                };
            }

            type Bar1 {
                multi link update_foo -> Foo {
                    # if not supplied, update some specific Foo and link it
                    default := (
                        UPDATE Foo
                        FILTER .val = 1
                        SET {
                            val := .val + 1
                        }
                    )
                };
            }

            type Bar2 {
                multi link for_foo -> Foo {
                    # if not supplied, select some specific Foo using FOR
                    default := (
                        FOR x IN {2, 3}
                        UNION (
                            SELECT Foo
                            FILTER .val = x
                        )
                    )
                };
            }

            type Bar3 {
                property delete_foo -> int64 {
                    # if not supplied, update some specific Foo and link it
                    default := (
                        SELECT (
                            DELETE Foo
                            FILTER .val > 1
                            LIMIT 1
                        ).val
                    )
                };

            }
            ''',

            'DESCRIBE TYPE Bar0 AS SDL',

            '''
            type test::Bar0 {
                link insert_foo -> test::Foo {
                    default := (INSERT
                        test::Foo
                        {
                            val := -1
                        });
                };
            };
            ''',
            'DESCRIBE TYPE Bar1 AS SDL',

            '''
            type test::Bar1 {
                multi link update_foo -> test::Foo {
                    default := (UPDATE
                        test::Foo
                    FILTER
                        (.val = 1)
                    SET {
                        val := (.val + 1)
                    });
                };
            };
            ''',
            'DESCRIBE TYPE Bar2 AS SDL',

            '''
            type test::Bar2 {
                multi link for_foo -> test::Foo {
                    default := (FOR x IN {2, 3}
                    UNION
                        (SELECT
                            test::Foo
                        FILTER
                            (.val = x)
                        ));
                };
            };
            ''',
            'DESCRIBE TYPE Bar3 AS SDL',

            '''
            type test::Bar3 {
                property delete_foo -> std::int64 {
                    default := (SELECT
                        ((DELETE
                            test::Foo
                        FILTER
                            (.val > 1)
                        LIMIT
                            1
                        )).val
                    );
                };
            };
            ''',
        )

    def test_schema_describe_alias_01(self):
        self._assert_describe(
            """
            type Foo {
                property name -> str;
            };

            alias Bar := (SELECT Foo {name, calc := 1});
            """,

            'DESCRIBE MODULE test',

            """
            CREATE TYPE test::Foo {
                CREATE PROPERTY name -> std::str;
            };
            CREATE ALIAS test::Bar := (
                SELECT test::Foo {
                    name,
                    calc := 1
                }
            );
            """
        )

    def test_schema_describe_alias_02(self):
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

            'DESCRIBE MODULE test',

            """
            CREATE TYPE test::Foo {
                CREATE PROPERTY name -> std::str;
            };
            CREATE ALIAS test::Bar {
                USING (
                    SELECT test::Foo {
                        name,
                        calc := 1
                    }
                );
                CREATE ANNOTATION std::title := 'bar alias';
            };
            """
        )

    def test_schema_describe_alias_03(self):
        self._assert_describe(
            """
            alias scalar_alias := {1, 2, 3};
            """,

            'DESCRIBE MODULE test',

            """
            CREATE ALIAS test::scalar_alias := (
                {1, 2, 3}
            );
            """
        )

    def test_schema_describe_alias_04(self):
        self._assert_describe(
            """
            alias tuple_alias := (1, 2, 3);
            alias array_alias := [1, 2, 3];
            """,

            'DESCRIBE MODULE test',

            """
            CREATE ALIAS test::array_alias := (
                [1, 2, 3]
            );
            CREATE ALIAS test::tuple_alias := (
                (1, 2, 3)
            );
            """
        )

    def test_schema_describe_alias_05(self):
        self._assert_describe(
            r"""
            type Foo {
                property name -> str;
            };

            alias Bar := (
                # Test what happens to the default module declared here
                WITH MODULE test
                SELECT Foo {name, calc := 1}
            );
            """,

            'DESCRIBE MODULE test',

            """
            CREATE TYPE test::Foo {
                CREATE PROPERTY name -> std::str;
            };
            CREATE ALIAS test::Bar := (
                SELECT test::Foo {
                    name,
                    calc := 1
                }
            );
            """
        )

    def test_schema_describe_computable_01(self):
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

            'DESCRIBE MODULE test',

            """
            CREATE TYPE test::Foo {
                CREATE LINK annotated_link {
                    USING (SELECT test::Foo LIMIT 1);
                    CREATE ANNOTATION std::title := 'complink';
                };
                CREATE LINK complink :=
                    (SELECT test::Foo LIMIT 1);
                CREATE PROPERTY annotated_compprop {
                    USING ('foo');
                    CREATE ANNOTATION std::title := 'compprop';
                };
                CREATE PROPERTY compprop := ('foo');
            };
            """
        )

    def test_schema_describe_computable_02(self):
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
                CREATE LINK annotated_link {
                    USING (SELECT test::Foo LIMIT 1);
                    CREATE ANNOTATION std::title := 'complink';
                };
                CREATE LINK complink := (
                    SELECT test::Foo LIMIT 1
                );
                CREATE PROPERTY annotated_compprop {
                    USING ('foo');
                    CREATE ANNOTATION std::title := 'compprop';
                };
                CREATE PROPERTY compprop := ('foo');
            };
            """
        )

    def test_schema_describe_computable_03(self):
        self._assert_describe(
            r"""
            type Foo {
                property name -> str;
                property comp := (
                    WITH x := count(Object)
                    SELECT .name ++ <str>x
                )
            };
            """,

            'DESCRIBE MODULE test',

            """
            CREATE TYPE test::Foo {
                CREATE PROPERTY name -> std::str;
                CREATE PROPERTY comp := (WITH
                    x :=
                        std::count(std::Object)
                SELECT
                    (.name ++ <std::str>x)
                );
            };
            """
        )

    def test_schema_describe_builtins_01(self):
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
                CREATE MULTI LINK access_policies
                  EXTENDING schema::reference -> schema::AccessPolicy {
                    ON TARGET DELETE ALLOW;
                    CREATE CONSTRAINT std::exclusive;
                };
                CREATE MULTI LINK intersection_of -> schema::ObjectType;
                CREATE MULTI LINK union_of -> schema::ObjectType;
                CREATE PROPERTY compound_type := (
                    (EXISTS (.union_of) OR EXISTS (.intersection_of))
                );
                CREATE PROPERTY is_compound_type := (.compound_type);
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
                multi link access_policies
                  extending schema::reference -> schema::AccessPolicy {
                    on target delete allow;
                    constraint std::exclusive;
                };
                multi link intersection_of -> schema::ObjectType;
                multi link links := (.pointers[IS schema::Link]);
                multi link properties := (
                    .pointers[IS schema::Property]
                );
                multi link union_of -> schema::ObjectType;
                property compound_type := (
                    (EXISTS (.union_of) OR EXISTS (.intersection_of))
                );
                property is_compound_type := (.compound_type);
            };
            """,
        )

    def test_schema_describe_bad_01(self):
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

    def test_schema_describe_on_target_delete_01(self):
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
                CREATE LINK bar -> std::Object {
                    ON TARGET DELETE ALLOW;
                };
            };
            """,

            'DESCRIBE TYPE Foo AS SDL',

            """
            type test::Foo {
                link bar -> std::Object {
                    on target delete  allow;
                };
            };
            """,

            'DESCRIBE TYPE Foo AS TEXT',

            """
            type test::Foo {
                required single link __type__ -> schema::ObjectType {
                    readonly := true;
                };
                optional single link bar -> std::Object {
                    on target delete  allow;
                };
                required single property id -> std::uuid {
                    readonly := true;
                };
            };
            """,
        )

    def test_schema_describe_escape(self):
        self._assert_describe(
            r"""
            function foo() -> str using ( SELECT r'\1' );
            """,

            'DESCRIBE OBJECT foo AS TEXT',

            r"function test::foo() ->  std::str "
            r"using (SELECT r'\1');"
        )

    def test_schema_describe_poly_01(self):
        self._assert_describe(
            """
            type Object {
                property real -> bool;
            }

            function all() -> bool {
                using (
                    SELECT true
                );
            }
            """,

            'DESCRIBE OBJECT all AS TEXT',

            """
            function test::all() -> std::bool using (SELECT
                true
            );
            # The following builtins are masked by the above:

            # function std::all(vals: SET OF std::bool) ->  std::bool {
            #     volatility := 'Immutable';
            #     annotation std::description := 'Generalized boolean `AND`
                      applied to the set of *values*.';
            #     using sql function 'bool_and'
            # ;};
            """,

            'DESCRIBE OBJECT Object AS TEXT',

            """
            type test::Object {
                required single link __type__ -> schema::ObjectType {
                    readonly := true;
                };
                required single property id -> std::uuid {
                    readonly := true;
                };
                optional single property real -> std::bool;
            };

            # The following builtins are masked by the above:

            # abstract type std::Object extending std::BaseObject {
            #     required single link __type__ -> schema::ObjectType {
            #         readonly := true;
            #     };
            #     required single property id -> std::uuid {
            #         readonly := true;
            #     };
            # };
            """,
        )

    def test_schema_describe_ddl_01(self):
        self._assert_describe(
            """
            CREATE MODULE test;
            CREATE TYPE Tree {
                CREATE REQUIRED PROPERTY val -> str {
                    CREATE CONSTRAINT exclusive;
                };
                CREATE LINK parent -> Tree;
                CREATE MULTI LINK children := .<parent[IS Tree];
                CREATE MULTI LINK test_comp := (
                    SELECT Tree FILTER .val = 'test'
                )
            };
            """,

            'DESCRIBE MODULE test',

            """
            CREATE TYPE test::Tree {
                CREATE LINK parent -> test::Tree;
                CREATE MULTI LINK children := (.<parent[IS test::Tree]);
                CREATE REQUIRED PROPERTY val -> std::str {
                    CREATE CONSTRAINT std::exclusive;
                };
                CREATE MULTI LINK test_comp := (
                    SELECT
                        test::Tree
                    FILTER
                        (.val = 'test')
                );
            };
            """,
            as_ddl=True,
        )

    def test_schema_describe_schema_01(self):
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
            }

            type Parent2 {
                link foo -> Foo;
                index on (.foo);
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

            'DESCRIBE SCHEMA AS DDL',

            """
            CREATE MODULE default IF NOT EXISTS;
            CREATE MODULE test IF NOT EXISTS;
            CREATE ABSTRACT ANNOTATION test::anno;
            CREATE SCALAR TYPE test::int_t EXTENDING std::int64 {
                CREATE ANNOTATION test::anno := 'ext int';
                CREATE CONSTRAINT std::max_value(15);
            };
            CREATE ABSTRACT LINK test::f {
                CREATE PROPERTY p -> test::int_t {
                    CREATE CONSTRAINT std::max_value(10);
                    CREATE ANNOTATION test::anno := 'annotated link property';
                };
            };
            CREATE TYPE test::Foo;
            CREATE TYPE test::Parent {
                CREATE MULTI PROPERTY name -> std::str;
            };
            CREATE TYPE test::Parent2 {
                CREATE LINK foo -> test::Foo;
                CREATE INDEX ON (.foo);
            };
            CREATE TYPE test::Child EXTENDING test::Parent, test::Parent2 {
                CREATE ANNOTATION test::anno := 'annotated';
                ALTER LINK foo {
                    EXTENDING test::f;
                    SET OWNED;
                    SET TYPE test::Foo;
                    CREATE ANNOTATION test::anno := 'annotated link';
                    CREATE CONSTRAINT std::exclusive {
                        CREATE ANNOTATION test::anno := 'annotated constraint';
                    };
                };
            };
            """,

            'DESCRIBE SCHEMA AS SDL',

            r"""
            module default{};
            module test {
                abstract annotation anno;
                abstract link f {
                    property p -> test::int_t {
                        annotation test::anno := 'annotated link property';
                        constraint std::max_value(10);
                    };
                };
                scalar type int_t extending std::int64 {
                    annotation test::anno := 'ext int';
                    constraint std::max_value(15);
                };
                type Child extending test::Parent, test::Parent2 {
                    annotation test::anno := 'annotated';
                    overloaded link foo extending test::f -> test::Foo {
                        annotation test::anno := 'annotated link';
                        constraint std::exclusive {
                            annotation test::anno := 'annotated constraint';
                        };
                    };
                };
                type Foo;
                type Parent {
                    multi property name -> std::str;
                };
                type Parent2 {
                    index on (.foo);
                    link foo -> test::Foo;
                };
            };
            """,
        )

    def test_schema_describe_schema_02(self):
        self._assert_describe(
            """
            using extension notebook version '1.0';
            module default {
                type Foo {
                    link bar -> test::Bar;
                };
            };
            module test {
                type Bar {
                    link foo -> default::Foo;
                };
            };
            """,

            'DESCRIBE SCHEMA AS DDL',

            """
            CREATE MODULE default IF NOT EXISTS;
            CREATE MODULE test IF NOT EXISTS;
            CREATE EXTENSION NOTEBOOK VERSION '1.0';
            CREATE TYPE default::Foo;
            CREATE TYPE test::Bar {
                CREATE LINK foo -> default::Foo;
            };
            ALTER TYPE default::Foo {
                CREATE LINK bar -> test::Bar;
            };
            """,

            'DESCRIBE SCHEMA AS SDL',

            r"""
            using extension notebook version '1.0';
            module default {
                type Foo {
                    link bar -> test::Bar;
                };
            };
            module test {
                type Bar {
                    link foo -> default::Foo;
                };
            };
            """,
            explicit_modules=True,
        )

    def test_schema_describe_except_01(self):
        # Test that except works right
        self._assert_describe(
            """
            abstract constraint always_ok {
                using (true);
            };
            type ExceptTest {
                property e -> std::bool;
                constraint always_ok on (.e);
                constraint always_ok on (.e) except (.e);
                constraint expression on (true) except (.e);
                index on (.id) except (.e);
            };
            """,

            'DESCRIBE TYPE ExceptTest',

            """
            create type test::ExceptTest {
                create property e -> std::bool;
                create constraint std::expression on (true) except (.e);
                create constraint test::always_ok on (.e) except (.e);
                create constraint test::always_ok on (.e);
                create index on (.id) except (.e);
            };
            """,
        )

    def test_schema_describe_missing_01(self):
        with self.assertRaisesRegex(
                errors.InvalidReferenceError, "function 'lol' does not exist"):

            self._assert_describe(
                """
                # nothing, whatever
                """,

                'describe function lol',

                """
                # we'll error instead of checking this
                """,
            )

    def test_schema_describe_missing_02(self):
        with self.assertRaisesRegex(
                errors.InvalidReferenceError, "module 'lol' does not exist"):

            self._assert_describe(
                """
                # nothing, whatever
                """,

                'describe module lol',

                """
                # we'll error instead of checking this
                """,
            )

    def test_schema_describe_missing_03(self):
        with self.assertRaisesRegex(
                errors.InvalidReferenceError,
                "object type 'std::lol' does not exist"):

            self._assert_describe(
                """
                # nothing, whatever
                """,

                'describe type lol',

                """
                # we'll error instead of checking this
                """,
            )


class TestCreateMigration(tb.BaseSchemaTest):

    def test_schema_create_migration_on_empty_01(self):
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')

        m1 = 'm1vrzjotjgjxhdratq7jz5vdxmhvg2yun2xobiddag4aqr3y4gavgq'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m1} ONTO initial {{
                    CREATE TYPE Foo;
                }};
            '''
        )

    def test_schema_create_migration_on_empty_02(self):
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')

        m1 = 'm1vrzjotjgjxhdratq7jz5vdxmhvg2yun2xobiddag4aqr3y4gavgq'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m1} {{
                    CREATE TYPE Foo;
                }};
            '''
        )

    def test_schema_create_migration_on_empty_bad_01(self):
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')

        with self.assertRaisesRegex(
            errors.SchemaDefinitionError,
            "specified migration parent does not exist",
        ):
            m1 = 'm1cfpoaozuh3gl3hzsckdzfyvf2q2p23zskal5sotmuhfkrsuqy43a'
            schema = self.run_ddl(
                schema,
                f'''
                    CREATE MIGRATION {m1} ONTO foo {{
                        CREATE TYPE Foo;
                    }};
                '''
            )

    def test_schema_create_migration_sequence_01(self):
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')

        m1 = 'm1vrzjotjgjxhdratq7jz5vdxmhvg2yun2xobiddag4aqr3y4gavgq'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m1} {{
                    CREATE TYPE Foo;
                }};
            '''
        )

        m2 = 'm1fgy2elz3ks3t5wdpujxsjnmojs24n4ov7i5yvgtz7x643ekda6oq'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m2} ONTO {m1} {{
                    CREATE TYPE Bar;
                }};
            '''
        )
        # This does not specify parent. So parent is computed as a last
        # migration and then it is used to calculate hash. And we ensure that
        # migration contexts match hash before checking if that revision is
        # already applied.
        with self.assertRaisesRegex(
            errors.SchemaDefinitionError,
            f"specified migration name does not match the name "
            f"derived from the migration contents",
        ):
            schema = self.run_ddl(
                schema,
                f'''
                    CREATE MIGRATION {m1} {{
                        CREATE TYPE Bar;
                    }};
                '''
            )

        with self.assertRaisesRegex(
            errors.DuplicateMigrationError,
            f"migration {m2!r} is already applied",
        ):
            schema = self.run_ddl(
                schema,
                f'''
                    CREATE MIGRATION {m2} ONTO {m1} {{
                        CREATE TYPE Bar;
                    }};
                '''
            )

        with self.assertRaisesRegex(
            errors.SchemaDefinitionError,
            f"specified migration parent is not the most recent migration, "
            f"expected {str(m2)!r}",
        ):
            m3 = 'm1ehveozttov2emc33uh362ojjnenn6kd3secmi5el6y3euhifq5na'
            schema = self.run_ddl(
                schema,
                f'''
                    CREATE MIGRATION {m3} ONTO {m1} {{
                        CREATE TYPE Baz;
                    }};
                '''
            )

        m3_bad = 'm1vrzjotjgjxhdratq7jz5vdxmhvg2yun2xobiddag4aqr3y4gavgq'
        m3_good = 'm1ccjw4emykq2c5i4bvaglxjvx7ebr2cgrurvcroggpemdzyjrn6da'
        with self.assertRaisesRegex(
            errors.SchemaDefinitionError,
            f"specified migration name does not match the name derived from "
            f"the migration contents: {m3_bad!r}, expected {m3_good!r}"
        ):
            schema = self.run_ddl(
                schema,
                f'''
                    CREATE MIGRATION {m3_bad} ONTO {m2} {{
                        CREATE TYPE Baz;
                    }};
                '''
            )

    def test_schema_create_migration_hashing_01(self):
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')
        m1 = 'm1tjyzfl33vvzwjd5izo5nyp4zdsekyvxpdm7zhtt5ufmqjzczopdq'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m1} ONTO initial;
            '''
        )

    def test_schema_create_migration_hashing_02(self):
        # this should yield the same hash as hashing_01
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')
        m1 = 'm1tjyzfl33vvzwjd5izo5nyp4zdsekyvxpdm7zhtt5ufmqjzczopdq'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m1} ONTO initial {{
                }};
            '''
        )

    def test_schema_create_migration_hashing_03(self):
        # this is different from the above because
        # of the semicolon arrangement.
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')
        m1 = 'm1sdg27s7lffr7knqhlzq5oegfqr74esj5k3busddccorbj5vv2afa'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m1} ONTO initial {{
                    ;
                }};
            '''
        )

    def test_schema_create_migration_hashing_04(self):
        # this is different from the above because
        # of the semicolon arrangement.
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')
        m1 = 'm1cbiul6yeoa52xehujfb4l4uh34ty2vrsu5mvxk7h63q6ov57lqtq'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m1} ONTO initial {{
                    ;;
                }};
            '''
        )

    def test_schema_create_migration_hashing_05(self):
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')
        m1 = 'm1vrzjotjgjxhdratq7jz5vdxmhvg2yun2xobiddag4aqr3y4gavgq'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m1} ONTO initial {{
                    CREATE TYPE Foo;
                }};
            '''
        )

    def test_schema_create_migration_hashing_06(self):
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')
        m1 = 'm1oppdh5pqk2mi45e6s7zw3zbmwqgcmbwyew2vwa7pkqs7evmx3eca'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m1} ONTO initial {{
                    CREATE TYPE Foo;;
                }};
            '''
        )

    def test_schema_create_migration_hashing_07(self):
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')
        m1 = 'm1qunrujj5tnobsit2cpok4tpbdpagvfr5kqqvwqva3b2lurt7kzia'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m1} ONTO initial {{
                    CREATE TYPE Foo {{}}
                }};
            '''
        )

    def test_schema_create_migration_hashing_08(self):
        schema = self.schema
        schema = self.run_ddl(schema, 'CREATE MODULE default;')
        m1 = 'm1usqifmekhxos6pmrjuqdl7qdewxhz32uqfh3loaywiyafdswqdaa'
        schema = self.run_ddl(
            schema,
            f'''
                CREATE MIGRATION {m1} ONTO initial {{
                    CREATE TYPE Foo {{}};
                }};
            '''
        )
