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
from typing import Type, TYPE_CHECKING

import random
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
from edb.schema import properties as s_props

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

    @tb.must_fail(errors.SchemaError,
                  "cannot redefine property 'name' of object type "
                  "'test::UniqueName_2' as scalar type 'std::bytes'",
                  position=196)
    def test_schema_overloaded_prop_11(self):
        """
            type UniqueName {
                property name -> str;
            };

            type UniqueName_2 extending UniqueName {
                overloaded property name -> bytes;
            };
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

    @tb.must_fail(errors.SchemaDefinitionError,
                  "index expressions must be immutable")
    def test_schema_index_computed_01(self):
        """
        type SignatureStatus {
          required property signature -> str;
          link memo := (
            select Memo filter .signature = SignatureStatus.signature limit 1);

          index on (.memo);
        }

        type Memo {
          required property signature -> str {
            constraint exclusive;
          }
        }
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  "index expressions must be immutable")
    def test_schema_index_computed_02(self):
        """
        type SignatureStatus {
          required property signature -> str;
          link memo := (
            select Memo filter .signature = SignatureStatus.signature limit 1);

          index on (__subject__ { lol := .memo }.lol);
        }

        type Memo {
          required property signature -> str {
            constraint exclusive;
          }
        }
        """

    def test_schema_index_computed_03(self):
        """
        type SignatureStatus {
          required property signature -> str;
          link memo_: Memo;
          link memo := .memo_;

          index on (.memo);
        }

        type Memo {
          required property signature -> str {
            constraint exclusive;
          }
        }
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

    @tb.must_fail(
        errors.InvalidLinkTargetError,
        "required links may not use `on target delete deferred restrict`",
    )
    def test_schema_bad_link_05(self):
        """
            type A;
            type Foo {
                required link foo -> A {
                    on target delete deferred restrict;
                }
            };
        """

    @tb.must_fail(
        errors.SchemaDefinitionError,
        "illegal for the computed link.*to extend an abstract link",
    )
    def test_schema_bad_link_06(self):
        """
            abstract link abs { property foo: str };
            type T { multi link following extending abs -> T {using (T)} }
        """

    @tb.must_fail(errors.InvalidDefinitionError,
                  "cannot place a link property on a property")
    def test_schema_link_prop_on_prop_01(self):
        """
            type Test1 {
                title : str {
                    sub_title : str
                }
            };
        """

    @tb.must_fail(errors.InvalidDefinitionError,
                  "cannot place a deletion policy on a property")
    def test_schema_deletion_policy_on_prop_01(self):
        """
            type Test1 {
                title : str {
                    on source delete allow;
                }
            };
        """

    @tb.must_fail(errors.InvalidDefinitionError,
                  "cannot place a deletion policy on a property")
    def test_schema_deletion_policy_on_prop_02(self):
        """
            type Test1 {
                title : str {
                    on target delete restrict;
                }
            };
        """

    @tb.must_fail(errors.QueryError,
                  "could not resolve partial path")
    def test_schema_partial_path_in_default_of_link_prop_01(self):
        """
            module default {
                type Person {
                    required name: str {
                        constraint exclusive;
                    }

                    multi friends : Person {
                        note: str {
                            default := .name
                        }
                    }

                }
            }
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
                  position=74)
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
                  "invalid type: pseudo type 'anytype' is a generic type")
    def test_schema_bad_type_12(self):
        """
            type Foo {
                property val -> anytype;
            }
        """

    @tb.must_fail(errors.SchemaError,
                  "invalid type: pseudo type 'anytype' is a generic type")
    def test_schema_bad_type_13(self):
        """
            type Foo {
                link val -> anytype;
            }
        """

    @tb.must_fail(errors.SchemaError,
                  "invalid type: pseudo type 'anytuple' is a generic type")
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

    @tb.must_fail(errors.InvalidDefinitionError,
                  "index of object type 'test::Foo' was already declared")
    def test_schema_bad_type_16(self):
        """
            type Foo {
                property val -> str;
                index on (.val);
                index on (.val);
            };
        """

    @tb.must_fail(
        errors.InvalidDefinitionError,
        "index 'fts::index' of object type 'test::Foo' was already declared",
    )
    def test_schema_bad_type_17a(self):
        """
        type Foo {
            property val -> str;
            index fts::index on (
                fts::with_options(.val, language := fts::Language.eng)
            );
            index fts::index on (
                fts::with_options(.val, language := fts::Language.ita)
            );
            index fts::index on (
                fts::with_options(.val, language := fts::Language.eng)
            );
        };
        """

    @tb.must_fail(
        errors.InvalidDefinitionError,
        "multiple std::fts::index indexes defined for test::Foo",
    )
    def test_schema_bad_type_17b(self):
        """
        type Foo {
            property val -> str;
            index fts::index on (
                fts::with_options(.val, language := fts::Language.eng)
            );
            index fts::index on (
                fts::with_options(.val, language := fts::Language.ita)
            );
        };
        """

    @tb.must_fail(
        errors.InvalidPropertyDefinitionError,
        "this type cannot be anonymous",
    )
    def test_schema_bad_type_18(self):
        """
        type Foo {
            property val -> enum<VariantA, VariantB>;
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

    @tb.must_fail(
        errors.SchemaDefinitionError,
        "module 'super' is a reserved module name"
    )
    def test_schema_module_reserved_01(self):
        """
            module foo {
                module super {}
            }
        """

    @tb.must_fail(
        errors.InvalidDefinitionError,
        "field 'default' .*was already declared"
    )
    def test_schema_field_dupe_01(self):
        """
        type SimpleNumbers {
            property bar: str;
            property foo: str {
                default := '';
                default := '';
            }
        }
        """

    @tb.must_fail(
        errors.InvalidDefinitionError,
        "field 'default' .*was already declared"
    )
    def test_schema_field_dupe_02(self):
        """
        type SimpleNumbers {
            property bar: str;
            property foo: str {
                default := .bar;
                default := .bar;
            }
        }
        """

    @tb.must_fail(
        errors.InvalidDefinitionError,
        "link or property 'foo' .*was already declared"
    )
    def test_schema_field_dupe_03(self):
        """
        type SimpleNumbers {
            bar: str;
            foo := .bar ++ "!";
            foo := .bar ++ "!";
        }
        """

    @tb.must_fail(
        errors.InvalidPropertyTargetError,
        "got object type"
    )
    def test_schema_object_as_lprop_01(self):
        """
        type Tgt;
        type Tgt2;
        type Src {
          multi tgts: Tgt {
            lprop: Tgt2;
          }
        };
        """

    @tb.must_fail(
        errors.InvalidPropertyTargetError,
        "got object type"
    )
    def test_schema_object_as_lprop_02(self):
        """
        type Tgt;
        type Tgt2;
        type Src {
          multi tgts: Tgt {
          }
        };
        type Src2 extending Src {
          overloaded multi tgts: Tgt {
            lprop: Tgt2;
          }
        };
        """

    @tb.must_fail(
        errors.InvalidFunctionDefinitionError,
        r"cannot create the `test::foo\(VARIADIC bar: "
        r"OPTIONAL array<std::int64>\)` function: "
        r"variadic argument `bar` illegally declared "
        r"with optional type in user-defined function"
    )
    def test_schema_func_optional_variadic_01(self):
        """
            function foo(variadic bar: optional int64) -> array<int64>
                using (assert_exists(bar));
        """

    def test_schema_global_01(self):
        """
          global two_things: TwoThings;
          scalar type TwoThings extending enum<One, Two>;
       """

    def test_schema_hard_sorting_01(self):
        # This is hard to sort properly because we don't understand the types.
        # From #4683.
        """
            global current_user_id -> uuid;
            global current_user := (
              select User filter .id = global current_user_id
            );
            global current_user_role := (
              (global current_user).role.slug
            );

            type Role {
              property slug -> str;
            }

            type User {
              required link role -> Role;
            }
        """

    def test_schema_hard_sorting_02(self):
        # This is hard to sort properly because we don't understand the types.
        # From #5163
        """
            type Branch;
            type CO2DataPoint{
                required link datapoint -> DataPoint;
                link branch := .datapoint.data_entry.branch;
            }
            type DataPoint{
                required link data_entry := assert_exists(
                    .<data_points[is DataEntry]);
            }

            type DataEntry{
                required link branch -> Branch;
                multi link data_points -> DataPoint;
            }
       """

    def test_schema_hard_sorting_03(self):
        # This is hard to sort properly because we don't understand the types.
        """
            type A {
                property foo := assert_exists(B).bar;
            };
            type B {
                property bar := 1;
            };
       """

    def test_schema_hard_sorting_04(self):
        # This is hard to sort properly because we don't understand the types.
        """
            type A {
                property foo := (
                    with Z := assert_exists(B) select Z.bar);
            };
            type B {
                property bar := 1;
            };
       """

    def test_schema_hard_sorting_05(self):
        """
            type T {
                multi as: A;
                multi bs: B;
                sections := (
                    select (.as union .bs)
                    filter .index > 0
                );
            }

            abstract type I {
                required index: int16;
            }

            type A extending I;
            type B extending I;
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

    def test_schema_refs_04(self):
        with self.assertRaisesRegex(
            errors.InvalidReferenceError,
            "__subject__ cannot be used in this expression",
        ):
            self.load_schema(
                """
                type User3 {
                    required property nick: str;
                    required property name: str {
                        default := (select __subject__.nick);
                    };
                }
                """
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

    @tb.must_fail(errors.SchemaDefinitionError,
                  "missing value for required property",
                  line=10, col=25)
    def test_schema_rewrite_missing_required_01(self):
        """
            type Project {
                required name: str;
                required owner: User;
            }

            type User {
                link default_project: Project {
                    rewrite insert using (
                        insert Project {
                            owner := __subject__,
                        }
                    )
                };
            }
        """

    def test_schema_rewrite_order_01(self):
        """
            type EventSession extending Timed {

              lastSeen: datetime {
                rewrite update using (
                  __old__.foo
                )
              }
              lastSeen2: datetime {
                rewrite insert using (
                  __subject__.foo
                )
              }
            }
            abstract type Timed {
              required foo: datetime {
                default := datetime_current();
              }
            }
        """

    def test_schema_rewrite_order_02(self):
        # One of the properties is going to reference the other property
        # before it is created in its rewrite via __specified__.
        # Ensure that this gets ordered correctly.
        """
            type User {
              property foo -> bool {
                rewrite insert using (__specified__.bar);
              };
              property bar -> bool {
                rewrite insert using (__specified__.foo);
              };
            };
        """

    def test_schema_scalar_order_01(self):
        # Make sure scalar types account for base types when tracing SDL
        # dependencies.
        """
            scalar type two extending one;
            scalar type one extending str;
        """

    def test_schema_trigger_order_01(self):
        """
            type Feed extending Entity {
              trigger sync_trigger_on_update after update for each when (
                __old__.field ?= __new__.field
              )
              do (1)
            }

            abstract type Entity {
              field: str;
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
            CREATE TYPE default::C EXTENDING B, A;
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
            "index of object type 'test::Object1'",
        )

    def test_schema_advanced_types(self):
        schema = self.load_schema("""
            type D;
            abstract type F {
                property f -> int64;
                link df -> D {
                    property df_prop -> str;
                }
            }
            type T1 extending F {
                property a_ -> str;
                property a1 -> str;
                link d_ -> D {
                    property d_prop_ -> str;
                    property d_prop1 -> str;
                };
                link d1 -> D;
            };
            type T2 extending F {
                property a_ -> str;
                property a2 -> str;
                link d_ -> D {
                    property d_prop_ -> str;
                    property d_prop2 -> str;
                };
                link d2 -> D;
            };
            type T3;

            type A {
                link t -> T1 | T2;
                link t2 := .t[IS T2];
                link tf := .t[IS F];
            }
        """)

        A = schema.get('test::A')
        T1 = schema.get('test::T1')
        T2 = schema.get('test::T2')
        F = schema.get('test::F')

        # Checking (T1 | T2)
        A_t_link = A.getptr(schema, s_name.UnqualName('t'))
        A_t = A_t_link.get_target(schema)
        # Checking type
        self.assertTrue(T1.issubclass(schema, A_t))
        self.assertTrue(T2.issubclass(schema, A_t))
        self.assertTrue(A_t.issubclass(schema, F))
        # Checking properties
        A_t.getptr(schema, s_name.UnqualName('a_'))
        A_t.getptr(schema, s_name.UnqualName('f'))
        self.assertIsNone(A_t.maybe_get_ptr(schema, s_name.UnqualName('a1')))
        self.assertIsNone(A_t.maybe_get_ptr(schema, s_name.UnqualName('a2')))
        # Checking links
        A_t_d = A_t.getptr(schema, s_name.UnqualName('d_'))
        A_t_df = A_t.getptr(schema, s_name.UnqualName('df'))
        self.assertIsNone(A_t.maybe_get_ptr(schema, s_name.UnqualName('d1')))
        self.assertIsNone(A_t.maybe_get_ptr(schema, s_name.UnqualName('d2')))
        # Checking link properties
        A_t_d.getptr(schema, s_name.UnqualName('d_prop_'))
        self.assertIsNone(A_t_d.maybe_get_ptr(schema, 'd_prop1'))
        self.assertIsNone(A_t_d.maybe_get_ptr(schema, 'd_prop2'))
        A_t_df.getptr(schema, s_name.UnqualName('df_prop'))

        # Checking ((T1 | T2) & T2)
        A_t2_link = A.getptr(schema, s_name.UnqualName('t2'))
        A_t2 = A_t2_link.get_target(schema)
        # Checking type
        self.assertTrue(A_t2.issubclass(schema, T2))
        self.assertTrue(T2.issubclass(schema, A_t2))
        self.assertTrue(A_t2.issubclass(schema, F))
        self.assertTrue(A_t2.issubclass(schema, A_t))
        # Checking properties
        A_t2.getptr(schema, s_name.UnqualName('a_'))
        A_t2.getptr(schema, s_name.UnqualName('f'))
        self.assertIsNone(A_t2.maybe_get_ptr(schema, s_name.UnqualName('a1')))
        A_t2.getptr(schema, s_name.UnqualName('a2'))
        # Checking links
        A_t2_d = A_t2.getptr(schema, s_name.UnqualName('d_'))
        A_t2_df = A_t2.getptr(schema, s_name.UnqualName('df'))
        self.assertIsNone(A_t2.maybe_get_ptr(schema, s_name.UnqualName('d1')))
        A_t2.getptr(schema, s_name.UnqualName('d2'))
        # Checking link properties
        A_t2_d.getptr(schema, s_name.UnqualName('d_prop_'))
        self.assertIsNone(A_t2_d.maybe_get_ptr(schema, 'd_prop1'))
        self.assertIsNone(A_t2_d.maybe_get_ptr(schema, 'd_prop2'))
        A_t2_df.getptr(schema, s_name.UnqualName('df_prop'))

        # Checking ((T1 | T2) & F)
        A_tf_link = A.getptr(schema, s_name.UnqualName('tf'))
        A_tf = A_tf_link.get_target(schema)
        # Checking type
        self.assertTrue(T1.issubclass(schema, A_tf))
        self.assertTrue(T2.issubclass(schema, A_tf))
        self.assertTrue(A_tf.issubclass(schema, F))
        self.assertTrue(A_tf.issubclass(schema, A_t))
        # Checking properties
        A_tf.getptr(schema, s_name.UnqualName('a_'))
        A_tf.getptr(schema, s_name.UnqualName('f'))
        self.assertIsNone(A_tf.maybe_get_ptr(schema, s_name.UnqualName('a1')))
        self.assertIsNone(A_tf.maybe_get_ptr(schema, s_name.UnqualName('a2')))
        # Checking links
        A_tf_d = A_tf.getptr(schema, s_name.UnqualName('d_'))
        A_tf_df = A_tf.getptr(schema, s_name.UnqualName('df'))
        self.assertIsNone(A_tf.maybe_get_ptr(schema, s_name.UnqualName('d1')))
        self.assertIsNone(A_tf.maybe_get_ptr(schema, s_name.UnqualName('d2')))
        # Checking link properties
        A_tf_d.getptr(schema, s_name.UnqualName('d_prop_'))
        self.assertIsNone(A_tf_d.maybe_get_ptr(schema, 'd_prop1'))
        self.assertIsNone(A_tf_d.maybe_get_ptr(schema, 'd_prop2'))
        A_tf_df.getptr(schema, s_name.UnqualName('df_prop'))

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
        expr_ast = asdf.get_expr(schema).parse()
        self.assertEqual(
            expr_ast.span.name,
            f'<{asdf.id} expr>'
        )

        schema = self.run_ddl(schema, """
            alter type test::Foo {
                create property x -> str { set default := "test" };
            }
        """)
        x = obj.getptr(schema, s_name.UnqualName('x'))
        default_ast = x.get_default(schema).parse()
        self.assertEqual(
            default_ast.span.name,
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

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "unsupported range subtype: test::Age",
    )
    def test_schema_range_01(self):
        """
        scalar type Age extending int64;
        type Y {
            property age_requirement -> range<Age>
        }
        """

    def test_schema_enum_01(self):
        pass
    test_schema_enum_01.__doc__ = (
        "scalar type LongLabel extending enum<\n"
        "    AAAAAAAAAA"
            "BBBBBBBBBB"
            "CCCCCCCCCC"
            "DDDDDDDDDD"
            "EEEEEEEEEE"
            "FFFFFFFFFF"
            "GGG\n"
        ">"
    )

    def test_schema_enum_02(self):
        pass
    test_schema_enum_02.__doc__ = (
        "scalar type LongLabel extending enum<\n"
        "    'AAAAAAAAAA"
            "BBBBBBBBBB"
            "CCCCCCCCCC"
            "DDDDDDDDDD"
            "EEEEEEEEEE"
            "FFFFFFFFFF"
            "GGG'\n"
        ">"
    )

    @tb.must_fail(
        errors.SchemaDefinitionError,
        "enum labels cannot exceed 63 characters",
    )
    def test_schema_enum_03(self):
        pass
    test_schema_enum_03.__doc__ = (
        "scalar type LongLabel extending enum<\n"
        "    AAAAAAAAAA"
            "BBBBBBBBBB"
            "CCCCCCCCCC"
            "DDDDDDDDDD"
            "EEEEEEEEEE"
            "FFFFFFFFFF"
            "GGGG\n"
        ">"
    )

    @tb.must_fail(
        errors.SchemaDefinitionError,
        "enum labels cannot exceed 63 characters",
    )
    def test_schema_enum_04(self):
        pass
    test_schema_enum_04.__doc__ = (
        "scalar type LongLabel extending enum<\n"
        "    'AAAAAAAAAA"
            "BBBBBBBBBB"
            "CCCCCCCCCC"
            "DDDDDDDDDD"
            "EEEEEEEEEE"
            "FFFFFFFFFF"
            "GGGG'\n"
        ">"
    )

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "cannot use SET OF operator 'std::DISTINCT' "
        "in a constraint",
    )
    def test_schema_constraint_non_singleton_01(self):
        """
        type ConstraintNonSingletonTest {
            property has_bad_constraint -> str {
                constraint expression on (
                    distinct __subject__ = __subject__
                )
            }
        }
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "cannot use SET OF operator 'std::DISTINCT' "
        "in a constraint",
    )
    def test_schema_constraint_non_singleton_02(self):
        """
        type ConstraintNonSingletonTest {
            property has_bad_constraint -> str {
                constraint exclusive on (
                    distinct __subject__
                )
            }
        }
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "cannot use SET OF operator 'std::DISTINCT' "
        "in a constraint",
    )
    def test_schema_constraint_non_singleton_03(self):
        """
        type ConstraintNonSingletonTest {
            property has_bad_constraint -> str;

            constraint exclusive on (distinct .has_bad_constraint);
        }
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "set returning operator 'std::DISTINCT' is not supported "
        "in singleton expressions",
    )
    def test_schema_constraint_non_singleton_04(self):
        """
        type ConstraintNonSingletonTest {
            property has_bad_constraint -> str;

            constraint exclusive on (.has_bad_constraint) except (
                distinct __subject__ = __subject__
            );
        }
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "set returning operator 'std::DISTINCT' is not supported "
        "in singleton expressions",
    )
    def test_schema_constraint_non_singleton_05(self):
        """
        abstract constraint bad_constraint {
            using (distinct __subject__ = __subject__);
        }
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "aggregate function 'std::count' is not supported "
        "in singleton expressions",
    )
    def test_schema_constraint_non_singleton_06(self):
        """
        abstract constraint bad_constraint {
            using (count(__subject__) <= 2);
        }
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "cannot use SET OF function 'std::count' "
        "in a constraint",
    )
    def test_schema_constraint_non_singleton_07(self):
        """
        type Foo;
        type ConstraintNonSingletonTest {
            link has_bad_constraint -> Foo {
                constraint expression on (
                    count(__subject__) <= 2
                )
            }
        }
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "cannot use SET OF function 'std::count' "
        "in a constraint",
    )
    def test_schema_constraint_non_singleton_08(self):
        """
        type Foo;
        type ConstraintNonSingletonTest {
            multi link has_bad_constraint -> Foo {
                constraint expression on (
                    count(__subject__) <= 2
                )
            }
        }
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "cannot use SET OF operator 'std::DISTINCT' "
        "in a constraint",
    )
    def test_schema_constraint_non_singleton_09(self):
        """
        type Foo;
        type ConstraintNonSingletonTest {
            link has_bad_constraint -> Foo {
                constraint expression on (
                    distinct __subject__ = __subject__
                )
            }
        }
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "cannot use SET OF operator 'std::DISTINCT' "
        "in a constraint",
    )
    def test_schema_constraint_non_singleton_10(self):
        """
        type Foo;
        type ConstraintNonSingletonTest {
            multi link has_bad_constraint -> Foo {
                constraint expression on (
                    distinct __subject__ = __subject__
                )
            }
        }
        """

    def test_schema_constraint_singleton_01a(self):
        # `IN` allowed in singleton
        """
        type X {
            property a -> int64 {
                constraint expression on (
                    __subject__ in {1}
                );
            }
        };
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "cannot use SET OF operator 'std::IN' "
        "in a constraint",
    )
    def test_schema_constraint_singleton_01b(self):
        """
        type X {
            multi property a -> int64 {
                constraint expression on (
                    __subject__ in {1}
                );
            }
        };
        """

    def test_schema_constraint_singleton_02a(self):
        # `NOT IN` allowed in singleton
        """
        type X {
            property a -> int64 {
                constraint expression on (
                    __subject__ not in {1}
                );
            }
        };
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "cannot use SET OF operator 'std::NOT IN' "
        "in a constraint",
    )
    def test_schema_constraint_singleton_02b(self):
        """
        type X {
            multi property a -> int64 {
                constraint expression on (
                    __subject__ not in {1}
                );
            }
        };
        """

    def test_schema_constraint_singleton_03a(self):
        # `EXISTS` allowed in singleton
        """
        type X {
            property a -> int64 {
                constraint expression on (
                    exists(__subject__)
                );
            }
        };
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "cannot use SET OF operator 'std::EXISTS' "
        "in a constraint",
    )
    def test_schema_constraint_singleton_03b(self):
        """
        type Foo;
        type ConstraintNonSingletonTest {
            multi link has_bad_constraint -> Foo {
                constraint expression on (
                    exists(__subject__)
                )
            }
        }
        """

    def test_schema_constraint_singleton_04a(self):
        # `??` allowed in singleton
        """
        type X {
            property a -> int64 {
                constraint expression on (
                    __subject__ ?? 1 = 0
                );
            }
        };
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        r"cannot use SET OF operator 'std::\?\?' "
        r"in a constraint",
    )
    def test_schema_constraint_singleton_04b(self):
        """
        type X {
            multi property a -> int64 {
                constraint expression on (
                    __subject__ ?? 1 = 0
                );
            }
        };
        """

    def test_schema_constraint_singleton_05a(self):
        # `IF` allowed in singleton
        """
        type X {
            property a -> tuple<bool, int64> {
                constraint expression on (
                    __subject__.1 < 0
                    if __subject__.0 else
                    __subject__.1 >= 0
                );
            }
        };
        """

    @tb.must_fail(
        errors.UnsupportedFeatureError,
        "cannot use SET OF operator 'std::IF' "
        "in a constraint",
    )
    def test_schema_constraint_singleton_05b(self):
        """
        type X {
            multi property a -> tuple<bool, int64> {
                constraint expression on (
                    __subject__.1 < 0
                    if __subject__.0 else
                    __subject__.1 >= 0
                );
            }
        };
        """

    @tb.must_fail(
        errors.SchemaDefinitionError,
        "cannot use SET OF operator 'std::DISTINCT' "
        "in an index expression",
    )
    def test_schema_index_non_singleton_01(self):
        """
        type IndexNonSingletonTest {
            property has_bad_index -> str;

            index on (distinct .has_bad_index)
        }
        """

    @tb.must_fail(
        errors.SchemaDefinitionError,
        "cannot use SET OF function 'std::count' "
        "in an index expression",
    )
    def test_schema_index_non_singleton_02(self):
        """
        type IndexNonSingletonTest {
            property has_bad_index -> str;

            index on (count(.has_bad_index))
        }
        """

    @tb.must_fail(
        errors.SchemaError,
        "cannot create union \\(test::X | test::Y\\) with property 'a' using "
        "incompatible types std::int64, std::str",
    )
    def test_schema_incompatible_union_01(self):
        """
        type X {
            property a -> int64;
        }
        type Y {
            property a -> str;
        }
        type Z {
            link xy -> X | Y;
        }
        """

    @tb.must_fail(
        errors.SchemaError,
        "cannot create union \\(test::X | test::Y\\) with link 'a' using "
        "incompatible types std::int64, test::A",
    )
    def test_schema_incompatible_union_02(self):
        """
        type A;
        type X {
            property a -> int64;
        }
        type Y {
            link a -> A;
        }
        type Z {
            link xy -> X | Y;
        }
        """

    @tb.must_fail(
        errors.SchemaError,
        "cannot create union \\(test::X | test::Y\\) with link 'a' with "
        "property 'b' using incompatible types std::int64, std::str",
    )
    def test_schema_incompatible_union_03(self):
        """
        type A;
        type X {
            link a -> A {
                b -> int64
            };
        }
        type Y {
            link a -> A {
                b -> str
            }
        }
        type Z {
            link xy -> X | Y;
        }
        """

    @tb.must_fail(
        errors.SchemaError,
        "query parameters are not allowed in schemas",
    )
    def test_schema_query_parameter_01(self):
        """
        type Foo { foo := <int64>$0 }
        """

    @tb.must_fail(
        errors.SchemaError,
        "query parameters are not allowed in schemas",
    )
    def test_schema_query_parameter_02(self):
        """
        global foo := <int64>$0
        """

    @tb.must_fail(
        errors.InvalidReferenceError,
        "type 'test::C' does not exist",
    )
    def test_schema_unknown_typename_01(self):
        """
        type A;
        type B {
            link a -> A;
            property x := <C>.a;
        }
        """

    @tb.must_fail(
        errors.InvalidReferenceError,
        "type 'test::C' does not exist",
    )
    def test_schema_unknown_typename_02(self):
        """
        type A;
        type B {
            link a -> A;
            property x := .a is C;
        }
        """

    @tb.must_fail(
        errors.InvalidReferenceError,
        "type 'test::null' does not exist",
        hint='Did you mean to use `exists` to check if a set is empty?'
    )
    def test_schema_unknown_typename_03(self):
        """
        type A;
        type B {
            link a -> A;
            property x := .a is null;
        }
        """

    @tb.must_fail(
        errors.InvalidReferenceError,
        "type 'test::NONE' does not exist",
        hint='Did you mean to use `exists` to check if a set is empty?'
    )
    def test_schema_unknown_typename_04(self):
        """
        type A;
        type B {
            link a -> A;
            property x := .a is NONE;
        }
        """

    @tb.must_fail(
        errors.InvalidReferenceError,
        "type 'test::C' does not exist",
    )
    def test_schema_unknown_typename_05(self):
        """
        type B {
            property x := (introspect C).name;
        }
        """

    def _run_migration_to(self, schema_text: str) -> None:
        migration_text = f'''
            START MIGRATION TO {{
                {schema_text}
            }};
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        '''

        self.run_ddl(self.schema, migration_text)

    def _check_valid_queries(
        self,
        schema_text: str,
        valid_queries: list[str],
    ) -> None:

        for query in valid_queries:
            query_text = f'''
                module default {{ alias query := ({query}); }}
            '''
            self._run_migration_to(schema_text + query_text)

    def _check_invalid_queries(
        self,
        schema_text: str,
        invalid_queries: list[str],
        error_type: Type,
        error_message: str,
    ) -> None:
        for query in invalid_queries:
            query_text = f'''
                module default {{ alias query := ({query}); }}
            '''
            with self.assertRaisesRegex(error_type, error_message):
                self._run_migration_to(schema_text + query_text)

    def test_schema_with_module_01(self):
        schema_text = f'''
            module dummy {{}}
            module A {{
                type Foo;
            }}
        '''
        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]
        with_mod = 'WITH MODULE A '
        queries += [
            (NO_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dum::Foo>{}'),
        ]
        with_mod = 'WITH AAA as MODULE A '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <AAA::Foo>{}'),
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <s::Foo>{}'),
        ]
        with_mod = 'WITH std as MODULE A '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (NO_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]
        with_mod = 'WITH A as MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT <Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <std::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <dummy::Foo>{}'),
            (REF_ERR, with_mod + 'SELECT <A::Foo>{}'),
        ]

        self._check_valid_queries(
            schema_text,
            [query for error, query in queries if error == NO_ERR],
        )
        self._check_invalid_queries(
            schema_text,
            [query for error, query in queries if error == REF_ERR],
            errors.InvalidReferenceError,
            "Foo' does not exist",
        )

    def test_schema_with_module_02(self):
        schema_text = f'''
            module dummy {{}}
            module A {{
                function abs(x: int64) -> int64 using (x);
            }}
        '''
        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
        ]
        with_mod = 'WITH MODULE A '
        queries += [
            (NO_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dum::abs(1)'),
        ]
        with_mod = 'WITH AAA as MODULE A '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
            (NO_ERR, with_mod + 'SELECT AAA::abs(1)'),
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
            (REF_ERR, with_mod + 'SELECT s::abs(1)'),
        ]
        with_mod = 'WITH std as MODULE A '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (NO_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (NO_ERR, with_mod + 'SELECT A::abs(1)'),
        ]
        with_mod = 'WITH A as MODULE std '
        queries += [
            (REF_ERR, with_mod + 'SELECT abs(1)'),
            (REF_ERR, with_mod + 'SELECT std::abs(1)'),
            (REF_ERR, with_mod + 'SELECT dummy::abs(1)'),
            (REF_ERR, with_mod + 'SELECT A::abs(1)'),
        ]

        self._check_valid_queries(
            schema_text,
            [query for error, query in queries if error == NO_ERR],
        )
        self._check_invalid_queries(
            schema_text,
            [query for error, query in queries if error == REF_ERR],
            errors.InvalidReferenceError,
            "abs' does not exist",
        )

    def test_schema_with_module_03(self):
        schema_text = f'''
            module dummy {{}}
        '''
        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dum::int64>{} = 1'),
        ]
        with_mod = 'WITH def as MODULE default '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <def::int64>{} = 1'),
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <s::int64>{} = 1'),
        ]
        with_mod = 'WITH std as MODULE dummy '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]

        self._check_valid_queries(
            schema_text,
            [query for error, query in queries if error == NO_ERR],
        )
        self._check_invalid_queries(
            schema_text,
            [query for error, query in queries if error == REF_ERR],
            errors.InvalidReferenceError,
            "int64' does not exist",
        )

    def test_schema_with_module_04(self):
        schema_text = f'''
            module dummy {{}}
            module default {{ type int64; }}
        '''

        NO_ERR = 1
        REF_ERR = 2
        TYPE_ERR = 3

        queries = []

        with_mod = ''
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            (NO_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dum::int64>{} = 1'),
        ]
        with_mod = 'WITH def as MODULE default '
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <def::int64>{} = 1'),
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
            (NO_ERR, with_mod + 'SELECT <s::int64>{} = 1'),
        ]
        with_mod = 'WITH std as MODULE dummy '
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]
        with_mod = 'WITH std as MODULE default '
        queries += [
            (TYPE_ERR, with_mod + 'SELECT <int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <std::int64>{} = 1'),
            (TYPE_ERR, with_mod + 'SELECT <default::int64>{} = 1'),
            (REF_ERR, with_mod + 'SELECT <dummy::int64>{} = 1'),
        ]

        self._check_valid_queries(
            schema_text,
            [query for error, query in queries if error == NO_ERR],
        )
        self._check_invalid_queries(
            schema_text,
            [query for error, query in queries if error == REF_ERR],
            errors.InvalidReferenceError,
            "int64' does not exist",
        )
        self._check_invalid_queries(
            schema_text,
            [query for error, query in queries if error == TYPE_ERR],
            errors.InvalidTypeError,
            "operator '=' cannot be applied",
        )

    def test_schema_with_module_05(self):
        schema_text = f'''
            module dummy {{}}
        '''

        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module dummy '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module _test '
        queries += [
            (NO_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module std '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module std::_test '
        queries += [
            (NO_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with t as module _test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (NO_ERR, with_mod + 'select t::abs(1)'),
        ]
        with_mod = 'with s as module std '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (REF_ERR, with_mod + 'select s::abs(1)'),
        ]
        with_mod = 'with st as module std::_test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (NO_ERR, with_mod + 'select st::abs(1)'),
        ]
        with_mod = 'with std as module _test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (NO_ERR, with_mod + 'select _test::abs(1)'),
            (REF_ERR, with_mod + 'select std::_test::abs(1)'),
            (NO_ERR, with_mod + 'select std::abs(1)'),
        ]

        self._check_valid_queries(
            schema_text,
            [query for error, query in queries if error == NO_ERR],
        )
        self._check_invalid_queries(
            schema_text,
            [query for error, query in queries if error == REF_ERR],
            errors.InvalidReferenceError,
            "abs' does not exist",
        )

    def test_schema_with_module_06(self):
        schema_text = f'''
            module dummy {{}}
            module _test {{}}
        '''

        NO_ERR = 1
        REF_ERR = 2

        queries = []

        with_mod = ''
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module dummy '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module _test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module std '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with module std::_test '
        queries += [
            (NO_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
        ]
        with_mod = 'with t as module _test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (REF_ERR, with_mod + 'select t::abs(1)'),
        ]
        with_mod = 'with s as module std '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (REF_ERR, with_mod + 'select s::abs(1)'),
        ]
        with_mod = 'with st as module std::_test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (NO_ERR, with_mod + 'select std::_test::abs(1)'),
            (NO_ERR, with_mod + 'select st::abs(1)'),
        ]
        with_mod = 'with std as module _test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (REF_ERR, with_mod + 'select std::_test::abs(1)'),
            (REF_ERR, with_mod + 'select std::abs(1)'),
        ]
        with_mod = 'with std as module std::_test '
        queries += [
            (REF_ERR, with_mod + 'select abs(1)'),
            (REF_ERR, with_mod + 'select _test::abs(1)'),
            (REF_ERR, with_mod + 'select std::_test::abs(1)'),
            (NO_ERR, with_mod + 'select std::abs(1)'),
        ]

        self._check_valid_queries(
            schema_text,
            [query for error, query in queries if error == NO_ERR],
        )
        self._check_invalid_queries(
            schema_text,
            [query for error, query in queries if error == REF_ERR],
            errors.InvalidReferenceError,
            "abs' does not exist",
        )


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
        explicit_modules: bool = False,
    ) -> s_schema.Schema:
        if explicit_modules:
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

        schemas = []
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
            schemas.append(multi_migration)

            diff = s_ddl.delta_schemas(multi_migration, cur_state)

            note = ('' if i + 1 < len(migrations)
                    else ' (migrating to empty schema)')

            if list(diff.get_subcommands()):
                self.fail(
                    f'unexpected difference in schema produced by '
                    f'incremental migration on step {i + 1}{note}:\n'
                    f'{markup.dumps(diff)}\n'
                )

        return schemas

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

    def test_schema_get_migration_54(self):
        schema = r'''
            type Venue {
                multi link meta_bookings := .<venue[is MetaBooking];
            }
            abstract type MetaBooking {
                link venue -> Venue;
            }

            type Booking extending MetaBooking;
            type ExternalBooking extending MetaBooking {
                overloaded link venue -> Venue;
            }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_get_migration_55(self):
        # In Bar, `.foo.b` refers to `.a`. Ensure that when tracing, `.a` is
        # correctly `Foo.a`, otherwise a recurisve definition is found.
        schema = r'''
            type Foo {
                property a -> str;
                property b := .a;
            }

            type Bar {
                property a := .foo.b;
                link foo -> Foo;
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

        self._assert_migration_consistency(schema, explicit_modules=True)

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

        self._assert_migration_consistency(schema, explicit_modules=True)

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

        self._assert_migration_consistency(schema, explicit_modules=True)

    def test_schema_get_migration_multi_module_04(self):
        # View and type from different modules
        schema = r'''
            alias default::X := (SELECT other::Foo.name);

            type other::Foo {
                property name -> str;
            }
        '''

        self._assert_migration_consistency(schema, explicit_modules=True)

    def test_schema_get_migration_multi_module_05(self):
        # View and type from different modules
        schema = r'''
            alias default::X := (SELECT other::Foo FILTER .name > 'a');

            type other::Foo {
                property name -> str;
            }
        '''

        self._assert_migration_consistency(schema, explicit_modules=True)

    def test_schema_get_migration_multi_module_06(self):
        # Type and annotation from different modules.
        schema = r'''
        type default::Foo {
            property name -> str;
            annotation other::my_anno := 'Foo';
        }

        abstract annotation other::my_anno;
        '''

        self._assert_migration_consistency(schema, explicit_modules=True)

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

        self._assert_migration_consistency(schema, explicit_modules=True)

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

        self._assert_migration_consistency(schema, explicit_modules=True)

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

        self._assert_migration_consistency(schema, explicit_modules=True)

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

        self._assert_migration_consistency(schema, explicit_modules=True)

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

        self._assert_migration_consistency(schema, explicit_modules=True)

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

        self._assert_migration_consistency(schema, explicit_modules=True)

    def test_schema_get_migration_nested_module_01(self):
        schema = r'''
        type foo::bar::Y;
        module foo {
          module bar {
            type X {
                link y -> Y;
            }
          }
        }
        '''

        self._assert_migration_consistency(schema, explicit_modules=True)

    def test_schema_get_migration_nested_module_02(self):
        schema = r'''
        module foo {
          type Z;
          type Y {
              link x1 -> foo::bar::X;
              link x2 := foo::bar::X;
              link z1 -> Z;
              link z2 := Z;
          };
          module bar {
            type X;
          }
        }
        '''

        self._assert_migration_consistency(schema, explicit_modules=True)

    def test_schema_get_migration_nested_module_03(self):
        schema = r'''
        module default {
            alias x := _test::abs(-1);
        };
        '''

        self._assert_migration_consistency(schema, explicit_modules=True)

    def test_schema_get_migration_nested_module_04(self):
        schema = r'''
        module _test { };
        module default {
            alias x := _test::abs(-1);
        };
        '''
        with self.assertRaisesRegex(
            errors.InvalidReferenceError,
            "function '_test::abs' does not exist"
        ):
            self._assert_migration_consistency(schema, explicit_modules=True)

    def test_schema_get_migration_nested_module_05(self):
        schema = r'''
        module foo {
          module bar {
            type X;
          }
        };
        module default {
          alias x := (with m as module foo select m::bar::X);
        }
        '''

        self._assert_migration_consistency(schema, explicit_modules=True)

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

    def test_schema_trigger_01(self):
        schema = '''
            type User {
              trigger logInsert after insert for each do (
                insert Log {
                  user := __new__,
                  action := 'Insert',
                }
              );
            }

            type Log {
              required link user -> User;
              required property action -> str;
            }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_trigger_02(self):
        schema = '''
            type User {
              trigger logInsert after insert for each do (
                update Log set {
                  user := __new__,
                  action := 'Insert',
                }
              );
            }

            type Log {
              required link user -> User;
              required property action -> str;
            }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_trigger_03(self):
        schema = '''
            type User {
              trigger logInsert after insert for each do (
                insert Log {
                  user := __new__,
                  action := 'Insert',
                } unless conflict on .action else (
                  update Log set { user := __new__ }
                )
              );
            }

            type Log {
              required link user -> User;
              required property action -> str { constraint exclusive; }
            }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_trigger_04(self):
        schema = '''
            type User {
              trigger logInsert after insert for each do (
                insert Log {
                  user := __new__,
                  action := 'Insert',
                } unless conflict on .action else (
                  insert BackupLog { user := __new__, action := '???' }
                )
              );
            }

            type Log {
              required link user -> User;
              required property action -> str { constraint exclusive; }
            }
            type BackupLog {
              required link user -> User;
              required property action -> str;
            }
        '''

        self._assert_migration_consistency(schema)

    def test_schema_globals_funcs_01(self):
        schema = '''
            required global x1 -> int64 { default := 0 };
            required global x2 -> int64 { default := 0 };
            required global x3 -> int64 { default := 0 };
            required global x4 -> int64 { default := 0 };

            function f1() -> int64 using (
              global x1 + global x2 + global x3 + global x4);
            function f2() -> int64 using (f1());
        '''

        self._assert_migration_consistency(schema)

    def test_schema_pointer_kind_infer_01(self):
        tschema = r'''
        type Bar;
        scalar type scl extending str;
        abstract link friendship {
            property strength: float64;
            index on (__subject__@strength);
        };
        type Foo {
            name: str;
            required address: str {
                default := "n" ++ "/a";
            }
            foo: Foo;
            multi foos: Foo;
            bar: Bar;
            bar2 extending friendship: Bar;
            bar3: Bar {
               lprop: str {
                   default := "foo" ++ "bar";
               }
            };
            or_: Foo | Bar;
            array1: array<str>;
            array2: array<scl>;

            cprop1 := .name;
            multi cprop2 := (
              with us := .name,
              select (select .foos filter .name != us).name
            );
            required cprop3 := assert_exists(.name);

            clink1 := (select .foo filter .name != 'Elvis');
        };
        type Child extending Foo {
            overloaded foo {
                lprop: str;
            };
        }
        '''

        schema = self._assert_migration_consistency(tschema)

        obj = schema.get('default::Foo')
        obj.getptr(schema, s_name.UnqualName('name'), type=s_props.Property)
        obj.getptr(schema, s_name.UnqualName('address'), type=s_props.Property)
        obj.getptr(schema, s_name.UnqualName('array1'), type=s_props.Property)
        obj.getptr(schema, s_name.UnqualName('array2'), type=s_props.Property)
        obj.getptr(schema, s_name.UnqualName('foo'), type=s_links.Link)
        obj.getptr(schema, s_name.UnqualName('bar'), type=s_links.Link)
        obj.getptr(schema, s_name.UnqualName('bar2'), type=s_links.Link)
        obj.getptr(schema, s_name.UnqualName('or_'), type=s_links.Link)

        obj.getptr(schema, s_name.UnqualName('cprop1'), type=s_props.Property)
        obj.getptr(schema, s_name.UnqualName('cprop2'), type=s_props.Property)
        obj.getptr(schema, s_name.UnqualName('cprop3'), type=s_props.Property)

        obj.getptr(schema, s_name.UnqualName('clink1'), type=s_links.Link)

        ptr = obj.getptr(schema, s_name.UnqualName('bar3'), type=s_links.Link)
        ptr.getptr(schema, s_name.UnqualName('lprop'), type=s_props.Property)

        obj2 = schema.get('default::Child')
        ptr = obj2.getptr(schema, s_name.UnqualName('foo'), type=s_links.Link)
        ptr.getptr(schema, s_name.UnqualName('lprop'), type=s_props.Property)

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

    def test_schema_migrations_equivalence_57c(self):
        self._assert_migration_equivalence([r"""
            type X;
            alias Z := (with lol := X, select count(lol));
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
        self._assert_migration_equivalence(
            [
                r"""
                type User {
                    required property name -> str;
                };
                """,
                r"""
                type User {
                    required property name -> str;
                    index pg::spgist on (.name);
                };
                """,
                r"""
                type User {
                    required property name -> str;
                    index pg::spgist on (.name) {
                        annotation description := 'test';
                    };
                };
                """,
            ]
        )

    def test_schema_migrations_equivalence_61(self):
        self._assert_migration_equivalence(
            [
            r"""
            type Child {
                property foo -> str;
            }

            type Base {
                link bar -> Child;
            }
            """,
            r"""
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
            """,
            ]
        )

    def test_schema_migrations_equivalence_constr_rebase_01(self):
        self._assert_migration_equivalence(
            [
            r"""
            abstract type Foo;

            type Bar {
              required property baz -> str {
                constraint max_len_value(280);
              }
            }
            """,
            r"""
            abstract type Foo;

            type Bar extending Foo {
              required property baz -> str {
                constraint max_len_value(280);
              }
            }
            """,
            r"""
            abstract type Foo;

            type Bar {
              required property baz -> str {
                constraint max_len_value(280);
              }
            }
            """,
            ]
        )

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
            "property 'val' of object type 'default::Foo' and "
            "property 'val' of object type 'default::Bar'"
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
        self._assert_migration_equivalence(
            [
                r"""
                type Base {
                    property first_name -> str;
                    property last_name -> str;
                    property name := .first_name ++ ' ' ++ .last_name;
                }
                """,
                r"""
                type Base {
                    property first_name -> str;
                    property last_name -> str;
                    property name := .first_name ++ ' ' ++ .last_name;
                    # an index on a computable
                    index fts::index on (
                        fts::with_options(.name, language := fts::Language.eng)
                    );
                }
                """,
            ]
        )

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

    def test_schema_migrations_equivalence_index_06(self):
        self._assert_migration_equivalence([r"""
            type Base {
                required property name -> str;
                required property year -> int64;
                index on ((.name, .year));
            }
        """, r"""
            type Base {
                required property name -> str;
                required property year -> int64;
                index on ((.year, .name));
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

    def test_schema_migrations_equivalence_constraint_11(self):
        self._assert_migration_equivalence([r"""
            type Foo {
              required property name -> str {
                constraint max_len_value(200) {
                  errmessage := "name is too long";
                }
              }
              constraint exclusive on (.name) {
                errmessage := "exclusivity!";
              }
            }
        """, r"""
            type Foo {
              required property name -> str {
                constraint max_len_value(201) {
                  errmessage := "name is too long";
                }
              }
              constraint exclusive on (.name) {
                errmessage := "exclusivity!";
              }
            }
        """, r"""
            type Foo {
              required property name -> str;
              constraint exclusive on (.name) {
                errmessage := "exclusivity!";
              }
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

    def test_schema_migrations_equivalence_globals_funcs_02(self):
        schema1, schema2, _ = self._assert_migration_equivalence([r"""
            required global foo -> int64 { default := 0};
            required global bar -> int64 { default := 0};

            function f1() -> int64 using (global foo);
            function f2() -> int64 using (f1());
        """, r"""
            required global foo -> int64 { default := 0};
            required global bar -> int64 { default := 0};

            function f1() -> int64 using (global foo + global bar);
            function f2() -> int64 using (f1());
        """])

        self.assertEqual(
            schema1.get_functions('default::f2'),
            schema2.get_functions('default::f2'),
            "function got deleted/recreated and should have been altered",
        )

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

    def test_schema_migrations_deferred_index_01(self):
        self._assert_migration_equivalence([r"""
            abstract index test() {
                code := ' ((__col__) NULLS FIRST)';
                deferrability := 'Permitted';
            };

            type Foo {
                property bar -> str;
                deferred index test on (.bar);
            };
        """, r"""
            abstract index test() {
                code := ' ((__col__) NULLS FIRST)';
                deferrability := 'Permitted';
            };

            type Foo {
                property bar -> str;
                index test on (.bar);
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

    def test_schema_migrations_rename_and_modify_01(self):
        self._assert_migration_equivalence([
            r"""
                type Branch{
                  property branchURL: std::str {
                    constraint max_len_value(500);
                    constraint min_len_value(5);
                  };
                };
            """,
            r"""
                type Branch{
                  property email: std::str {
                    constraint max_len_value(50);
                    constraint min_len_value(5);
                  };
                };
            """
        ])

    def test_schema_migrations_rename_and_modify_02(self):
        self._assert_migration_equivalence([
            r"""
                type X {
                    obj: Object {
                        foo: str;
                    };
                };
            """,
            r"""
                type X {
                    obj2: Object {
                        bar: int64;
                    };
                };
            """
        ])

    def test_schema_migrations_rename_and_modify_03(self):
        self._assert_migration_equivalence([
            r"""
                type Branch{
                  property branchName: std::str {
                    constraint min_len_value(0);
                    constraint max_len_value(255);
                  };
                  property branchCode: std::int64;
                  property branchURL: std::str {
                    constraint max_len_value(500);
                    constraint regexp("url");
                    constraint min_len_value(5);
                  };
                };
            """,
            r"""
                type Branch{
                  property branchName: std::str {
                    constraint min_len_value(0);
                    constraint max_len_value(255);
                  };
                  property branchCode: std::int64;
                  property phoneNumber: std::str {
                    constraint min_len_value(5);
                    constraint max_len_value(50);
                    constraint regexp(r"phone");
                  };
                  property email: std::str {
                    constraint min_len_value(5);
                    constraint max_len_value(50);
                    constraint regexp(r"email");
                  };
                };
            """
        ])

    def test_schema_migrations_rename_and_modify_04(self):
        self._assert_migration_equivalence([
            r"""
                type Branch{
                  property branchName: std::str {
                    constraint min_len_value(0);
                    constraint max_len_value(255);
                  };
                  property branchCode: std::int64;
                  property branchURL: std::str {
                    constraint max_len_value(500);
                    constraint regexp("url");
                    constraint min_len_value(5);
                  };
                };
            """,
            r"""
                type Branch2 {
                  property branchName: std::str {
                    constraint min_len_value(0);
                    constraint max_len_value(255);
                  };
                  property branchCode: std::int64;
                  property phoneNumber: std::str {
                    constraint min_len_value(5);
                    constraint max_len_value(50);
                    constraint regexp(r"phone");
                  };
                  property email: std::str {
                    constraint min_len_value(5);
                    constraint max_len_value(50);
                    constraint regexp(r"email");
                  };
                };
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
                type F extending C2, C, B;
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
                type F extending C2, C, B;
                type F2 extending F, C2, C, B;
            """,
            r"""
            """,
        ])

    def test_schema_migrations_equivalence_nested_module_01(self):
        self._assert_migration_equivalence([r"""
            module foo { module bar {} }
        """, r"""
            module foo { module bar { module baz {} } }
        """])

    def test_schema_migrations_property_aliases(self):
        self._assert_migration_equivalence([
            r"""
                abstract type NamedObject {
                    required property name: std::str;
                };
                type Person extending default::User;
                type User extending default::NamedObject {
                    multi link fav_users := (.favorites[is default::User]);
                    multi link favorites: default::NamedObject;
                };
            """,
            r"""
            """,
        ])

    def test_schema_migrations_rewrites_01(self):
        self._assert_migration_equivalence([
            r"""
                type User {
                    name: str {
                        rewrite update, insert using (.name ++ "!")
                    }
                };
            """,
            r"""
            """,
        ])

    def test_schema_migrations_rewrites_02(self):
        self._assert_migration_equivalence([
            r"""
                type User {
                    property foo -> bool;
                    property bar -> bool;
                };
            """,
            r"""
                type User {
                    property foo -> bool {
                        rewrite insert using (
                            __specified__.bar and __specified__.baz
                        );
                    };
                    property bar -> bool {
                        rewrite insert using (
                            __specified__.foo and __specified__.baz
                        );
                    };
                    property baz -> bool {
                        rewrite insert using (
                            __specified__.foo and __specified__.bar
                        );
                    };
                };
            """,
            r"""
                type User {
                    property foo -> bool;
                    property bar -> bool;
                    property baz -> bool;
                };
            """,
            r"""
            """,
        ])

    def test_schema_migrations_implicit_type_01(self):
        self._assert_migration_equivalence([
            r"""
                abstract type Pinnable {
                    property pinned := __source__ in <Pinnable>{};
                }
            """,
            r"""
                abstract type Pinnable {
                    property pinned := __source__ in <Pinnable>{};
                }
                type Foo extending Pinnable {}
            """,
        ])

    def test_schema_migrations_implicit_type_02(self):
        self._assert_migration_equivalence([
            r"""
                abstract type Person {
                    multi link friends : Person{
                        constraint expression on (
                                __subject__ != __subject__@source
                            );
                    };

                }
            """,
            r"""
                abstract type Person {
                    multi link friends : Person{
                        constraint expression on (
                                __subject__ != __subject__@source
                            );
                    };

                }

                type Employee extending Person{
                    department: str;
                }
            """,
        ])

    def test_schema_migrations_inh_ordering_01(self):
        self._assert_migration_equivalence([
            r"""
            type Tag extending Named {
              index on (.name);
              constraint expression on (.name != "");
              constraint max_value(10) on (.cnt);
            }

            abstract type Named  {
              required property name -> str;
              required property cnt -> int64;
              index on (.name);
              constraint expression on (.name != "");
              constraint max_value(10) on (.cnt);
            }
            """,
            r"""
            abstract type Named  {
              required property name -> str;
              required property cnt -> int64;
              index on (.name);
              constraint expression on (.name != "");
              constraint max_value(10) on (.cnt);
            }

            type Tag extending Named {
              index on (.name);
              constraint expression on (.name != "");
              constraint max_value(10) on (.cnt);
            }
            """,
        ])

    def test_schema_migrations_alias_alter_01(self):
        self._assert_migration_equivalence([
            r"""
            alias X := '0';
            alias Y := X;
            alias Z := Y;
            """,
            r"""
            alias X := '1';
            alias Y := X;
            alias Z := Y;
            """,
        ])

    def test_schema_migrations_union_ptrs_01(self):
        self._assert_migration_equivalence([
            r"""
            type A;
            type A_ extending A;
            type B;
            type C {
                # link of type and base type
                link foo -> A | A_;
            };
            """,
            r"""
            type A;
            type A_ extending A;
            type B;
            type C {
                # link of type, base type, and other type
                link foo -> A | A_ | B;
            };
            """,
            r"""
            type A;
            type A_ extending A;
            type B;
            type C {
                # remove link
            };
            """,
        ])
        schema = r'''
        '''

        self._assert_migration_consistency(schema)


class BaseDescribeTest(tb.BaseSchemaLoadTest):

    re_filter = re.compile(r'[\s]+|(,(?=\s*[})]))')
    maxDiff = 10000

    def _load_schema(
        self,
        schema_text,
        *,
        as_ddl,
        default_module,
        explicit_modules,
    ):
        if as_ddl:
            schema = tb._load_std_schema()
            schema = self.run_ddl(schema, schema_text, default_module)
        elif explicit_modules:
            sdl_schema = qlparser.parse_sdl(schema_text)
            schema = tb._load_std_schema()
            schema, _ = s_ddl.apply_sdl(
                sdl_schema,
                base_schema=schema,
                current_schema=schema,
            )
        else:
            schema = self.load_schema(schema_text, modname=default_module)

        return schema


class TestDescribe(BaseDescribeTest):
    """Test the DESCRIBE command."""

    DEFAULT_MODULE = 'test'

    def _assert_describe(
        self,
        schema_text,
        *tests,
        as_ddl=False,
        default_module='test',
        explicit_modules=False,
    ):
        schema = self._load_schema(
            schema_text,
            as_ddl=as_ddl,
            default_module=default_module,
            explicit_modules=explicit_modules,
        )

        tests = [iter(tests)] * 2

        for stmt_text, expected_output in zip(*tests):
            qltrees = qlparser.parse_block(stmt_text, {None: 'test'})
            [qltree,] = qltrees
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
                overloaded link foo: test::Foo {
                    extending test::f;
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
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                overloaded single link foo: test::Foo {
                    extending test::f;
                    annotation test::anno := 'annotated link';
                    constraint std::exclusive {
                        annotation test::anno := 'annotated constraint';
                    };
                    optional single property p: test::int_t {
                        constraint std::max_value(10);
                    };
                };
                required single property id: std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
                optional multi property name: std::str;
            };
            """,

            'DESCRIBE TYPE Child AS TEXT',

            """
            type test::Child extending test::Parent, test::Parent2 {
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                overloaded single link foo: test::Foo {
                    extending test::f;
                    optional single property p: test::int_t;
                };
                required single property id: std::uuid {
                    readonly := true;
                };
                optional multi property name: std::str;
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
                    link foobar: (test::Foo | test::Bar);
                };
                """,
                """
                type test::Spam {
                    link foobar: (test::Bar | test::Foo);
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
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                required single property id: std::uuid {
                    readonly := true;
                };
                required single property middle_name: std::str {
                    default := 'abc';
                    readonly := true;
                };
            };
            """,

            'DESCRIBE TYPE Foo AS TEXT VERBOSE',

            """
            type test::Foo {
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                required single property id: std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
                required single property middle_name: std::str {
                    default := 'abc';
                    readonly := true;
                };
            };
            """,

            'DESCRIBE TYPE Bar AS TEXT',

            """
            type test::Bar extending test::Foo {
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                required single property id: std::uuid {

                    readonly := true;
                };
                required single property middle_name: std::str {
                    default := 'abc';
                    readonly := true;
                };
            };
            """,

            'DESCRIBE TYPE Foo AS SDL',

            '''
            type test::Foo {
                required single property middle_name: std::str {
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
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                required single property id: std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
                required single property image: std::str;
                optional single property name: std::str;
            };
            """,

            'DESCRIBE TYPE User AS TEXT',

            """
            type test::User extending test::HasImage {
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                required single property id: std::uuid {
                    readonly := true;
                };
                required single property image: std::str;
                optional single property name: std::str;
            };
            """,

            'DESCRIBE TYPE User AS SDL',

            '''
            type test::User extending test::HasImage {
                property name: std::str;
            };
            ''',

            'DESCRIBE TYPE HasImage AS TEXT VERBOSE',

            '''
            abstract type test::HasImage {
                index on (__subject__.image);
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                required single property id: std::uuid {
                    readonly := true;
                    constraint std::exclusive;
                };
                required single property image: std::str;
            };
            ''',

            'DESCRIBE TYPE HasImage AS TEXT',

            '''
            abstract type test::HasImage {
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                required single property id: std::uuid {
                    readonly := true;
                };
                required single property image: std::str;
            };
            ''',

            'DESCRIBE TYPE HasImage AS SDL',

            '''
            abstract type test::HasImage {
                index on (__subject__.image);
                required property image: std::str;
            };
            ''',

            'DESCRIBE TYPE HasImage AS DDL',

            '''
            CREATE ABSTRACT TYPE test::HasImage {
                CREATE REQUIRED PROPERTY image: std::str;
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
                link translated_label: test::Label {
                    extending test::translated_label;
                    constraint std::exclusive on (
                        (__subject__@source, __subject__@lang)
                    );
                    constraint std::exclusive on (__subject__@prop1);
                };
            };
            ''',

            'DESCRIBE OBJECT UniqueName AS TEXT',

            '''
            type test::UniqueName {
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                optional single link translated_label: test::Label {
                    extending test::translated_label;
                    optional single property lang: std::str;
                    optional single property prop1: std::str;
                };
                required single property id: std::uuid {
                    readonly := true;
                };
            };
            ''',

            'DESCRIBE OBJECT UniqueName AS TEXT VERBOSE',

            '''
            type test::UniqueName {
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                optional single link translated_label: test::Label {
                    extending test::translated_label;
                    constraint std::exclusive on (
                        (__subject__@source, __subject__@lang));
                    constraint std::exclusive on (__subject__@prop1);
                    optional single property lang: std::str;
                    optional single property prop1: std::str;
                };
                required single property id: std::uuid {
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
                CREATE PROPERTY bar: std::str {
                    SET readonly := false;
                };
            };
            """,
            'DESCRIBE TYPE Foo AS SDL',

            """
            type test::Foo {
                property bar: std::str {
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
                    (std::math::abs((__subject__ + val)) > 2)
                );
            };
            ''',

            'DESCRIBE CONSTRAINT my_constr1 AS SDL',

            '''
            abstract constraint test::my_constr1(val: std::int64) {
                using (
                    SELECT
                        (std::math::abs((__subject__ + val)) > 2)
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
                        (std::math::abs(x) > 2)
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
                link insert_foo: test::Foo {
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
                multi link update_foo: test::Foo {
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
                multi link for_foo: test::Foo {
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
                property delete_foo: std::int64 {
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
                CREATE PROPERTY name: std::str;
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
                CREATE PROPERTY name: std::str;
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
                CREATE PROPERTY name: std::str;
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
                CREATE PROPERTY name: std::str;
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
            EXTENDING schema::Source,
                      schema::ConsistencySubject,
                      schema::InheritingObject,
                      schema::Type,
                      schema::AnnotationSubject
            {
                CREATE MULTI LINK access_policies: schema::AccessPolicy {
                    EXTENDING schema::reference;
                    ON TARGET DELETE ALLOW;
                    CREATE CONSTRAINT std::exclusive;
                };
                CREATE MULTI LINK intersection_of: schema::ObjectType;
                CREATE MULTI LINK union_of: schema::ObjectType;
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
                CREATE MULTI LINK triggers: schema::Trigger {
                    EXTENDING schema::reference;
                    ON TARGET DELETE ALLOW;
                    CREATE CONSTRAINT std::exclusive;
                };
            };
            """,

            'DESCRIBE TYPE schema::ObjectType AS SDL',

            """
            type schema::ObjectType
            extending schema::Source,
                      schema::ConsistencySubject,
                      schema::InheritingObject,
                      schema::Type,
                      schema::AnnotationSubject
            {
                multi link access_policies: schema::AccessPolicy {
                    extending schema::reference;
                    on target delete allow;
                    constraint std::exclusive;
                };
                multi link intersection_of: schema::ObjectType;
                multi link links := (.pointers[IS schema::Link]);
                multi link properties := (
                    .pointers[IS schema::Property]
                );
                multi link triggers: schema::Trigger {
                    extending schema::reference;
                    on target delete allow;
                    constraint std::exclusive;
                };
                multi link union_of: schema::ObjectType;
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
                CREATE LINK bar: std::Object {
                    ON TARGET DELETE ALLOW;
                };
            };
            """,

            'DESCRIBE TYPE Foo AS SDL',

            """
            type test::Foo {
                link bar: std::Object {
                    on target delete  allow;
                };
            };
            """,

            'DESCRIBE TYPE Foo AS TEXT',

            """
            type test::Foo {
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                optional single link bar: std::Object {
                    on target delete  allow;
                };
                required single property id: std::uuid {
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
                required single link __type__: schema::ObjectType {
                    readonly := true;
                };
                required single property id: std::uuid {
                    readonly := true;
                };
                optional single property real: std::bool;
            };

            # The following builtins are masked by the above:

            # abstract type std::Object extending std::BaseObject {
            #     required single link __type__: schema::ObjectType {
            #         readonly := true;
            #     };
            #     required single property id: std::uuid {
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
                CREATE LINK parent: test::Tree;
                CREATE MULTI LINK children := (.<parent[IS test::Tree]);
                CREATE REQUIRED PROPERTY val: std::str {
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
                CREATE PROPERTY p: test::int_t {
                    CREATE CONSTRAINT std::max_value(10);
                    CREATE ANNOTATION test::anno := 'annotated link property';
                };
            };
            CREATE TYPE test::Foo;
            CREATE TYPE test::Parent {
                CREATE MULTI PROPERTY name: std::str;
            };
            CREATE TYPE test::Parent2 {
                CREATE LINK foo: test::Foo;
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
                    property p: test::int_t {
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
                    overloaded link foo: test::Foo {
                        extending test::f;
                        annotation test::anno := 'annotated link';
                        constraint std::exclusive {
                            annotation test::anno := 'annotated constraint';
                        };
                    };
                };
                type Foo;
                type Parent {
                    multi property name: std::str;
                };
                type Parent2 {
                    index on (.foo);
                    link foo: test::Foo;
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
            CREATE EXTENSION NOTEBOOK VERSION '1.0';
            CREATE MODULE default IF NOT EXISTS;
            CREATE MODULE test IF NOT EXISTS;
            CREATE TYPE default::Foo;
            CREATE TYPE test::Bar {
                CREATE LINK foo: default::Foo;
            };
            ALTER TYPE default::Foo {
                CREATE LINK bar: test::Bar;
            };
            """,

            'DESCRIBE SCHEMA AS SDL',

            r"""
            using extension notebook version '1.0';
            module default {
                type Foo {
                    link bar: test::Bar;
                };
            };
            module test {
                type Bar {
                    link foo: default::Foo;
                };
            };
            """,
            explicit_modules=True,
        )

    @test.xfail('''
        describe command includes module pgvector

        ... this *doesn't* happen when actually testing via the CLI, though?
    ''')
    def test_schema_describe_schema_03(self):
        self._assert_describe(
            """
            using extension pgvector version '0.5';
            module default {
                scalar type v3 extending ext::pgvector::vector<3>;

                type Foo {
                    data: v3;
                }
            };
            """,

            'describe schema as ddl',

            """
            create extension vector version '0.5';
            create module default if not exists;
            create scalar type default::v3 extending ext::pgvector::vector<3>;
            create type default::Foo {
                create property data: default::v3;
            };
            """,

            'describe schema as sdl',

            r"""
            using extension pgvector version '0.5';
            module default {
                scalar type v3 extending ext::pgvector::vector<3>;
                type Foo {
                    property data: default::v3;
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
                create property e: std::bool;
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

    def test_schema_describe_name_override_01(self):
        self._assert_describe(
            """
            type Other {
                obj: Object;
            }
            type Object;
            """,

            'DESCRIBE MODULE test',

            """
            create type test::Object;
            create type test::Other {
                create link obj: test::Object;
            };
            """
        )

    def test_schema_describe_name_override_02(self):
        self._assert_describe(
            """
            type Object;
            type Other {
                obj: test::Object;
            }
            """,

            'DESCRIBE MODULE test',

            """
            create type test::Object;
            create type test::Other {
                create link obj: test::Object;
            };
            """
        )

    def test_schema_describe_name_override_03(self):
        self._assert_describe(
            """
            type User {
              single link identity: Identity;
            }

            abstract type BaseObject {}

            type Identity extending BaseObject {
              link user := .<identity[is User];
            }

            type IdentityCredential extending BaseObject {}
            """,

            'DESCRIBE MODULE test',

            """
            create abstract type test::BaseObject;
            create type test::Identity extending test::BaseObject;
            create type test::IdentityCredential extending test::BaseObject;
            create type test::User {
                create single link identity: test::Identity;
            };
            alter type test::Identity {
                create link user := (.<identity[is test::User]);
            };
            """
        )

    def test_schema_describe_overload_01(self):
        self._assert_describe(
            """
            abstract type Animal {
                name: str;
                parent: Animal;
            }
            type Human extending Animal {
                overloaded parent: Human;
            }
            """,

            'describe type test::Human as sdl',

            """
            type test::Human extending test::Animal {
                overloaded link parent: test::Human;
            };
            """,
        )

    def test_schema_describe_with_module_01(self):
        schema_text = f'''
            module dummy {{}}
            module A {{
                type Foo;
            }}
        '''

        queries = []

        with_mod = ''
        queries += [
            with_mod + 'SELECT <A::Foo>{}',
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            with_mod + 'SELECT <A::Foo>{}',
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            with_mod + 'SELECT <A::Foo>{}',
        ]
        with_mod = 'WITH MODULE A '
        queries += [
            with_mod + 'SELECT <Foo>{}',
            with_mod + 'SELECT <A::Foo>{}',
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            with_mod + 'SELECT <A::Foo>{}',
        ]
        with_mod = 'WITH AAA as MODULE A '
        queries += [
            with_mod + 'SELECT <A::Foo>{}',
            with_mod + 'SELECT <AAA::Foo>{}',
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            with_mod + 'SELECT <A::Foo>{}',
        ]
        with_mod = 'WITH std as MODULE A '
        queries += [
            with_mod + 'SELECT <std::Foo>{}',
            with_mod + 'SELECT <A::Foo>{}',
        ]
        with_mod = 'WITH A as MODULE std '
        queries += []

        normalized = 'SELECT <A::Foo>{}'
        for query in queries:
            self._assert_describe(
                schema_text + f'''
                    module default {{ alias query := ({query}); }}
                ''',

                'describe module default as sdl',

                f'''
                    alias default::query := ({normalized});
                ''',
                explicit_modules=True,
            )

    def test_schema_describe_with_module_02(self):
        schema_text = f'''
            module dummy {{}}
            module A {{
                function abs(x: int64) -> int64 using (x);
            }}
        '''

        queries = []

        with_mod = ''
        queries += [
            with_mod + 'SELECT A::abs(1)',
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            with_mod + 'SELECT A::abs(1)',
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            with_mod + 'SELECT A::abs(1)',
        ]
        with_mod = 'WITH MODULE A '
        queries += [
            with_mod + 'SELECT abs(1)',
            with_mod + 'SELECT A::abs(1)',
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            with_mod + 'SELECT A::abs(1)',
        ]
        with_mod = 'WITH AAA as MODULE A '
        queries += [
            with_mod + 'SELECT A::abs(1)',
            with_mod + 'SELECT AAA::abs(1)',
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            with_mod + 'SELECT A::abs(1)',
        ]
        with_mod = 'WITH std as MODULE A '
        queries += [
            with_mod + 'SELECT std::abs(1)',
            with_mod + 'SELECT A::abs(1)',
        ]
        with_mod = 'WITH A as MODULE std '
        queries += []

        normalized = 'SELECT A::abs(1)'
        for query in queries:
            self._assert_describe(
                schema_text + f'''
                    module default {{ alias query := ({query}); }}
                ''',

                'describe module default as sdl',

                f'''
                    alias default::query := ({normalized});
                ''',
                explicit_modules=True,
            )

    def test_schema_describe_with_module_03(self):
        schema_text = f'''
            module dummy {{}}
        '''

        queries = []

        with_mod = ''
        queries += [
            with_mod + 'SELECT <int64>{} = 1',
            with_mod + 'SELECT <std::int64>{} = 1',
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            with_mod + 'SELECT <int64>{} = 1',
            with_mod + 'SELECT <std::int64>{} = 1',
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            with_mod + 'SELECT <int64>{} = 1',
            with_mod + 'SELECT <std::int64>{} = 1',
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            with_mod + 'SELECT <int64>{} = 1',
            with_mod + 'SELECT <std::int64>{} = 1',
        ]
        with_mod = 'WITH def as MODULE default '
        queries += [
            with_mod + 'SELECT <int64>{} = 1',
            with_mod + 'SELECT <std::int64>{} = 1',
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            with_mod + 'SELECT <int64>{} = 1',
            with_mod + 'SELECT <std::int64>{} = 1',
            with_mod + 'SELECT <s::int64>{} = 1',
        ]
        with_mod = 'WITH std as MODULE dummy '
        queries += [
            with_mod + 'SELECT <int64>{} = 1',
        ]

        normalized = 'SELECT (<std::int64>{} = 1)'
        for query in queries:
            self._assert_describe(
                schema_text + f'''
                    module default {{ alias query := ({query}); }}
                ''',

                'describe module default as sdl',

                f'''
                    alias default::query := ({normalized});
                ''',
                explicit_modules=True,
            )

    def test_schema_describe_with_module_04a(self):
        schema_text = f'''
            module dummy {{}}
            module default {{ type int64; }}
        '''

        queries = []

        with_mod = ''
        queries += [
            with_mod + 'SELECT <std::int64>{} = 1',
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            with_mod + 'SELECT <int64>{} = 1',
            with_mod + 'SELECT <std::int64>{} = 1',
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            with_mod + 'SELECT <int64>{} = 1',
            with_mod + 'SELECT <std::int64>{} = 1',
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            with_mod + 'SELECT <std::int64>{} = 1',
        ]
        with_mod = 'WITH def as MODULE default '
        queries += [
            with_mod + 'SELECT <std::int64>{} = 1',
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            with_mod + 'SELECT <std::int64>{} = 1',
            with_mod + 'SELECT <s::int64>{} = 1',
        ]
        with_mod = 'WITH std as MODULE dummy '
        queries += []
        with_mod = 'WITH std as MODULE default '
        queries += []

        normalized = 'SELECT (<std::int64>{} = 1)'
        for query in queries:
            self._assert_describe(
                schema_text + f'''
                    module default {{ alias query := ({query}); }}
                ''',

                'describe module default as sdl',

                f'''
                    alias default::query := ({normalized});
                    type default::int64;
                ''',
                explicit_modules=True,
            )

    def test_schema_describe_with_module_04b(self):
        schema_text = f'''
            module dummy {{}}
            module default {{ type int64; }}
        '''

        queries = []

        with_mod = ''
        queries += [
            with_mod + 'SELECT <int64>{}',
            with_mod + 'SELECT <default::int64>{}',
        ]
        with_mod = 'WITH MODULE dummy '
        queries += [
            with_mod + 'SELECT <default::int64>{}',
        ]
        with_mod = 'WITH MODULE std '
        queries += [
            with_mod + 'SELECT <default::int64>{}',
        ]
        with_mod = 'WITH dum as MODULE dummy '
        queries += [
            with_mod + 'SELECT <default::int64>{}',
        ]
        with_mod = 'WITH def as MODULE default '
        queries += [
            with_mod + 'SELECT <default::int64>{}',
            with_mod + 'SELECT <def::int64>{}',
        ]
        with_mod = 'WITH s as MODULE std '
        queries += [
            with_mod + 'SELECT <default::int64>{}',
        ]
        with_mod = 'WITH std as MODULE dummy '
        queries += []
        with_mod = 'WITH std as MODULE default '
        queries += []

        normalized = 'SELECT <default::int64>{}'
        for query in queries:
            self._assert_describe(
                schema_text + f'''
                    module default {{ alias query := ({query}); }}
                ''',

                'describe module default as sdl',

                f'''
                    alias default::query := ({normalized});
                    type default::int64;
                ''',
                explicit_modules=True,
            )

    def test_schema_describe_with_module_05(self):
        schema_text = f'''
            module dummy {{}}
        '''

        queries = []

        with_mod = ''
        queries += [
            with_mod + 'select _test::abs(1)',
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with module dummy '
        queries += [
            with_mod + 'select _test::abs(1)',
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with module _test '
        queries += [
            with_mod + 'select abs(1)',
            with_mod + 'select _test::abs(1)',
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with module std '
        queries += [
            with_mod + 'select _test::abs(1)',
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with module std::_test '
        queries += [
            with_mod + 'select abs(1)',
            with_mod + 'select _test::abs(1)',
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with t as module _test '
        queries += [
            with_mod + 'select _test::abs(1)',
            with_mod + 'select std::_test::abs(1)',
            with_mod + 'select t::abs(1)',
        ]
        with_mod = 'with s as module std '
        queries += [
            with_mod + 'select _test::abs(1)',
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with st as module std::_test '
        queries += [
            with_mod + 'select _test::abs(1)',
            with_mod + 'select std::_test::abs(1)',
            with_mod + 'select st::abs(1)',
        ]
        with_mod = 'with std as module _test '
        queries += [
            with_mod + 'select _test::abs(1)',
            with_mod + 'select std::abs(1)',
        ]

        normalized = 'SELECT std::_test::abs(1)'
        for query in queries:
            self._assert_describe(
                schema_text + f'''
                    module default {{ alias query := ({query}); }}
                ''',

                'describe module default as sdl',

                f'''
                    alias default::query := ({normalized});
                ''',
                explicit_modules=True,
            )

    def test_schema_describe_with_module_06(self):
        schema_text = f'''
            module dummy {{}}
            module _test {{}}
        '''

        queries = []

        with_mod = ''
        queries += [
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with module dummy '
        queries += [
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with module _test '
        queries += [
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with module std '
        queries += [
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with module std::_test '
        queries += [
            with_mod + 'select abs(1)',
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with t as module _test '
        queries += [
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with s as module std '
        queries += [
            with_mod + 'select std::_test::abs(1)',
        ]
        with_mod = 'with st as module std::_test '
        queries += [
            with_mod + 'select std::_test::abs(1)',
            with_mod + 'select st::abs(1)',
        ]
        with_mod = 'with std as module _test '
        queries += []
        with_mod = 'with std as module std::_test '
        queries += [
            with_mod + 'select std::abs(1)',
        ]

        normalized = 'SELECT std::_test::abs(1)'
        for query in queries:
            self._assert_describe(
                schema_text + f'''
                    module default {{ alias query := ({query}); }}
                ''',

                'describe module default as sdl',

                f'''
                    alias default::query := ({normalized});
                ''',
                explicit_modules=True,
            )


class TestSDLTextFromSchema(BaseDescribeTest):

    def _load_schema(
        self,
        schema_text,
        *,
        as_ddl,
        default_module,
        explicit_modules,
    ):
        if as_ddl:
            schema = tb._load_std_schema()
            schema = self.run_ddl(schema, schema_text, default_module)
        elif explicit_modules:
            sdl_schema = qlparser.parse_sdl(schema_text)
            schema = tb._load_std_schema()
            schema, _ = s_ddl.apply_sdl(
                sdl_schema,
                base_schema=schema,
                current_schema=schema,
            )
        else:
            schema = self.load_schema(schema_text, modname=default_module)

        return schema

    def _assert_sdl_text_from_schema(
        self,
        schema_text,
        expected_text,
        default_module='default',
        explicit_modules=False,
    ):
        schema = self._load_schema(
            schema_text,
            as_ddl=False,
            default_module=default_module,
            explicit_modules=explicit_modules,
        )

        sdl_text = s_ddl.sdl_text_from_schema(schema)

        self.assert_equal(
            expected_text,
            sdl_text,
        )

    annotation_statements = [
        "annotation default::AnnotationA := 'A';",
        "annotation default::AnnotationB := 'B';",
        "annotation default::AnnotationC := 'C';",
        "annotation default::AnnotationD := 'D';",
    ]
    exclusive_constraint_statements = [
        "constraint std::exclusive on ('A');",
        "constraint std::exclusive on ('B');",
        "constraint std::exclusive on ('C');",
        "constraint std::exclusive on ('D');",
        "constraint std::exclusive on (1);",
        "constraint std::exclusive on (2);",
        "constraint std::exclusive on (3);",
        "constraint std::exclusive on (4);",
        "constraint std::exclusive on (true) except (('A' = 'A'));",
        "constraint std::exclusive on (true) except (('A' = 'B'));",
        "constraint std::exclusive on (true) except (('B' = 'A'));",
        "constraint std::exclusive on (true) except (('B' = 'B'));",
    ]
    expression_constraint_statements = [
        "constraint std::expression on (('A' = 'A'));",
        "constraint std::expression on (('A' = 'B'));",
        "constraint std::expression on (('B' = 'A'));",
        "constraint std::expression on (('B' = 'B'));",
        "constraint std::expression on (NOT (false));",
        "constraint std::expression on (std::contains([1], 1));",
        "constraint std::expression on (true) except (('A' = 'A'));",
        "constraint std::expression on (true) except (('A' = 'B'));",
        "constraint std::expression on (true) except (('B' = 'A'));",
        "constraint std::expression on (true) except (('B' = 'B'));",
    ]
    index_statements_caps = [
        "index on ('A');",
        "index on ('B');",
        "index on ('C');",
        "index on ('D');",
    ]
    index_statements_nums = [
        "index on (1);",
        "index on (2);",
        "index on (3);",
        "index on (4);",
    ]
    index_statements_except = [
        "index on (true) except (('A' = 'A'));",
        "index on (true) except (('A' = 'B'));",
        "index on (true) except (('B' = 'A'));",
        "index on (true) except (('B' = 'B'));",
    ]
    access_policy_statements = [
        "access policy AccessPolicyA allow all;"
        "access policy AccessPolicyB allow all;"
        "access policy AccessPolicyC allow all;"
        "access policy AccessPolicyD allow all;"
    ]

    def test_schema_sdl_text_order_alias_01(self):
        # Test that alias contents are in order

        ordered_statements = (
            ["using (1);"]
            + TestSDLTextFromSchema.annotation_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "alias Foo {\n"
                + ''.join(
                    ' ' * 4 + s + '\n'
                    for s in shuffled_statements
                ) +
            "}",

            "module default {\n"
            "    alias Foo {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in ordered_statements
                    ) +
            "    };\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "};",
        )

    def test_schema_sdl_text_order_annotation_01(self):
        # Test that annotation contents are in order

        ordered_statements = (
            TestSDLTextFromSchema.annotation_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "abstract annotation Foo {\n"
                + ''.join(
                    ' ' * 4 + s + '\n'
                    for s in shuffled_statements
                ) +
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    abstract annotation Foo {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in ordered_statements
                    ) +
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_constraint_01(self):
        # Test that abstract constraint contents are in order

        ordered_statements = (
            [
                "errmessage := 'Oh no!';",
                "using (true);",
            ]
            + TestSDLTextFromSchema.annotation_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "abstract constraint Foo {\n"
            + ''.join(
                ' ' * 4 + s + '\n'
                for s in shuffled_statements
            ) +
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    abstract constraint Foo {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in ordered_statements
                    ) +
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_constraint_02(self):
        # Test that object constraint contents are in order

        ordered_statements = (
            [
                "errmessage := 'Oh no!';",
            ]
            + TestSDLTextFromSchema.annotation_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "type Foo {\n"
            "    property n -> int64;\n"
            "    constraint expression on (true) {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in shuffled_statements
                    ) +
            "    };"
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    type Foo {\n"
            "        constraint std::expression on (true) {\n"
                        + ''.join(
                            ' ' * 12 + s + '\n'
                            for s in ordered_statements
                        ) +
            "        };\n"
            "        property n: std::int64;\n"
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_constraint_03(self):
        # Test that pointer constraint contents are in order

        ordered_statements = (
            [
                "errmessage := 'Oh no!';",
            ]
            + TestSDLTextFromSchema.annotation_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "type Foo {\n"
            "    property n -> int64 {\n"
            "        constraint expression on (true) {\n"
                        + ''.join(
                            ' ' * 12 + s + '\n'
                            for s in shuffled_statements
                        ) +
            "        };"
            "    };"
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    type Foo {\n"
            "        property n: std::int64 {\n"
            "            constraint std::expression on (true) {\n"
                            + ''.join(
                                ' ' * 16 + s + '\n'
                                for s in ordered_statements
                            ) +
            "            };\n"
            "        };\n"
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_function_01(self):
        # Test that function contents are in order

        ordered_statements = (
            ["volatility := 'Immutable';"]
            + TestSDLTextFromSchema.annotation_statements
            + ["using (true);"]
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "function Foo() -> std::bool {\n"
                + ''.join(
                    ' ' * 4 + s + '\n'
                    for s in shuffled_statements
                ) +
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    function Foo() -> std::bool {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in ordered_statements
                    ) +
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_global_01(self):
        # Test that function non-computed global are in order

        ordered_statements = (
            ["default := true;"]
            + TestSDLTextFromSchema.annotation_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "global Foo -> std::bool {\n"
                + ''.join(
                    ' ' * 4 + s + '\n'
                    for s in shuffled_statements
                ) +
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    global Foo -> std::bool {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in ordered_statements
                    ) +
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_global_02(self):
        # Test that function computed global are in order

        ordered_statements = (
            ["using (true);"]
            + TestSDLTextFromSchema.annotation_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "global Foo {\n"
                + ''.join(
                    ' ' * 4 + s + '\n'
                    for s in shuffled_statements
                ) +
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    global Foo {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in ordered_statements
                    ) +
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_index_01(self):
        # Test that index contents are in order

        ordered_statements = (
            TestSDLTextFromSchema.annotation_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "type Foo {\n"
            "    index on (true) {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in shuffled_statements
                    ) +
            "    };\n"
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    type Foo {\n"
            "        index on (true) {\n"
                        + ''.join(
                            ' ' * 12 + s + '\n'
                            for s in ordered_statements
                        ) +
            "        };\n"
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_link_01(self):
        # Test that abstract link contents are in order

        ordered_statements = (
            [
                "extending default::Base;",
                "readonly := true;",
            ]
            + TestSDLTextFromSchema.annotation_statements
            + TestSDLTextFromSchema.exclusive_constraint_statements
            + TestSDLTextFromSchema.expression_constraint_statements
            + TestSDLTextFromSchema.index_statements_caps
            + TestSDLTextFromSchema.index_statements_nums
            + [
                "index on (@a);",
                "index on (@b);",
                "index on (@c);",
                "index on (@d);",
            ]
            + TestSDLTextFromSchema.index_statements_except
            + [
                "property a: std::int64;",
                "property b := (1);",
                "property c: std::int64;",
                "property d := (1);",
            ]
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "abstract link Base;\n"
            "abstract link Foo {\n"
                + ''.join(
                    ' ' * 4 + s + '\n'
                    for s in shuffled_statements
                ) +
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    abstract link Base;\n"
            "    abstract link Foo {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in ordered_statements
                    ) +
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_link_02(self):
        # Test that non-computed concrete link contents are in order

        ordered_statements = (
            [
                "extending default::Base;",
                "on source delete allow;",
                "on target delete restrict;",
                "default := (select default::Bar limit 1);",
                "readonly := true;",
            ]
            + TestSDLTextFromSchema.annotation_statements
            + TestSDLTextFromSchema.exclusive_constraint_statements
            + TestSDLTextFromSchema.expression_constraint_statements
            + TestSDLTextFromSchema.index_statements_caps
            + TestSDLTextFromSchema.index_statements_nums
            + [
                "index on (@a);",
                "index on (@b);",
                "index on (@c);",
                "index on (@d);",
            ]
            + TestSDLTextFromSchema.index_statements_except
            + [
                "property a: std::int64;",
                "property b := (1);",
                "property c: std::int64;",
                "property d := (1);",
            ]
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "abstract link Base;\n"
            "type Bar;\n"
            "type Foo {\n"
            "    link bar -> Bar {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in shuffled_statements
                    ) +
            "    };\n"
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    abstract link Base;\n"
            "    type Bar;\n"
            "    type Foo {\n"
            "        link bar: default::Bar {\n"
                        + ''.join(
                            ' ' * 12 + s + '\n'
                            for s in ordered_statements
                        ) +
            "        };\n"
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_link_03(self):
        # Test that computed concrete link contents are in order

        ordered_statements = (
            [
                "on source delete allow;",
                "on target delete restrict;",
                "using (select default::Bar limit 1);",
                "readonly := true;",
            ]
            + TestSDLTextFromSchema.annotation_statements
            + TestSDLTextFromSchema.exclusive_constraint_statements
            + TestSDLTextFromSchema.expression_constraint_statements
            + TestSDLTextFromSchema.index_statements_caps
            + TestSDLTextFromSchema.index_statements_nums
            + [
                "index on (@a);",
                "index on (@b);",
                "index on (@c);",
                "index on (@d);",
            ]
            + TestSDLTextFromSchema.index_statements_except
            + [
                "property a: std::int64;",
                "property b := (1);",
                "property c: std::int64;",
                "property d := (1);",
            ]
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "type Bar;\n"
            "type Foo {\n"
            "    link bar -> Bar {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in shuffled_statements
                    ) +
            "    };\n"
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    type Bar;\n"
            "    type Foo {\n"
            "        link bar {\n"
                        + ''.join(
                            ' ' * 12 + s + '\n'
                            for s in ordered_statements
                        ) +
            "        };\n"
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_module_01(self):
        # Test that module contents are in order

        ordered_statements = [
            "alias AliasA := (1);",
            "alias AliasB := (1);",
            "alias AliasC := (1);",
            "alias AliasD := (1);",
            "abstract annotation AnnotationA;",
            "abstract annotation AnnotationB;",
            "abstract annotation AnnotationC;",
            "abstract annotation AnnotationD;",
            "abstract constraint ConstraintA;",
            "abstract constraint ConstraintB;",
            "abstract constraint ConstraintC;",
            "abstract constraint ConstraintD;",
            "function FunctionA() -> std::bool using (true);",
            "function FunctionB() -> std::bool using (true);",
            "function FunctionC() -> std::bool using (true);",
            "function FunctionD() -> std::bool using (true);",
            "global GlobalA := (1);",
            "global GlobalB := (1);",
            "global GlobalC := (1);",
            "global GlobalD := (1);",
            "abstract link LinkA;",
            "abstract link LinkB;",
            "abstract link LinkC;",
            "abstract link LinkD;",
            "abstract property PropertyA;",
            "abstract property PropertyB;",
            "abstract property PropertyC;",
            "abstract property PropertyD;",
            "abstract scalar type AScalarA extending std::int64;",
            "abstract scalar type AScalarB extending std::int64;",
            "abstract scalar type AScalarC extending std::int64;",
            "abstract scalar type AScalarD extending std::int64;",
            "scalar type ScalarA extending std::int64;",
            "scalar type ScalarB extending std::int64;",
            "scalar type ScalarC extending std::int64;",
            "scalar type ScalarD extending std::int64;",
            "abstract type ATypeA;",
            "abstract type ATypeB;",
            "abstract type ATypeC;",
            "abstract type ATypeD;",
            "type TypeA;",
            "type TypeB;",
            "type TypeC;",
            "type TypeD;",
        ]
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            ''.join(
                ' ' * 4 + s + '\n'
                for s in shuffled_statements
            ),

            "module default {\n"
                + ''.join(
                    ' ' * 4 + s + '\n'
                    for s in ordered_statements
                ) +
            "};",
        )

    def test_schema_sdl_text_order_module_02(self):
        # Test that sdl text sorts sub modules.
        ordered_names = [
            chr(ord('A') + c)
            for c in range(10)
        ]
        shuffled_names = ordered_names[:]
        random.Random(1).shuffle(shuffled_names)

        self._assert_sdl_text_from_schema(
            ''.join(
                'module ' + name + ' {};\n'
                for name in shuffled_names
            ),

            'module default {};\n'
            + ''.join(
                'module default::' + name + ' {};\n'
                for name in ordered_names
            ),
        )

    def test_schema_sdl_text_order_property_01(self):
        # Test that abstract property contents are in order

        ordered_statements = (
            [
                "extending default::Base;",
                "readonly := true;",
            ]
            + TestSDLTextFromSchema.annotation_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "abstract property Base;\n"
            "abstract property Foo {\n"
                + ''.join(
                    ' ' * 4 + s + '\n'
                    for s in shuffled_statements
                ) +
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    abstract property Base;\n"
            "    abstract property Foo {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in ordered_statements
                    ) +
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_property_02(self):
        # Test that non-computed concrete property contents are in order

        ordered_statements = (
            [
                "extending default::Base;",
                "default := 1;",
                "readonly := true;",
            ]
            + TestSDLTextFromSchema.annotation_statements
            + TestSDLTextFromSchema.exclusive_constraint_statements
            + TestSDLTextFromSchema.expression_constraint_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "abstract property Base;\n"
            "type Foo {\n"
            "    property bar -> std::int64 {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in shuffled_statements
                    ) +
            "    };\n"
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    abstract property Base;\n"
            "    type Foo {\n"
            "        property bar: std::int64 {\n"
                        + ''.join(
                            ' ' * 12 + s + '\n'
                            for s in ordered_statements
                        ) +
            "        };\n"
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_property_03(self):
        # Test that computed concrete property contents are in order

        ordered_statements = (
            [
                "using (1);",
                "readonly := true;",
            ]
            + TestSDLTextFromSchema.annotation_statements
            + TestSDLTextFromSchema.exclusive_constraint_statements
            + TestSDLTextFromSchema.expression_constraint_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "type Foo {\n"
            "    property bar -> std::int64 {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in shuffled_statements
                    ) +
            "    };\n"
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    type Foo {\n"
            "        property bar {\n"
                        + ''.join(
                            ' ' * 12 + s + '\n'
                            for s in ordered_statements
                        ) +
            "        };\n"
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_scalar_01(self):
        # Test that type contents are in order

        ordered_statements = (
            TestSDLTextFromSchema.annotation_statements
            + TestSDLTextFromSchema.expression_constraint_statements
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "scalar type Foo extending std::int64 {\n"
                + ''.join(
                    ' ' * 4 + s + '\n'
                    for s in shuffled_statements
                ) +
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    scalar type Foo extending std::int64 {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in ordered_statements
                    ) +
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_type_01(self):
        # Test that type contents are in order

        ordered_statements = (
            TestSDLTextFromSchema.annotation_statements
            + TestSDLTextFromSchema.access_policy_statements
            + TestSDLTextFromSchema.exclusive_constraint_statements
            + TestSDLTextFromSchema.expression_constraint_statements
            + TestSDLTextFromSchema.index_statements_caps
            + [
                "index on (.a);",
                "index on (.b);",
                "index on (.c);",
            ]
            + TestSDLTextFromSchema.index_statements_nums
            + TestSDLTextFromSchema.index_statements_except
            + [
                "link b: default::Bar;",
                "link d := (select default::Bar limit 1);",
                "link f: default::Bar;",
                "link h := (select default::Bar limit 1);",
                "property a: std::int64;",
                "property c := (1);",
                "property e: std::int64;",
                "property g := (1);",
            ]
        )
        shuffled_statements = ordered_statements[:]
        random.Random(1).shuffle(shuffled_statements)

        self._assert_sdl_text_from_schema(
            "abstract annotation AnnotationA;\n"
            "abstract annotation AnnotationB;\n"
            "abstract annotation AnnotationC;\n"
            "abstract annotation AnnotationD;\n"
            "type Bar;\n"
            "type Foo {\n"
                + ''.join(
                    ' ' * 4 + s + '\n'
                    for s in shuffled_statements
                ) +
            "}",

            "module default {\n"
            "    abstract annotation AnnotationA;\n"
            "    abstract annotation AnnotationB;\n"
            "    abstract annotation AnnotationC;\n"
            "    abstract annotation AnnotationD;\n"
            "    type Bar;\n"
            "    type Foo {\n"
                    + ''.join(
                        ' ' * 8 + s + '\n'
                        for s in ordered_statements
                    ) +
            "    };\n"
            "};",
        )

    def test_schema_sdl_text_order_type_02(self):
        # Test that sdl text sorts pointers.
        ordered_names = [
            chr(ord('A') + c)
            for c in range(10)
        ]
        shuffled_names = ordered_names[:]
        random.Random(1).shuffle(shuffled_names)

        statements = [
            (
                'type Foo {\n',
                '    property X: std::int64;',
                '};',
            ),
            (
                'type Bar; type Foo {\n',
                '    link X: default::Bar;',
                '};',
            ),
            (
                'type Bar; type Foo { link bar: default::Bar {\n',
                '    property X: std::int64;',
                '};};',
            ),
        ]

        for prefix, statement, suffix in statements:
            self._assert_sdl_text_from_schema(
                prefix
                + ''.join(
                    statement.replace('X', name) + '\n'
                    for name in shuffled_names
                )
                + suffix,
                'module default {\n'
                + prefix
                + ''.join(
                    ' ' * 4 + statement.replace('X', name) + '\n'
                    for name in ordered_names
                )
                + suffix
                + '};',
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
