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

from edb.lang import _testbase as tb

from edb.lang.schema import links as s_links
from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import pointers as s_pointers


class TestSchema(tb.BaseSchemaLoadTest):
    def test_schema_inherited_01(self):
        """
            type UniqueName:
                property name -> str:
                    constraint exclusive

            type UniqueName_2 extending UniqueName:
                inherited property name -> str:
                    constraint exclusive
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  'test::name must be declared using the `inherited` keyword',
                  position=178)
    def test_schema_inherited_02(self):
        """
            type UniqueName:
                property name -> str:
                    constraint exclusive

            type UniqueName_2 extending UniqueName:
                property name -> str:
                    constraint exclusive
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  'test::name cannot be declared `inherited`',
                  position=46)
    def test_schema_inherited_03(self):
        """
            type UniqueName:
                inherited property name -> str
        """

    @tb.must_fail(errors.InvalidLinkTargetError,
                  'invalid link target, expected object type, got ScalarType',
                  position=54)
    def test_schema_bad_link_01(self):
        """
            type Object:
                link foo -> str
        """

    @tb.must_fail(errors.InvalidLinkTargetError,
                  'invalid link target, expected object type, got ScalarType',
                  position=51)
    def test_schema_bad_link_02(self):
        """
            type Object:
                link foo := 1 + 1
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  'link or property name length exceeds the maximum.*',
                  position=42)
    def test_schema_bad_link_03(self):
        """
            type Object:
                link f123456789_123456789_123456789_123456789_123456789\
_123456789_123456789_123456789 -> Object
        """

    @tb.must_fail(errors.InvalidPropertyTargetError,
                  'invalid property target, expected primitive type, '
                  'got ObjectType',
                  position=58)
    def test_schema_bad_prop_01(self):
        """
            type Object:
                property foo -> Object
        """

    @tb.must_fail(errors.InvalidPropertyTargetError,
                  'invalid property target, expected primitive type, '
                  'got ObjectType',
                  position=55)
    def test_schema_bad_prop_02(self):
        """
            type Object:
                property foo := (SELECT Object)
        """

    @tb.must_fail(errors.SchemaDefinitionError,
                  'link or property name length exceeds the maximum.*',
                  position=42)
    def test_schema_bad_prop_03(self):
        """
            type Object:
                property f123456789_123456789_123456789_123456789_123456789\
_123456789_123456789_123456789 -> str
        """

    @tb.must_fail(errors.InvalidReferenceError,
                  'reference to a non-existent schema item: int',
                  position=58,
                  hint='did you mean one of these: int16, int32, int64?')
    def test_schema_bad_type_01(self):
        """
            type Object:
                property foo -> int
        """

    def test_schema_computable_cardinality_inference_01(self):
        schema = self.load_schema("""
            type Object:
                property foo -> str
                property bar -> str
                property foo_plus_bar := __source__.foo ++ __source__.bar
        """)

        obj = schema.get('test::Object')
        self.assertEqual(
            obj.getptr(schema, 'foo_plus_bar').get_cardinality(schema),
            s_pointers.Cardinality.ONE)

    def test_schema_computable_cardinality_inference_02(self):
        schema = self.load_schema("""
            type Object:
                multi property foo -> str
                property bar -> str
                property foo_plus_bar := __source__.foo ++ __source__.bar
        """)

        obj = schema.get('test::Object')
        self.assertEqual(
            obj.getptr(schema, 'foo_plus_bar').get_cardinality(schema),
            s_pointers.Cardinality.MANY)

    def test_schema_refs_01(self):
        schema = self.load_schema("""
            type Object1
            type Object2:
                link foo -> Object1
            type Object3 extending Object1
            type Object4 extending Object1
            type Object5:
                link bar -> Object2
            type Object6 extending Object4
        """)

        Obj1 = schema.get('test::Object1')
        Obj2 = schema.get('test::Object2')
        Obj3 = schema.get('test::Object3')
        Obj4 = schema.get('test::Object4')
        Obj5 = schema.get('test::Object5')
        Obj6 = schema.get('test::Object6')
        foo = Obj2.getptr(schema, 'foo')
        foo_target = foo.getptr(schema, 'target')
        bar = Obj5.getptr(schema, 'bar')

        self.assertEqual(
            schema.get_referrers(Obj1),
            frozenset({
                foo,         # Object 1 is a Object2.foo target
                foo_target,  # and also a target of its @target property
                Obj3,        # It is also in Object3's bases and mro
                Obj4,        # Likewise for Object4
                Obj6,        # Object6 through its mro
            })
        )

        self.assertEqual(
            schema.get_referrers(Obj1, scls_type=s_objtypes.ObjectType),
            {
                Obj3,        # It is also in Object3's bases and mro
                Obj4,        # Likewise for Object4
                Obj6,        # Object6 through its mro
            }
        )

        self.assertEqual(
            schema.get_referrers(Obj2, scls_type=s_links.Link),
            {
                foo,        # Obj2 is foo's source
                bar,        # Obj2 is bar's target
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
                Obj3,        # It is also in Object3's bases and mro
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
            })
        )

    def test_schema_attribute_inheritance(self):
        schema = self.load_schema("""
            abstract attribute noninh
            abstract inheritable attribute inh

            type Object1:
                attribute noninh := 'bar'
                attribute inh := 'inherit me'

            type Object2 extending Object1
        """)

        Object1 = schema.get('test::Object1')
        Object2 = schema.get('test::Object2')

        self.assertEqual(Object1.get_attribute(schema, 'test::noninh'), 'bar')
        # Attributes are non-inheritable by default
        self.assertIsNone(Object2.get_attribute(schema, 'test::noninh'))

        self.assertEqual(
            Object1.get_attribute(schema, 'test::inh'), 'inherit me')
        self.assertEqual(
            Object2.get_attribute(schema, 'test::inh'), 'inherit me')
