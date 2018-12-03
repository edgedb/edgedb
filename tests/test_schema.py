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


from edb.lang import _testbase as tb

from edb.lang.schema import error as s_err
from edb.lang.schema import pointers as s_pointers


class TestSchema(tb.BaseSchemaTest):
    def test_schema_inherited_01(self):
        """
            type UniqueName:
                property name -> str:
                    constraint exclusive

            type UniqueName_2 extending UniqueName:
                inherited property name -> str:
                    constraint exclusive
        """

    @tb.must_fail(s_err.SchemaError,
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

    @tb.must_fail(s_err.SchemaError,
                  'test::name cannot be declared `inherited`',
                  position=46)
    def test_schema_inherited_03(self):
        """
            type UniqueName:
                inherited property name -> str
        """

    @tb.must_fail(s_err.SchemaError,
                  'invalid link target, expected object type, got ScalarType',
                  position=54)
    def test_schema_bad_link_01(self):
        """
            type Object:
                link foo -> str
        """

    @tb.must_fail(s_err.SchemaError,
                  'invalid link target, expected object type, got ScalarType',
                  position=51)
    def test_schema_bad_link_02(self):
        """
            type Object:
                link foo := 1 + 1
        """

    @tb.must_fail(s_err.SchemaError,
                  'invalid property target, expected primitive type, '
                  'got ObjectType',
                  position=58)
    def test_schema_bad_prop_01(self):
        """
            type Object:
                property foo -> Object
        """

    @tb.must_fail(s_err.SchemaError,
                  'invalid property target, expected primitive type, '
                  'got ObjectType',
                  position=55)
    def test_schema_bad_prop_02(self):
        """
            type Object:
                property foo := (SELECT Object)
        """

    @tb.must_fail(s_err.SchemaError,
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
