##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang import _testbase as tb
from edgedb.lang.schema import error as s_err


class TestSchema(tb.BaseSchemaTest):
    def test_schema_inherited_01(self):
        """
            type UniqueName:
                property name -> str:
                    constraint unique

            type UniqueName_2 extending UniqueName:
                inherited property name -> str:
                    constraint unique
        """

    @tb.must_fail(s_err.SchemaError,
                  'test::name must be declared using the `inherited` keyword',
                  position=175)
    def test_schema_inherited_02(self):
        """
            type UniqueName:
                property name -> str:
                    constraint unique

            type UniqueName_2 extending UniqueName:
                property name -> str:
                    constraint unique
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
