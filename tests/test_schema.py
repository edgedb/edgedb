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
                link name -> str:
                    constraint unique

            type UniqueName_2 extending UniqueName:
                inherited link name -> str:
                    constraint unique
        """

    @tb.must_fail(s_err.SchemaError,
                  'test::name must be declared using the `inherited` keyword',
                  position=171)
    def test_schema_inherited_02(self):
        """
            type UniqueName:
                link name -> str:
                    constraint unique

            type UniqueName_2 extending UniqueName:
                link name -> str:
                    constraint unique
        """

    @tb.must_fail(s_err.SchemaError,
                  'test::name cannot be declared `inherited`',
                  position=46)
    def test_schema_inherited_03(self):
        """
            type UniqueName:
                inherited link name -> str
        """
