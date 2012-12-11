##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang.yaml.validator.tests.base import SchemaTest, raises, result


class TestImports(SchemaTest):
    @staticmethod
    def setup_class(cls):
        cls.schema = cls.get_schema('imports.Schema')

    @result(expected_result={'test0': {'test1': 1, 'test2': 'str2'}, 'test3': '3'})
    def test_validator_imports1(self):
        """
        test0:
            test1: 1
            test2: str2
        test3: '3'
        """

    @raises(Exception, 'expected integer')
    def test_validator_imports2(self):
        """
        test0:
            test1: wrong
            test2: 2
        test3: '3'
        """
