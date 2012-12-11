##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang.yaml.validator.tests.base import SchemaTest, raises


class TestUnique(SchemaTest):
    @staticmethod
    def setup_class(cls):
        cls.schema = cls.get_schema('unique1.Schema')

    @raises(Exception, 'unique value "test" is already used')
    def test_validator_unique_map_key(self):
        """
        test1:
            - test
            - test
        """

    @raises(Exception, 'unique value "2" is already used')
    def test_validator_unique_map_key2(self):
        """
        test1:
            - test
            - test2
        test2:
            - 2
            - 2
        """

    @raises(Exception, 'enum validation failed')
    def test_validator_unique_map_key_enum_error(self):
        """
        test1:
            - test
            - test2
        test2:
            - 0
            - 0
        """
