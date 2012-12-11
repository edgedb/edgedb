##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang.yaml import validator
from metamagic.utils.lang.yaml.validator.tests.base import SchemaTest, raises, result


class TestPerson(SchemaTest):
    @staticmethod
    def setup_class(cls):
        cls.schema = cls.get_schema('person.Schema')

    @raises(validator.SchemaValidationError, 'list expected')
    def test_validator_root_sequence(self):
        """
        name: Yuri
        phone: 416-509-280
        """

    @raises(validator.SchemaValidationError, 'pattern validation failed')
    def test_validator_pattern(self):
        """
        - name: Yuri
          phone: 416-509-280
        """

    @raises(validator.SchemaValidationError, 'range-max-ex validation failed')
    def test_validator_range_max(self):
        """
        - name: "123456789012345678901"
          phone: 416-509-2801
        """

    @result([{'phone': '416-509-2801', 'name': 'John', 'sex': 'male'}])
    def test_validator_default1(self):
        """
        - name: "John"
          phone: 416-509-2801
        """

    @raises(validator.SchemaValidationError, 'enum validation failed')
    def test_validator_enum1(self):
        """
        - name: "John"
          phone: 416-509-2801
          sex: unknown
        """

    @raises(validator.SchemaValidationError, 'unique key "name", value "Anya" is already used')
    def test_validator_unique(self):
        """
        - name: "Anya"
          phone: 416-509-2801
          sex: female
        - name: "Anya"
          phone: 416-509-2801
          sex: female
        """

    @result([{'phone': '416-509-2801', 'name': 'Anya', 'sex': 'female'},
             {'phone': '416-509-2101', 'name': 'John Doe', 'sex': 'male'}])
    def test_validator_person_seq1(self):
        """
        - name: "Anya"
          phone: 416-509-2801
          sex: female
        - name: "John Doe"
          phone: 416-509-2101
        """
