##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from semantix.utils.lang.yaml import validator
from semantix.utils.lang.yaml.validator.tests.base import SchemaTest, raises, result


class TestTypes(SchemaTest):
    @staticmethod
    def setup_class(cls):
        cls.schema = cls.get_schema('types.Schema')

    @raises(validator.SchemaValidationError, 'expected none')
    def test_validator_types_none_fail1(self):
        """
        none: '12'
        """

    @result(key='none', value=None)
    def test_validator_types_none_result(self):
        """
        none:
        """

    @raises(validator.SchemaValidationError, 'expected integer')
    def test_validator_types_int_fail1(self):
        """
        int: '12'
        """

    @raises(validator.SchemaValidationError, 'expected integer')
    def test_validator_types_int_fail2(self):
        """
        int: 123.2
        """

    @result(key='int', value=31415)
    def test_validator_types_int_result(self):
        """
        int: 31415
        """

    @raises(validator.SchemaValidationError, 'expected number (int or float)')
    def test_validator_types_number_fail1(self):
        """
        number: [123, 1]
        """

    @result(key='number', value=31415)
    def test_validator_types_number_int_result(self):
        """
        number: 31415
        """

    @result(key='number', value=31415.2)
    def test_validator_types_number_float_result(self):
        """
        number: 31415.2
        """

    @raises(validator.SchemaValidationError, 'expected text (number or str)')
    def test_validator_types_text_fail1(self):
        """
        text: [123, 1]
        """

    @result(key='text', value='31415')
    def test_validator_types_text_int_result(self):
        """
        text: 31415
        """

    @result(key='text', value='31415.123')
    def test_validator_types_text_float_result(self):
        """
        text: 31415.123
        """

    @result(key='bool', value=True)
    def test_validator_types_bool_yes_result(self):
        """
        bool: yes
        """

    @result(key='bool', value=True)
    def test_validator_types_bool_True_result(self):
        """
        bool: True
        """

    @result(key='bool', value=True)
    def test_validator_types_bool_true_result(self):
        """
        bool: true
        """

    @result(key='bool', value=False)
    def test_validator_types_bool_yes_result2(self):
        """
        bool: no
        """

    @result(key='bool', value=False)
    def test_validator_types_bool_True_result2(self):
        """
        bool: false
        """

    @raises(validator.SchemaValidationError, 'expected bool')
    def test_validator_types_bool_fail1(self):
        """
        bool: 1
        """

    @raises(validator.SchemaValidationError, 'expected bool')
    def test_validator_types_bool_fail2(self):
        """
        bool: 'yes'
        """

    @raises(validator.SchemaValidationError, 'mapping expected')
    def test_validator_types_map_fail1(self):
        """
        dict: 'WRONG'
        """

    @raises(validator.SchemaValidationError, 'unexpected key "wrongkey"')
    def test_validator_types_map_fail2(self):
        """
        dict:
            wrongkey: 1
        """

    @result(key='dict', value={'test1': 3, 'test2': 'a'})
    def test_validator_types_map_defaults(self):
        """
        dict:
        """

    @raises(validator.SchemaValidationError, 'the number of elements in mapping must not be less than 2')
    def test_validator_types_map_constraints1(self):
        """
        fdict:
            a: "1"
        """

    @raises(validator.SchemaValidationError, 'the number of elements in mapping must not exceed 3')
    def test_validator_types_map_constraints2(self):
        """
        fdict:
            a: "1"
            b: "2"
            c: "3"
            d: "4"
        """

    @result(key='fdict', value={'a': "1", 'b': "2"})
    def test_validator_types_map_constraints_ok(self):
        """
        fdict:
            a: "1"
            b: "2"
        """

    @raises(validator.SchemaValidationError, 'duplicate mapping key "A"')
    def test_validator_types_map_duplicate_key_check(self):
        """
        fdict:
            A: "1"
            A: "2"
        """

    @result(key='fdict', value={'a': "1", ('b', 'c'): "2"})
    def test_validator_types_map_nonscalar_key(self):
        """
        fdict:
            a: "1"
            [b, c]: "2"
        """

    @result(key='minmax', value=3)
    def test_validator_types_int_minmax(self):
        """
        minmax: 3
        """

    @raises(validator.SchemaValidationError, 'range-min validation failed')
    def test_validator_types_int_minmax_fail(self):
        """
        minmax: 2
        """

    @raises(validator.SchemaValidationError, 'range-max-ex validation failed')
    def test_validator_types_int_minmax_fail2(self):
        """
        minmax: 20
        """

    @result(key='odict', value=collections.OrderedDict([('A', 1), ('B', 2), ('C', 3), ('D', 4)]))
    def test_validator_types_ordered_map(self):
        """
        odict:
            A: 1
            B: 2
            C: 3
            D: 4
        """
