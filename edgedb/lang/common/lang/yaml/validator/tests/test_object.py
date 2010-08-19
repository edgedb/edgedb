##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import yaml

from semantix.utils.lang.meta import Object, ObjectError
from semantix.utils.lang.yaml import validator
from semantix.utils.lang.yaml.validator.tests.base import SchemaTest, result, raises


class A(Object):
    def __init__(self, *, name=None, description=None, context=None, data=None):
        super().__init__(context, data)
        self.name = name
        self.description = description

    def __eq__(self, other):
        return isinstance(other, A) and other.name == self.name and other.description == self.description

    def construct(self):
        self.name = self.data['name']
        self.description = self.data['description']

    @classmethod
    def get_yaml_validator_config(cls):
        return {'customfield': {'type': 'int', 'range': {'min': 3, 'max-ex': 20}}}


class Bad(object):
    pass


class ScalarContainer(Object):
    def __init__(self, *, scalar=None, context=None, data=None):
        super().__init__(context, data)
        self.scalar = scalar

    def __eq__(self, other):
        return isinstance(other, ScalarContainer) and other.scalar == self.scalar \
               and isinstance(self.scalar, Scalar) and isinstance(other.scalar, Scalar)

    def construct(self):
        self.scalar = self.data['scalar']


class Scalar(Object, str):
    def __new__(cls, context, data):
        return str.__new__(cls, data)


class CustomValidator(Object):
    def construct(self):
        name = self.data['name']
        description = self.data['description']

        if name != description:
            raise ObjectError('name must be equal to description')

        self.name = name
        self.description = description


class TestObject(SchemaTest):
    @staticmethod
    def setup_class(cls):
        cls.schema = cls.get_schema('object.Schema')

    @result(key='test1', value=A(name='testname', description='testdescription'))
    def test_validator_object(self):
        """
        test1:
            name: testname
            description: testdescription
        """

    @raises(yaml.constructor.ConstructorError, "while constructing a Python object")
    def test_validator_object_fail(self):
        """
        fail:
            name: fail
        """

    @raises(ObjectError, "name must be equal to description")
    def test_validator_object_custom_validation(self):
        """
        customvalidation:
            name: custom
            description: validation
        """

    @result(key='classtype', value=A(name='a', description='b'))
    def test_validator_object_class_type(self):
        """
        classtype:
            semantix.utils.lang.yaml.validator.tests.test_object.A:
                name: a
                description: b
        """

    @raises(validator.SchemaValidationError, 'range-max-ex validation failed')
    def test_validator_object_class_type_validation(self):
        """
        classtype:
            semantix.utils.lang.yaml.validator.tests.test_object.A:
                name: a
                description: b
                customfield: 21
        """

    @result(key='properdefault', value=ScalarContainer(scalar=Scalar(None, 'default scalar')))
    def test_validator_object_default(self):
        """
        properdefault: {}
        """
