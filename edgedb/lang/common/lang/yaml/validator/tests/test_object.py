##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import yaml

from semantix.utils.lang.meta import Object, ObjectError
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


class Bad(object):
    pass


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
        cls.schema = cls.get_schema('ymls/object.yml')

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
