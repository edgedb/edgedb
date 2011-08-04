##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.slots import SlotsMeta
from semantix.utils.datastructures.all import _Marker
from semantix.utils.functional.types import Checker, FunctionValidator, checktypes, \
                                            ChecktypeExempt, TypeChecker, CombinedChecker


__all__ = 'value',


_std_type = type

class _empty(_Marker):
    pass


class value(ChecktypeExempt, metaclass=SlotsMeta):
    __slots__ = ('_name', '_default', '_value', '_value_context', '_doc', '_validator', '_type',
                 '_bound_to', '_required')

    def __init__(self, default=_empty, *, doc=None, validator=None, type=None):
        self._name = None
        self._value = self._default = default
        self._value_context = None
        self._doc = doc
        self._type = type
        self._bound_to = None

        if validator:
            if not isinstance(validator, Checker):
                validator = Checker.get(validator)

            if type and isinstance(type, _std_type):
                validator = CombinedChecker(TypeChecker(type), validator)

            self._validator = validator

        elif type and isinstance(type, _std_type):
            self._validator = TypeChecker(type)

        else:
            self._validator = None

    doc = property(lambda self: self._doc)
    bound_to = property(lambda self: self._bound_to)
    default = property(lambda self: self._default)
    required = property(lambda self: self._default is _empty)
    name = property(lambda self: self._name)

    def _bind(self, owner):
        self._bound_to = owner

    def _set_type(self, type_):
        assert isinstance(type_, type)
        self._type = type_

    type = property(lambda self: self._type, _set_type)

    def _get_value(self):
        if self._value is _empty:
            raise ValueError('{!r} is a required value'.format(self._name))

        return self._value

    def _set_value(self, value, value_context=None):
        self._value = value
        self._value_context = value_context
        self._validate()

    @checktypes
    def _set_validator(self, validator:Checker):
        self._validator = validator

    def _validate(self):
        if self._validator and self._value is not _empty:
            try:
                FunctionValidator.check_value(self._validator, self._value,
                                              self._name, 'default value')
            except TypeError as ex:
                raise TypeError('Invalid value {!r} for "{}"{}'.format( \
                                self._value, self._name,
                                ' in "{}"'.format(self._value_context \
                                                        if self._value_context else ''))) from ex

    def __get__(self, instance, owner):
        return self._get_value()

    def __set__(self, instance, value):
        raise TypeError('{} is a read-only value'.format(self._name))

    def __delete__(self, instance):
        raise TypeError('{} is a read-only value'.format(self._name))

    def __repr__(self):
        return "<{} at 0x{:x} ({!r})>".format(type(self).__name__, id(self), self._value)
