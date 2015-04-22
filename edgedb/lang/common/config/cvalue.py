##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types
from string import Template

from metamagic.utils.slots import SlotsMeta
from metamagic.utils.datastructures import Marker
from metamagic.utils.functional.types import Checker, FunctionValidator, \
                                            ChecktypeExempt, TypeChecker, CombinedChecker

from .base import ConfigRootNode, _get_head
from .tree import *
from .exceptions import ConfigError


__all__ = 'cvalue',


_std_type = type

class _no_default(Marker):
    pass


class cvalue(ChecktypeExempt, metaclass=SlotsMeta):
    __slots__ = ('_name', '_default', '_doc', '_validator', '_type', '_owner', '_lazy_default')

    def __init__(self, default=_no_default, *, doc=None, validator=None, type=None):
        self._name = None

        if (isinstance(default, types.FunctionType) and
                    (type is None or not isinstance(type, _std_type)
                                or not issubclass(type, types.FunctionType))):
            self._lazy_default = default
            default = _no_default
        else:
            self._lazy_default = None

        self._default = default

        self._doc = doc
        self._type = type

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

    type = property(lambda self: self._type)
    doc = property(lambda self: self._doc)
    default = property(lambda self: self._default)
    required = property(lambda self: self._default is _no_default)
    name = property(lambda self: self._name)
    fullname = property(lambda self: self._owner.__module__ + '.' + \
                                                self._owner.__name__ + '.' + self.name)

    def _set_name(self, name):
        self._name = name
        CvalueRootNode._set_value(CVALUES, self.fullname, self, None)

    def _get_value(self, cls):
        if self._name is None:
            if getattr(cls, '__sx_configurable__', False):
                # The class is configurable and currently is initializing
                # (to be precise, it's between ConfigurableMeta.__new__ and
                # ConfigurableMeta.__init__ calls)
                # The attribute is probably inspected from the metaclass
                #
                return self
            raise ConfigError('Unable to get value of uninitialized cvalue: ' \
                              'no name set {!r}'.format(self),
                              hint='Perhaps, class that hosts the cvalue is not configurable; ' \
                                   'check its metaclass to be a subclass of ConfigurableMeta')

        top_conf_link = _get_head()
        if top_conf_link is not None:
            try:
                # Can cache here, as configs chain is immutable, and we're working
                # from the current top of it to the bottom (very first with statements.)
                #
                return top_conf_link.cache_get((cls, self))
            except LookupError:
                pass

            result = _no_default

            for base in cls.__mro__:
                conf_link = top_conf_link
                while conf_link is not None:
                    fullname = base.__module__ + '.' + base.__name__ + '.' + self._name

                    try:
                        value = ConfigRootNode._get_value(conf_link.node, fullname)
                    except AttributeError:
                        conf_link = conf_link.parent
                    else:
                        cval = value.value
                        if isinstance(cval, Template):
                            try:
                                cval = cval.substitute(conf_link.node.__node_ctx__)
                            except KeyError:
                                raise ConfigError('Exception during template evaluation of {!r} ' \
                                                  'config value'.format(self.fullname))
                        self._validate(cval, fullname, value.context)
                        result = cval
                        break

                if result is not _no_default or base is self._owner:
                    break

            if result is not _no_default:
                top_conf_link.cache_set((cls, self), result)
                return result

        if self._lazy_default is not None:
            value = self._lazy_default()
            self._validate(value, self.fullname, 'lazy default evaluation')
            if top_conf_link is not None:
                top_conf_link.cache_set((cls, self), value)
            return value

        if self._default is _no_default:
            raise ValueError('{!r} is a required config value'.format(self.fullname))

        if top_conf_link is not None:
            top_conf_link.cache_set((cls, self), self._default)

        return self._default

    def _validate(self, value, fullname, context=None):
        if self._validator is not None:
            try:
                FunctionValidator.check_value(self._validator, value,
                                              self._name, 'default value')
            except TypeError as ex:
                ctx = '' if context is None else ' in {!r}'.format(context)
                raise TypeError('Invalid value {!r} for "{}"{}'. \
                                format(value, fullname, ctx)) from ex

    def __get__(self, instance, owner):
        cls = owner
        if instance is not None:
            cls = type(instance)
        return self._get_value(cls)

    def __repr__(self):
        return "<{} at 0x{:x} ({!r})>".format(type(self).__name__, id(self), self._default)


class CvalueContainer(TreeValue):
    __slots__ = ()


class CvalueNode(TreeNode):
    __slots__ = ()


class CvalueRootNode(CvalueNode, TreeRootNode):
    __slots__ = ()

    node_cls = CvalueNode
    value_cls = CvalueContainer


CVALUES = CvalueRootNode('CVALUES')
