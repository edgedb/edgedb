##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import abc
import builtins
import collections

from semantix.utils.datastructures.all import Void


__all__ = 'TypedList', 'TypedDict'


class TypedCollectionMeta(abc.ABCMeta):
    def __new__(mcls, name, bases, dct, **kwargs):
        cls = super().__new__(mcls, name, bases, dct)

        for arg in cls._TYPE_ARGS:
            try:
                arg_value = kwargs[arg]
            except KeyError:
                raise TypeError('{!r} class argument is required for {!r}'.format(arg, cls))

            if arg_value is not None and not isinstance(arg_value, builtins.type):
                raise ValueError('type expected for {!r} class, got object {!r}'. \
                                 format(cls, arg_value))

            setattr(cls, arg, arg_value)

        return cls

    def __init__(cls, name, bases, dct, **kwargs):
        return super().__init__(name, bases, dct)


class AbstractTypedCollection(metaclass=TypedCollectionMeta):
    _TYPE_ARGS = ()

    def __init__(self):
        for arg in self._TYPE_ARGS:
            _type = getattr(self, arg, None)
            if _type is None:
                raise ValueError('cannot instantiate typed collection {!r} without "type"'. \
                                 format(self))

            setattr(self, arg, _type)

    def _check_type(self, value, _type, name):
        if not isinstance(value, _type):
            raise ValueError('{!r} accepts only {} of type {!r}, got {!r}'. \
                             format(type(self), name, _type, type(value)))


class AbstractTypedSequence(AbstractTypedCollection, type=None):
    _TYPE_ARGS = ('type',)

    def _check_item(self, value):
        return AbstractTypedCollection._check_type(self, value, self.type, 'items')

    def _check_items(self, lst):
        if isinstance(lst, TypedList) and issubclass(lst.type, self.type):
            return

        for item in lst:
            self._check_item(item)


class AbstractTypedMapping(AbstractTypedCollection, keytype=None, valuetype=None):
    _TYPE_ARGS = ('keytype', 'valuetype')

    def _check_key(self, key):
        AbstractTypedCollection._check_type(self, key, self.keytype, 'keys')

    def _check_value(self, value):
        AbstractTypedCollection._check_type(self, value, self.valuetype, 'values')

    def _check_values(self, dct):
        for key, value in dct.items():
            self._check_key(key)
            self._check_value(value)


class TypedDict(AbstractTypedMapping, collections.UserDict, keytype=None, valuetype=None):
    """Dict-like mapping with typed keys and values.

    .. code-block:: pycon

        >>> class StrIntMapping(TypedDict, keytype=str, valuetype=int):
        ...    pass

        >>> dct = StrIntMapping()
        >>> dct['foo'] = 42

        >>> dct['foo'] = 'spam'
        ValueError
    """

    def __init__(self, initdict=Void, **kwargs):
        """
        :param kwargs: Initial values.
        """

        AbstractTypedCollection.__init__(self)
        collections.UserDict.__init__(self)

        if initdict is not Void:
            if isinstance(initdict, collections.Mapping):
                self.update(initdict)
            else:
                kwargs['initdict'] = initdict

        if kwargs:
            self.update(kwargs)

    def __setitem__(self, key, value):
        self._check_key(key)
        self._check_value(value)
        super().__setitem__(key, value)


class TypedList(AbstractTypedSequence, collections.UserList, type=None):
    """List of one-type only elements.  All other properties and interface is
    the same as for the :py:class:`builtins.list`.

    .. code-block:: pycon

        >>> class IntList(TypedList, type=int):
        ...     pass

        >>> tl = IntList()

        >>> tl.append(1)

        >>> tl.append('2')
        ValueError
    """

    def __init__(self, initlist=None):
        """
        :param iterable initlist: Values to initialize typed list with.
        """

        AbstractTypedCollection.__init__(self)
        collections.UserList.__init__(self, initlist)

        if initlist is not None:
            self._check_items(self.data)

    def __iter__(self):
        return iter(self.data)

    def __setitem__(self, i, item):
        self._check_item(item)
        self.data[i] = item

    def __add__(self, other):
        self._check_items(other)

        if isinstance(other, collections.UserList):
            return self.__class__(self.data + other.data)
        elif isinstance(other, type(self.data)):
            return self.__class__(self.data + other)
        return self.__class__(self.data + list(other))

    def __radd__(self, other):
        self._check_items(other)

        if isinstance(other, collections.UserList):
            return self.__class__(other.data + self.data)
        elif isinstance(other, type(self.data)):
            return self.__class__(other + self.data)
        return self.__class__(list(other) + self.data)

    def __iadd__(self, other):
        self._check_items(other)
        return super().__iadd__(other)

    def append(self, item):
        self._check_item(item)
        return self.data.append(item)

    def insert(self, i, item):
        self._check_item(item)
        return self.data.insert(i, item)

    def extend(self, other):
        self._check_items(other)
        return super().extend(other)


class IntList(TypedList, type=int):
    """List that contains only ``int`` values"""


class StrList(TypedList, type=str):
    """List that contains only ``str`` values"""
