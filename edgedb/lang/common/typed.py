##
# Copyright (c) 2011-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import abc
import builtins
import collections

__all__ = 'TypedList', 'TypedDict', 'OrderedTypedDict'


class TypedCollectionMeta(abc.ABCMeta):
    def __new__(mcls, name, bases, dct, **kwargs):
        cls = super().__new__(mcls, name, bases, dct)

        for arg in cls._TYPE_ARGS:
            try:
                arg_value = kwargs[arg]
            except KeyError:
                raise TypeError(
                    '{!r} class argument is required for {!r}'.format(
                        arg, cls))

            if arg_value is not None and not isinstance(
                    arg_value, builtins.type):
                raise ValueError('type expected for {!r} class, '
                                 'got object {!r}'.format(cls, arg_value))

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
                raise ValueError(
                    'cannot instantiate typed collection {!r} '
                    'without "type"'.format(self))

            setattr(self, arg, _type)

    def _check_type(self, value, _type, name):
        if not isinstance(value, _type):
            raise ValueError(
                '{!r} accepts only {} of type {!r}, got {!r}'.format(
                    type(self), name, _type, type(value)))


class AbstractTypedSequence(AbstractTypedCollection, type=None):
    _TYPE_ARGS = ('type', )

    def _check_item(self, value):
        return AbstractTypedCollection._check_type(
            self, value, self.type, 'items')

    def _check_items(self, lst):
        if isinstance(lst, TypedList) and issubclass(lst.type, self.type):
            return

        for item in lst:
            self._check_item(item)


class AbstractTypedSet(AbstractTypedCollection, type=None):
    _TYPE_ARGS = ('type', )

    def _check_item(self, value):
        return AbstractTypedCollection._check_type(
            self, value, self.type, 'items')

    def _check_items(self, set):
        if isinstance(set, TypedSet) and issubclass(set.type, self.type):
            return

        for item in set:
            self._check_item(item)


class AbstractTypedMapping(
        AbstractTypedCollection, keytype=None, valuetype=None):
    _TYPE_ARGS = ('keytype', 'valuetype')

    def _check_key(self, key):
        AbstractTypedCollection._check_type(self, key, self.keytype, 'keys')

    def _check_value(self, value):
        AbstractTypedCollection._check_type(
            self, value, self.valuetype, 'values')

    def _check_values(self, dct):
        for key, value in dct.items():
            self._check_key(key)
            self._check_value(value)


class _AbstractTypedDict(AbstractTypedMapping, keytype=None, valuetype=None):
    _base_dict_cls = None

    def __init__(self, *args, **kwargs):
        """
        :param kwargs: Initial values.
        """

        AbstractTypedCollection.__init__(self)
        self.__class__._base_dict_cls.__init__(self)

        if len(args) == 1:
            self.update(args[0])
        elif len(args) > 1:
            msg = 'TypedDict expected at most 1 arguments, got {}'
            raise TypeError(msg.format(len(args)))

        if kwargs:
            self.update(kwargs)

    def __setitem__(self, key, value):
        self._check_key(key)
        self._check_value(value)
        super().__setitem__(key, value)


class TypedDict(
        _AbstractTypedDict, collections.UserDict, keytype=None,
        valuetype=None):
    """Dict-like mapping with typed keys and values.

    .. code-block:: pycon

        >>> class StrIntMapping(TypedDict, keytype=str, valuetype=int):
        ...    pass

        >>> dct = StrIntMapping()
        >>> dct['foo'] = 42

        >>> dct['foo'] = 'spam'
        ValueError
    """

    _base_dict_cls = collections.UserDict


class OrderedTypedDict(
        _AbstractTypedDict, collections.OrderedDict, keytype=None,
        valuetype=None):
    _base_dict_cls = collections.OrderedDict


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
        if isinstance(i, slice):
            self._check_items(item)
        else:
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


class TypedSet(AbstractTypedSet, collections.MutableSet, type=None):
    """Set of one-type only elements.  All other properties and interface is
    the same as for the :py:class:`builtins.set`.

    .. code-block:: pycon

        >>> class IntSet(TypedSet, type=int):
        ...     pass

        >>> tl = IntSet()

        >>> tl.add(1)

        >>> tl.add('2')
        ValueError
    """

    def __init__(self, inititerable=None):
        AbstractTypedSet.__init__(self)
        if inititerable is not None:
            inititerable = set(inititerable)
            self._check_items(inititerable)
            self._data = inititerable
        else:
            self._data = set()

    def __contains__(self, item):
        return item in self._data

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def add(self, item):
        self._check_item(item)
        self._data.add(item)

    def discard(self, item):
        return self._data.discard(item)

    def __and__(self, other):
        self._check_items(other)

        if isinstance(other, TypedSet):
            return self.__class__(other._data & self._data)
        elif isinstance(other, collections.Set):
            return self.__class__(other & self._data)
        return self.__class__(set(other) & self._data)

    __rand__ = __and__

    def __or__(self, other):
        self._check_items(other)

        if isinstance(other, TypedSet):
            return self.__class__(other._data | self._data)
        elif isinstance(other, collections.Set):
            return self.__class__(other | self._data)
        return self.__class__(set(other) | self._data)

    __ror__ = __or__

    def __sub__(self, other):
        self._check_items(other)

        if isinstance(other, TypedSet):
            return self.__class__(self._data - other._data)
        elif isinstance(other, collections.Set):
            return self.__class__(self._data - other)
        return self.__class__(self._data - set(other))

    def __rsub__(self, other):
        self._check_items(other)

        if isinstance(other, TypedSet):
            return self.__class__(other._data - self._data)
        elif isinstance(other, collections.Set):
            return self.__class__(other - self._data)
        return self.__class__(set(other) - self._data)

    def __xor__(self, other):
        self._check_items(other)
        if isinstance(other, TypedSet):
            return self.__class__(self._data ^ other._data)
        elif isinstance(other, collections.Set):
            return self.__class__(self._data ^ other)
        return self.__class__(self._data ^ set(other))

    __rxor__ = __xor__

    def isdisjoint(self, other):
        return self._data.isdisjoint(other)

    def clear(self):
        return self._data.clear()

    def pop(self, item):
        return self._data.pop(item)

    def remove(self, item):
        return self._data.remove(item)

    def __ior__(self, other):
        self._check_items(other)
        self._data |= set(other)
        return self

    def __iand__(self, other):
        self._check_items(other)
        self._data &= set(other)
        return self

    def __ixor__(self, other):
        self._check_items(other)
        self._data ^= set(other)
        return self

    def __isub__(self, other):
        self._check_items(other)
        self._data -= set(other)
        return self

    def __str__(self):
        return str(self._data)

    update = __ior__
    difference_update = __isub__
    symmetric_difference_update = __ixor__
    intersection_update = __iand__
