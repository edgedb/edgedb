##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import abc
import bisect
import collections


class GenericWrapperMeta(abc.ABCMeta):

    def __init__(cls, name, bases, dict):
        super().__init__(name, bases, dict)

        write_methods = set()
        read_methods = set()

        for i in range(0, len(cls.__mro__) - 1):
            lst = getattr(cls, '_' + cls.__mro__[i].__name__ + '__read_methods', set())
            read_methods.update(lst)
            lst = getattr(cls, '_' + cls.__mro__[i].__name__ + '__write_methods', set())
            write_methods.update(lst)

        cls.read_methods = read_methods
        cls.write_methods = write_methods

        cls._sethook('read')
        cls._sethook('write')


class GenericWrapper(object, metaclass=GenericWrapperMeta):
    write_methods = ()
    read_methods = ()

    @classmethod
    def _sethook(cls, hookname):
        hookmethod = '_' + hookname + '_hook'
        if hasattr(cls, hookmethod):
            hook = getattr(cls, hookmethod)
            methods = getattr(cls, hookname + '_methods')

            if hook:
                for method in methods:
                    setattr(cls, method + '_orig', getattr(cls, method, None))
                    setattr(cls, method, cls._gethook(hook, method))

    @classmethod
    def _gethook(cls, hook, method):
        def hookbody(self, *args, **kwargs):
            original = getattr(cls, method + '_orig', None)
            if not original:
                raise NotImplementedError
            hook(self, method, *args, **kwargs)
            return original(self, *args, **kwargs)

        return hookbody


class Container(GenericWrapper):
    __read_methods = ('__contains__',)


class Hashable(GenericWrapper):
    __read_methods = ('__hash__',)


class Iterable(GenericWrapper):
    __read_methods = ('__iter__',)


class Sized(GenericWrapper):
    __read_methods = ('__len__',)


class Callable(GenericWrapper):
    __read_methods = ('__call__',)


class Sequence(Sized, Iterable, Container):
    __read_methods = ('__getitem__', '__reversed__', 'index', 'count')


class MutableSequence(Sequence):
    __write_methods = ('__setitem__', '__delitem__', 'insert', 'append', 'reverse', 'extend', 'pop', 'remove',
                       '__iadd__')


class Set(Sized, Iterable, Container):
    __read_methods = ('__le__', '__lt__', '__eq__', '__ne__', '__gt__', '__ge__', '__and__', '__or__',
                      '__sub__', '__xor__', 'isdisjoint')

class MutableSet(Set):
    __write_methods = ('add', 'discard', 'clear', 'pop', 'remove', '__ior__', '__iand__', '__ixor__',
                       '__isub__')


class Mapping(Sized, Iterable, Container):
    __read_methods = ('__getitem__', '__contains__', 'keys', 'items', 'values', 'get', '__eq__', '__ne__')


class MutableMapping(Mapping):
    __write_methods = ('__setitem__', '__delitem__', 'pop', 'popitem', 'clear', 'update')


class SetWrapper(set, MutableSet):

    __write_methods = ('update', 'intersection_update', 'difference_update', 'symmetric_difference_update')

    __read_methods = ('issubset', 'issuperset', 'union', 'intersection', 'difference', 'symmetric_difference')

    original_base = set


class ListWrapper(list, MutableSequence):
    original_base = list


class SortedList(list):
    """
    A list that maintains it's order by a given criteria
    """

    def __init__(self, data=None):
        if data is None:
            data = []

        super(SortedList, self).__init__(data)

        self.sort()
        self.appending = False
        self.extending = False
        self.inserting = False

    def append(self, x):
        if not self.appending:
            self.appending = True
            bisect.insort_right(self, x)
            self.appending = False
        else:
            super(SortedList, self).append(x)

    def extend(self, L):
        if not self.extending:
            self.extending = True
            for x in L:
                bisect.insort_right(self, x)
            self.extending = False
        else:
            super(SortedList, self).extend(L)

    def insert(self, i, x):
        if not self.inserting:
            self.inserting = True
            bisect.insort_right(self, x)
            self.inserting = False
        else:
            super(SortedList, self).insert(i, x)


class BaseOrderedSet(collections.MutableSet):

    def __init__(self, iterable=None):
        self.map = collections.OrderedDict()
        if iterable is not None:
            self.update(iterable)

    @staticmethod
    def key(item):
        return item

    def __del__(self):
        self.clear()

    def add(self, key):
        k = self.key(key)
        if k not in self.map:
            self.map[k] = key

    def discard(self, key):
        key = self.key(key)
        if key in self.map:
            self.map.pop(key)

    def popitem(self, last=True):
        key, value = self.map.popitem(last)
        return key

    update = collections.MutableSet.__ior__
    difference_update = collections.MutableSet.__isub__
    symmetric_difference_update = collections.MutableSet.__ixor__
    intersection_update = collections.MutableSet.__iand__

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        key = self.key(key)
        return key in self.map

    def __iter__(self):
        return iter(list(self.map.values()))

    def __reversed__(self):
        return reversed(self.map)

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return len(self) == len(other) and list(self) == list(other)
        return not self.isdisjoint(other)

    def copy(self):
        return self.__class__(self)


class OrderedSet(BaseOrderedSet, collections.MutableSequence):

    def __getitem__(self, key):
        # XXX
        return list(self.map.keys())[key]

    def __setitem__(self, slice):
        raise NotImplementedError

    def __delitem__(self, slice):
        raise NotImplementedError

    append = BaseOrderedSet.add

    def insert(self, pos, item):
        raise NotImplementedError

    def index(self, key):
        #XXX
        return list(self.map.keys()).index(key)

    def count(self, key):
        return int(key in self)


class OrderedSetWrapper(OrderedSet, MutableSet, MutableSequence):
    original_base = OrderedSet


class OrderedIndex(BaseOrderedSet, collections.MutableMapping):
    def __init__(self, iterable=None, *, key=None):
        self.key = key or hash
        super().__init__(iterable)

    def keys(self):
        return self.map.keys()

    def values(self):
        return self.map.values()

    def items(self):
        return self.map.items()

    def __getitem__(self, key):
        return self.map.get(key, self.map[self.key(key)])

    def __setitem__(self, item):
        key = self.key(item)
        self.map[key] = item

    def __delitem__(self, item):
        key = self.key(item)
        del self.map[key]


class StrictOrderedIndex(OrderedIndex):
    def __setitem__(self, item):
        if item in self:
            raise ValueError('item %s is already present in the index' % item)
        super().__setitem__(item)


class Record(type):
    def __new__(mcls, name, fields, default=None):
        dct = {'fields': fields, 'default': default}
        bases = (RecordBase,)
        return super(Record, mcls).__new__(mcls, name, bases, dct)

    def __init__(mcls, name, fields, default):
        pass


class RecordBase:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if k not in self.__class__.fields:
                raise TypeError('__init__() got an unexpected keyword argument %s' % k)
            setattr(self, k, v)

        for k in set(self.__class__.fields) - set(kwargs.keys()):
            setattr(self, k, self.__class__.default)
