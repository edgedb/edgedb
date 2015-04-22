##
# Copyright (c) 2008-2012, 2015 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import sys


class _MarkerMeta(type):
    def __repr__(cls):
        repr_ = cls.__repr__
        if repr_ is object.__repr__:
            repr_ = type.__repr__
        return repr_(cls)

    def __str__(cls):
        return cls.__name__


class _Marker(metaclass=_MarkerMeta):
    def __init__(self):
        raise TypeError('%r cannot be instantiated' % self.__class__.__name__)

    def __str__(cls):
        return cls.__name__

    __repr__ = __str__


class _VoidMeta(_MarkerMeta):
    def __bool__(cls):
        return False


class Void(_Marker, metaclass=_VoidMeta):
    pass


class SetView(collections.Set):
    def __init__(self, set):
        self._set = set

    def __contains__(self, item):
        return item in self._set

    def __iter__(self):
        return iter(self._set)

    def __len__(self):
        return len(self._set)


class OrderedSet(collections.MutableSet):
    def __init__(self, iterable=None):
        self.map = collections.OrderedDict()
        if iterable is not None:
            self.update(iterable)

    @staticmethod
    def key(item):
        return item

    def add(self, key, *, last=None):
        k = self.key(key)
        self.map[k] = key
        if last is not None:
            self.map.move_to_end(k, last=last)

    def discard(self, key):
        key = self.key(key)
        self.map.pop(key, None)

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
        return reversed(self.map.values())

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return len(self) == len(other) and self.map == other.map
        elif other is None:
            return False
        else:
            return not self.isdisjoint(other)

    def copy(self):
        return self.__class__(self)

    def clear(self):
        self.map.clear()


class ExtendedSet(collections.MutableSet):
    def __init__(self, *args, key=hash, **kwargs):
        self._set = set()
        self._key = key
        self._index = set()

        if args:
            self.update(args[0])

    def __contains__(self, item):
        return self._key(item) in self._index

    def __iter__(self):
        return iter(self._set)

    def __len__(self):
        return len(self._set)

    def add(self, item):
        self._index.add(self._key(item))
        self._set.add(item)

    def discard(self, item):
        self._index.discard(self._key(item))
        self._set.discard(item)

    def remove(self, item):
        self._index.remove(self._key(item))
        self._set.remove(item)

    def clear(self):
        self._index.clear()
        self._set.clear()

    def copy(self):
        return self.__class__(self, key=self._key)

    update = collections.MutableSet.__ior__
    difference_update = collections.MutableSet.__isub__
    symmetric_difference_update = collections.MutableSet.__ixor__
    intersection_update = collections.MutableSet.__iand__


class OrderedIndex(OrderedSet, collections.MutableMapping):
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
        try:
            return self.map[key]
        except KeyError:
            return self.map[self.key(key)]

    def __setitem__(self, item):
        key = self.key(item)
        self.map[key] = item

    def __delitem__(self, item):
        key = self.key(item)
        del self.map[key]

    def __mm_serialize__(self):
        return list(self.map.values())


class Record(type):
    def __new__(mcls, name, fields, default=None):
        dct = {'_fields___': fields, '_default___': default}
        bases = (RecordBase,)
        return super(Record, mcls).__new__(mcls, name, bases, dct)

    def __init__(mcls, name, fields, default):
        pass


class RecordBase:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if k not in self.__class__._fields___:
                msg = '__init__() got an unexpected keyword argument %s' % k
                raise TypeError(msg)
            setattr(self, k, v)

        for k in set(self.__class__._fields___) - set(kwargs.keys()):
            setattr(self, k, self.__class__._default___)


    def __setattr__(self, name, value):
        if name not in self.__class__._fields___:
            msg = '%s has no attribute %s' % (self.__class__.__name__, name)
            raise AttributeError(msg)
        super().__setattr__(name, value)

    def __eq__(self, tup):
        if not isinstance(tup, tuple):
            return NotImplemented

        return tuple(self) == tup

    def __getitem__(self, index):
        return getattr(self, self.__class__._fields___[index])

    def __iter__(self):
        for name in self.__class__._fields___:
            yield getattr(self, name)

    def __len__(self):
        return len(self.__class__._fields___)

    def items(self):
        for name in self.__class__._fields___:
            yield name, getattr(self, name)

    def keys(self):
        return iter(self.__class__._fields___)

    def __str__(self):
        f = ', '.join(str(v) for v in self)
        if len(self) == 1:
            f += ','
        return '(%s)' % f

    __repr__ = __str__


class xvalue:
    """xvalue is a "rich" value that can have an arbitrary set of additional
    attributes attached to it."""


    __slots__ = ('value', 'attrs')

    def __init__(self, value, **attrs):
        self.value = value
        self.attrs = attrs

    def __repr__(self):
        attrs = ', '.join('%s=%r' % (k, v) for k, v in self.attrs.items())
        return '<xvalue "%r"; %s>' % (self.value, attrs)

    def __eq__(self, other):
        if not isinstance(other, xvalue):
            return NotImplemented

        return self.value == other.value and self.attrs == other.attrs

    def __hash__(self):
        return hash((self.value, frozenset(self.attrs.items())))

    __str__ = __repr__


class StrSingleton(str):
    def __new__(cls, val=''):
        name = cls._map.get(val)
        if name:
            ns = sys.modules[cls.__module__].__dict__
            return ns.get(name, str.__new__(cls, val))
        else:
            raise ValueError('invalid value for %s: %s' % (cls.__name__, val))

    @classmethod
    def keys(cls):
        return iter(cls._map.values())

    @classmethod
    def values(cls):
        return iter(cls._map.keys())
