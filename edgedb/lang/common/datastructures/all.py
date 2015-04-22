##
# Copyright (c) 2008-2012, 2015 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections


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
