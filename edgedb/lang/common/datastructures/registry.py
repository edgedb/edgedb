##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import functools
import weakref


class WeakObjectRegistry(collections.MutableMapping):
    """Weak object mapping class.

    Dictionary entries will be discarded once there are no strong references
    to the key, similar to weakref.WeakKeyDictionary.  Unlike WeakKeyDictionary,
    WeakObjectRegistry allows unhashable objects as keys, since it uses id()
    instead of hash() to derive the keys.
    """

    def __init__(self, mapping=None):
        """
        :param dict mapping: Optional initial values.
        """

        self.data = {}
        self.refs = {}

        def _remove(ref, key, selfref=weakref.ref(self)):
            self = selfref()

            if self is not None:
                if self._iterating:
                    self._pending_removals.append(key)
                else:
                    del self.data[key]
                    del self.refs[key]

        self._remove = _remove

        # _IterationGuard data
        self._pending_removals = []
        self._iterating = set()

        if mapping is not None:
            self.update(mapping)

    def _commit_removals(self):
        # _IterationGuard helper

        pending = self._pending_removals
        while pending:
            key = pending.pop()

            try:
                del self.data[key]
            except KeyError:
                pass
            try:
                del self.refs[key]
            except KeyError:
                pass

    def __contains__(self, key):
        ref = self.refs.get(id(key))
        return ref is not None and ref() is not None

    def __len__(self):
        return len(self.data) - len(self._pending_removals)

    def __getitem__(self, key):
        return self.data[id(key)]

    def get(self, key, default=None):
        return self.data.get(id(key), default)

    def items(self):
        with weakref._IterationGuard(self):
            for key, value in self.data.items():
                ref = self.refs[key]
                obj = ref()
                if obj is not None:
                    yield obj, value

    def keys(self):
        with weakref._IterationGuard(self):
            for key in self.data:
                ref = self.refs[key]
                obj = ref()
                if obj is not None:
                    yield obj

    __iter__ = keys

    def values(self):
        with weakref._IterationGuard(self):
            for key, value in self.data.items():
                ref = self.refs[key]
                obj = ref()
                if obj is not None:
                    yield value

    def __setitem__(self, key, value):
        id_key = id(key)
        self.data[id_key] = value
        self.refs[id_key] = weakref.ref(key, functools.partial(self._remove, key=id_key))

    def __delitem__(self, key):
        key = id(key)
        del self.data[key]
        del self.refs[key]

    def popitem(self):
        while True:
            key, value = self.data.popitem()
            ref = self.refs.pop(key)
            obj = ref()
            if obj is not None:
                return obj, value

    def pop(self, key, *args):
        key = id(key)
        result = self.data.pop(key, *args)
        self.refs.pop(key)
        return result

    def setdefault(self, key, default=None):
        try:
            value = self[key]
        except KeyError:
            self[key] = default
            return default
        else:
            return value

    def update(self, mapping=None, **kwargs):
        if mapping is not None:
            items = getattr(mapping, 'items', None)
            if items is None:
                items = dict(mapping).items

            for key, value in items:
                self[key] = value

        if kwargs:
            self.update(kwargs)

    def copy(self):
        new = WeakObjectRegistry()
        for key, value in self.data.items():
            obj = self.refs[key]()
            if obj is not None:
                new[obj] = value

        return new

    __copy__ = copy

    def __deepcopy__(self, memo):
        from copy import deepcopy

        new = WeakObjectRegistry()
        for key, value in self.data.items():
            obj = self.refs[key]()
            if obj is not None:
                new[obj] = deepcopy(value, memo)

        return new

    def __repr__(self):
        return '<WeakObjectRegistry at %s>' % id(self)
