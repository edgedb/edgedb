#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import collections
import collections.abc


class OrderedSet(collections.abc.MutableSet):
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

    update = collections.abc.MutableSet.__ior__
    difference_update = collections.abc.MutableSet.__isub__
    symmetric_difference_update = collections.abc.MutableSet.__ixor__
    intersection_update = collections.abc.MutableSet.__iand__

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
            return '%s()' % (self.__class__.__name__, )
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


class OrderedIndex(OrderedSet, collections.abc.MutableMapping):
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
