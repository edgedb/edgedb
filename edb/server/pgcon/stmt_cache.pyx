#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


cdef object _LRU_MARKER = object()


cdef class StatementsCache:

    # We use an OrderedDict for LRU implementation.  Operations:
    #
    # * We use a simple `__setitem__` to push a new entry:
    #       `entries[key] = new_entry`
    #   That will push `new_entry` to the *end* of the entries dict.
    #
    # * When we have a cache hit, we call
    #       `entries.move_to_end(key, last=True)`
    #   to move the entry to the *end* of the entries dict.
    #
    # * When we need to remove entries to maintain `max_size`, we call
    #       `entries.popitem(last=False)`
    #   to remove an entry from the *beginning* of the entries dict.
    #
    # So new entries and hits are always promoted to the end of the
    # entries dict, whereas the unused one will group in the
    # beginning of it.

    def __init__(self, *, maxsize):
        if maxsize <= 0:
            raise ValueError(
                f'maxsize is expected to be greater than 0, got {maxsize}')

        self._dict = collections.OrderedDict()
        self._dict_move_to_end = self._dict.move_to_end
        self._dict_get = self._dict.get
        self._maxsize = maxsize

    cdef get(self, key, default):
        o = self._dict_get(key, _LRU_MARKER)
        if o is _LRU_MARKER:
            return default
        self._dict_move_to_end(key)  # last=True
        return o

    cdef needs_cleanup(self):
        return len(self._dict) > self._maxsize

    cdef cleanup_one(self):
        k, _ = self._dict.popitem(last=False)
        return k

    def __getitem__(self, key):
        o = self._dict[key]
        self._dict_move_to_end(key)  # last=True
        return o

    def __setitem__(self, key, o):
        if key in self._dict:
            self._dict[key] = o
            self._dict_move_to_end(key)  # last=True
        else:
            self._dict[key] = o

    def __delitem__(self, key):
        del self._dict[key]

    def __contains__(self, key):
        return key in self._dict

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)
