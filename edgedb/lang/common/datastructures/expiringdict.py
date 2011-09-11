##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import heapq
from datetime import timedelta, datetime


class ExpiringDict(collections.UserDict):
    """A dict-like object with an expiry time on its values.

    >>> dct = ExpiringDict(expiry=1.0)
    >>> dct['foo'] = 'bar'

    And, slightly after a second:
    >>> time.sleep(1.1)
    >>> 'foo' in dct
    ... False
    """

    def __init__(self, *, expiry:float):
        super().__init__()

        if not isinstance(expiry, timedelta):
            if not isinstance(expiry, (int, float)):
                raise ValueError('expected expiry to be float or integer value, got {}:{!r}'. \
                                 format(type(expiry), expiry))
            expiry = timedelta(seconds=expiry)
        self.expiry = expiry
        self.keyheap = []

    def _cleanup(self):
        now = datetime.now()

        if self.keyheap and self.keyheap[0][0] <= now:
            keys_to_delete = []

            while self.keyheap and self.keyheap[0][0] <= now:
                data = heapq.heappop(self.keyheap)
                keys_to_delete.append(data[1])

            for key in keys_to_delete:
                self.__delitem__(key)

    def __delitem__(self, key):
        heap_location = None

        for idx, key_data in enumerate(self.keyheap):
            if key_data[1] == key:
                heap_location = idx

        if heap_location is not None:
            del self.keyheap[heap_location]

        super().__delitem__(key)

    def __setitem__(self, key, value):
        self._cleanup()

        if key in self:
            self.__delitem__(key)

        heapq.heappush(self.keyheap, (datetime.now() + self.expiry, key))
        super().__setitem__(key, value)

    def __getitem__(self, key):
        self._cleanup()
        return super().__getitem__(key)

    def __iter__(self):
        self._cleanup()
        return super().__iter__()

    def __len__(self):
        self._cleanup()
        return super().__len__()
