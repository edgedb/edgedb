##
# Copyright (c) 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import collections


class LRUDict(collections.UserDict):
    """Size-limited last-recently-used ordered dict."""

    def __init__(self, *, size):
        assert size > 1
        self.size = size
        super().__init__()
        self.data = collections.OrderedDict()

    def __getitem__(self, key):
        result = self.data[key]
        self.data.move_to_end(key)
        return result

    def __setitem__(self, key, value):
        while len(self.data) + 1 > self.size:
            self.data.popitem(False)
        self.data[key] = value
