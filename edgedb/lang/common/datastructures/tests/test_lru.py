##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.datastructures.lru import LRUDict
from metamagic.utils.debug import assert_raises


class TestUtilsDSLru:
    def test_utils_ds_lru_1(self):
        d = LRUDict(size=2)

        d['a'] = 1
        d['b'] = 2
        d['c'] = 3

        assert len(d) == 2
        assert d['b'] == 2 and d['c'] == 3

        d['b'] = 2
        d['d'] = 4

        assert len(d) == 2
        assert d['b'] == 2 and d['d'] == 4

        with assert_raises(KeyError):
            d['c']
