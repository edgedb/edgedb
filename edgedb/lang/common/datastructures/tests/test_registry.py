##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import gc

from metamagic.utils.datastructures.registry import WeakObjectRegistry
from metamagic.utils.debug import assert_raises


class Obj:
    pass


class TestDatastructMultidict:
    def test_utils_ds_registry_weak(self):
        r = WeakObjectRegistry()

        obj = Obj()
        r[obj] = 'test'

        assert obj in r
        assert len(r) == 1
        assert r[obj] == 'test'

        del obj
        gc.collect()

        assert len(r) == 0

        def _test():
            nonhashable = set()

            r[nonhashable] = 'nonhashable'
            assert len(r) == 1

            for k, v in r.items():
                if v == 'nonhashable':
                    del nonhashable
                    gc.collect()

        _test()

        assert len(r) == 0

        obj1 = Obj()
        r[obj1] = 'Obj'

        del obj1
        gc.collect()

        assert len(r) == 0
