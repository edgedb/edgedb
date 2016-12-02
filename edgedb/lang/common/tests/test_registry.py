##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import gc
import unittest

from edgedb.lang.common.registry import WeakObjectRegistry


class Obj:
    pass


class RegistryTests(unittest.TestCase):

    def test_common_registry_weak(self):
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
