##
# Copyright (c) 2013-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import unittest
import uuid

from edgedb.lang.common.persistent_hash import persistent_hash
from edgedb.lang.common.persistent_hash import PersistentlyHashable


class PersistentHashTests(unittest.TestCase):
    def test_common_persistent_hash_1(self):
        assert persistent_hash(1) == persistent_hash(1)
        assert persistent_hash((1, '2')) == persistent_hash((1, '2'))

        u = uuid.uuid4()
        assert persistent_hash(u) != persistent_hash(uuid.uuid4())
        assert persistent_hash(u) != persistent_hash(u.hex)
        assert persistent_hash(u) == persistent_hash(u)

    def test_common_persistent_hash_2(self):
        class Foo:
            def persistent_hash(self):
                return 123

        val = frozenset(('aaaa', 'bbb', 21, 33.123, b'aaa', True, None, Foo()))
        exp = 2133544778164784224964840886447826668319307184951404439004
        self.assertEqual(persistent_hash(val), exp)

    def test_common_persistent_hash_3(self):
        class NoPH:
            pass

        with self.assertRaisesRegex(TypeError, 'un.*hashable'):
            persistent_hash(NoPH())

        self.assertFalse(issubclass(NoPH, PersistentlyHashable))
        self.assertFalse(isinstance(NoPH, PersistentlyHashable))
        self.assertFalse(isinstance(NoPH(), PersistentlyHashable))

        class PH:
            def persistent_hash(self):
                return 123

        self.assertTrue(issubclass(PH, PersistentlyHashable))
        self.assertFalse(isinstance(PH, PersistentlyHashable))
        self.assertTrue(isinstance(PH(), PersistentlyHashable))

        self.assertEqual(persistent_hash(PH()), 123)
