##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import unittest

from edgedb.lang.common.multidict import Multidict


class MultidictTests(unittest.TestCase):

    def test_common_multidict(self):
        a = Multidict((('a', 'b'), ('c', 'dd'), ('c', 'c'), ('z', (1, 2, 3)),
                       ('z', 2)))

        assert a['a'] == 'b'
        assert a['c'] == 'dd'
        assert a['z'] == 1

        assert 'z' in a
        assert 'zzzz' not in a

        assert a.getlist('a') == ['b']
        assert a.getlist('c') == ['dd', 'c']
        assert a.getlist('z') == [1, 2, 3, 2]

        with self.assertRaises(KeyError):
            a['v']

        assert a.get('v') is None
        assert a.getlist('v') is None
        assert a.getlist('v', 'ddd') == ['ddd']
        assert a.getlist('v', None) is None

        a['z'] = 'zz'
        assert a['z'] == 'zz'

        a['z'] = 'zz', 'zzz'
        assert a['z'] == 'zz'
        assert a.getlist('z') == ['zz', 'zzz']

        tmp = [1, 2]
        a['z'] = tmp
        assert a['z'] == 1
        tmp[0] = 3
        assert a['z'] == 1

        assert a.get('z') == 1
        assert a.get('foo', 'bar') == 'bar'

        assert list(a.itemlists()) == [('a', ['b']), ('c', ['dd', 'c']), (
            'z', [1, 2])]

        assert list(a.keys()) == ['a', 'c', 'z']
        assert list(a.values()) == ['b', 'dd', 1]

        b = Multidict({'a': 1, 'b': [1, 2]})
        assert b['a'] == 1
        assert b['b'] == 1
        assert b.getlist('b') == [1, 2]
        assert b.getlist('a') == [1]

        b.add('b', 3)
        assert b.getlist('b') == [1, 2, 3]

        b.add('c', 1)
        assert b.getlist('c') == [1]
        b.add('c', [2, 3])
        assert b.getlist('c') == [1, [2, 3]]

        b['c'] = [1, 2]
        assert b['c'] == 1
        assert b.getlist('c') == [1, 2]

        old = b['c']
        assert b.pop('c') == old == 1
        assert b.pop('c') == 2
        assert b.pop('c', default=None) is None
        assert b.pop('c', default=22) is 22
        with self.assertRaises(KeyError):
            b.pop('c')
        assert 'c' not in b
