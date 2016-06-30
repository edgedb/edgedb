##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.datastructures import immutables
from edgedb.lang.common.debug import assert_raises


class TestDatastructImmutables:
    def test_utils_ds_immutables_frozendict(self):
        import pickle

        mutable = {'a': 'b'}
        immutable = immutables.frozendict(mutable)

        assert immutable['a'] == mutable['a']

        with assert_raises(TypeError):
            immutable['a'] = 2

        with assert_raises(TypeError):
            del immutable['a']

        with assert_raises(TypeError):
            immutable.pop('a')

        with assert_raises(TypeError):
            immutable.setdefault('b', 'c')

        assert immutable['a'] == mutable['a']

        pickled = pickle.loads(pickle.dumps(immutable))
        assert isinstance(pickled, immutables.frozendict)
        assert pickled == immutable

        assert len(immutables.frozendict()) == 0
