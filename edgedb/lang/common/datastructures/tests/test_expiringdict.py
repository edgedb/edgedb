##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import time
from semantix.utils.datastructures.expiringdict import ExpiringDict
from semantix.utils.debug import assert_raises

class TestUtilsDSExpiringDict:
    def test_utils_ds_expiringdict_1(self):
        dct = ExpiringDict(expiry=0.3)

        dct['foo'] = 'bar'
        time.sleep(0.4)

        assert len(dct) == 0

        dct['foo'] = 'bar'
        time.sleep(0.1)
        assert len(dct) == 1
        assert 'foo' in dct
        assert dct['foo'] == 'bar'
        time.sleep(0.3)
        with assert_raises(KeyError):
            dct['foo']

        dct['ham'] = 'spam'
        dct['spam'] = 'ham'
        time.sleep(0.2)
        dct['spam'] = 'foobar'
        time.sleep(0.2)
        assert dct['spam'] == 'foobar'
        with assert_raises(KeyError):
            dct['ham']
        assert len(dct) == 1

        dct.update({'a': 'b', 'c': 'd'})
        time.sleep(0.2)
        assert len(dct) == 2
        time.sleep(0.2)
        assert len(dct) == 0
