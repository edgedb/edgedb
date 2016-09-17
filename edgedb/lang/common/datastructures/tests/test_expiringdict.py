##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import time
from edgedb.lang.common.datastructures.expiringdict import ExpiringDict
from edgedb.lang.common.debug import assert_raises


class TestUtilsDSExpiringDict:
    def test_utils_ds_expiringdict_1(self):
        dct = ExpiringDict(default_expiry=0.3)

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

        dct.set('foo', 'bar', expiry=0.1)
        dct.set('spam', 'ham')
        dct.set('key', 'value', expiry=None)
        assert len(dct) == 3
        assert 'foo' in dct
        time.sleep(0.2)
        assert 'foo' not in dct
        assert 'spam' in dct
        time.sleep(0.2)
        assert 'spam' not in dct
        assert dct['key'] == 'value'

    def test_utils_ds_expiringdict_2(self):
        dct = ExpiringDict()
        dct['foo'] = 'bar'
        assert dct['foo'] == 'bar'
        time.sleep(0.1)
        assert dct['foo'] == 'bar'
        del dct['foo']
        assert 'foo' not in dct

        dct.set('foo', 'bar', expiry=0.1)
        del dct['foo']
        assert len(dct) == 0
        time.sleep(0.2)
        assert len(dct) == 0
