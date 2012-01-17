##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


# from json import loads as std_loads, dumps as std_dumps
# from semantix.utils.json import _encoder -> _encoder.dumps
# from semantix.utils.json import dumps,dumps_binary

import py.test
from json import loads as std_loads, dumps as std_dumps
from decimal import Decimal
from collections import OrderedDict
from uuid import UUID
import random
from datetime import datetime,tzinfo,timedelta

from semantix.utils.debug import assert_raises

class _BaseJsonEncoderTest:
    encoder = None

    def __new__(cls):
        obj = super().__new__(cls)
        obj.dumps = lambda *args, **kwargs: cls.encoder().dumps(*args, **kwargs)
        obj.dumpb = lambda *args, **kwargs: cls.encoder().dumpb(*args, **kwargs)
        return obj

    def encoder_test(self, obj, encoded, convertable_back = True, matches_default_encoder = True):
        """Tests that our dumps behaves exactly like the default json module
        Should not be used for cases where we don't like the default module behavior
        """
        assert self.dumps(obj) == encoded

        encoded_b = encoded.encode('ascii')
        assert self.dumpb(obj) == encoded_b

        if convertable_back:
            # test decode: not always possible, e.g. for Decimals or integers larger than Java-max
            assert std_loads(self.dumps(obj)) == obj

        if matches_default_encoder:
            # test compliance with standard encoder; use compact encoding (set
            #  separators) - ow lists are encoded with extra spaces between items
            assert std_dumps(obj, separators=(',',':')) == self.dumps(obj)

    def test_utils_json_encoder_literals(self):
        self.encoder_test(None,  'null')

        self.encoder_test(True,  'true')
        self.encoder_test(False, 'false')

        self.encoder_test('foo', '"foo"')
        self.encoder_test('f"o"o', '"f\\"o\\"o"')
        self.encoder_test('foo\nbar', '"foo\\nbar"')
        self.encoder_test('\tf\\oo\n ', '"\\tf\\\\oo\\n "')

        self.encoder_test('\x00', '"\\u0000"')
        self.encoder_test('a\x19b', '"a\\u0019b"')
        self.encoder_test('a\x20b', '"a b"')

        self.encoder_test('\x00a\nbcعمان', '"\\u0000a\\nbc\\u0639\\u0645\\u0627\\u0646"')

        # std json does not escape '/'
        self.encoder_test('/\\"\n\r\t\b\f', '"\/\\\\\\"\\n\\r\\t\\b\\f"', True, False)

        # once a bug was introduced in the c version which passed all other tests but not
        # this one (for characted \u0638) - so need to keep this long UTF string as a
        # "more complicated" utf sample
        long_utf_string = "نظام الحكم سلطاني وراثي في الذكور من ذرية السيد تركي بن سعيد بن سلطان ويشترط فيمن يختار لولاية الحكم من بينهم ان يكون مسلما رشيدا عاقلا ًوابنا شرعيا لابوين عمانيين "
        assert std_dumps(long_utf_string, separators=(',',':')) == self.dumps(long_utf_string)

    def test_utils_json_encoder_numbers(self):
        self.encoder_test(0, '0')
        self.encoder_test(1, '1')
        self.encoder_test(-123456, '-123456')

        self.encoder_test(2.2, '2.2')
        self.encoder_test(1.2345678, '1.2345678')
        self.encoder_test(-1.23456e-123, '-1.23456e-123')

        # 9007199254740992 is the largest possible integer in JavaScript
        self.encoder_test(9007199254740992, '9007199254740992')

        # std module just returns a string representation for
        # all valid python numbers (even for non-jscript representable numbers)
        with assert_raises(ValueError, error_re='out of range'):
            self.encoder_test( 9007199254740992 + 1, None)
        with assert_raises(ValueError, error_re='out of range'):
            self.encoder_test(-9007199254740992 - 1, None)
        with assert_raises(ValueError, error_re='NaN is not supported'):
            self.encoder_test(float('NaN'), None)

        # std module does not support Decimals
        # note: Decimal(1.17) != Decimal("1.17"), for testing need initialization from a string
        self.encoder_test(Decimal("1.17"), '"1.17"', False, False)

        # complex numbers are not JSON serializable
        with assert_raises(TypeError, error_re='not JSON serializable'):
            self.encoder_test(1+2j, None)

    def test_utils_json_encoder_lists(self):
        self.encoder_test([],                '[]')
        self.encoder_test(['abc',True],      '["abc",true]')
        self.encoder_test(['abc',[],[True]], '["abc",[],[true]]')

        # a tuple is converted to the same string as a "similar" list, so no conversion back
        # (but should match the default encoder)
        self.encoder_test((), '[]',                               False, True)
        self.encoder_test((1,), '[1]',                            False, True)
        self.encoder_test((1,2,[1,2,3]), '[1,2,[1,2,3]]',         False, True)
        self.encoder_test(('a','b',('c',1)), '["a","b",["c",1]]', False, True)

        self.encoder_test((99,100,999,1000,9999,10000,99999,100000),
                          '[99,100,999,1000,9999,10000,99999,100000]', False, True)

        # std module does not support sets
        self.encoder_test({1,2,3}, '[1,2,3]', False, False)

        # test max recursion level checks
        with assert_raises(ValueError, error_re='Exceeded maximum allowed recursion level'):
            self.dumps([[[1,2],3],4],1)
        with assert_raises(ValueError, error_re='Exceeded maximum allowed recursion level'):
            self.dumps([[[1,2],3],4],2)
        # this should work
        assert(self.dumps([[[1,2],3],4],3) == '[[[1,2],3],4]')
        # create infinite recursion
        a=[1]
        a[0]=a
        with assert_raises(ValueError, error_re='Exceeded maximum allowed recursion level'):
            self.dumps(a)

        # Python type "bytes" is not JSON serializable according to spec
        with assert_raises(TypeError, error_re='not JSON serializable'):
            self.encoder_test(bytes([1,2,3]), None)

        # bytearrays are not JSON serializable according to spec
        with assert_raises(TypeError, error_re='not JSON serializable'):
            self.encoder_test(bytearray([1,2,3]), '[1,2,3]', False, False)


    def test_utils_json_encoder_dict(self):
        self.encoder_test({}, '{}')
        self.encoder_test({'foo':1, 'bar':2}, '{"foo":1,"bar":2}')
        self.encoder_test({'foo':[1,2], 'bar':[3,4]}, '{"foo":[1,2],"bar":[3,4]}')

        # shold match std encoder, but conversion back converts to lists not tuples
        self.encoder_test({'foo':(1,2), 'bar':(3,4)}, '{"foo":[1,2],"bar":[3,4]}', False, True)

        # std encoder does not support sets
        self.encoder_test({'foo':{1,2}, 'bar':{3,4}}, '{"foo":[1,2],"bar":[3,4]}', False, False)

        # std encoder does nto support OrderedDicts
        d = {'banana': 3, 'apple':4, 'pear': 1, 'orange': 2}
        ordered_d = OrderedDict(sorted(d.items(), key=lambda t: t[0]))
        self.encoder_test( ordered_d, '{"apple":4,"banana":3,"orange":2,"pear":1}', False, False)

        # JSON spec does not support keys which are not a string or a number
        with assert_raises(TypeError, error_re='is not a valid dictionary key'):
            self.encoder_test({1:1}, '{1:1}')
        with assert_raises(TypeError, error_re='is not a valid dictionary key'):
            self.encoder_test({(1,2):'a'}, '{[1,2]:"a"}')

        class DerivedDict(dict):
            pass
        self.encoder_test(DerivedDict({'foo':1, 'bar':2}), '{"foo":1,"bar":2}')

    def test_utils_json_encoder_uuid(self):
        # std encodencoderer does not support UUIDs
        self.encoder_test(UUID('{12345678-1234-5678-1234-567812345678}'),
                         '"12345678-1234-5678-1234-567812345678"',
                         False, False)

    def test_utils_json_encoder_datetime(self):

        dt1 = datetime(2012,2,1,12,20,22,100)
        dt2 = datetime(1990,12,11,1,1,1,0)

        # std encodr does not support datetime
        self.encoder_test(dt1, '"2012-02-01T12:20:22.000100"', False, False)
        self.encoder_test(dt2, '"1990-12-11T01:01:01"', False, False)

        class GMT5(tzinfo):
            def utcoffset(self,dt):
                return timedelta(hours=5,minutes=30)
            def tzname(self,dt):
                return "GMT +5"
            def dst(self,dt):
                return timedelta(0)
        gmt5 = GMT5()
        dt3 = datetime(2012,2,1,12,20,22,100,gmt5)
        self.encoder_test(dt3, '"2012-02-01T12:20:22.000100+05:30"', False, False)

    def test_utils_json_encoder_sx_json(self):

        class Foo:
            def __sx_serialize__(self):
                return 'foo'

        # none of these are suported by std encoder
        self.encoder_test(Foo(), '"foo"', False, False)
        self.encoder_test({Foo():'bar'}, '{"foo":"bar"}', False, False)
        self.encoder_test({'bar':Foo()}, '{"bar":"foo"}', False, False)

    def test_utils_json_default(self):
        class Foo:
            pass

        class Bar:
            pass

        class Encoder(self.encoder):
            def default(self, obj):
                if isinstance(obj, Foo):
                    return ['Foo', 'Foo']
                return super().default(obj)

        assert Encoder().dumps([Foo()]) == '[["Foo","Foo"]]'

        with assert_raises(TypeError, error_re='is not JSON seri'):
            Encoder().dumps(Bar())

    def test_utils_json_c_reftest(self):
        # test for leaked references
        big_list = []
        for _ in range(256):
            d = {str(random.random()*20): int(random.random()*1000000), str(random.random()*20): int(random.random()*1000000), str(random.random()*20): int(random.random()*1000000), str(random.random()*20): int(random.random()*1000000)}
            ordered_d = OrderedDict(sorted(d.items(), key=lambda t: t[0]))
            big_list.append(ordered_d)
        self.dumps(big_list)

        big_list2 = []
        for _ in range(256):
            big_list2.append(str(random.random()*100000))
        self.dumps(big_list2)

    def test_utils_json_integer_output(self):
        for _ in range(5000):
            x = random.randint(-9007199254740992, 9007199254740992)
            assert std_dumps(x) == self.dumps(x)


from ..encoder import Encoder as PyEncoder

class TestPyJsonEncoder(_BaseJsonEncoderTest):
    encoder = PyEncoder


SKIPC = 'False'
CEncoder = PyEncoder
try:
    from .._encoder import Encoder as CEncoder
except ImportError:
    SKIPC = 'True'

@py.test.mark.skipif(SKIPC)
class TestCJsonEncoder(_BaseJsonEncoderTest):
    encoder = CEncoder


def test_utils_json_dump():
    #test bindings
    from semantix.utils.json import dumps, dumpb

    assert dumps(True) == 'true'
    assert dumpb(True) == b'true'
