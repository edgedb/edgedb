##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.json._encoder import Encoder as CEncoder
from semantix.utils.json.encoder import Encoder as PyEncoder

from json import dumps as std_dumps
import time
import random
import marshal
from collections import OrderedDict
from decimal import Decimal


import gc

class JsonBenchmark:
    def __init__(self, c, python, marshal, test_c_binary):
        self.test_c = c
        self.test_python = python
        self.test_marshal = marshal
        self.test_c_binary = test_c_binary

    def timing_test(self, obj, num_loops):

        gc.collect()
        gc.collect()
        before_objects = len(gc.get_objects())

        tsecbase = 0
        tstart = time.clock()
        try:
            for _ in range(num_loops):
                std_dumps(obj)
        except TypeError:
            print("  std json:     failed to serialize")
        else:
            tend = time.clock()
            tsecbase = round(tend-tstart,5)
            print ("  std json: ", repr(tsecbase).rjust(7), "sec,   ", \
                   repr(int(num_loops/tsecbase)).rjust(7), "req/sec")

        if self.test_python:
            tstart = time.clock()
            for _ in range(num_loops):
                PyEncoder().dumps(obj)
            tend = time.clock()
            tsec = round(tend-tstart,5)
            print ("   my json: ", repr(tsec).rjust(7), "sec,   ", \
                   repr(int(num_loops/tsec)).rjust(7), "req/sec")

        if self.test_c:
            tstart = time.clock()
            for _ in range(num_loops):
                CEncoder().dumps(obj)
            tend = time.clock()
            tsec = round(tend-tstart,5)
            ratio = round(tsecbase/tsec,1)
            print (" my c json: ", repr(tsec).rjust(7), "sec,   ", \
                   repr(int(num_loops/tsec)).rjust(7), "req/sec  ( " + repr(ratio) + "x )")

            tstart = time.clock()
            encoder = CEncoder()
            for _ in range(num_loops):
                encoder.dumps(obj)
            tend = time.clock()
            tsec = round(tend-tstart,5)
            ratio = round(tsecbase/tsec,1)
            print (" my c 1ini: ", repr(tsec).rjust(7), "sec,   ", \
                   repr(int(num_loops/tsec)).rjust(7), "req/sec  ( " + repr(ratio) + "x )")

        if self.test_c_binary:
            tstart = time.clock()
            for _ in range(num_loops):
                CEncoder().dumpb(obj)
            tend = time.clock()
            tsec = round(tend-tstart,5)
            ratio = round(tsecbase/tsec,1)
            print ("my c dumpb: ", repr(tsec).rjust(7), "sec,   ", \
                   repr(int(num_loops/tsec)).rjust(7), "req/sec  ( " + repr(ratio) + "x )")

        if self.test_marshal:
            tstart = time.clock()
            try:
                for _ in range(num_loops):
                    marshal.dumps(obj)
            except ValueError:
                print ("   marshal:     failed to serialize")
            else:
                tend = time.clock()
                tsec = round(tend-tstart,5)
                print ("   marshal: ", repr(tsec).rjust(7), "sec,   ", \
                       repr(int(num_loops/tsec)).rjust(7), "req/sec")

        gc.collect()
        after_objects = len(gc.get_objects())

        assert before_objects == after_objects

    def run(self):

        print ("\n\n-[ timing tests ]------------------------------")

        #-----------------------
        print ("Array with 256 short ascii strings:")
        testObject1 = []
        for _ in range(256):
            testObject1.append("A pretty long string which is in a list")

        self.timing_test(testObject1, 30000)

        #-----------------------
        print ("Array with 2048 3-char ascii strings:")
        testObject1b = []
        for _ in range(2048):
            testObject1b.append("abc")

        self.timing_test(testObject1b, 2000)

        #-----------------------
        print ("Array with 256 long ascii strings:")
        testObject2 = []
        for _ in range(256):
            testObject2.append("abcabczzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")

        assert std_dumps(testObject2, separators=(',',':')) == PyEncoder().dumps(testObject2)
        assert std_dumps(testObject2, separators=(',',':')) == CEncoder().dumps(testObject2)

        self.timing_test(testObject2, 5000)

        #-----------------------
        print ("Array with 256 long utf-8 strings:")
        testObject3 = []
        for _ in range(256):
            testObject3.append("نظام الحكم سلطاني وراثي في الذكور من ذرية السيد تركي بن سعيد بن سلطان ويشترط فيمن يختار لولاية الحكم من بينهم ان يكون مسلما رشيدا عاقلا ًوابنا شرعيا لابوين عمانيين ")

        assert std_dumps(testObject3, separators=(',',':')) == PyEncoder().dumps(testObject3)
        assert std_dumps(testObject3, separators=(',',':')) == CEncoder().dumps(testObject3)

        self.timing_test(testObject3, 2000)

        #-----------------------
        print ("Medium complex object:")

        user        = { "userId": 3381293, "age": 213, "username": "johndoe", "fullname": "John Doe the Second", "isAuthorized": True, "liked": 31231.31231202, "approval": 31.1471, "jobs": [ 1, 2 ], "currJob": None }
        friends     = [ user, user, user, user, user, user, user, user ]
        testObject4 = [ [user, friends],  [user, friends],  [user, friends],  [user, friends],  [user, friends],  [user, friends]]

        assert std_dumps(testObject4, separators=(',',':')) == PyEncoder().dumps(testObject4)
        assert std_dumps(testObject4, separators=(',',':')) == CEncoder().dumps(testObject4)

        self.timing_test(testObject4, 10000)

        #-----------------------
        print ("Array with 256 doubles:")
        testObject5 = []
        for _ in range(256):
            testObject5.append(10000000 * random.random())

        self.timing_test(testObject5, 10000)

        #-----------------------
        print ("Array with 256 ints:")
        testObject5b = []
        for _ in range(256):
            testObject5b.append(int(10000000 * random.random()))

        self.timing_test(testObject5b, 20000)

        #-----------------------
        print ("Array with 256 small ints:")
        testObject5c = []
        for _ in range(256):
            testObject5b.append(int(10000 * random.random()))

        self.timing_test(testObject5c, 200000)

        #-----------------------
        print ("Array with 256 Decimals:")

        testObject5d = []
        for _ in range(256):
            testObject5d.append(Decimal(str(random.random()*100000)))

        self.timing_test(testObject5d, 8000)

        #-----------------------
        print ("Array with 256 True values:")
        testObject6 = []
        for _ in range(256):
            testObject6.append(True)

        self.timing_test(testObject6, 80000)

        #-----------------------
        print ("Array with 256 False values:")
        testObject6b = []
        for _ in range(256):
            testObject6b.append(False)

        self.timing_test(testObject6b, 80000)

        #-----------------------
        print ("Array with 256 dict{string, int} pairs:")
        testObject7 = []
        for _ in range(256):
            testObject7.append({str(random.random()*20): int(random.random()*1000000)})

        self.timing_test(testObject7, 8000)

        #-----------------------
        print ("Array with 256 dict-based{string, int} pairs:")

        class DerivedDict(dict):
            pass
        testObject7b = []
        for _ in range(256):
            testObject7b.append(DerivedDict({str(random.random()*20): int(random.random()*1000000)}))

        self.timing_test(testObject7b, 8000)

        #-----------------------
        print ("Array with 256 orderedDict{string, int} pairs:")
        testObject7b = []
        for _ in range(256):
            d = {str(random.random()*20): int(random.random()*1000000), str(random.random()*20): int(random.random()*1000000), str(random.random()*20): int(random.random()*1000000), str(random.random()*20): int(random.random()*1000000)}
            ordered_d = OrderedDict(sorted(d.items(), key=lambda t: t[0]))
            testObject7b.append(ordered_d)

        self.timing_test(testObject7b, 1000)

        #-----------------------
        print ("Dict with 256 arrays with 256 dict{string, int} pairs:")
        testObject8 = {}
        for _ in range(256):
            arrays = []
            for _ in range(256):
                arrays.append({str(random.random()*20): int(random.random()*1000000)})
            testObject8[str(random.random()*20)] = arrays

        assert std_dumps(testObject8, separators=(',',':')) == PyEncoder().dumps(testObject8)
        assert std_dumps(testObject8, separators=(',',':')) == CEncoder().dumps(testObject8)

        self.timing_test(testObject8, 50)


def main(*, c=True, python=False, marshal=False, test_c_binary=False):
    JsonBenchmark(c, python, marshal, test_c_binary).run()


if __name__ == '__main__':
    main()
