##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import functional

class TestUtilsFunctional(object):
    def test_utils_functional_cachedproperty(self):
        CHK = 0

        class Test:
            def __init__(self, val):
                self.value = val

            @functional.cachedproperty
            def square(self):
                nonlocal CHK
                CHK += 1
                return self.value ** 2

        t = Test(10)
        assert t.square + t.square + t.square == 300
        assert CHK == 1

        t2 = Test(20)
        assert t2.square == 400 and t.square == 100
