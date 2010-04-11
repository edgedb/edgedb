##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types

from semantix.utils.debug import assert_raises
from semantix.utils.config import *
from semantix.utils.functional import checktypes, hybridmethod


class TestUtilsConfig(object):
    def test_utils_config_func_cargs_args(self):
        CHK = 0

        @configurable
        def test(test=carg(0.0, doc="Test Config",
                           validator=lambda arg: isinstance(arg, float) and arg >= 0.0),

                 flag:bool=carg(True)):

            nonlocal CHK

            assert test == CHK
            assert flag

        CHK = 0.0
        test()

        CHK = 1.0
        config._set_value('semantix.utils.config.tests.test_config.test.test', CHK)
        test()

        CHK = 10.0
        test(10.0)

        assert test.__name__ == 'test'
        assert config.semantix.utils.config.tests.test_config.test.flag is True

    def test_utils_config_func_cargs_kwargs(self):
        CHK = 0

        @configurable
        def test2(foo, *,
                 test=carg(0.0, doc="Test Config",
                           validator=lambda arg: isinstance(arg, float) and arg >= 0.0),

                 flag:bool=carg(True)):

            nonlocal CHK

            assert test == CHK
            assert flag

        CHK = 0.0
        test2(1)

        CHK = 1.0
        config._set_value('semantix.utils.config.tests.test_config.test2.test', CHK)
        test2(1)

        CHK = 10.0
        test2(1, test=10.0)

        with assert_raises(TypeError):
            test2(1, 10.0)

    def test_utils_config_overlapping(self):
        with assert_raises(ConfigError):
            @configurable
            def test3(test=carg(1.0)):
                pass

            @configurable
            def test3(test=carg(1.0)):
                pass

    def test_utils_config_readonlyness(self):
        @configurable
        def test4(test=carg(1.0)):
            pass

        with assert_raises(ConfigError):
            config.semantix.utils.config.tests.test_config.test4.test = 10

    def test_utils_config_not_exist_value(self):
        @configurable
        def test5(test=carg(1.0)):
            pass

        with assert_raises(ConfigError):
            config.semantix.utils.config.tests.test_config.test5.fuuuu

    def test_utils_config_reuse(self):
        @configurable
        def test6(test=carg(0.0, doc="Test Config",
                           validator=lambda arg: isinstance(arg, float) and arg >= 0.0),

                 flag:bool=carg(True)):
            pass

        @configurable
        def test7(test=config.semantix.utils.config.tests.test_config.test6.test):
            assert test == 0.0

        test7()

        with assert_raises(ConfigError):
            config.semantix.utils.config.tests.test_config.test7.flag

    def test_utils_config_defaults_validation(self):
        with assert_raises(TypeError):
            @configurable
            def test1000(test:int=carg(0.0)):
                pass

    def test_utils_config_class_methods_base(self):
        @configurable
        class TM:
            def tm(self, a, foo=carg(10)):
                assert isinstance(self, TM)
                assert a == foo

        assert TM.tm.__name__ == 'tm'
        assert isinstance(TM.tm, types.FunctionType)
        t = TM()
        t.tm(10)
        assert isinstance(t.tm, types.MethodType)


        @configurable
        class TM2:
            @classmethod
            def tm(cls, a, foo=carg(10)):
                assert cls is TM2
                assert a == foo

            @staticmethod
            def tm2(a, foo=carg(11)):
                assert a == foo

        TM2.tm(10)
        TM2.tm2(11)

        assert config.semantix.utils.config.tests.test_config.TM2.tm.foo == 10
        assert config.semantix.utils.config.tests.test_config.TM2.tm2.foo == 11

        config._set_value('semantix.utils.config.tests.test_config.TM2.tm2.foo', 4)
        TM2.tm(10)
        TM2.tm2(4)

    def test_utils_config_class_methods_checktypes(self):
        @configurable
        @checktypes
        class CTM1:
            def tm(self, a:int, foo=carg(10)):
                assert isinstance(self, CTM1)
                assert a == foo

            @classmethod
            def tm2(cls, a:int, foo=carg(10)):
                assert cls is CTM1
                assert a == foo

        CTM1.tm2(10)
        c = CTM1()
        c.tm(10)
        c.tm2(10)

        with assert_raises(TypeError):
            c.tm('1')

        with assert_raises(TypeError):
            c.tm2('1')

        with assert_raises(TypeError):
            CTM1.tm2('1')

        ########

        @checktypes
        @configurable
        class CTM2:
            def tm(self, a:int, foo=carg(10)):
                assert isinstance(self, CTM2)
                assert a == foo

        c = CTM2()
        c.tm(10)

        with assert_raises(TypeError):
            c.tm('2')

    def test_utils_config_func_checktypes(self):
        @configurable
        @checktypes
        def test_chk1(foo:int, *, bar:str=carg('')):
            assert foo == 123
            assert bar == ''

        test_chk1(123)

        with assert_raises(TypeError):
            test_chk1('123')

        ########

        @checktypes
        @configurable
        def test_chk2(foo:int, *, bar:str=carg('')):
            assert foo == 123
            assert bar == ''

        test_chk2(123)

        with assert_raises(TypeError):
            test_chk2('123')

    def test_utils_config_class_methods_hybrid(self):
        @configurable
        @checktypes
        class HTM1:
            @hybridmethod
            def tm(smth, a:int, foo=carg(10)):
                assert a == foo

        c = HTM1()
        c.tm(10)

        with assert_raises(TypeError):
           c.tm('1')

        HTM1.tm(10)

        with assert_raises(TypeError):
           HTM1.tm('1')

    def test_utils_config_class_properties(self):
        @configurable
        class TMP:
            param = cvar('a', type=str)

            def test(self):
                assert self.param == 'a'

        assert TMP.param == 'a'

        TMP().test()

        @configurable
        class TMP2:
            __slots__ = ()
            tmp = 1
            tmp2 = cvar(333)
            tmp3 = config.semantix.utils.config.tests.test_config.TMP.param

        assert TMP2.tmp is 1
        assert TMP2.tmp2 == 333

        config._set_value('semantix.utils.config.tests.test_config.TMP2.tmp2', 4)
        assert TMP2.tmp2 == 4

        assert TMP2.tmp3 == TMP.param == 'a'


    def test_utils_config_yaml(self):
        from semantix.utils.config.tests import app

        @configurable
        def test_yaml1(foo=carg(1), bar:str=carg('')):
            assert foo == 123
            assert bar == 'zzz'

        test_yaml1()

        with assert_raises(TypeError):
            @configurable
            def test_yaml2(*, a:int=carg(0)):
                pass

    def test_utils_config_loadflow(self):
        config._set_value('semantix.utils.config.tests.test_config.YML.foo', 33)

        with assert_raises(ConfigError):
            config.semantix.utils.config.tests.test_config.YML.foo

        @configurable
        class YML:
            foo = cvar(-1, type=int)

        assert YML.foo == 33
        assert config.semantix.utils.config.tests.test_config.YML.foo == 33

        config._set_value('semantix.utils.config.tests.test_config.YML.foo', 133)

        assert YML.foo == 133
        assert config.semantix.utils.config.tests.test_config.YML.foo == 133
