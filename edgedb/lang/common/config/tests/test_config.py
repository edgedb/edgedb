##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types

from semantix.utils.debug import assert_raises
from semantix.utils.config import *
from semantix.utils.config import set_value, get_cvalue
from semantix.utils.functional import checktypes, hybridmethod


class TestUtilsConfig(object):
    def test_utils_config_func_cargs_args(self):
        CHK = 0

        @configurable
        def test(test=cvalue(0.0, doc="Test Config",
                             validator=lambda arg: isinstance(arg, float) and arg >= 0.0),

                 flag:bool=cvalue(True)):

            nonlocal CHK

            assert test == CHK
            assert flag

        CHK = 0.0
        test()

        CHK = 1.0
        set_value('semantix.utils.config.tests.test_config.test.test', CHK)
        test()

        assert get_cvalue('semantix.utils.config.tests.test_config.test.test').doc == "Test Config"
        assert get_cvalue('semantix.utils.config.tests.test_config.test.flag').type is bool
        assert get_cvalue('semantix.utils.config.tests.test_config.test.flag').bound_to.__name__ == 'test'

        CHK = 10.0
        test(10.0)

        assert test.__name__ == 'test'
        assert config.semantix.utils.config.tests.test_config.test.flag is True

        #########

        CHK = 0.0

        @configurable
        def test0(test=cvalue(0.0, doc="Test Config",
                              validator=lambda arg: isinstance(arg, float) and arg >= 0.0),
                  test2=0,
                  flag:bool=cvalue(True)):

            nonlocal CHK

            assert test == CHK
            assert test2 == 0
            assert flag is True

        test0()

    def test_utils_config_func_cvalue_defs(self):
        CHK = 0

        @configurable
        def test_def1(test=cvalue(42)):
            nonlocal CHK
            assert CHK == test

        @configurable
        def test_def2(test=cvalue(config.semantix.utils.config.tests.test_config.test_def1.test)):
            nonlocal CHK
            assert CHK == test

        CHK = 42
        test_def1()
        test_def2()

        CHK = 3.14
        set_value('semantix.utils.config.tests.test_config.test_def1.test', CHK)
        test_def1()

        with assert_raises(AssertionError):
            test_def2()

        @configurable
        def test_def3(test=cvalue(get_cvalue('semantix.utils.config.tests.test_config.test_def1.test'))):
            nonlocal CHK
            assert CHK == test

        test_def3()

        CHK = 2.17
        set_value('semantix.utils.config.tests.test_config.test_def1.test', CHK)
        test_def3()

    def test_utils_config_func_cargs_kwargs(self):
        CHK = 0

        @configurable
        def test2(foo, *,
                 test=cvalue(0.0, doc="Test Config",
                             validator=lambda arg: isinstance(arg, float) and arg >= 0.0),

                 flag:bool=cvalue(True)):

            nonlocal CHK

            assert test == CHK
            assert flag

        CHK = 0.0
        test2(1)

        CHK = 1.0
        set_value('semantix.utils.config.tests.test_config.test2.test', CHK)
        test2(1)

        CHK = 10.0
        test2(1, test=10.0)

        with assert_raises(TypeError):
            test2(1, 10.0)

    def test_utils_config_overlapping(self):
        with assert_raises(ConfigError, error_re='Overlapping'):
            @configurable
            def test3(test=cvalue(1.0)):
                pass

            @configurable
            def test3(test=cvalue(1.0)):
                pass

    def test_utils_config_readonlyness(self):
        @configurable
        def test4(test=cvalue(1.0)):
            pass

        with assert_raises(ConfigError, error_re='read-only'):
            config.semantix.utils.config.tests.test_config.test4.test = 10

    def test_utils_config_not_exist_value(self):
        @configurable
        def test5(test=cvalue(1.0)):
            pass

        with assert_raises(ConfigError):
            config.semantix.utils.config.tests.test_config.test5.fuuuu

    def test_utils_config_reuse(self):
        @configurable
        def test6(test=cvalue(0.0, doc="Test Config",
                           validator=lambda arg: isinstance(arg, float) and arg >= 0.0),

                 flag:bool=cvalue(True)):
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
            def test1000(test:int=cvalue(0.0)):
                pass

    def test_utils_config_class_methods_base(self):
        @configurable
        class TM:
            def tm(self, a, foo=cvalue(10)):
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
            def tm(cls, a, foo=cvalue(10)):
                assert cls is TM2
                assert a == foo

            @staticmethod
            def tm2(a, foo=cvalue(11)):
                assert a == foo

        TM2.tm(10)
        TM2.tm2(11)

        assert config.semantix.utils.config.tests.test_config.TM2.tm.foo == 10
        assert config.semantix.utils.config.tests.test_config.TM2.tm2.foo == 11

        set_value('semantix.utils.config.tests.test_config.TM2.tm2.foo', 4)
        TM2.tm(10)
        TM2.tm2(4)

    def test_utils_config_class_methods_checktypes(self):
        @configurable
        @checktypes
        class CTM1:
            def tm(self, a:int, foo=cvalue(10)):
                assert isinstance(self, CTM1)
                assert a == foo

            @classmethod
            def tm2(cls, a:int, foo=cvalue(10)):
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
            def tm(self, a:int, foo=cvalue(10)):
                assert isinstance(self, CTM2)
                assert a == foo

        c = CTM2()
        c.tm(10)

        with assert_raises(TypeError):
            c.tm('2')

    def test_utils_config_decorator_no_cvalue(self):
        def test(foo:int, *, bar):
            pass

        tmp = test
        assert configurable(test) is tmp

    def test_utils_config_nodefault(self):
        @configurable
        def test_nd1(foo:int, *, bar:str=cvalue()):
            assert foo == int(bar)

        with assert_raises(ConfigRequiredValueError):
            test_nd1()

        with assert_raises(TypeError, error_re='Invalid value'):
            set_value('semantix.utils.config.tests.test_config.test_nd1.bar', 142)

        set_value('semantix.utils.config.tests.test_config.test_nd1.bar', '142')
        test_nd1(142)

    def test_utils_config_func_checktypes(self):
        @configurable
        @checktypes
        def test_chk1(foo:int, *, bar:str=cvalue('')):
            assert foo == 123
            assert bar == ''

        test_chk1(123)

        with assert_raises(TypeError):
            test_chk1('123')

        ########

        @checktypes
        @configurable
        def test_chk2(foo:int, *, bar:str=cvalue('')):
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
            def tm(smth, a:int, foo=cvalue(10)):
                assert a == foo

        c = HTM1()
        c.tm(10)

        with assert_raises(TypeError):
           c.tm('1')

        HTM1.tm(10)

        with assert_raises(TypeError):
           HTM1.tm('1')

    def test_utils_config_class_methods_property(self):
        @configurable
        class PTM1:
            foo = cvalue(1100, type=int)

            def __init__(self):
                self.attr = 0

            @property
            def tm(self, tmp=cvalue(1)):
                return self.foo + tmp + self.attr

            @tm.setter
            def tm(self, val):
                self.attr = val


        t = PTM1()
        assert t.tm == 1101

        set_value('semantix.utils.config.tests.test_config.PTM1.tm.tmp', 4)
        assert t.tm == 1104

        t.tm = 100
        assert t.tm == 1204

    def test_utils_config_class_properties(self):
        @configurable
        class TMP:
            param = cvalue('a', type=str, doc='''Lorem Ipsum''')

            def test(self):
                assert self.param == 'a'

        assert TMP.param == 'a'

        TMP().test()

        @configurable
        class TMP2:
            __slots__ = ()
            tmp = 1
            tmp2 = cvalue(333)
            tmp3 = config.semantix.utils.config.tests.test_config.TMP.param

        assert TMP2.tmp is 1
        assert TMP2.tmp2 == 333

        set_value('semantix.utils.config.tests.test_config.TMP2.tmp2', 4)
        assert TMP2.tmp2 == 4

        assert TMP2.tmp3 == TMP.param == 'a'

        assert get_cvalue('semantix.utils.config.tests.test_config.TMP2.tmp2').bound_to is TMP2
        assert get_cvalue('semantix.utils.config.tests.test_config.TMP.param').bound_to is TMP
        assert get_cvalue('semantix.utils.config.tests.test_config.TMP.param').type is str

        with assert_raises(TypeError):
            set_value('semantix.utils.config.tests.test_config.TMP.param', 10)

    def test_utils_config_invalid_name(self):
        with assert_raises(ConfigError, error_re='Unable to apply'):
            @configurable
            def _name(test=cvalue('root')):
                return test

    def test_utils_config_interpolation(self):
        @configurable
        def inter_test1(test=cvalue('root')):
            return test

        @configurable
        def inter_test2(test=cvalue('smth/${semantix.utils.config.tests.test_config.inter_test1.test}/')):
            return test

        @configurable
        def inter_test3(test=cvalue('smth/$semantix.utils.config.tests.test_config.inter_test1.test}/')):
            return test

        assert inter_test2() == 'smth/root/'

        with assert_raises(ConfigError, error_re='Malformed substitution syntax'):
            inter_test3()

        set_value('semantix.utils.config.tests.test_config.inter_test1.test', '33')
        assert inter_test2() == 'smth/33/'

    def test_utils_config_yaml(self):
        from semantix.utils.config.tests import app

        @configurable
        def test_yaml1(foo=cvalue(1), bar:str=cvalue('')):
            assert foo == 123
            assert bar == 'zzz'

        test_yaml1()

        with assert_raises(TypeError):
            @configurable
            def test_yaml2(*, a:int=cvalue(0)):
                pass

    def test_utils_config_loadflow(self):
        set_value('semantix.utils.config.tests.test_config.YML.foo', 33)

        with assert_raises(ConfigError):
            config.semantix.utils.config.tests.test_config.YML.foo

        @configurable
        class YML:
            foo = cvalue(-1, type=int)

        assert YML.foo == 33
        assert config.semantix.utils.config.tests.test_config.YML.foo == 33

        set_value('semantix.utils.config.tests.test_config.YML.foo', 133)

        assert YML.foo == 133
        assert config.semantix.utils.config.tests.test_config.YML.foo == 133
