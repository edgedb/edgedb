##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types

from semantix.utils.config import ConfigurableMeta, cvalue, configurable, inline
from semantix.utils.config import base as config_base
from semantix.utils.debug import assert_raises
from semantix.utils.functional import checktypes


class TestConfig:
    def test_utils_conf_basic_import(self):
        from semantix.utils.config.tests.testdata.test1 import config

        assert isinstance(config, config_base.ConfigRootNode)
        assert config.onemore == 42
        assert isinstance(config.foo, config_base.ConfigNode)
        assert config.foo.bar1 == 1
        assert config.foo.bar2.test

        with assert_raises(AttributeError):
            config.aaa

        v = config_base.ConfigRootNode._get_value(config, 'foo.bar1')
        assert isinstance(v, config_base.ConfigValue)
        assert v.value == 1
        assert 'line:' in v.context

    def test_utils_conf_nesting_static(self):
        class Foo(metaclass=ConfigurableMeta):
            bar = cvalue(1)

        assert Foo.bar == 1

        from semantix.utils.config.tests.testdata.test2_1 import config1, config2
        from semantix.utils.config.tests.testdata.test2_2 import config as config3

        with config1:
            assert Foo.bar == 2

            with config2:
                assert Foo.bar == 3

                with config3:
                    assert Foo.bar == 4

                assert Foo.bar == 3

            assert Foo.bar == 2

        assert Foo.bar == 1

    def test_utils_conf_class_methods_base(self):
        class TM(metaclass=ConfigurableMeta):
            def tm(self, a, foo=cvalue(10)):
                assert isinstance(self, TM)
                assert a == foo

        assert TM.tm.__name__ == 'tm'
        assert isinstance(TM.tm, types.FunctionType)
        t = TM()
        t.tm(10)
        assert isinstance(t.tm, types.MethodType)


        class TM2(metaclass=ConfigurableMeta):
            @classmethod
            def tm(cls, a, foo=cvalue(10)):
                assert cls is TM2
                assert a == foo

            @staticmethod
            def tm2(a, foo=cvalue(11)):
                assert a == foo

        TM2.tm(10)
        TM2.tm2(11)

        with inline({'semantix.utils.config.tests.test_conf.TM2.tm2.foo': 4}):
            TM2.tm(10)
            TM2.tm2(4)

    def test_utils_conf_defaults_validation(self):
        with assert_raises(TypeError):
            @configurable
            def test1000(test=cvalue(0.0, type=int)):
                pass

        with assert_raises(TypeError):
            @configurable
            def test1001(test=cvalue(0.0, validator=lambda v: v>0)):
                pass

        with assert_raises(TypeError):
            @configurable
            def test1002(test=cvalue(1, validator=lambda v: v>0, type=str)):
                pass

        with assert_raises(TypeError):
            @configurable
            def test1003(test=cvalue(1, validator=lambda v: isinstance(v, str), type=int)):
                pass

        with assert_raises(TypeError):
            class test1004(metaclass=ConfigurableMeta):
                test = cvalue(0.0, validator=lambda v: v>0)

        with assert_raises(TypeError):
            class test1005(metaclass=ConfigurableMeta):
                test = cvalue(0.0, type=int)

        with assert_raises(TypeError):
            class test1006(metaclass=ConfigurableMeta):
                test = cvalue(1, type=str, validator=lambda v: v>0)

        with assert_raises(TypeError):
            class test1007(metaclass=ConfigurableMeta):
                test = cvalue(1, type=int, validator=lambda v: isinstance(v, str))

    def test_utils_conf_decorator_no_cvalue(self):
        def test(foo:int, *, bar):
            pass

        tmp = test
        assert configurable(test) is tmp

    def test_utils_conf_nodefault(self):
        @configurable
        def test_nd1(foo:int, *, bar:str=cvalue()):
            assert foo == int(bar)

        with assert_raises(ValueError):
            test_nd1()

        with inline({'semantix.utils.config.tests.test_conf.test_nd1.bar': '142'}):
            test_nd1(142)

    def test_utils_conf_class_methods_checktypes(self):
        @checktypes
        class CTM1(metaclass=ConfigurableMeta):
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
        class CTM2(metaclass=ConfigurableMeta):
            def tm(self, a:int, foo=cvalue(10)):
                assert isinstance(self, CTM2)
                assert a == foo

        c = CTM2()
        c.tm(10)

        with assert_raises(TypeError):
            c.tm('2')
