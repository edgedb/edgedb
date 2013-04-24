##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import types

from metamagic.utils.config import ConfigurableMeta, cvalue, inline
from metamagic.utils.config import base as config_base
from metamagic.utils.debug import assert_raises
from metamagic.utils.config.exceptions import ConfigError


class TestConfig:
    def test_utils_config_invalid_class(self):
        class Test:
            test = cvalue(42)
        with assert_raises(ConfigError, error_re='Unable to get value of uninitialized cvalue'):
            Test.test

    def test_utils_config_inheritance(self):
        class InhBase_1(metaclass=ConfigurableMeta):
            attr = cvalue(42, type=int)

        class Inh_1(InhBase_1):
            pass

        class Inh_11(InhBase_1):
            pass

        class Inh2(Inh_11, Inh_1):
            pass

        assert Inh_1.attr == 42
        assert Inh_1().attr == 42

        with inline({'metamagic.utils.config.tests.test_conf.Inh_1.attr': 10}):
            assert Inh_1.attr == 10
            assert Inh_1().attr == 10
            assert Inh2.attr == 10

        with inline({'metamagic.utils.config.tests.test_conf.Inh_1.attr': 10}):
            with inline({'metamagic.utils.config.tests.test_conf.Inh_11.attr': 11}):
                assert Inh_1.attr == 10
                assert Inh_1().attr == 10
                assert Inh2.attr == 11

        with inline({'metamagic.utils.config.tests.test_conf.Inh_1.attr': '10'}):
            with assert_raises(TypeError, error_re='Invalid value'):
                Inh_1.attr

    def test_utils_config_lazy_default(self):
        class LD(metaclass=ConfigurableMeta):
            attr = cvalue(type=int, default=(lambda: 43))
            attr2 = cvalue(default=(lambda: 44))
            attr3 = cvalue(type=str, default=(lambda: 44))
            attr4 = cvalue(default=(lambda:45))
            attr5 = cvalue(type=types.FunctionType, default=(lambda:45))

        class LDD(LD):
            pass

        assert LD.attr == 43
        assert LD.attr == 43 # cache
        assert LD.attr2 == 44
        assert LDD.attr == 43

        with assert_raises(TypeError, error_re='lazy default evaluation'):
            LDD.attr3

        with inline({'metamagic.utils.config.tests.test_conf.LDD.attr': 10}):
            assert LD.attr == 43
            assert LDD.attr == 10

        assert LD.attr4 == 45
        assert isinstance(LD.attr5, types.LambdaType)

    def test_utils_config_writeable_cvalue(self):
        class WC(metaclass=ConfigurableMeta):
            attr = cvalue(20, type=int)

        class WCD(WC):
            pass

        class WCDD(WCD):
            pass

        assert WC.attr == WCD.attr == WCDD.attr == 20

        wcd = WCD()
        wcd.attr = 42
        assert wcd.attr == 42
        assert WC.attr == WCD.attr == WCDD.attr == 20
        assert WC().attr == WCD().attr == WCDD().attr == 20

        WCD.attr = 50
        assert WC.attr == WC().attr == 20
        assert WCD.attr == WCD().attr == WCDD.attr == WCDD().attr == 50

    def test_utils_config_basic_import(self):
        from metamagic.utils.config.tests.testdata.test1 import config

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

    def test_utils_config_nesting_static(self):
        class Foo(metaclass=ConfigurableMeta):
            bar = cvalue(1)

        assert Foo.bar == 1

        from metamagic.utils.config.tests.testdata.test2_1 import config1, config2
        from metamagic.utils.config.tests.testdata.test2_2 import config as config3

        with config1:
            assert Foo.bar == 2

            with config2:
                assert Foo.bar == 3

                with config3:
                    assert Foo.bar == 4

                assert Foo.bar == 3

            assert Foo.bar == 2

        assert Foo.bar == 1

    def test_utils_config_defaults_validation(self):
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

    def test_utils_config_nodefault(self):
        class NDV1(metaclass=ConfigurableMeta):
            bar = cvalue()

        with assert_raises(ValueError):
            NDV1.bar

        with inline({'metamagic.utils.config.tests.test_conf.NDV1.bar': '142'}):
            assert NDV1.bar == '142'

    def test_utils_config_tpl(self):
        class TestTpl(metaclass=ConfigurableMeta):
            tpl1 = cvalue(type=str)
            tpl2 = cvalue(type=str)
            tpl3 = cvalue(type=str)
            tpl4 = cvalue(type=str)
            tpl5 = cvalue(type=str)

        from metamagic.utils.config.tests.testdata.test_tpl import config

        with config:
            assert os.path.abspath(TestTpl.tpl1).endswith('/config/tests')
            assert os.path.abspath(TestTpl.tpl2).endswith('/config/tests/testdata/foo')
            assert TestTpl.tpl3 == '$__dir__'

            with assert_raises(ConfigError, error_re='Exception during template evaluation'):
                TestTpl.tpl4

            assert TestTpl.tpl5 == 'tpl5'
