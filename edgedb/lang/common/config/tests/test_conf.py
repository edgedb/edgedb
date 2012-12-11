##
# Copyright (c) 2011-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os

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
