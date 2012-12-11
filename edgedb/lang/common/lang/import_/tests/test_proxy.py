##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
import pickle
import sys
import types

from metamagic.utils.lang.import_ import module as module_utils
from metamagic.utils.lang.import_ import utils as import_utils


class TestLangImportModuleProxy:
    def test_lang_import_module_light_proxy(self):
        mod = importlib.import_module(__package__ + '.testdata.proxiedmod')
        proxiedmod = module_utils.LightProxyModule(mod.__name__, mod)

        assert hasattr(proxiedmod, 'sys')
        assert isinstance(proxiedmod.sys, types.ModuleType)

        assert hasattr(proxiedmod, 'a')
        assert proxiedmod.a == 10

        assert hasattr(proxiedmod, 'Klass')
        assert isinstance(proxiedmod.Klass, type)
        assert issubclass(proxiedmod.Klass, mod.Klass)

        proxiedmod.a = 20
        assert proxiedmod.a == 20
        assert mod.a == 20

        del proxiedmod.a
        assert not hasattr(proxiedmod, 'a')
        assert not hasattr(mod, 'a')

        assert proxiedmod.func(10) == 10

        KlassRef = proxiedmod.Klass
        proxiedmod = import_utils.reload(proxiedmod)

        # Light proxy does not handle attribute references, only module ref is preserved
        assert KlassRef is not proxiedmod.Klass
        del sys.modules[mod.__name__]

    def test_lang_import_module_proxy(self):
        mod = importlib.import_module(__package__ + '.testdata.proxiedmod')
        proxiedmod = module_utils.ProxyModule(mod.__name__, mod)

        assert hasattr(proxiedmod, 'sys')
        assert isinstance(proxiedmod.sys, types.ModuleType)

        assert hasattr(proxiedmod, 'a')
        assert proxiedmod.a == 10
        assert proxiedmod.a > 1
        assert proxiedmod.a < 100
        assert proxiedmod.a + 1 == 11

        assert isinstance(proxiedmod.a, int)

        assert hasattr(proxiedmod, 'Klass')
        assert isinstance(proxiedmod.Klass, type)
        assert issubclass(proxiedmod.Klass, mod.Klass)

        proxiedmod.a = 20
        assert proxiedmod.a == 20
        assert mod.a == 20

        del proxiedmod.a
        assert not hasattr(proxiedmod, 'a')
        assert not hasattr(mod, 'a')

        assert proxiedmod.func(10) == 10

        KlassRef = proxiedmod.Klass
        proxiedmod = import_utils.reload(proxiedmod)

        # Refs are kept after reload
        assert KlassRef is proxiedmod.Klass
        del sys.modules[mod.__name__]

    def test_lang_import_module_autoloading_light_proxy(self):
        mod = importlib.import_module(__package__ + '.testdata.proxiedmod')
        proxiedmod = module_utils.AutoloadingLightProxyModule(mod.__name__, mod)

        assert hasattr(proxiedmod, 'Klass')
        K_1 = proxiedmod.Klass

        dumped = pickle.dumps(proxiedmod)
        loaded = pickle.loads(dumped)

        assert hasattr(loaded, 'Klass')
        assert loaded.Klass is K_1

        del loaded
        del proxiedmod
        del sys.modules[mod.__name__]
        del mod

        proxiedmod = module_utils.AutoloadingLightProxyModule(__package__ + '.testdata.proxiedmod')
        assert hasattr(proxiedmod, 'Klass')
        assert proxiedmod.Klass is not K_1
