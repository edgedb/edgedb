##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
import py.test

from metamagic.utils.debug import assert_raises
from metamagic.utils.lang import context as lang_context
from metamagic.utils.lang.yaml import exceptions as yaml_errors


class TestLangImport(object):
    def test_utils_lang_yaml_import(self):
        modname = 'metamagic.utils.lang.yaml.tests.testdata.test_import'
        mod = importlib.import_module(modname)
        assert hasattr(mod, 'SimpleImport') and mod.SimpleImport['attr1'] == 'test'

    def test_utils_lang_yaml_module_import(self):
        modname = 'metamagic.utils.lang.yaml.tests.testdata.test_module_import'
        mod = importlib.import_module(modname)
        assert hasattr(mod, 'attr1') and hasattr(mod, 'attr2') and hasattr(mod, 'attr3')

    def test_utils_lang_yaml_module_import_bad1(self):
        modname = 'metamagic.utils.lang.yaml.tests.testdata.test_module_import_bad1'
        err = 'unexpected document after module-level schema document'
        with assert_raises(ImportError, cause=yaml_errors.YAMLCompositionError, error_re=err):
            importlib.import_module(modname)

    def test_utils_lang_yaml_module_import_bad2(self):
        modname = 'metamagic.utils.lang.yaml.tests.testdata.test_module_import_bad2'
        err = 'unexpected module-level schema document'
        with assert_raises(ImportError, cause=yaml_errors.YAMLCompositionError, error_re=err):
            importlib.import_module(modname)

    @py.test.mark.xfail
    def test_utils_lang_yaml_ambiguous_import(self):
        with assert_raises(ImportError):
            from metamagic.utils.lang.yaml.tests.testdata.ambig import test

    def test_utils_lang_yaml_module_import_import(self):
        modname = 'metamagic.utils.lang.yaml.tests.testdata.test_module_import_import'
        importlib.import_module(modname)
