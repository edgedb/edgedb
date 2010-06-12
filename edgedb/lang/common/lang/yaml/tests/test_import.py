##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
from semantix.utils.lang.yaml.loader import AttributeMappingNode
from semantix.utils.lang.yaml.schema import Schema
from semantix.utils.debug import assert_raises

class DictImportSchema(Schema):
    def check(self, node):
        return AttributeMappingNode.from_map_node(node)


class TestLangImport(object):
    def test_lang_import(self):
        mod = importlib.import_module('semantix.utils.lang.yaml.tests.testdata.test_import')
        assert hasattr(mod, 'SimpleImport') and mod.SimpleImport['attr1'] == 'test'

    def test_lang_dict_import(self):
        mod = importlib.import_module('semantix.utils.lang.yaml.tests.testdata.test_dict_import')
        assert hasattr(mod, 'attr1') and hasattr(mod, 'attr2') and hasattr(mod, 'attr3')

    def test_lang_ambiguous_import(self):
        with assert_raises(ImportError):
            from semantix.utils.lang.yaml.tests.testdata.ambig import test
