##
# Copyright (c) 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang.python import utils


class TestUtilsLangPythonUtils:
    def test_utils_lang_python_utils_resolve(self):
        assert utils.resolve('a', {'a': 42}) == 42
        assert utils.resolve('a.__class__.__name__[0].__class__.__name__', {'a': 42}) == 'str'
        assert utils.resolve('a["b"]', {'a': {'b': 42}}) == 42
