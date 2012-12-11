##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import metamagic.utils.lang
from metamagic.utils.debug import assert_raises


class TestUtilsLangImports:
    def test_utils_lang_import_filenamecase(self):
        """Mostly relevant for MS Windows & Mac OS with HFS"""

        with assert_raises(ImportError):
            # test on package
            import metamagic.Utils

        with assert_raises(ImportError):
            # test on module
            from metamagic.utils import Debug
