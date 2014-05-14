##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import BaseJPlusTest, expected_fail
from metamagic.utils.lang.javascript.parser.jsparser import UnexpectedToken, UnknownToken


class TestTranslation(BaseJPlusTest):
    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 23})
    def test_utils_lang_jp_parser_func_params_1(self):
        '''JS+
        function a(*, *, a) {}
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 31})
    def test_utils_lang_jp_parser_func_params_2(self):
        '''JS+
        function a(*, **kwargs, a) {}
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 22})
    def test_utils_lang_jp_parser_func_params_3(self):
        '''JS+
        function a(a *) {}
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 21})
    def test_utils_lang_jp_parser_func_params_4(self):
        '''JS+
        function a(*) {}
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 20})
    def test_utils_lang_jp_parser_func_params_5(self):
        '''JS+
        function a(, a) {}
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 22})
    def test_utils_lang_jp_parser_func_params_6(self):
        '''JS+
        function a(a,) {}
        '''

    @expected_fail(UnknownToken, attrs={'line': 1, 'col': 19})
    def test_utils_lang_jp_parser_func_params_7(self):
        '''JS+
        function a(...a) {}
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 26})
    def test_utils_lang_jp_parser_func_params_8(self):
        '''JS+
        function a(a=1, a) {}
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 36})
    def test_utils_lang_jp_parser_func_params_9(self):
        '''JS+
        function a(a=1, *, a, a=1, *) {}
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 14})
    def test_utils_lang_jp_parser_fat_arrow_1(self):
        '''JS+
        a = (*, a) => {}
        '''

    @expected_fail(UnknownToken, attrs={'line': 1, 'col': 13})
    def test_utils_lang_jp_parser_fat_arrow_2(self):
        '''JS+
        a = (...a) => {}
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 22})
    def test_utils_lang_jp_parser_call_1(self):
        '''JS+
        foo(a, b=2, c)
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 27})
    def test_utils_lang_jp_parser_call_2(self):
        '''JS+
        foo(a, (b=2), c, *)
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 28})
    def test_utils_lang_jp_parser_call_3(self):
        '''JS+
        foo(a, *foo, a=1, b)
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 27})
    def test_utils_lang_jp_parser_call_4(self):
        '''JS+
        foo(a, *foo, a=1, (b=1))
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 21})
    def test_utils_lang_jp_parser_call_5(self):
        '''JS+
        foo(a, **foo, a)
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 22})
    def test_utils_lang_jp_parser_call_6(self):
        '''JS+
        foo(a, *foo, *foo)
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 2, 'col': 9})
    def test_utils_lang_jp_parser_import_1(self):
        '''JS+
        import
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 2, 'col': 23})
    def test_utils_lang_jp_parser_import_2(self):
        '''JS+
        import
            foo, bar, *
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 23})
    def test_utils_lang_jp_parser_import_3(self):
        '''JS+
        from . import *
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 14})
    def test_utils_lang_jp_parser_import_4(self):
        '''JS+
        from from import *
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 16})
    def test_utils_lang_jp_parser_import_5(self):
        '''JS+
        import import
        '''

    @expected_fail(UnexpectedToken, attrs={'line': 1, 'col': 16})
    def test_utils_lang_jp_parser_import_6(self):
        '''JS+
        import (a)
        '''
