##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import base as base_test


class TestTranslation(base_test.BaseJPlusTest):
    def test_utils_lang_jp_tr_scope_1(self):
        '''JS+
        a = 2;
        function aaa() {
            a = 3;
        }
        aaa();
        print(a);
        %%
        2
        '''

    def test_utils_lang_jp_tr_scope_2(self):
        '''JS+
        a = 2;
        function aaa() {
            nonlocal a;
            a = 3;
        }
        aaa();
        print(a);
        %%
        3
        '''

    def test_utils_lang_jp_tr_scope_3(self):
        '''JS+
        a = 2;
        function aaa(a) {
            a = 3;
        }
        aaa();
        print(a);
        %%
        2
        '''
