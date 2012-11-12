##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import base as base_test


class TestTranslation(base_test.BaseJPlusTest):
    def test_utils_lang_jp_tr_empty(self):
        '''JS+
        '''

    def test_utils_lang_jp_tr_almost_empty(self):
        '''JS+
        print(1);

        %%
        1
        '''

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

    def test_utils_lang_jp_tr_class_1(self):
        '''JS+

        class Foo {
            function bar() {
                return 10;
            }
        }

        print(Foo().bar())

        %%
        10
        '''

    def test_utils_lang_jp_tr_class_2(self):
        '''JS+

        class Foo {
            x = 10;

            function bar(a, b) {
                return this.x + (a || 0) + (b || 0);
            }
        }

        class Bar {}

        class Spam(Foo, Bar) {
            function bar() {
                return super().bar(1, 2) + 29;
            }
        }

        print(Spam().bar() + new Spam().bar())

        %%
        84
        '''

    def test_utils_lang_jp_tr_class_3(self):
        '''JS+

        class Foo {
            static x = 10;

            static function bar(a, b) {
                return this.x + (a || 0) + (b || 0);
            }
        }

        class Bar {}

        class Spam(Foo, Bar) {
            static function bar() {
                return super().bar(1, 2) + 29;
            }
        }

        print(Spam.bar())

        %%
        42
        '''
