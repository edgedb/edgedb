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

    def test_utils_lang_jp_tr_dec_1(self):
        '''JS+

        function make_dec(inc, sign) {
            return function dec(func) {
                return function() {
                    if (!sign) {
                        return func.apply(this, arguments) + inc;
                    } else {
                        return func.apply(this, arguments) * inc;
                    }
                }
            }
        }

        dec1 = make_dec(10, '+');

        @dec1
        @make_dec(20, '*')
        function abc() {
            return 20;
        }

        print(abc());

        %%
        4000
        '''

    def test_utils_lang_jp_tr_dec_n_classes_1(self):
        '''JS+

        function dec(func) {
            return function() {
                return func.apply(this, arguments) + 10;
            }
        }

        function class_dec(cls) {
            cls.attr = -11;
            return cls;
        }

        @class_dec
        class Foo {
            @dec
            function spam() {
                return 11;
            }

            @dec
            static function ham() {
                return 22;
            }
        }

        print(Foo().spam() + Foo.ham() + Foo.attr)

        %%
        42
        '''

    def test_utils_lang_jp_tr_dec_n_classes_2(self):
        '''JS+

        function one_dec(obj) {
            return 1;
        }

        @one_dec
        class Foo {}

        class Bar {
            @one_dec
            static function abc() {
                return 10;
            }

            @one_dec
            @one_dec
            static function edf() {
                return 10;
            }
        }

        print(Foo + Bar.abc + Bar.edf)

        %%
        3
        '''

    def test_utils_lang_jp_tr_func_defaults_1(self):
        '''JS+

        function a(a, b, c=10) {
            return a + b + c;
        }

        print(a(1, 2) + a(2, 3, 4));

        %%
        22
        '''

    def test_utils_lang_jp_tr_foreach_array(self):
        '''JS+

        Array.prototype.foo = '123';

        a = [1, 2, 3, 4];
        cnt = 0;

        foreach (value in []) {
            cnt += 100000;
        }

        foreach (i in a) {
            if (i == 3) {
                continue;
            }
            cnt += i;
        }

        print(cnt);

        %%
        7
        '''

    def test_utils_lang_jp_tr_foreach_obj(self):
        '''JS+

        Object.prototype.foo = '123';

        a = {'0': 1, '1': 2, '2': 3, '3': 4};
        cnt = 0;

        foreach (value in {}) {
            cnt += 100000;
        }

        foreach (idx, value in a) {
            cnt += parseInt(idx) * 100 + value * 1000;
        }

        foreach (i in a) {
            if (i[1] == 3) {
                continue;
            }
            cnt += i[1];
        }

        foreach (i in a) {
            if (i[1] == 3) {
                break;
            }
            cnt += i[1];
        }

        print(cnt);

        %%
        10610
        '''

    def test_utils_lang_jp_tr_foreach_str(self):
        '''JS+

        out = [];

        foreach(ch in 'abc') {
            out.push(ch);
        }

        print(out.join('-'));

        %%
        a-b-c
        '''

    def test_utils_lang_jp_tr_for_in(self):
        '''JS+
        cnt = 0;

        a = {'10': 2, '20': 3}
        for (i in a) {
            if (i == '20') break;
            if (a.hasOwnProperty(i)) {
                cnt += parseInt(i);
            }
        }

        print(cnt);

        %%
        10
        '''

    def test_utils_lang_jp_tr_for_plain(self):
        '''JS+
        cnt = 0;
        a = [10, 20, 30];
        for (i = 0, len = a.length, j=1000; i < len; i++, j+=1000) {
            if (j > 10e6) continue;
            cnt += i * j + a[i];
        }

        print(cnt);

        %%
        8060
        '''

    def _test_utils_lang_jp_tr_try1(self):
        '''JS+
        class E {}
        class E1(E) {}
        class E2(E) {}
        class E3(E) {}

        try {
            throw E2();
        }
        catch (E1 as ex) {
            e = ex;
        }
        catch ([E2, E3] as ex) {
            e = ex;
        }
        else {
            e = 42;
        }

        print(e.$cls.$name)

        %%
        E2
        '''
