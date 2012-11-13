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

        for each (value in []) {
            cnt += 100000;
        }

        for each (i in a) {
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

        for each (value in {}) {
            cnt += 100000;
        }

        for each (idx, value in a) {
            cnt += parseInt(idx) * 100 + value * 1000;
        }

        for each (i in a) {
            if (i[1] == 3) {
                continue;
            }
            cnt += i[1];
        }

        for each (i in a) {
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

        for each (ch in 'abc') {
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

    def test_utils_lang_jp_tr_foreach_switch(self):
        '''JS+

        function a() {
            res = '';
            for each (v in [1]) {
                switch (42) {
                    case 42:
                        res += '-';
                    case 42:
                        res += '42';
                        break;
                }
                res += '-';
            }
            return res;
        }

        print(a());

        %%
        -42-
        '''

    def test_utils_lang_jp_tr_foreach_forin_cont(self):
        '''JS+

        function a() {
            res = '-';
            for each (v in [1]) {
                for (i in [2, 3]) {
                    if (i != '1') {
                        continue;
                    }
                    res += i;
                }
                res += '-';
            }
            return res;
        }

        print(a());

        %%
        -1-
        '''

    def test_utils_lang_jp_tr_foreach_while_cont(self):
        '''JS+

        function a() {
            res = '-';
            for each (v in [1]) {
                i = 0;
                while (i < 10) {
                    i ++;
                    if (i != 3) {
                        continue;
                    }
                    res += i;
                }
                res += '-';
            }
            return res;
        }

        print(a());

        %%
        -3-
        '''

    def test_utils_lang_jp_tr_foreach_dowhile_cont(self):
        '''JS+

        function a() {
            res = '-';
            for each (v in [1]) {
                i = 0;
                do {
                    i ++;
                    if (i != 3) {
                        continue;
                    }
                    res += i;
                } while (i < 10);
                res += '-';
            }
            return res;
        }

        print(a());

        %%
        -3-
        '''

    def test_utils_lang_jp_tr_foreach_for_cont(self):
        '''JS+

        function a() {
            res = '-';
            for each (v in [1]) {
                for (i = 0; i < 10; i++) {
                    if (i != 2) {
                        continue;
                    }
                    res += i;
                }
                res += '-';
            }
            return res;
        }

        print(a());

        %%
        -2-
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

    def test_utils_lang_jp_tr_try_1(self):
        '''JS+
        class E {}
        class E1(E) {}
        class E2(E) {}
        class E3(E) {}
        class EA(E2, E3) {}

        function t(n=0) {
            try {
                switch (n) {
                    case 0: throw E();
                    case 1: throw E1();
                    case 2: throw E2();
                    case 3: throw E3();
                    case 4: throw EA();
                }
            }
            catch (E1 as ex) {
                e = '.' + ex.$cls.$name + '.';
            }
            catch ([E2, E3] as ex) {
                e ='+' + ex.$cls.$name + '+';
            }
            catch (E) {
                e = '-e-';
            }
            else {
                e = '=42=';
            }
            finally {
                if (e) {
                    e += '^';
                } else {
                    e = '^'
                }
            }

            return ' ' + e + ' ';
        }

        e = t() + t(100) + t(1) + t(2) + t(3) + t(4);

        print(e);

        %%
        -e-^  =42=^  .E1.^  +E2+^  +E3+^  +EA+^
        '''

    def test_utils_lang_jp_tr_try_2(self):
        '''JS+

        r = ''
        try {
            abc.foo
        }
        catch {
            r += 'caught'
        }
        finally {
            r += '-and-fail'
        }

        print(r)

        %%
        caught-and-fail
        '''

    def test_utils_lang_jp_tr_try_3(self):
        '''JS+

        r = '';

        try {
            try {
                abc.foo
            }
            finally {
                r += '-and-fail'
            }
        } catch {}

        print(r)

        %%
        -and-fail
        '''

    def test_utils_lang_jp_tr_try_4(self):
        '''JS+

        r = '';

        try {
            abc.foo
        } catch {
            try {
                abc.foo
            }
            catch {}
            finally {
                r += '-and-fail'
            }
        }

        print(r)

        %%
        -and-fail
        '''

    def test_utils_lang_jp_tr_try_5(self):
        '''JS+

        r = '';

        try {
        }
        catch {}
        else {
            try {
                abc.foo
            }
            catch {}
            finally {
                r += '-and-fail'
            }
        }

        print(r)

        %%
        -and-fail
        '''

    def test_utils_lang_jp_tr_try_6(self):
        '''JS+

        r = '';

        try {
        }
        catch {}
        finally {
            try {
                abc.foo
            }
            catch {}
            finally {
                r += '-and-fail'
            }
        }

        print(r)

        %%
        -and-fail
        '''

    def test_utils_lang_jp_tr_try_7(self):
        '''JS+

        r = '';

        class E {}

        try {
            throw '123';
        }
        catch (Object as e) {
            r += e;
        }
        finally {
            r += '-';
        }

        print(r)

        %%
        123-
        '''

    def test_utils_lang_jp_tr_try_8(self):
        '''JS+

        r = '';

        class E {}

        try {
            throw '123';
        }
        catch (E) {
            r += 'E'
        }
        catch (Object) {
            r += 'smth'
        }
        finally {
            r += '-';
        }

        print(r)

        %%
        smth-
        '''

    def test_utils_lang_jp_tr_multiline_sq_string(self):
        r"""JS+
        a = '''''' + '''!''' + '''

        12

        '''

        print(a.replace(/\n/g, '~').replace(/\s/g, '.'))
        %%
        !~~........12~~........
        """

    def test_utils_lang_jp_tr_multiline_dq_string(self):
        r'''JS+
        a = """""" + """!""" + """

        12

        """

        print(a.replace(/\n/g, '~').replace(/\s/g, '.'))
        %%
        !~~........12~~........
        '''
