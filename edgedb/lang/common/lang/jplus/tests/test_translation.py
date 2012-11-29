##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import py.test

from .base import BaseJPlusTest, transpiler_opts


class TestTranslation(BaseJPlusTest):
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
        function aaa() {
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
            bar() {
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

            bar(a, b) {
                return this.x + (a || 0) + (b || 0);
            }
        }

        class Bar {}

        class Spam(Foo, Bar) {
            bar() {
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

            static bar(a, b) {
                return this.x + (a || 0) + (b || 0);
            }
        }

        class Bar {}

        class Spam(Foo, Bar) {
            static bar() {
                return super().bar(1, 2) + 29;
            }
        }

        print(Spam.bar())

        %%
        42
        '''

    def test_utils_lang_jp_tr_metaclass_1(self):
        '''JS+

        class MA(type) {
            static constructor(name, bases, dct) {
                dct.foo = name + name;
                return super().constructor(name, bases, dct)
            }
        }

        class A(metaclass=MA) {}

        class B(A, object) {}

        print(A().foo + ' | ' + B().foo)

        %%
        __main__.A__main__.A | __main__.B__main__.B
        '''

    def test_utils_lang_jp_tr_class_super_1(self):
        '''JS+

        class Foo {
            constructor(base) {
                this.base = base;
            }

            ham(b) {
                return this.base + b;
            }
        }

        class Bar(Foo) {
            ham(b) {
                return super().ham.apply(this, arguments) + 100;
            }
        }

        class Baz(Bar) {
            ham(base, b) {
                meth = super().ham;
                if (base) {
                    return meth.call(base, b);
                } else {
                    return meth.call(this, b);
                }
            }

        }

        baz = Baz(10)
        baz2 = Baz(20000)
        print(baz.ham(null, 10) + '|' + baz.ham(baz2, 10))

        %%
        120|20110
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
            spam() {
                return 11;
            }

            @dec
            static ham() {
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
            static abc() {
                return 10;
            }

            @one_dec
            @one_dec
            static edf() {
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

    def test_utils_lang_jp_tr_func_defaults_2(self):
        '''JS+

        function a(a, b=a*a) {
            return a + b;
        }

        print(a(10) + a(2, 3));

        %%
        115
        '''

    def test_utils_lang_jp_tr_forof_array(self):
        '''JS+

        Array.prototype.foo = '123';

        a = [1, 2, 3, 4];
        cnt = 0;

        for (value of []) {
            cnt += 100000;
        }

        for (i of a) {
            if (i == 3) {
                continue;
            }
            cnt += i;
        }

        print(cnt);

        %%
        7
        '''

    def test_utils_lang_jp_tr_forof_obj(self):
        '''JS+

        Object.prototype.foo = '123';

        a = {'0': 1, '1': 2, '2': 3, '3': 4};
        cnt = 0;

        for (value of keys({})) {
            cnt += 100000;
        }

        for (key of keys(a)) {
            value = a[key];
            cnt += parseInt(key) * 100 + value * 1000;
        }

        print(cnt);

        %%
        10600
        '''

    def test_utils_lang_jp_tr_forof_str(self):
        '''JS+

        out = [];

        for (ch of 'abc') {
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

    def test_utils_lang_jp_tr_try_1(self):
        '''JS+
        class E {}
        class E1(E) {}
        class E2(E) {}
        class E3(E) {}
        class EA(E2, E3) {}
        class E4() {}

        function t(n=0) {
            try {
                switch (n) {
                    case 0: throw E();
                    case 1: throw E1();
                    case 2: throw E2();
                    case 3: throw E3();
                    case 4: throw EA();
                    case 5: throw E4();
                }
            }
            except (E1 as ex) {
                e = '.' + ex.$cls.$name + '.';
            }
            except ([E2, E3] as ex) {
                e ='+' + ex.$cls.$name + '+';
            }
            except (E) {
                e = '-e-';
            }
            except {
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

        e = t() + t(100) + t(1) + t(2) + t(3) + t(4) + t(5);

        print(e);

        %%
        -e-^  ^  .E1.^  +E2+^  +E3+^  +EA+^  =42=^
        '''

    def test_utils_lang_jp_tr_try_2(self):
        '''JS+

        r = ''
        abc = null
        try {
            abc.foo
        }
        except {
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
        abc = null;

        try {
            try {
                abc.foo
            }
            finally {
                r += '-and-fail'
            }
        } except {}

        print(r)

        %%
        -and-fail
        '''

    def test_utils_lang_jp_tr_try_4(self):
        '''JS+

        r = '';
        abc = void(0);

        try {
            abc.foo
        } except {
            try {
                abc.foo
            }
            except {}
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
        abc = null;

        try {
        }
        except {}
        else {
            try {
                abc.foo
            }
            except {}
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
        abc = null;

        try {
        }
        except {}
        finally {
            try {
                abc.foo
            }
            except {}
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
        except (BaseObject as e) {
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
        except (E) {
            r += 'E'
        }
        except (BaseObject) {
            r += 'smth'
        }
        finally {
            r += '-';
        }

        print(r)

        %%
        smth-
        '''

    def test_utils_lang_jp_tr_try_9(self):
        '''JS+

        r = '';

        class E {}

        try {
            1
        }
        except (E) {
            r += 'E'
        }
        else {
            r += 'OK'
        }
        print(r)

        %%
        OK
        '''

    def test_utils_lang_jp_tr_try_10(self):
        '''JS+

        r = '';

        class E {}

        try {
            throw E()
        }
        except (E) {
            r += 'E'
        }
        else {
            r += 'OK'
        }
        print(r)

        %%
        E
        '''

    def test_utils_lang_jp_tr_try_std_1(self):
        '''JS+

        r = '';

        try {
            throw '123';
        }
        catch (e) {
            r += '=' + e;
        }
        finally {
            r += '=';
        }

        try {
            throw '123';
        }
        catch (e) {
            r += '=' + e;
        }

        try {
            try {
                throw '123';
            }
            finally {
                r += 'finally'
            }
        }
        except {}

        print(r);

        %%
        =123==123finally
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

    def test_utils_lang_jp_tr_with_1(self):
        '''JS+

        chk = '';

        class E {}

        class W1 {
            enter() {
                nonlocal chk;
                chk += '-enter-';
                return {'a': 'b'};
            }

            exit(exc) {
                nonlocal chk;
                chk += '-exit-';
                if (exc) {
                    chk += '(' + exc.$cls.$name + ')';
                } else {
                    chk += '()';
                }
                return true;
            }
        }

        with (W1() as w) {
            chk += w['a'];
        }

        chk += '|';

        with (W1() as w) {
            throw E();
        }

        print(chk)

        %%
        -enter-b-exit-()|-enter--exit-(E)
        '''

    def test_utils_lang_jp_tr_with_2(self):
        '''JS+

        chk = '';

        class W {
            constructor(name) {
                this.name = name;
                nonlocal chk;
                chk += 'create(' + name + ')-';
            }

            enter() {
                nonlocal chk;
                chk += 'enter(' + this.name + ')-';
                return this;
            }

            exit() {
                nonlocal chk;
                chk += 'exit(' + this.name + ')-';
            }
        }

        with (W('a0') as a0, W('a1'), W('a2')) {
            chk += '|' + a0.name + '|';
        }

        print(chk);

        %%

        create(a0)-enter(a0)-create(a1)-enter(a1)-create(a2)-enter(a2)-|a0|exit(a2)-exit(a1)-exit(a0)-
        '''

    def test_utils_lang_jp_tr_with_3(self):
        '''JS+
        try {
            with (1) {}
            print('fail');
        } catch (e) {
            e2 = e + '';
            if (e2.indexOf("context managers must") > 0) {
                print('ok');
            } else {
                throw e;
            }
        }

        %%
        ok
        '''

    def test_utils_lang_jp_tr_func_rest_1(self):
        '''JS+

        function test0(*a) {
            return a.join('+') + '~'
        }

        function test1(a, *b) {
            return b.join('-') + '|';
        }

        function test2(a, b=10, *c) {
            return '(' + b + ')' + c.join('*') + '$';
        }

        print(test0() + test0(1) + test0(1, 2) +
              test1(1) + test1(1, 2) + test1(1, 2, 3) +
              test2(1) + test2(1, 2) + test2(1, 2, 3) + test2(1, 2, 3, 4))

        %%
        ~1~1+2~|2|2-3|(10)$(2)$(2)3$(2)3*4$
        '''

    def test_utils_lang_jp_is_isnt(self):
        '''JS+

        print((1 is 1 isnt false is true) && (NaN is NaN));

        %%
        true
        '''

    def test_utils_lang_jp_is_instanceof(self):
        '''JS+

        class Foo {}
        class Bar(Foo) {}

        print((Bar() instanceof Foo) && ('bar' instanceof BaseObject)
              && ([] instanceof Array) && !([] instanceof String))

        %%
        true
        '''

    def test_utils_lang_jp_tr_name_resolution_1(self):
        '''JS+

        print(isinstance(1, BaseObject) && 1 instanceof BaseObject);

        %%

        true
        '''

    def test_utils_lang_jp_tr_name_resolution_2(self):
        '''JS+

        print(1 instanceof BaseObject);

        %%

        true
        '''

    def test_utils_lang_jp_tr_name_resolution_3(self):
        '''JS+

        print(object.$name);

        %%
        object
        '''

    def test_utils_lang_jp_tr_name_resolution_4(self):
        '''JS+

        print(object + type);

        %%
        <class object><class type>
        '''

    def test_utils_lang_jp_tr_name_resolution_5(self):
        '''JS+

        object

        %%
        '''

    def test_utils_lang_jp_tr_name_resolution_6(self):
        '''JS+

        print([1, object, type][0])

        %%
        1
        '''

    def test_utils_lang_jp_tr_name_resolution_7(self):
        '''JS+

        print({'a': 'b', 'c': object}['a']);

        %%
        b
        '''

    def test_utils_lang_jp_tr_name_resolution_8(self):
        '''JS+

        a = {'b' : {'c': function() { print('aaaa')}}};

        a.b.c();

        %%
        aaaa
        '''

    def test_utils_lang_jp_tr_name_resolution_9(self):
        '''JS+

        new object()

        %%
        '''

    def test_utils_lang_jp_tr_name_resolution_10(self):
        '''JS+

        print(typeof object)

        %%
        function
        '''

    def test_utils_lang_jp_tr_name_resolution_11(self):
        '''JS+

        print(object['$name'])

        %%
        object
        '''

    def test_utils_lang_jp_tr_name_resolution_12(self):
        '''JS+

        print(void object)

        %%
        undefined
        '''

    def test_utils_lang_jp_tr_name_resolution_13(self):
        '''JS+

        print('abc' in object)

        %%
        false
        '''

    def test_utils_lang_jp_tr_name_resolution_14(self):
        '''JS+

        print(object instanceof Object)

        %%
        false
        '''

    def test_utils_lang_jp_tr_name_resolution_15(self):
        '''JS+

        print(isinstance((1, object), type))

        %%
        true
        '''

    def test_utils_lang_jp_tr_name_resolution_16(self):
        '''JS+

        print((--object) + (type++))

        %%
        NaN
        '''

    def test_utils_lang_jp_tr_name_resolution_17(self):
        '''JS+

        print((object ? '1' : '2') + (1 ? type : '3') + (0 ? 1 : BaseObject))

        %%
        1<class type><class BaseObject>
        '''

    def test_utils_lang_jp_tr_name_resolution_18(self):
        '''JS+

        function a(a=object) {
            return a;
        }

        print(a()+'')

        %%
        <class object>
        '''

    def test_utils_lang_jp_tr_name_resolution_19(self):
        '''JS+

        a = object;
        print(a+'')

        %%
        <class object>
        '''

    def test_utils_lang_jp_tr_dest_assign_1(self):
        '''JS+

        c = [a, b] = [2, 3, 4, 5];
        [z] = [100]

        print(c.join('*') + '|' + a + '-' + b + '|' + z);

        %%
        2*3*4*5|2-3|100
        '''

    def test_utils_lang_jp_tr_dest_assign_2(self):
        '''JS+

        dct = {'a': 1, 'b': 2, 'c': 3}

        res = {a, c} = dct
        {b} = {a:100, b: 1000}

        print(res.b + '|' + a + '|' + c + '|' + b)

        %%
        2|1|3|1000
        '''

    def test_utils_lang_jp_tr_dest_assign_3(self):
        '''JS+

        [a, [b, [c, d], e], f, [g]] = [1, [2, [3, 4], 5], 6, [7, 8]];

        print(a, b, c, d, e, f, g)

        %%
        1 2 3 4 5 6 7
        '''

    def test_utils_lang_jp_tr_dest_assign_4(self):
        '''JS+

        [a, {x, z, y}, f, [g]] = [1, {z:[10], y:'aa', x:42}, 6, [7, 8]];

        print(a, z[0], y[0], x, f, g)

        %%
        1 10 'a' 42 6 7
        '''

    def test_utils_lang_jp_tr_dest_assign_5(self):
        '''JS+

        x = 'x'; y = 'y';

        [x, y] = [y, x];

        print(x + ':' + y)

        %%
        y:x
        '''

    def test_utils_lang_jp_tr_dest_assign_6(self):
        '''JS+

        {$name, $module} = type

        print($name, $module)

        %%
        type builtins
        '''

    def test_utils_lang_jp_tr_dest_assign_forof_1(self):
        '''JS+

        for ([x, y, [z]] of [ [1, 2, [7, 8]], [3, 4, [8, 9]] ]) {
            print(x + '-' + y + '-' + z)
        }

        %%
        1-2-7\n3-4-8
        '''

    def test_utils_lang_jp_tr_dest_assign_forof_2(self):
        '''JS+

        for ([x, {y}] of [[1, {y: [20]}]]) {
            print(x + '-' + y[0])
        }

        %%
        1-20
        '''

    def test_utils_lang_jp_tr_dest_assign_forof_3(self):
        '''JS+

        for ({x} of [{x:1},{x:2}]) {
            print(x);
        }

        %%
        1\n2
        '''

    def test_utils_lang_jp_tr_assert_1(self):
        '''JS+

        try {
            assert (1, 0), ('Error!', 'and fail...')
        } except (Error) {
            print('ok')
        } else {
            print('fail')
        }

        try {
            assert true, ('Error!', 'and fail...')
        } except (Error) {
            print('fail2')
        } else {
            print('ok2')
        }

        try {
            assert !object
        } except (Error) {
            print('ok3')
        } else {
            print('fail3')
        }

        %%
        ok\nok2\nok3
        '''

    @transpiler_opts(debug=False)
    def test_utils_lang_jp_tr_assert_2(self):
        '''JS+

        assert 0
        print('here')

        %%
        here
        '''

    def test_utils_lang_jp_tr_comprehension_for_of_1(self):
        '''JS+

        out = [ch for (ch of 'abc')];
        print(out.join('-'));

        out = [ch for (ch of [1, 2, 3, 4]) if (ch > 2)];
        print(out.join('-'));

        out = [ch+10 for (ar of [[2, 3], [4, 5]]) for (ch of ar) if (ch > 2)];
        print(out.join('-'));

        %%
        a-b-c\n3-4\n13-14-15
        '''

    def test_utils_lang_jp_tr_comprehension_for_of_2(self):
        # Python list-comp for ref
        # [[el*2 for el in lst if el % 2] for lst in [[1,2,3],[4,5,6],[3]] if len(lst)>2]
        '''JS+

        a = [  [el*2 for (el of lst) if (el % 2)]

                            for (lst of [[1, 2, 3], [4, 5, 6], [3]]) if (len(lst) > 2)]

        print(JSON.stringify(a))

        %%
        [[2,6],[10]]
        '''

    def test_utils_lang_jp_tr_comprehension_for_1(self):
        '''JS+

        [print(i) for (i = 0; i < 4; i++)]

        %%
        0\n1\n2\n3
        '''

    def test_utils_lang_jp_tr_fat_arrow_1(self):
        '''JS+

        x = b => {return b*b}

        print(x(10))

        %%
        100
        '''

    def test_utils_lang_jp_tr_fat_arrow_2(self):
        '''JS+

        x = [b => b*i for (i = 1; i < 3; i++)]

        print(x[0](100) + '|' + x[1](200))

        %%
        300|600
        '''

    def test_utils_lang_jp_tr_fat_arrow_3(self):
        '''JS+

        print((() => {})())
        print(typeof () => {})

        %%
        undefined\nfunction
        '''

    def test_utils_lang_jp_tr_fat_arrow_4(self):
        '''JS+

        print((function() {
            x = x => this.a
            return x()
        }).apply({a:42}))

        print((function() {
            x = x => {
                y = () => this.a
                return y()
            }
            return x()
        }).apply({a:43}))

        %%
        42\n43
        '''

    def test_utils_lang_jp_tr_fat_arrow_5(self):
        '''JS+

        x = () => ({a: 'b'})
        a = x().a
        print(a)
        %%
        b
        '''

    def test_utils_lang_jp_tr_pyargs_1(self):
        '''JS+

        function test1(a, *, d, e=1, f) {
            return a+'|'+d+'|'+e+'|'+f+'--';
        }

        print(test1(1, d=6, f=42)+test1(1, d=6, e=10, f=42))
        %%
        1|6|1|42--1|6|10|42--
        '''

    def test_utils_lang_jp_tr_pyargs_2(self):
        '''JS+

        class assert_raises {
            constructor(exc_cls, msg=null) {
                this.exc_cls = exc_cls;
                this.msg = msg;
            }

            enter() {}

            exit(exc) {
                if (!exc) {
                    throw new Error('no exception was thrown, expected to get ' + this.exc_cls);
                }

                if (!isinstance(exc, this.exc_cls)) {
                    throw new Error('expected ' + this.exc_cls + ' got ' + exc);
                }

                if (this.msg) {
                    msg = exc.toString();

                    if (msg.indexOf(this.msg) < 0) {
                        throw new Error('expected ' + this.exc_cls + ' message to contain "' +
                                        this.msg + '" got "' + msg + '"');
                    }
                }

                return true;
            }
        }

        try {
            with (assert_raises(Error)) {
            }
        } except (Error) {}
        else { assert 0 }

        try {
            with (assert_raises(Error)) {
                throw new Error('aaa')
            }
        } except (Error) {assert 0}


        function test1(a) {
            print(a)
        }
        with (assert_raises(TypeError, 'takes 1 of positional only arguments (2 given)')) {
            test1(1, 2)
        }
        with (assert_raises(TypeError, 'takes 1 of positional only arguments (0 given)')) {
            test1()
        }

        function test2(a, b=1) {
            print(a)
        }

        with (assert_raises(TypeError, 'got an unexpected keyword argument b')) {
            test2(1, b=2)
        }

        with (assert_raises(TypeError, 'got an unexpected keyword argument c')) {
            test2(1, c=2)
        }

        with (assert_raises(TypeError, 'takes 2 of positional only arguments (3 given)')) {
            test2(1, 2, 3)
        }

        with (assert_raises(TypeError, 'got an unexpected keyword argument c')) {
            test2(1, 2, c=2)
        }

        function test3(a, *, b) {
            print(a)
        }

        with (assert_raises(TypeError, 'takes 1 of positional only arguments (2 given)')) {
            test3(1, 2)
        }

        with (assert_raises(TypeError, 'needs keyword-only argument b')) {
            test3(1)
        }

        with (assert_raises(TypeError, 'needs keyword-only argument b')) {
            test3(1, c=2)
        }

        with (assert_raises(TypeError, 'test3() got an unexpected keyword argument c')) {
            test3(1, b=10, c=2)
        }
        '''

    def test_utils_lang_jp_tr_pyargs_3(self):
        '''JS+

        function test1(a, *arg, d=2) {
            return a + ':' + arg.join('|') + ':' + d + '--';
        }

        print(test1(1, 2, 3) + test1(1) + test1(2, d=6) + test1(1, 2, d=7))
        %%
        1:2|3:2--1::2--2::6--1:2:7--
        '''

    def test_utils_lang_jp_tr_pyargs_4(self):
        '''JS+

        function test1(arg_len, *arg, kwargs_len, **kwargs) {
            assert !kwargs.__jpkw

            assert len(arg) == arg_len
            assert len(kwargs) == kwargs_len

            return ('[' + arg.join(',') + ']+{' +
                            [k+':'+kwargs[k] for (k of keys(kwargs))].join(',') + '} ');
        }

        print(test1(2, 1, 2, kwargs_len=3, a=1, b=2, c=3) +
              test1(0, kwargs_len=0))
        %%
        [1,2]+{a:1,b:2,c:3} []+{}
        '''
