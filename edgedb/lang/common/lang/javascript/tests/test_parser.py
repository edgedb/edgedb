##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import MetaJSParserTest_Functional, jxfail
from metamagic.utils.lang.javascript.parser.jsparser import \
    UnknownToken, UnexpectedToken, UnknownOperator,\
    SecondDefaultToken, IllegalBreak, IllegalContinue, UndefinedLabel, DuplicateLabel,\
    UnexpectedNewline


class TestJSParser(metaclass=MetaJSParserTest_Functional):
    def test_utils_lang_js_parser_literals1(self):
        """print(1, "hello", -2.3e-4);"""

    def test_utils_lang_js_parser_literals2(self):
        r"""print("'\"Hello\tworld\r!\nblah\b\101\x61\u0061");"""

    def test_utils_lang_js_parser_literals3(self):
        r"""print('\'"Hello\tworld\r!\nblah\b\101\x61\u0061');"""

    def test_utils_lang_js_parser_literals4(self):
        r"""print([1,2,'a']);"""

    def test_utils_lang_js_parser_literals5(self):
        r"""print('Hello,\
        World!');"""

    def test_utils_lang_js_parser_literals6(self):
        """print('function');"""

    def test_utils_lang_js_parser_array1(self):
        r"""print([1,2,'a',]);"""

    def test_utils_lang_js_parser_array2(self):
        r"""print([,,,]);"""

    def test_utils_lang_js_parser_array3(self):
        r"""print([,,,1,,2,,]);"""

    def test_utils_lang_js_parser_array4(self):
        "print([0, 1, 2][0, 1, 2]);"

    def test_utils_lang_js_parser_array5(self):
        "print(['1', '[', '2', ']', '3'].join(''));"

    def test_utils_lang_js_parser_basic1(self):
        """print(1);"""

    def test_utils_lang_js_parser_basic2(self):
        """print(1+2);"""

    def test_utils_lang_js_parser_basic3(self):
        """print(("aa",print("foo"),3,true,1+4));"""

    def test_utils_lang_js_parser_basic4(self):
        """;;;;for(var i=0; i<3; print(i++));;;;;;;"""

    def test_utils_lang_js_parser_basic5(self):
        """var a,b,c=[,,7,,,8];print(1,2,3,4,"aa",a,b,c);"""

    def test_utils_lang_js_parser_basic6(self):
        """a=b=c=d='hello'; print(a,b,c,d);"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 1})
    def test_utils_lang_js_parser_basic7(self):
        """@a=1; print(@a);"""

    @jxfail(UnknownToken, attrs={'line' : 1, 'col' : 1})
    def test_utils_lang_js_parser_basic8(self):
        """#a=1; print(a);"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 8})
    def test_utils_lang_js_parser_basic9(self):
        """print(1~2);"""

    def test_utils_lang_js_parser_basic10(self):
        """null; true; false; this;"""

    def test_utils_lang_js_parser_basic11(self):
        "print(')')"

    def test_utils_lang_js_parser_object1(self):
        """var a = {do : 1}; print(a.do);"""

    def test_utils_lang_js_parser_object2(self):
        """var a = {get while () {return "yes";}}; print(a.while);"""

    def test_utils_lang_js_parser_object3(self):
        """var a = {set get (a) {print("boo!");}}; print(a.get=2);"""

    def test_utils_lang_js_parser_object4(self):
        """
        var a = {
            do : 1,
            get while () {return "yes";},
            set get (a) {print("boo!");}
            };
        print(a.do, a.while, a.get=2);"""

    @jxfail(UnexpectedToken, attrs={'line' : 2, 'col' : 29})
    def test_utils_lang_js_parser_object5(self):
        """
        var a = {2 : "foo", {} : "bar", {a : 2} : "weird"};
        print(a[2], a[{}], a[{a:2}]);"""

    def test_utils_lang_js_parser_object6(self):
        """
        var a = [];
        a[2] = "foo"; a[{}] = "bar"; a[{a : 2}] = "weird";
        print(a[2], a[{}], a[{a:2}]);
        """

    def test_utils_lang_js_parser_object7(self):
        """
        var a = [];
        function fill(array) {
            array[2] = "foo"; array[{}] = "bar"; array[{b : 2}] = "weird";
            return a;
        };
        print(fill(a)[2], fill(a)[{}], fill(a)[{b:2}]);
        """

    def test_utils_lang_js_parser_object8(self):
        """print({get : 2}.get);"""

    def test_utils_lang_js_parser_object11(self):
        """print({a: "test"} ["a"]);"""

    def test_utils_lang_js_parser_object12(self):
        """print({a: "test"}.a);"""

    def test_utils_lang_js_parser_object13(self):
        """print({a : 2}(2));"""

    def test_utils_lang_js_parser_object14(self):
        """print({a : 2}++);"""

    def test_utils_lang_js_parser_object15(self):
        """print(delete {a: 2});"""

    def test_utils_lang_js_parser_object16(self):
        """print({a: 2} + {b : print("test")}.b);"""

    def test_utils_lang_js_parser_object17(self):
        """print("a" in {a: 2});"""

    def test_utils_lang_js_parser_object18(self):
        """print({a: 2} ? {b:3}.b : false);"""

    def test_utils_lang_js_parser_object19(self):
        """
        var a = {default : {a:{a:{}}}};
        print(a.default.a.a);
        """

    def test_utils_lang_js_parser_object20(self):
        """
        var a = {for : function(blah) {print('insane', blah);}};
        a
        // comments obscuring the '.' there
        /*
        * More obscure comments....
        */.
        /*
        *.
        *... even more stupid comments
        */
        for (a in [1,2,3])
            print(a);
        """

    def test_utils_lang_js_parser_object21(self):
        "print({a: '1', '}':'}'});"

    def test_utils_lang_js_parser_object22(self):
        """a=1
        {b:1}
        print(a);"""

#    def test_utils_lang_js_parser_object23(self):
#        "{a: funciton() {print(1)}}.foo()"

    def test_utils_lang_js_parser_unary1(self):
        """
        a=1;
        print(a++, ++a, a--, --a);
        print(+42, -42);
        print(!42)
        print(~42)
        print(void a);
        print(typeof a);

        b=['a','b','c'];
        print(b);
        delete b[1];
        print(b);
        """

    def test_utils_lang_js_parser_unary2(self):
        """var a = 3; print(-a++);"""

    def test_utils_lang_js_parser_unary3(self):
        """var a = 3;
print(---a);"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 22})
    def test_utils_lang_js_parser_unary4(self):
        """var a = 3; print(-a++++);"""

    def test_utils_lang_js_parser_unary5(self):
        """var date = +new Date;"""

    def test_utils_lang_js_parser_unary6(self):
        """b = 3.14; print(~~b);"""

    def test_utils_lang_js_parser_unary7(self):
        """
        var a = new Boolean(false); // how to quickly check if (a)?
        print(typeof a); // "object"
        print(!!a); // always true, because a is an object
        print(!!+a);
        // right result, because "+" is calling valueOf
        // method and convert result to number "0..1"
        """

    def test_utils_lang_js_parser_unary8(self):
        '''
        +1
        function a() { print('123'); }
        a()
        '''

    def test_utils_lang_js_parser_binexpr1(self):
        """
        print(2 + 3);
        print(2 - 3);
        print(2 * 3);
        print(2 / 3);
        print(2 % 3);

        print(-42<<3);
        print(-42>>3);
        print(-42>>>3);

        print(2 < 3);
        print(2 > 3);
        print(2 <= 3);
        print(2 >= 3);
        print(2 >= 3);
        print(1 in ['a', 'b']);
        a = String('test');
        print(a instanceof String);

        print(1 == '1');
        print(1 != '1');
        print(1 === '1');
        print(1 !== '1');

        print(1 & 5);
        print(1 | 4);
        print(1 ^ 5);

        print(3 && false);
        print(3 || false);

        print(true ? 'foo' : 'bar');
        """

    def test_utils_lang_js_parser_binexpr2(self):
        """
        print(a = 2);
        print(a += 3);
        print(a -= 3);
        print(a *= 3);
        print(a /= 3);
        print(a <<= 3);
        print(a >>= 1);
        print(a >>>= 2);
        print(a |= 196);
        print(a &= 15);
        print(a ^= 115);
        """

    def test_utils_lang_js_parser_binexpr3(self):
        """var a = 3; print(1 + 2 / 5 - a * 4 + 6.7 % 3);"""

    def test_utils_lang_js_parser_binexpr4(self):
        """var a = -3; print(a << 2, a >> 1, a >>> 1);"""

    def test_utils_lang_js_parser_binexpr5(self):
        """var a = 3; print(a < 3, a <= 3, a > 3, a >= 3);"""

    def test_utils_lang_js_parser_binexpr6(self):
        """var a = 3; print(a == "3", a != "a", a === "3", a !== 3);"""

    def test_utils_lang_js_parser_binexpr7(self):
        """var a = 3; print(a | 70 ^ 100 & 96, ((a | 70) ^ 100) & 96);"""

    def test_utils_lang_js_parser_binexpr8(self):
        """var a = 3; print(a == 2 || true && a == 4);"""

    def test_utils_lang_js_parser_binexpr9(self):
        """var a = 3; print(16 | false == 0 <= 1 + 2 / 5 - a * 4 + 6.7 % 3);"""

    def test_utils_lang_js_parser_ifexpr1(self):
        """var a = -4; a*=3; print(a > 0 ? "positive" : "negative");"""

    def test_utils_lang_js_parser_ifexpr2(self):
        """var a = -4; a*=3; print(a = 0 ? "positive" : "negative");"""

    def test_utils_lang_js_parser_ifexpr3(self):
        """var a, b = true ? a = 3 : 1; print(a, b);"""

    def test_utils_lang_js_parser_throw(self):
        """throw "oops!";"""

    def test_utils_lang_js_parser_ifelse1(self):
        """if (1==1) print("duh!");"""

    def test_utils_lang_js_parser_ifelse2(self):
        """if (1==2) print("impossible"); else print("normal");"""

    def test_utils_lang_js_parser_ifelse3(self):
        """if (true) if ("s"!=3) if (1==2) print("impossible"); else print("normal");"""

    def test_utils_lang_js_parser_ifelse4(self):
        """if (true) while (true) if (false) print("bad");
        else break; else print("trick");
        print("out");"""

    def test_utils_lang_js_parser_ifelse5(self):
        """if (true) with (a=2) if (a!=2) print("bad");
        else print("good"); else print("trick");"""

    def test_utils_lang_js_parser_ifelse6(self):
        """
        if (true)
            if (1==2)
                if (false) print("impossible");
                else print("impossible too");
            else print("normal");
        """

    def test_utils_lang_js_parser_empty1(self):
        ""

    def test_utils_lang_js_parser_empty2(self):
        "      "

    def test_utils_lang_js_parser_empty3(self):
        "//  print(1);"

    def test_utils_lang_js_parser_empty4(self):
        "/* print(1); */"

    def test_utils_lang_js_parser_empty5(self):
        """/* print(1);
        */"""

    def test_utils_lang_js_parser_function1(self):
        """
        var f = function (a) { print("Hello,", a); };
        f(1); f("world");
        """

    def test_utils_lang_js_parser_function2(self):
        """
        function f (a) { print("Hello,", a); }
        f(1); f("world");
        """

    @jxfail(UnexpectedToken, attrs={'line' : 2, 'col' : 18})
    def test_utils_lang_js_parser_function3(self):
        """
        function (a) { print("Hello,", a); };
        f(1); f("world");
        """

    def test_utils_lang_js_parser_function4(self):
        """function len(s) {
    function foo() {
        print("Foo! " + s);
    }
    foo();
    return s.length;
}
print(1 / len("hello"));
foo();
        """

    def test_utils_lang_js_parser_function5(self):
        """function bar()
        {
            function foo()
            {
                print('a');
                print('b');
            }
            print(1);
            ;
            ;
            print(2);
            ;
            print(3);
            ;
            foo();
        }
        bar();
        """

    def test_utils_lang_js_parser_function6(self):
        """function foo(a, b) {
            return a + b;
        }
        print(foo('fu','bar'));
        """

    def test_utils_lang_js_parser_dowhile1(self):
        """
        var i = 0;
        do print(i++); while (i<3);
        """

    def test_utils_lang_js_parser_while1(self):
        """
        var i = 0;
        while (i<3) print(i++);
        """

    def test_utils_lang_js_parser_for1(self):
        """
        var i;
        for (i=0; i<4; i++) print(i);
        """

    def test_utils_lang_js_parser_for2(self):
        """for (var i=0; i<4; i++) print(i);"""

    def test_utils_lang_js_parser_for3(self):
        """
        var a = [2,"a",true], i;
        for (i in a) print(a[i]);
        """

    def test_utils_lang_js_parser_for4(self):
        """
        var a = [2,"a",true];
        for (var i in a) print(a[i]);
        """

    def test_utils_lang_js_parser_for5(self):
        """for ((print("blah"), 1) in [1]) ;"""

    def test_utils_lang_js_parser_for6(self):
        """for ((print("foo"), 1 in []);false;print("bar"));"""

    def test_utils_lang_js_parser_for7(self):
        """for (a in ["foo",42] || [1,2,3,4]) print(a);"""

    def test_utils_lang_js_parser_for8(self):
        """for (; print("yay!"); ) break;"""

    def test_utils_lang_js_parser_for9(self):
        """var a;
        for (print(1),print(2),3,4,5,print(5),a=2;print("foo"),true;a++)
        {print(a); break;}"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 13})
    def test_utils_lang_js_parser_for10(self):
        """for (1 in []; print("yay!"); ) break;"""

    def test_utils_lang_js_parser_for11(self):
        """for (2 + print(1); print("yay!"); ) break;"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 33})
    def test_utils_lang_js_parser_for12(self):
        """for (2,print(3),1 in [],print(2); print("yay!"); ) break;"""

    def test_utils_lang_js_parser_for13(self):
        """for ((2,print(3),1 in [],print(2)); print("yay!"); ) break;"""

    def test_utils_lang_js_parser_for14(self):
        """for ({a: 1 in []}[true?'a':2 in [1]] && (3 in [3]); print("yay!"); ) break;"""

    def test_utils_lang_js_parser_for15(self):
        """for (print(1 in []); print("yay!"); ) break;"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 16})
    def test_utils_lang_js_parser_for16(self):
        """for (var a,b,c in [1,2,3]) print(42);"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 16})
    def test_utils_lang_js_parser_for17(self):
        """for (var i = 0;) print(1)"""

    def test_utils_lang_js_parser_switch1(self):
        """switch (1) {}"""

    def test_utils_lang_js_parser_switch2(self):
        """
        switch (1) {
            case "a": print("bad"); break;
            case "1": print("hmmm"); break;
            case 1: print("right on!"); break;
            case 2: print("uh-oh"); break;
        }
        """

    def test_utils_lang_js_parser_switch3(self):
        """
        switch (10) {
            case "a": print("bad");
            case "10": print("also bad");
            default: print("default");
            case 1: print("one");
            case 2: print("two");
        }
        """

    @jxfail(SecondDefaultToken, attrs={'line' : 6, 'col' : 13})
    def test_utils_lang_js_parser_switch4(self):
        """
        switch (10) {
            case "a": print("bad");
            case "10": print("also bad");
            default: print("default");
            default: print("one");
            case 2: print("two");
        }
        """

    def test_utils_lang_js_parser_regexp1(self):
        """print(/a/);"""

    def test_utils_lang_js_parser_regexp2(self):
        """print(/ / / / /);"""

    def test_utils_lang_js_parser_regexp3(self):
        """print({}/2/3);"""

    def test_utils_lang_js_parser_regexp4(self):
        """print((2)/2/3);"""

    ### XXX FIX IT
    def _test_utils_lang_js_parser_regexp5(self):
        """{}/print(1)/print(2)/print(3);"""

    def test_utils_lang_js_parser_regexp6(self):
        r"""print(/a\r\"[q23]/i);"""

    def test_utils_lang_js_parser_regexp7(self):
        """
        function len(s) {return s.length;};
        print(3   /
        1/len("11111"));
        """

    def test_utils_lang_js_parser_regexp8(self):
        """
        function len(s) {return s.length;};
        print(3
        /1/len("11111"));
        """

    ### XXX FIX IT
    def _test_utils_lang_js_parser_regexp9(self):
        """
        function len(s) {return s.length;};
        print(3   /
        /1/len("11111"));
        """

    def test_utils_lang_js_parser_with1(self):
        """with (a=2) print(a);"""

    def test_utils_lang_js_parser_with2(self):
        """with ({a:function () { print("foo");}}) a();"""

    def test_utils_lang_js_parser_with3(self):
        """A: {with (a=2) {print(a); break A; print('never');}}"""

    def test_utils_lang_js_parser_block1(self):
        """{ print(1); }"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 9})
    def test_utils_lang_js_parser_block2(self):
        """{ {a:1,b:2}; }"""

    def test_utils_lang_js_parser_block3(self):
        """{ ({a:1,b:2}); }"""

    def test_utils_lang_js_parser_block4(self):
        """{ var a ={a:1,b:2}; } print(a.a, a.b);"""

    @jxfail(UnexpectedToken, attrs={'line' : 3, 'col' : 14})
    def test_utils_lang_js_parser_block5(self):
        """
        {
            {default : {a : {}}}
        }
        """

    def test_utils_lang_js_parser_block6(self):
        """
        {
            {b : {a : {}}}
        }
        print("done");
        """

    def test_utils_lang_js_parser_block7(self):
        """
        {
            print(
            {default : {a : {}}}
            .default);
        }
        """

    def test_utils_lang_js_parser_block8(self):
        """
        {
            print(
            {b : {a : {}}}
            .b.a);
        }
        """

    def test_utils_lang_js_parser_block9(self):
        """
        {
            {
                print('a');
                print('b');
            }
            print(1);
            ;
            ;
            print(2);
            ;
            print(3);
            ;
            ;
        }
        """

    def test_utils_lang_js_parser_new1(self):
        """
        c = new Function ('return "hello";');
        print(c());
        """

    def test_utils_lang_js_parser_new2(self):
        """
        c = new Function();
        print(c());
        """

    def test_utils_lang_js_parser_new3(self):
        """
        c = new Function;
        print(c());
        """

    def test_utils_lang_js_parser_try1(self):
        """
        try {
            ;print("no problem");
        }
        catch(err) {
            ;print("hmmm");
        }
        """

    def test_utils_lang_js_parser_try2(self):
        """
        try {
            print("no problem");
        }
        finally {
            print("hmmm");
        }
        """

    def test_utils_lang_js_parser_try3(self):
        """
        try {
            print("no problem");
        }
        catch(err) {
            print("hmmm");
        }
        finally {
            ;print("all done");
        }
        """

    def test_utils_lang_js_parser_try4(self):
        """
        try {
            print(a);
        }
        catch(err) {
            print(err);
        }
        finally {
            print("all done");
        }
        """

    def test_utils_lang_js_parser_break1(self):
        """print(1); b: {print(2); break b; print(42);} print("yay");"""

    def test_utils_lang_js_parser_break2(self):
        """print(1); b: {print(2); while (true) break; print(42);} print("yay");"""

    @jxfail(IllegalBreak, attrs={'line' : 1, 'col' : 25})
    def test_utils_lang_js_parser_break3(self):
        """print(1); b: {print(2); break; print(42);} print("yay");"""

    @jxfail(UndefinedLabel, attrs={'line' : 1, 'col' : 31})
    def test_utils_lang_js_parser_break4(self):
        """b: print(1); {print(2); break b; print(42);} print("yay");"""

    def test_utils_lang_js_parser_break5(self):
        """a: function foo() {a: {print("bar"); break a; print("never");}}; foo();"""

    @jxfail(DuplicateLabel, attrs={'line' : 1, 'col' : 38})
    def test_utils_lang_js_parser_break6(self):
        """a: function foo() {a: {print("bar"); a: {break a; print("never");}}}; foo();"""

    @jxfail(UndefinedLabel, attrs={'line' : 1, 'col' : 48})
    def test_utils_lang_js_parser_break7(self):
        """a: function foo() {b: {print("bar"); c: {break a; print("never");}}}; foo();"""

    def test_utils_lang_js_parser_break8(self):
        """a: function foo() {b: {print("bar"); c: {break c; print("never");}}}; foo();"""

    @jxfail(UndefinedLabel, attrs={'line' : 1, 'col' : 34})
    def test_utils_lang_js_parser_continue1(self):
        """print(1); b: {print(2); continue b; print(42);} print("yay");"""

    def test_utils_lang_js_parser_continue2(self):
        """var x = true;
        b: {print(2); while (x) {print(x); x = !x; continue; x = true;}} print("yay");"""

    @jxfail(IllegalContinue, attrs={'line' : 1, 'col' : 25})
    def test_utils_lang_js_parser_continue3(self):
        """print(1); b: {print(2); continue; print(42);} print("yay");"""

    @jxfail(UndefinedLabel, attrs={'line' : 1, 'col' : 34})
    def test_utils_lang_js_parser_continue4(self):
        """b: print(1); {print(2); continue b; print(42);} print("yay");"""

    def test_utils_lang_js_parser_continue5(self):
        """a: function foo(x)
        {a: while (x) {print(x); x = !x; continue a; print("never");}}; foo(true);"""

    @jxfail(DuplicateLabel, attrs={'line' : 2, 'col' : 24})
    def test_utils_lang_js_parser_continue6(self):
        """a: function foo(x)
        {a: while (x) {a: print(x); x = !x; continue a; print("never");}}; foo(true);"""

    @jxfail(UndefinedLabel, attrs={'line' : 2, 'col' : 54})
    def test_utils_lang_js_parser_continue7(self):
        """a: function foo(x)
        {b: while (x) {c: print(x); x = !x; continue a; print("never");}}; foo(true);"""

    @jxfail(UndefinedLabel, attrs={'line' : 2, 'col' : 54})
    def test_utils_lang_js_parser_continue8(self):
        """a: function foo(x)
        {b:{c: while (x) {print(x); x = !x; continue b; print("never");}}}; foo(true);"""

    def test_utils_lang_js_parser_label1(self):
        """A: print(1); A: print(2); A: print(3);"""

    @jxfail(DuplicateLabel, attrs={'line' : 1, 'col' : 15})
    def test_utils_lang_js_parser_label2(self):
        """A: {print(1); A: {print(2); A: {print(3);}}}"""

    def test_utils_lang_js_parser_label3(self):
        """A: {print(1); B: {print(2); C: {print(3);}}}"""

    @jxfail(DuplicateLabel, attrs={'line' : 1, 'col' : 15})
    def test_utils_lang_js_parser_label4(self):
        """A: {print(1); A: function foo () {print(2); A: {print(3);}} foo();}"""

    def test_utils_lang_js_parser_label5(self):
        """A: {print(1); B: function foo () {print(2); A: {print(3);}} foo();}"""

    def test_utils_lang_js_parser_label6(self):
        """
        A: var a = {A: function () {A: while (true) { print('obj'); break A; }}};
        a.A();
        """

    @jxfail(UndefinedLabel, attrs={'line' : 2, 'col' : 72})
    def test_utils_lang_js_parser_label7(self):
        """
        A: var a = {A: function () {while (true) { print('obj'); break A; }}};
        a.A();
        """

    def test_utils_lang_js_parser_label8(self):
        """
        A: var a = {get A () {A: while (true) { print('obj'); break A; }}};
        a.A;
        """

    @jxfail(UndefinedLabel, attrs={'line' : 2, 'col' : 66})
    def test_utils_lang_js_parser_label9(self):
        """
        A: var a = {get A () {while (true) { print('obj'); break A; }}};
        a.A;
        """

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 4})
    def test_utils_lang_js_parser_label10(self):
        """(a): print('foo');"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 4})
    def test_utils_lang_js_parser_label11(self):
        """b+a: print('foo');"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 3})
    def test_utils_lang_js_parser_label12(self):
        """10: print('foo');"""

    def test_utils_lang_js_parser_semicolon1(self):
        "print('hello')"

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 10})
    def test_utils_lang_js_parser_semicolon2(self):
        "if (true)"

    def test_utils_lang_js_parser_semicolon3(self):
        """
        for (
            a=1;
            a<3;
            a++) print(a)
        """

    @jxfail(UnexpectedToken, attrs={'line' : 4, 'col' : 13})
    def test_utils_lang_js_parser_semicolon4(self):
        """
        for (
            a=1
            a<3;
            a++) print(a)
        """

    def test_utils_lang_js_parser_semicolon5(self):
        """{print('hello')}"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 15})
    def test_utils_lang_js_parser_semicolon6(self):
        """print('hello')}"""

    def test_utils_lang_js_parser_semicolon7(self):
        """
        { 1
        2 } 3
        print('hello');
        """

    def test_utils_lang_js_parser_semicolon8(self):
        """{1+2}print('hello')"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 11})
    def test_utils_lang_js_parser_semicolon9(self):
        """{if (true)}"""

    @jxfail(UnexpectedToken, attrs={'line' : 3, 'col' : 9})
    def test_utils_lang_js_parser_semicolon10(self):
        """
        for (print('wrong');true
        ) break;
        """

    @jxfail(UnexpectedToken, attrs={'line' : 4, 'col' : 9})
    def test_utils_lang_js_parser_semicolon11(self):
        """
        print('right')
        if (false)
        else print('wrong')
        """

    def test_utils_lang_js_parser_semicolon12(self):
        """
        function b (bar) { print(bar); return {foo : function () {print('foo')} } }
        d = c = 'bar'
        a = 1 + b
        (d+c).foo()
        """

    @jxfail(UnexpectedNewline, attrs={'line' : 2, 'col' : 14})
    def test_utils_lang_js_parser_semicolon13(self):
        """
        throw
        'wrong';
        """

    def test_utils_lang_js_parser_semicolon14(self):
        """function foo(){
        return
                'foo'
        }
        print(foo());
        """

    def test_utils_lang_js_parser_semicolon15(self):
        """a:{
            a = 'hello'
            print(a);
            while (true)
                break
                        a;
            print('world');
            }
        """

    def test_utils_lang_js_parser_semicolon16(self):
        """a:{
            a = 'hello'
            print(a);
            while (true)
                break    a;
            print('world');
            }
        """

    def test_utils_lang_js_parser_semicolon17(self):
        """
        a = 'hello'
        a: while (a) {
            print(a);
            while (a) {
                a = false
                continue
                        a;
            }
            print('odd');
        }
        """

    def test_utils_lang_js_parser_semicolon18(self):
        """
        a = 'hello'
        a: while (a) {
            print(a);
            while (a) {
                a = false
                continue a;
            }
            print('odd');
        }
        """

    def test_utils_lang_js_parser_semicolon19(self):
        """a=b=1;
        a
        ++
        b
        print(a,b);
        """

    def test_utils_lang_js_parser_semicolon20(self):
        """a=b=1;
        a++
        ++
        b
        print(a,b);
        """

    def test_utils_lang_js_parser_semicolon21(self):
        """a=b=1;
        a
        +
        ++
        b
        print(a,b);
        """

    def test_utils_lang_js_parser_semicolon22(self):
        """
        var a = 'test'
        print(a)
        """

    @jxfail(UnexpectedToken, attrs={'line' : 2, 'col' : 32})
    def test_utils_lang_js_parser_semicolon23(self):
        """
        function f() {print(1) return}
        f()"""

    def test_utils_lang_js_parser_semicolon24(self):
        """function f() {print(1)
        return}
        f()"""

    @jxfail(UnexpectedToken, attrs={'line' : 1, 'col' : 22})
    def test_utils_lang_js_parser_semicolon25(self):
        """function f() {print(1}
        f()"""

    def test_utils_lang_js_parser_semicolon26(self):
        """function f() {return 1
        '}'}
        print(f())"""

    # weird bug WAS here due to ';' being remved at the end of func declaration
    def test_utils_lang_js_parser_semicolon27(self):
        """
        var a = [];
        function fill(array) {
            array[2] = "foo"; array[{}] = "bar"; array[{b : 2}] = "weird";
            return a;
        }
        print(fill(a)[2], fill(a)[{}], fill(a)[{b:2}]);
        """

    def test_utils_lang_js_parser_instanceof(self):
        """print([] instanceof Array)"""

