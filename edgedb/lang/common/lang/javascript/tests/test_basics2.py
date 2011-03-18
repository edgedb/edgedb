##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base2 import MetaTestJavascript, jxfail
from semantix.utils.lang.javascript.parser.jsparser2 import\
    UnknownToken, UnexpectedToken, UnknownOperator, MissingToken,\
    SecondDefaultToken, IllegalBreak, IllegalContinue, UndefinedLabel, DuplicateLabel,\
    UnexpectedNewline


class TestJavaScriptParsing(metaclass=MetaTestJavascript):
    def test_utils_lang_javascript2_literals1(self):
        """print(1, "hello", -2.3e-4);"""

    def test_utils_lang_javascript2_literals2(self):
        r"""print("'\"Hello\tworld\r!\nblah\b\101\x61\u0061");"""

    def test_utils_lang_javascript2_literals3(self):
        r"""print('\'"Hello\tworld\r!\nblah\b\101\x61\u0061');"""

    def test_utils_lang_javascript2_literals4(self):
        r"""print([1,2,'a']);"""

    def test_utils_lang_javascript2_array1(self):
        r"""print([1,2,'a',]);"""

    def test_utils_lang_javascript2_array2(self):
        r"""print([,,,]);"""

    def test_utils_lang_javascript2_array3(self):
        r"""print([,,,1,,2,,]);"""

    def test_utils_lang_javascript2_basic1(self):
        """print(1);"""

    def test_utils_lang_javascript2_basic2(self):
        """print(1+2);"""

    def test_utils_lang_javascript2_basic3(self):
        """print(("aa",print("foo"),3,true,1+4));"""

    def test_utils_lang_javascript2_basic4(self):
        """;;;;for(var i=0; i<3; print(i++));;;;;;;"""

    def test_utils_lang_javascript2_basic5(self):
        """var a,b,c=[,,7,,,8];print(1,2,3,4,"aa",a,b,c);"""

    def test_utils_lang_javascript2_basic6(self):
        """a=b=c=d='hello'; print(a,b,c,d);"""

    def test_utils_lang_javascript2_object1(self):
        """var a = {do : 1}; print(a.do);"""

    def test_utils_lang_javascript2_object2(self):
        """var a = {get while () {return "yes";}}; print(a.while);"""

    def test_utils_lang_javascript2_object3(self):
        """var a = {set get (a) {print("boo!");}}; print(a.get=2);"""

    def test_utils_lang_javascript2_object4(self):
        """
        var a = {
            do : 1,
            get while () {return "yes";},
            set get (a) {print("boo!");}
            };
        print(a.do, a.while, a.get=2);"""

    @jxfail(UnexpectedToken)
    def test_utils_lang_javascript2_object5(self):
        """
        var a = {2 : "foo", {} : "bar", {a : 2} : "weird"};
        print(a[2], a[{}], a[{a:2}]);"""

    def test_utils_lang_javascript2_object6(self):
        """
        var a = [];
        a[2] = "foo"; a[{}] = "bar"; a[{a : 2}] = "weird";
        print(a[2], a[{}], a[{a:2}]);
        """

    # weird bug here if ';' is remved at the end of func declaration
    def test_utils_lang_javascript2_object7(self):
        """
        var a = [];
        function fill(array) {
            array[2] = "foo"; array[{}] = "bar"; array[{b : 2}] = "weird";
            return a;
        };
        print(fill(a)[2], fill(a)[{}], fill(a)[{b:2}]);
        """

    def test_utils_lang_javascript2_object8(self):
        """print({get : 2}.get);"""

    def test_utils_lang_javascript2_object9(self):
        """new {};"""

    def test_utils_lang_javascript2_object10(self):
        """new {
    a : 1};"""

    def test_utils_lang_javascript2_object11(self):
        """print({a: "test"} ["a"]);"""

    def test_utils_lang_javascript2_object12(self):
        """print({a: "test"}.a);"""

    def test_utils_lang_javascript2_object13(self):
        """print({
    a : 2}(2));"""

    def test_utils_lang_javascript2_object14(self):
        """print({
    a : 2}++);"""

    def test_utils_lang_javascript2_object15(self):
        """print(delete {
        a: 2});"""

    def test_utils_lang_javascript2_object16(self):
        """print({a: 2} + {b : print("test")}.b);"""

    def test_utils_lang_javascript2_object17(self):
        """print("a" in {a: 2});"""

    def test_utils_lang_javascript2_object18(self):
        """print({a: 2} ? {b:3}.b : false);"""

    def test_utils_lang_javascript2_object19(self):
        """
        var a = {default : {a:{a:{}}}};
        print(a.default.a.a);
        """

    def test_utils_lang_javascript2_unary1(self):
        """var a = 3; print(-a++);"""

    def test_utils_lang_javascript2_unary2(self):
        """var a = 3;
print(---a);"""

    @jxfail(UnexpectedToken)
    def test_utils_lang_javascript2_unary3(self):
        """var a = 3; print(-a++++);"""

    def test_utils_lang_javascript2_binexpr1(self):
        """var a = 3; print(1 + 2 / 5 - a * 4 + 6.7 % 3);"""

    def test_utils_lang_javascript2_binexpr2(self):
        """var a = -3; print(a << 2, a >> 1, a >>> 1);"""

    def test_utils_lang_javascript2_binexpr3(self):
        """var a = 3; print(a < 3, a <= 3, a > 3, a >= 3);"""

    def test_utils_lang_javascript2_binexpr4(self):
        """var a = 3; print(a == "3", a != "a", a === "3", a !== 3);"""

    def test_utils_lang_javascript2_binexpr5(self):
        """var a = 3; print(a | 70 ^ 100 & 96, ((a | 70) ^ 100) & 96);"""

    def test_utils_lang_javascript2_binexpr6(self):
        """var a = 3; print(a == 2 || true && a == 4);"""

    def test_utils_lang_javascript2_binexpr7(self):
        """var a = 3; print(16 | false == 0 <= 1 + 2 / 5 - a * 4 + 6.7 % 3);"""

    def test_utils_lang_javascript2_ifexpr1(self):
        """var a = -4; a*=3; print(a > 0 ? "positive" : "negative");"""

    def test_utils_lang_javascript2_ifexpr2(self):
        """var a = -4; a*=3; print(a = 0 ? "positive" : "negative");"""

    def test_utils_lang_javascript2_throw(self):
        """throw "oops!";"""

    def test_utils_lang_javascript2_ifelse1(self):
        """if (1==1) print("duh!");"""

    def test_utils_lang_javascript2_ifelse2(self):
        """if (1==2) print("impossible"); else print("normal");"""

    def test_utils_lang_javascript2_ifelse3(self):
        """if (true) if ("s"!=3) if (1==2) print("impossible"); else print("normal");"""

    def test_utils_lang_javascript2_ifelse4(self):
        """if (true) while (true) if (false) print("bad"); else break; else print("trick"); print("out");"""

    def test_utils_lang_javascript2_ifelse5(self):
        """if (true) with (a=2) if (a!=2) print("bad"); else print("good"); else print("trick");"""

    def test_utils_lang_javascript2_ifelse6(self):
        """
        if (true)
            if (1==2)
                if (false) print("impossible");
                else print("impossible too");
            else print("normal");
        """

    def test_utils_lang_javascript2_empty(self):
        "      "

    def test_utils_lang_javascript2_function1(self):
        """
        var f = function (a) { print("Hello,", a); };
        f(1); f("world");
        """

    def test_utils_lang_javascript2_function2(self):
        """
        function f (a) { print("Hello,", a); }
        f(1); f("world");
        """

    @jxfail(UnexpectedToken)
    def test_utils_lang_javascript2_function3(self):
        """
        function (a) { print("Hello,", a); };
        f(1); f("world");
        """

    def test_utils_lang_javascript2_function4(self):
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

    def test_utils_lang_javascript2_function5(self):
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

    def test_utils_lang_javascript2_function6(self):
        """function foo(a, b) {
            return a + b;
        }
        print(foo('fu','bar'));
        """

    def test_utils_lang_javascript2_dowhile1(self):
        """
        var i = 0;
        do print(i++); while (i<3);
        """

    def test_utils_lang_javascript2_while1(self):
        """
        var i = 0;
        while (i<3) print(i++);
        """

    def test_utils_lang_javascript2_for1(self):
        """
        var i;
        for (i=0; i<4; i++) print(i);
        """

    def test_utils_lang_javascript2_for2(self):
        """for (var i=0; i<4; i++) print(i);"""

    def test_utils_lang_javascript2_for3(self):
        """
        var a = [2,"a",true], i;
        for (i in a) print(a[i]);
        """

    def test_utils_lang_javascript2_for4(self):
        """
        var a = [2,"a",true];
        for (var i in a) print(a[i]);
        """

    def test_utils_lang_javascript2_for5(self):
        """for ((print("blah"), 1) in [1,]) ;"""

    def test_utils_lang_javascript2_for6(self):
        """for ((print("foo"), 1 in []);false;print("bar"));"""

    def test_utils_lang_javascript2_for7(self):
        """for (a in ["foo",42] || [1,2,3,4]) print(a);"""

    def test_utils_lang_javascript2_for8(self):
        """for (; print("yay!"); ) break;"""

    def test_utils_lang_javascript2_for9(self):
        """var a;
        for (print(1),print(2),3,4,5,print(5),a=2;print("foo"),true;a++)
        {print(a); break;}"""

    @jxfail(MissingToken)
    def test_utils_lang_javascript2_for10(self):
        """for (1 in []; print("yay!"); ) break;"""

    def test_utils_lang_javascript2_for11(self):
        """for (2 + print(1); print("yay!"); ) break;"""

    @jxfail(MissingToken)
    def test_utils_lang_javascript2_for12(self):
        """for (2,print(3),1 in [],print(2); print("yay!"); ) break;"""

    def test_utils_lang_javascript2_for13(self):
        """for ((2,print(3),1 in [],print(2)); print("yay!"); ) break;"""

    def test_utils_lang_javascript2_for14(self):
        """for ({a: 1 in []}[true?'a':2 in [1]] && (3 in [3]); print("yay!"); ) break;"""

    def test_utils_lang_javascript2_for15(self):
        """for (print(1 in []); print("yay!"); ) break;"""

    @jxfail(MissingToken)
    def test_utils_lang_javascript2_for16(self):
        """for (var a,b,c in [1,2,3]) print(42);"""

    def test_utils_lang_javascript2_switch1(self):
        """switch (1) {}"""

    def test_utils_lang_javascript2_switch2(self):
        """
        switch (1) {
            case "a": print("bad"); break;
            case "1": print("hmmm"); break;
            case 1: print("right on!"); break;
            case 2: print("uh-oh"); break;
        }
        """

    def test_utils_lang_javascript2_switch3(self):
        """
        switch (10) {
            case "a": print("bad");
            case "10": print("also bad");
            default: print("default");
            case 1: print("one");
            case 2: print("two");
        }
        """

    @jxfail(SecondDefaultToken)
    def test_utils_lang_javascript2_switch4(self):
        """
        switch (10) {
            case "a": print("bad");
            case "10": print("also bad");
            default: print("default");
            default: print("one");
            case 2: print("two");
        }
        """

    def test_utils_lang_javascript2_regexp1(self):
        """print(/a/);"""

    def test_utils_lang_javascript2_regexp2(self):
        """print(/ / / / /);"""

    def test_utils_lang_javascript2_regexp3(self):
        """print({}/2/3);"""

    def test_utils_lang_javascript2_regexp4(self):
        """print((2)/2/3);"""

    def test_utils_lang_javascript2_regexp5(self):
        """{}/print(1)/print(2)/print(3);"""

    def test_utils_lang_javascript2_regexp6(self):
        r"""print(/a\r\"[q23]/i);"""

    def test_utils_lang_javascript2_regexp7(self):
        """
        function len(s) {return s.length;};
        print(3   /
        1/len("11111"));
        """

    def test_utils_lang_javascript2_regexp8(self):
        """
        function len(s) {return s.length;};
        print(3
        /1/len("11111"));
        """

    def test_utils_lang_javascript2_regexp9(self):
        """
        function len(s) {return s.length;};
        print(3   /
        /1/len("11111"));
        """

    def test_utils_lang_javascript2_with1(self):
        """with (a=2) print(a);"""

    def test_utils_lang_javascript2_with2(self):
        """with ({a:function () { print("foo");}}) a();"""

    def test_utils_lang_javascript2_with3(self):
        """A: {with (a=2) {print(a); break A; print('never');}}"""

    def test_utils_lang_javascript2_block1(self):
        """{ print(1); }"""

    @jxfail(MissingToken)
    def test_utils_lang_javascript2_block2(self):
        """{ {a:1,b:2}; }"""

    def test_utils_lang_javascript2_block3(self):
        """{ var a ={a:1,b:2}; } print(a.a, a.b);"""

    @jxfail(UnexpectedToken)
    def test_utils_lang_javascript2_block4(self):
        """
        {
            {default : {a : {}}}
        }
        """

    def test_utils_lang_javascript2_block5(self):
        """
        {
            {b : {a : {}}}
        }
        print("done");
        """

    def test_utils_lang_javascript2_block6(self):
        """
        {
            print(
            {default : {a : {}}}
            .default);
        }
        """

    def test_utils_lang_javascript2_block7(self):
        """
        {
            print(
            {b : {a : {}}}
            .b.a);
        }
        """

    def test_utils_lang_javascript2_block8(self):
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

    def test_utils_lang_javascript2_try1(self):
        """
        try {
            ;print("no problem");
        }
        catch(err) {
            ;print("hmmm");
        }
        """

    def test_utils_lang_javascript2_try2(self):
        """
        try {
            print("no problem");
        }
        finally {
            print("hmmm");
        }
        """

    def test_utils_lang_javascript2_try3(self):
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

    def test_utils_lang_javascript2_try4(self):
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

    def test_utils_lang_javascript2_break1(self):
        """print(1); b: {print(2); break b; print(42);} print("yay");"""

    def test_utils_lang_javascript2_break2(self):
        """print(1); b: {print(2); while (true) break; print(42);} print("yay");"""

    @jxfail(IllegalBreak)
    def test_utils_lang_javascript2_break3(self):
        """print(1); b: {print(2); break; print(42);} print("yay");"""

    @jxfail(UndefinedLabel)
    def test_utils_lang_javascript2_break4(self):
        """b: print(1); {print(2); break b; print(42);} print("yay");"""

    def test_utils_lang_javascript2_break5(self):
        """a: function foo() {a: {print("bar"); break a; print("never");}}; foo();"""

    @jxfail(DuplicateLabel)
    def test_utils_lang_javascript2_break6(self):
        """a: function foo() {a: {print("bar"); a: {break a; print("never");}}}; foo();"""

    @jxfail(UndefinedLabel)
    def test_utils_lang_javascript2_break7(self):
        """a: function foo() {b: {print("bar"); c: {break a; print("never");}}}; foo();"""

    def test_utils_lang_javascript2_break8(self):
        """a: function foo() {b: {print("bar"); c: {break c; print("never");}}}; foo();"""

    @jxfail(UndefinedLabel)
    def test_utils_lang_javascript2_continue1(self):
        """print(1); b: {print(2); continue b; print(42);} print("yay");"""

    def test_utils_lang_javascript2_continue2(self):
        """var x = true;b: {print(2); while (x) {print(x); x = !x; continue; x = true;}} print("yay");"""

    @jxfail(IllegalContinue)
    def test_utils_lang_javascript2_continue3(self):
        """print(1); b: {print(2); continue; print(42);} print("yay");"""

    @jxfail(UndefinedLabel)
    def test_utils_lang_javascript2_continue4(self):
        """b: print(1); {print(2); continue b; print(42);} print("yay");"""

    def test_utils_lang_javascript2_continue5(self):
        """a: function foo(x) {a: while (x) {print(x); x = !x; continue a; print("never");}}; foo(true);"""

    @jxfail(DuplicateLabel)
    def test_utils_lang_javascript2_continue6(self):
        """a: function foo(x) {a: while (x) {a: print(x); x = !x; continue a; print("never");}}; foo(true);"""

    @jxfail(UndefinedLabel)
    def test_utils_lang_javascript2_continue7(self):
        """a: function foo(x) {b: while (x) {c: print(x); x = !x; continue a; print("never");}}; foo(true);"""

    @jxfail(UndefinedLabel)
    def test_utils_lang_javascript2_continue8(self):
        """a: function foo(x) {b:{c: while (x) {print(x); x = !x; continue b; print("never");}}}; foo(true);"""

    def test_utils_lang_javascript2_label1(self):
        """A: print(1); A: print(2); A: print(3);"""

    @jxfail(DuplicateLabel)
    def test_utils_lang_javascript2_label2(self):
        """A: {print(1); A: {print(2); A: {print(3);}}}"""

    def test_utils_lang_javascript2_label3(self):
        """A: {print(1); B: {print(2); C: {print(3);}}}"""

    @jxfail(DuplicateLabel)
    def test_utils_lang_javascript2_label4(self):
        """A: {print(1); A: function foo () {print(2); A: {print(3);}} foo();}"""

    def test_utils_lang_javascript2_label5(self):
        """A: {print(1); B: function foo () {print(2); A: {print(3);}} foo();}"""

    def test_utils_lang_javascript2_label6(self):
        """
        A: var a = {A: function () {A: while (true) { print('obj'); break A; }}};
        a.A();
        """

    @jxfail(UndefinedLabel)
    def test_utils_lang_javascript2_label7(self):
        """
        A: var a = {A: function () {while (true) { print('obj'); break A; }}};
        a.A();
        """

    def test_utils_lang_javascript2_label8(self):
        """
        A: var a = {get A () {A: while (true) { print('obj'); break A; }}};
        a.A;
        """

    @jxfail(UndefinedLabel)
    def test_utils_lang_javascript2_label9(self):
        """
        A: var a = {get A () {while (true) { print('obj'); break A; }}};
        a.A;
        """

    @jxfail(MissingToken)
    def test_utils_lang_javascript2_label10(self):
        """(a): print('foo');"""

    @jxfail(MissingToken)
    def test_utils_lang_javascript2_label11(self):
        """b+a: print('foo');"""

    @jxfail(MissingToken)
    def test_utils_lang_javascript2_label12(self):
        """10: print('foo');"""

    def test_utils_lang_javascript2_semicolon1(self):
        "print('hello')"

    @jxfail(UnexpectedToken)
    def test_utils_lang_javascript2_semicolon2(self):
        "if (true)"

    def test_utils_lang_javascript2_semicolon3(self):
        """
        for (
            a=1;
            a<3;
            a++) print(a)
        """

    @jxfail(MissingToken)
    def test_utils_lang_javascript2_semicolon4(self):
        """
        for (
            a=1
            a<3;
            a++) print(a)
        """

    def test_utils_lang_javascript2_semicolon5(self):
        """{print('hello')}"""

    @jxfail(UnexpectedToken)
    def test_utils_lang_javascript2_semicolon6(self):
        """print('hello')}"""

    def test_utils_lang_javascript2_semicolon7(self):
        """
        { 1
        2 } 3
        print('hello');
        """

    def test_utils_lang_javascript2_semicolon8(self):
        """{1+2}print('hello')"""

    @jxfail(UnexpectedToken)
    def test_utils_lang_javascript2_semicolon9(self):
        """{if (true)}"""

    @jxfail(MissingToken)
    def test_utils_lang_javascript2_semicolon10(self):
        """
        for (print('wrong');true
        ) break;
        """

    @jxfail(UnexpectedToken)
    def test_utils_lang_javascript2_semicolon11(self):
        """
        print('right')
        if (false)
        else print('wrong')
        """

    def test_utils_lang_javascript2_semicolon12(self):
        """
        function b (bar) { print(bar); return {foo : function () {print('foo')} } }
        d = c = 'bar'
        a = 1 + b
        (d+c).foo()
        """

    @jxfail(UnexpectedNewline)
    def test_utils_lang_javascript2_semicolon13(self):
        """
        throw
        'wrong';
        """

    def test_utils_lang_javascript2_semicolon14(self):
        """function foo(){
        return
                'foo'
        }
        print(foo());
        """

    def test_utils_lang_javascript2_semicolon15(self):
        """a:{
            a = 'hello'
            print(a);
            while (true)
                break
                        a;
            print('world');
            }
        """

    def test_utils_lang_javascript2_semicolon16(self):
        """a:{
            a = 'hello'
            print(a);
            while (true)
                break    a;
            print('world');
            }
        """

    def test_utils_lang_javascript2_semicolon17(self):
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

    def test_utils_lang_javascript2_semicolon18(self):
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

    def test_utils_lang_javascript2_semicolon19(self):
        """a=b=1;
        a
        ++
        b
        print(a,b);
        """

    def test_utils_lang_javascript2_semicolon20(self):
        """a=b=1;
        a++
        ++
        b
        print(a,b);
        """

    def test_utils_lang_javascript2_semicolon21(self):
        """a=b=1;
        a
        +
        ++
        b
        print(a,b);
        """

