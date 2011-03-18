##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import MetaTestJavascript, jxfail
from semantix.utils.parsing import ParserError


class TestJavaScriptParsing(metaclass=MetaTestJavascript):
    def test_utils_lang_javascript_literals1(self):
        """print(1, "hello", -2.3e-4);"""

    def test_utils_lang_javascript_literals2(self):
        r"""print("Hello\tworld\r!\nblah\b\101\x61\u0061");"""

    def test_utils_lang_javascript_1(self):
        """print(1);"""

    def test_utils_lang_javascript_2(self):
        """print(1+2);"""

    def test_utils_lang_javascript_3(self):
        """print(("aa",print("foo"),3,true,1+4));"""

    def test_utils_lang_javascript_4(self):
        """;;;;for(var i=0; i<3; print(i++));;;;;;;"""

    def test_utils_lang_javascript_5(self):
        """var a,b,c=[,,7,,,8];print(1,2,3,4,"aa",a,b,c);"""

    def test_utils_lang_javascript_object1(self):
        """var a = {do : 1}; print(a.do);"""

    def test_utils_lang_javascript_object2(self):
        """var a = {get while () {return "yes";}}; print(a.while);"""

    def test_utils_lang_javascript_object3(self):
        """var a = {set get (a) {print("boo!");}}; print(a.get=2);"""

    def test_utils_lang_javascript_object4(self):
        """
        var a = {
            do : 1,
            get while () {return "yes";},
            set get (a) {print("boo!");}
            };
        print(a.do, a.while, a.get=2);"""

    def test_utils_lang_javascript_unary1(self):
        """var a = 3; print(-a++);"""

    def test_utils_lang_javascript_unary2(self):
        """var a = 3;
print(---a);"""

    def test_utils_lang_javascript_binexpr1(self):
        """var a = 3; print(1 + 2 / 5 - a * 4 + 6.7 % 3);"""

    def test_utils_lang_javascript_binexpr2(self):
        """var a = -3; print(a << 2, a >> 1, a >>> 1);"""

    def test_utils_lang_javascript_binexpr3(self):
        """var a = 3; print(a < 3, a <= 3, a > 3, a >= 3);"""

    def test_utils_lang_javascript_binexpr4(self):
        """var a = 3; print(a == "3", a != "a", a === "3", a !== 3);"""

    def test_utils_lang_javascript_binexpr5(self):
        """var a = 3; print(a | 70 ^ 100 & 96, ((a | 70) ^ 100) & 96);"""

    def test_utils_lang_javascript_binexpr6(self):
        """var a = 3; print(a == 2 || true && a == 4);"""

    def test_utils_lang_javascript_binexpr7(self):
        """var a = 3; print(16 | false == 0 <= 1 + 2 / 5 - a * 4 + 6.7 % 3);"""

    def test_utils_lang_javascript_ifexpr(self):
        """var a = -4; a*=3; print(a > 0 ? "positive" : "negative");"""

    def test_utils_lang_javascript_throw(self):
        """throw "oops!";"""

    def test_utils_lang_javascript_ifelse1(self):
        """if (1==1) print("duh!");"""

    def test_utils_lang_javascript_ifelse2(self):
        """if (1==2) print("impossible"); else print("normal");"""

    def test_utils_lang_javascript_ifelse3(self):
        """if (true) if ("s"!=3) if (1==2) print("impossible"); else print("normal");"""

    def test_utils_lang_javascript_ifelse4(self):
        """if (true) while (true) if (false) print("bad"); else break; else print("trick"); print("out");"""

    def test_utils_lang_javascript_ifelse5(self):
        """if (true) with (a=2) if (a!=2) print("bad"); else print("good"); else print("trick");"""

    def test_utils_lang_javascript_empty(self):
        "      "

    def test_utils_lang_javascript_function1(self):
        """
        var f = function (a) { print("Hello,", a); };
        f(1); f("world");
        """

    def test_utils_lang_javascript_function2(self):
        """
        function f (a) { print("Hello,", a); };
        f(1); f("world");
        """

    @jxfail(SyntaxError)
    def test_utils_lang_javascript_function3(self):
        """
        function (a) { print("Hello,", a); };
        f(1); f("world");
        """

    def test_utils_lang_javascript_iter1(self):
        """
        var i = 0;
        do print(i++); while (i<3);
        """

    def test_utils_lang_javascript_iter2(self):
        """
        var i = 0;
        while (i<3) print(i++);
        """

    def test_utils_lang_javascript_iter3(self):
        """
        var i;
        for (i=0; i<4; i++) print(i);
        """

    def test_utils_lang_javascript_iter4(self):
        """
        for (var i=0; i<4; i++) print(i);
        """

    def test_utils_lang_javascript_iter5(self):
        """
        var a = [2,"a",true], i;
        for (i in a) print(a[i]);
        """

    def test_utils_lang_javascript_iter6(self):
        """
        var a = [2,"a",true];
        for (var i in a) print(a[i]);
        """

    @jxfail(ParserError) # XXX , error_re='at col 1')
    def test_utils_lang_javascript_iter7(self):
        """
        for (print("blah"), 1 in []);
        """

    def test_utils_lang_javascript_iter8(self):
        """
        for ((print("foo"), 1 in []);false;print("bar"));
        """

    def test_utils_lang_javascript_switch1(self):
        """
        switch (1) {}
        """

    def test_utils_lang_javascript_switch2(self):
        """
        switch (1) {
            case "a": print("bad"); break;
            case "1": print("hmmm"); break;
            case 1: print("right on!"); break;
            case 2: print("uh-oh"); break;
        }
        """

    def test_utils_lang_javascript_switch3(self):
        """
        switch (10) {
            case "a": print("bad");
            case "10": print("also bad");
            default: print("default");
            case 1: print("one");
            case 2: print("two");
        }
        """

    def test_utils_lang_javascript_regexp1(self):
        """print(/a/);"""

    def test_utils_lang_javascript_regexp2(self):
        """print(/ / / / /);"""

    # problem here!!
    def _test_utils_lang_javascript_regexp3(self):
        """print({}/2/3);"""

    def _test_utils_lang_javascript_regexp4(self):
        """{}/print(1)/print(2)/print(3);"""

    def test_utils_lang_javascript_regexp5(self):
        r"""print(/a\r\"[q23]/i);"""

    def test_utils_lang_javascript_with1(self):
        """with (a=2) print(a);"""

    def test_utils_lang_javascript_with2(self):
        """with ({a:function () { print("foo");}}) a();"""

    def test_utils_lang_javascript_block1(self):
        """{ ; print(1); }"""

    def test_utils_lang_javascript_block2(self):
        """{ {a:1,b:2}; }"""

    def test_utils_lang_javascript_try1(self):
        """
        try {
            ;print("no problem");
        }
        catch(err) {
            ;print("hmmm");
        }
        """

    def test_utils_lang_javascript_try2(self):
        """
        try {
            ;print("no problem");
        }
        finally {
            ;print("hmmm");
        }
        """

    def test_utils_lang_javascript_try3(self):
        """
        try {
            ;print("no problem");
        }
        catch(err) {
            ;print("hmmm");
        }
        finally {
            ;print("all done");
        }
        """

    def test_utils_lang_javascript_try4(self):
        """
        try {
            ;print(a);
        }
        catch(err) {
            ;print(err);
        }
        finally {
            ;print("all done");
        }
        """
