##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import MetaJSParserTest_Base, jxfail, flags
from semantix.utils.lang.javascript.parser.jsparser import UnknownToken, UnexpectedToken


class TestJSParser_withExtras(metaclass=MetaJSParserTest_Base):
    @flags(yieldsupport=True)
    def test_utils_lang_js_parser_extra_yield1(self):
        '''function foo() { while (true) yield 42; }; print(foo().next());'''

    @jxfail(UnknownToken)
    def test_utils_lang_js_parser_extra_yield2(self):
        '''function foo() { while (true) yield 42; }; print(foo().next());'''

    @flags(letsupport=True)
    def test_utils_lang_js_parser_extra_let1(self):
        '''{let a, b = [3, 4]; print(a, b);}'''

    @flags(letsupport=True)
    def test_utils_lang_js_parser_extra_let2(self):
        '''{let [a, b] = [3, 4]; print(a, b);}'''

    @flags(letsupport=True)
    def test_utils_lang_js_parser_extra_let3(self):
        '''print(((let ([a, b] = [3, 4]) (a + b)) + 42));'''

    @flags(letsupport=True)
    def test_utils_lang_js_parser_extra_let4(self):
        '''let ([a, b] = [3, 4]) print(a, b);'''

    @flags(letsupport=True)
    def test_utils_lang_js_parser_extra_let5(self):
        '''for(let a in [1, 2, 3]) print('foo');'''

    @jxfail(UnknownToken)
    def test_utils_lang_js_parser_extra_let6(self):
        '''let ([a, b] = [3, 4]) print(a, b);'''

    @flags(foreachsupport=True)
    def test_utils_lang_js_parser_extra_foreach1(self):
        '''for each (a in [1, 3 , 42]) print(a);'''

    @jxfail(UnexpectedToken)
    def test_utils_lang_js_parser_extra_foreach2(self):
        '''for each (a in [1, 3 , 42]) print(a);'''

    @flags(catchifsupport=True)
    def test_utils_lang_js_parser_extra_trycatchif1(self):
        """
        try {
            print("no problem");
        }
        catch(err) {
            print("hmmm");
        }
        """

    @flags(catchifsupport=True)
    def test_utils_lang_js_parser_extra_trycatchif2(self):
        """
        try {
            print("no problem");
        }
        finally {
            print("hmmm");
        }
        """

    @flags(catchifsupport=True)
    def test_utils_lang_js_parser_extra_trycatchif3(self):
        """
        try {
            print("no problem");
        }
        catch(err) {
            print("hmmm");
        }
        finally {
            print("all done");
        }
        """

    @flags(catchifsupport=True)
    def test_utils_lang_js_parser_extra_trycatchif4(self):
        """
        try {
            print(a);
            throw "boo!";
        }
        catch(err if err == "boo!") {
            print("boo!")
        }
        catch(err if err == "foo!") {
            print("foo!")
        }
        catch(err) {
            print(err);
        }
        finally {
            print("all done");
        }
        """

    @jxfail(UnexpectedToken)
    def test_utils_lang_js_parser_extra_trycatchif5(self):
        """
        try {
            print(a);
            throw "boo!";
        }
        catch(err if err == "boo!") {
            print("boo!")
        }
        catch(err if err == "foo!") {
            print("foo!")
        }
        catch(err) {
            print(err);
        }
        finally {
            print("all done");
        }
        """

    @flags(arraycompsupport=True)
    def test_utils_lang_js_parser_extra_comprehension1(self):
        """
        a = [2,3,4,5];
        print([("foo" + el) for each (el in a) if ((el%2) == 1)]);
        """

    @flags(arraycompsupport=True)
    def test_utils_lang_js_parser_extra_comprehension2(self):
        """
        var girls = [ "Sarah", "Tricia", "Kit", "Joanna" ];
        var equations = [ "is", "is not" ];
        var adjectives = [ "hot", "awesome", "sexy" ];

        var collection = [
        (((((name + " ") + equation) + " so ") + adjective) + "!")
        for each (name in girls)
        for each (equation in equations)
        for each (adjective in adjectives)
        if (name != "Kit")
        ];

        print(collection);
        """

    @flags(generatorexprsupport=True)
    def test_utils_lang_js_parser_extra_comprehension3(self):
        """
        a = [2,3,4,5];
        gen = (("foo" + el) for (el in a) if ((el%2) == 1));
        print(gen.next());
        """

    @flags(generatorexprsupport=True)
    def test_utils_lang_js_parser_extra_comprehension4(self):
        """
        var girls = [ "Sarah", "Tricia", "Kit", "Joanna" ];
        var equations = [ "is", "is not" ];
        var adjectives = [ "hot", "awesome", "sexy" ];

        var collection = (
        (((((name + " ") + equation) + " so ") + adjective) + "!")
        for (name in girls)
        for (equation in equations)
        for (adjective in adjectives)
        if (name != "Kit")
        );

        print(collection.next());
        """

    @flags(generatorexprsupport=True)
    def test_utils_lang_js_parser_extra_comprehension5(self):
        """
        a = [2,3,4,5];
        gen = [("foo" + el) for (el in a) if ((el%2) == 1)];
        print(gen);
        """

    @flags(generatorexprsupport=True)
    def test_utils_lang_js_parser_extra_comprehension6(self):
        """
        var girls = [ "Sarah", "Tricia", "Kit", "Joanna" ];
        var equations = [ "is", "is not" ];
        var adjectives = [ "hot", "awesome", "sexy" ];

        var collection = [
        (((((name + " ") + equation) + " so ") + adjective) + "!")
        for (name in girls)
        for (equation in equations)
        for (adjective in adjectives)
        if (name != "Kit")
        ];

        print(collection);
        """

    @flags(generatorexprsupport=True)
    def test_utils_lang_js_parser_extra_comprehension7(self):
        """
        a = [2,3,4,5];
        gen = (("foo" + el) for each (el in a) if ((el%2) == 1));
        print(gen.next());
        """

    @flags(generatorexprsupport=True)
    @jxfail(UnexpectedToken)
    def test_utils_lang_js_parser_extra_comprehension8(self):
        """
        a = [2,3,4,5] for each;
        print(a);
        """

    @jxfail(UnexpectedToken)
    def test_utils_lang_js_parser_extra_comprehension9(self):
        """
        a = [2,3,4,5];
        print([("foo" + el) for each (el in a) if ((el%2) == 1)]);
        """

    def test_utils_lang_js_parser_extra_comprehension10(self):
        """
        a = [2,3,4,5];
        b = ("foo" + el)
        for (el in a) if ((el%2) == 1);
        print(b);
        """

    @flags(foreachsupport=True)
    def test_utils_lang_js_parser_extra_comprehension11(self):
        """
        a = [2,3,4,5];
        b = ("foo" + el)
        for each (el in a) if ((el%2) == 1);
        print(b);
        """

    @jxfail(UnexpectedToken)
    def test_utils_lang_js_parser_extra_comprehension12(self):
        """
        a = [2,3,4,5];
        b = ("foo" + el)
        for each (el in a) if ((el%2) == 1);
        print(b);
        """

    @jxfail(UnexpectedToken)
    def test_utils_lang_js_parser_extra_comprehension13(self):
        """
        a = [2,3,4,5];
        b = [("foo" + el)
        for (el in a) if ((el%2) == 1)];
        print(b);
        """

    @jxfail(UnexpectedToken)
    def test_utils_lang_js_parser_extra_comprehension14(self):
        """
        a = [2,3,4,5];
        b = (("foo" + el)
        for (el in a) if ((el%2) == 1));
        print(b);
        """

    @flags(foreachsupport=True, arraycompsupport=True)
    @jxfail(UnexpectedToken)
    def test_utils_lang_js_parser_extra_comprehension15(self):
        """
        a = [2,3,4,5];
        b = (("foo" + el)
        for each (el in a) if ((el%2) == 1));
        print(b);
        """

    @flags(foreachsupport=True, arraycompsupport=True)
#    @jxfail(UnexpectedToken)
    def test_utils_lang_js_parser_extra_comprehension16(self):
        """
        a = [2,3,4,5];
        b = ("foo" + el)
        for each (el in a) if ((el%2) == 1);
        print(b);
        """

    def test_utils_lang_js_parser_extra_expand1(self):
        '[a, b] = [2, 4];'

    def test_utils_lang_js_parser_extra_expand2(self):
        '[, b] = [2, 4];'

