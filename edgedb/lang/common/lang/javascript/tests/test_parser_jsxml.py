##
# Copyright (c) 2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import MetaJSParserTest_Base, jxfail, flags
from metamagic.utils.lang.javascript.parser.jsparser import \
    UnknownToken, UnexpectedToken, UnknownOperator,\
    SecondDefaultToken, IllegalBreak, IllegalContinue, UndefinedLabel, DuplicateLabel,\
    UnexpectedNewline


class TestJSParser_withXMLsupport(metaclass=MetaJSParserTest_Base):
    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag01(self):
        """var a = <foo />;"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag02(self):
        """a = <if />"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag03(self):
        """a = <foo.bar />"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag04(self):
        """a = <foo.bar:ham.spam />"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag05(self):
        """a = <foo.bar:ham.spam q="werty" />"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag06(self):
        """a = <foo.bar:ham.spam foo.bar.q.ham="werty" />"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag07(self):
        """a = <foo.bar:ham.spam foo.bar:q.ham="werty" />"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag08(self):
        """a = <foo.bar:ham.spam foo.bar:q1.ham="werty" foo.bar:q2.ham="12345" a="'whee'"/>"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag09(self):
        """var a = <foo>bar 344t</foo>;"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag10(self):
        """var a = <foo>
                        bar 344t<span>too</span>
                        silly
                   </foo>;"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag11(self):
        """var a = <foo>
                        bar 344t<span>too</span>
                        silly
                        <hr />
                        <hr />
                        <hr />
                   </foo>;"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag12(self):
        """var a = <foo a={null}>
                    blah
                   </foo>;"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag13(self):
        """var a = <foo a={null} b={(3 ? 'something': 'nothing')}>
                        { (56*7) }
                   </foo>;"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag14(self):
        """var a = <foo>
                        {(function(q) {print(q); return 'foo' + q;})}
                   </foo>;"""

    @flags(xmlsupport=True)
    def test_utils_lang_js_parser_jsxml_tag15(self):
        """var a = <foo a={null} b={(3 ? 'something': 'nothing')}>
                        { (56*7) }
                        {(function(q) {print(q); return 'foo' + q;}) (42)}
                   </foo>;"""
