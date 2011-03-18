##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import ast
import semantix.utils.lang.javascript.parser.jsparser as jsp
from semantix.utils.lang.javascript.codegen import JavascriptSourceGenerator

class TestJavaScriptParsing:
    def test_utils_lang_javascript_1(self):
        src = "1+2"
        jsparser = jsp.JSParser()
        tree = jsparser.parse(src)
        print(ast.dump.pretty_dump(tree, colorize=True))
        print(JavascriptSourceGenerator.to_source(tree))

