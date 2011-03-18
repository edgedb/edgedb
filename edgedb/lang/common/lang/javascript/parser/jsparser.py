##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import pyggy
from semantix.utils import parsing, ast

class JSParser(parsing.Parser):
    def get_parser_spec_module(self):
        from . import js
        #import js
        return js

    def get_debug(self):
        return True

#    def get_exception(self, native_err):
#        return PgSQLParserError(native_err.args[0])


#jsparser = JSParser()
#test = "var a=3,s='aaa',q=true, w={get a(){}, set a(r){}};"
#tree = jsparser.parse(test)
#print(test)
#print(ast.dump.pretty_dump(tree, colorize=True))
#
#while True:
#    code = input("Just type some JS code (or '#exit'):")
#    if code == '#exit':
#        break
#    else:
#        tree = JSParser().parse(code)
#        print(ast.dump.pretty_dump(tree, colorize=True))
