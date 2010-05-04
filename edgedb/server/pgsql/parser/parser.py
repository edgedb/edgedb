##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import pyggy

from semantix.utils import ast

from .error import PgSQLParserError
from .. import ast as pgast


class PgSQLParser:
    def __init__(self):
        self.lexer, self.ltab = pyggy.getlexer(self.getsrc("pgsql.pyl"))
        self.lexer.lineno = 1

        self.parser, self.ptab = pyggy.getparser(self.getsrc("pgsql.pyg"))
        self.parser.setlexer(self.lexer)

    def parse(self, expr):
        self.lexer.setinputstr(expr)

        try:
            tree = self.parser.parse()
        except pyggy.ParseError as e:
            raise PgSQLParserError(str(e), token=e.tok, expr=e.str, lineno=self.lexer.lineno)

        raw_ast = pyggy.proctree(tree, self.ptab)
        return ast.fix_parent_links(raw_ast)

    def getsrc(self, name):
        return os.path.join(os.path.dirname(__file__), name)

if __name__ == '__main__':
    import sys

    parser = PgSQLParser()

    input = sys.stdin.read()
    result = parser.parse(input)

    print(ast.dump.pretty_dump(result, colorize=True))
