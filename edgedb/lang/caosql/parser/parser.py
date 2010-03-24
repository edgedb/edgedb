##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import pyggy

import semantix.utils.ast

from .errors import CaosQLSyntaxError
from semantix.caos.caosql import ast as qlast

class CaosQLParser(object):
    def __init__(self):
        self.lexer, self.ltab = pyggy.getlexer(self.getsrc("caosql.pyl"))
        self.lexer.lineno = 1

        self.parser, self.ptab = pyggy.getparser(self.getsrc("caosql.pyg"))
        self.parser.setlexer(self.lexer)

    def parse(self, expr):
        self.lexer.setinputstr(expr)

        try:
            tree = self.parser.parse()
        except pyggy.ParseError as e:
            raise CaosQLSyntaxError(token=e.tok, expr=e.str, lineno=self.lexer.lineno)

        raw_ast = pyggy.proctree(tree, self.ptab)

        ast = semantix.utils.ast.fix_parent_links(raw_ast)

        return ast

    def normalize_select_query(self, query):
        nodetype = type(query)

        qtree = query

        if nodetype != qlast.SelectQueryNode:
            selnode = qlast.SelectQueryNode()
            selnode.targets = [qlast.SelectExprNode(expr=qtree)]
            qtree = selnode

        return qtree

    def parsepath(self, path):
        expr = self.parse(path)

        if not isinstance(expr, qlast.PathNode):
            raise CaosQLSyntaxError(token=None, expr=path, lineno=0)

        return expr

    def getsrc(self, name):
        return os.path.join(os.path.dirname(__file__), name)

if __name__ == '__main__':
    import sys

    parser = CaosQLParser()

    input = sys.stdin.read()
    result = parser.parse(input)

    import pprint
    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(result)
