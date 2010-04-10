##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import pyggy

from semantix.utils import ast

from .errors import CaosQLSyntaxError
from semantix.caos.caosql import ast as qlast, CaosQLQueryError


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
        return ast.fix_parent_links(raw_ast)

    def normalize_select_query(self, query, filters, sort):
        nodetype = type(query)

        qtree = query

        if nodetype != qlast.SelectQueryNode:
            selnode = qlast.SelectQueryNode()
            selnode.targets = [qlast.SelectExprNode(expr=qtree)]
            qtree = selnode

        if filters:
            targets = {t.alias: t.expr for t in qtree.targets}

            for name, value in filters.items():
                target = targets.get(name)
                if not target:
                    err = 'filters reference column %s which is not in query targets' % name
                    raise CaosQLQueryError(err)

                if qtree.where:
                    const = qlast.ConstantNode(value=None, index='__filter%s' % name)
                    left = qtree.where
                    right = qlast.BinOpNode(left=target, right=const, op=ast.ops.EQ)
                    qtree.where = qlast.BinOpNode(left=left, right=right, op=ast.ops.AND)

        if sort:
            targets = {t.alias: t.expr for t in qtree.targets}
            newsort = []

            for name, direction in sort:
                target = targets.get(name)
                if not target:
                    err = 'sort reference column %s which is not in query targets' % name
                    raise CaosQLQueryError(err)

                newsort.append(qlast.SortExprNode(path=target, direction=direction))

            qtree.orderby = newsort

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
