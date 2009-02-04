import os
import pyggy
from semantix.caos.caosql.parser.errors import CaosQLSyntaxError
import semantix.ast

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

        ast = semantix.ast.fix_parent_links(raw_ast)

        return ast

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
