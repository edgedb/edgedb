##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import parsing
from edgedb.lang.common.ast import NodeVisitor
from edgedb.lang.graphql import ast as gqlast

from .grammar import lexer
from .errors import GraphQLParserError


class Validator(NodeVisitor):
    def __init__(self):
        self._fragments = {}
        self._ops = {}
        self._curroot = None

    def visit_Document(self, node):
        self.generic_visit(node)
        fragnodes = [n for n in node.definitions
                     if isinstance(n, gqlast.FragmentDefinition)]
        opnodes = [n for n in node.definitions
                   if isinstance(n, gqlast.OperationDefinition)]

        # now detect cycles in fragments
        #
        for frag in fragnodes:
            cycle = self.detect_cycle(frag.name, {frag.name})
            if cycle:
                cycle.reverse()
                raise GraphQLParserError(
                    'cycle in fragment definitions: {}'.format(cycle),
                    context=frag.context)

        # validate variable usage in operations
        #
        for opnode in opnodes:
            op = self._ops[id(opnode)]

            op['all_spreads'] = set(op['spreads'].keys())
            for fragname, spread in op['spreads'].items():
                try:
                    frag = self._fragments[fragname]
                except KeyError:
                    raise GraphQLParserError(
                        "undefined fragment '{}' used in operation".format(
                            fragname),
                        context=spread.context)
                op['all_spreads'] |= frag['all_spreads']
                frag['used_in_spead'] = True

            for fragname in op['all_spreads']:
                frag = self._fragments[fragname]
                op['usedvars'].update(
                    {k: v for k, v in frag['usedvars'].items()
                     if k not in op['usedvars']})

            vardefs = {var.name for var in opnode.variables or ()}
            if not vardefs.issuperset(op['usedvars'].keys()):
                uvars = [v for k, v in op['usedvars'].items()
                         if k not in vardefs]
                uvars.sort(key=lambda x: (x.context.start.line,
                                          x.context.start.column))
                uvar = uvars[0]
                if opnode.name:
                    op_str = "{!r} ".format(opnode.name)
                else:
                    op_str = ""
                op_str += "at {}, {}".format(
                    opnode.context.start.line,
                    opnode.context.start.column)

                raise GraphQLParserError(
                    "undefined variable {!r} (at {}, {}) used in operation {}"
                    .format(
                        uvar.value,
                        uvar.context.start.line,
                        uvar.context.start.column,
                        op_str),
                    context=opnode.context)

        # detect unused fragments
        #
        for frag in fragnodes:
            if not self._fragments[frag.name].get('used_in_spead'):
                raise GraphQLParserError(
                    "unused fragment definition '{}'".format(frag.name),
                    context=frag.context)

    def detect_cycle(self, fragname, visited):
        self._fragments[fragname]['all_spreads'] = \
            set(self._fragments[fragname]['spreads'])

        for spread in self._fragments[fragname]['spreads']:
            # mark fragment usage to detect unused fragments
            #
            self._fragments[spread]['used_in_spead'] = True

            # base case cycle detection
            #
            if spread in visited:
                return [spread, fragname]

            # detect cycle recursively
            #
            cycle = self.detect_cycle(spread, visited | {spread})
            if cycle is not None:
                cycle.append(fragname)
                return cycle

            self._fragments[fragname]['all_spreads'] |= \
                self._fragments[spread]['all_spreads']

    def visit_FragmentDefinition(self, node):
        self._curroot = {'usedvars': {}, 'spreads': {}}
        self._fragments[node.name] = self._curroot
        self.generic_visit(node)

    def visit_OperationDefinition(self, node):
        self._curroot = {'usedvars': {}, 'spreads': {}}
        self._ops[id(node)] = self._curroot
        self.generic_visit(node)

    def visit_Variable(self, node):
        val = self._curroot['usedvars'].get(node.value, node)
        self._curroot['usedvars'][node.value] = val

    def visit_FragmentSpread(self, node):
        val = self._curroot['spreads'].get(node.name, node)
        self._curroot['spreads'][node.name] = val


class GraphQLParser(parsing.Parser):
    def get_parser_spec_module(self):
        from .grammar import document
        return document

    def get_lexer(self):
        return lexer.GraphQLLexer()

    def get_exception(self, native_err, context):
        return GraphQLParserError(native_err.args[0], context=context)

    def process_lex_token(self, mod, tok):
        if tok.attrs['type'] in {'NL', 'WS', 'COMMENT', 'COMMA'}:
            return None
        else:
            return super().process_lex_token(mod, tok)

    def parse(self, input):
        result = super().parse(input)

        validator = Validator()
        validator.visit(result)

        return result
