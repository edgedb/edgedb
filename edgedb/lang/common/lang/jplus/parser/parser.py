##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang.javascript.parser.jsparser import *
from metamagic.utils.lang.javascript import ast as js_ast

from . import keywords
from .. import ast


class IllegalStatic(SyntaxError):
    pass


class Parser(JSParser):
    keywords = keywords

    def __init__(self):
        super().__init__()

    def _get_operators_table(self):
        table = super()._get_operators_table()
        # (<bp>, <special type>, <token vals>, <active>)
        table.append(('rbp', 'Unary', ('@',), True));
        return table

    @stamp_state('loop', affectslabels=True)
    def parse_for_guts(self):
        """Parse for loop."""

        self.must_match('(')
        init_expr = None
        for_type = 'simple'

        # skip classical for, without initializer
        #
        if not self.tentative_match(';', consume=False):
            self.enter_state('noin')

            if self.tentative_match('var'):
                # var declaration
                init_expr = self.parse_var_guts(statement=False)
            else:
                init_expr = self.parse_expression()

            self.exit_state()

        expr = expr2 = expr3 = None

        if self.tentative_match('in', 'of', consume=False):
            for_type = self.token.value
            self.get_next_token()
            # for (x in [1,2,3]) ...
            #
            expr = self.parse_expression()

        else:
            # we've got 'classical' for
            #
            self.must_match(';', allowsemi=False)

            if not self.tentative_match(';'):
                expr2 = self.parse_expression()
                self.must_match(';', allowsemi=False)

            if not self.tentative_match(')', consume=False):
                expr3 = self.parse_expression()

        self.must_match(')')
        stmt = self.parse_statement()

        if for_type == 'simple':
            return js_ast.ForNode(part1=init_expr, part2=expr2, part3=expr3, statement=stmt);
        elif for_type == 'of':
            return ast.ForOfNode(init=init_expr, container=expr, statement=stmt);
        else:
            return js_ast.ForInNode(init=init_expr, container=expr, statement=stmt);

    @stamp_state('class')
    def parse_class_guts(self):
        name = self.parse_ID().name
        self.must_match('(', regexp=False)
        bases = self.parse_expression_list()
        self.must_match(')', regexp=False)

        self.must_match('{', regexp=False)

        body = []
        while not self.tentative_match('}', consume=True):
            if self.tentative_match('@', 'var', 'function', 'static', consume=False):
                statement = self.parse_statement()

                if statement:
                    body.append(statement)

                continue

            raise UnexpectedToken(self.token, parser=self)

        return ast.ClassNode(name=name, bases=bases, body=body)

    def nud_AT(self, token):
        # parse decorator list
        decorators = []
        at_rbp = token.rbp

        while True:
            if self.token.type != 'ID':
                raise UnexpectedToken(self.token, parser=self)

            decorators.append(self.parse_assignment_expression(at_rbp))

            if not self.tentative_match('@', regexp=False):
                break

        if not self.tentative_match('class', 'function', 'static', consume=False):
            raise UnexpectedToken(self.token, parser=self)

        return ast.DecoratedNode(node=self.parse_statement(),
                                 decorators=decorators)

    def nud_SUPER(self, token):
        return self.parse_super_guts()

    def parse_super_guts(self):
        self.must_match('(', regexp=False)

        cls = instance = None
        if self.token.type == 'ID':
            cls = self.parse_assignment_expression()

            if self.tentative_match(',', regexp=False):
                if self.token.type == 'ID' or self.token.type == 'THIS':
                    instance = self.parse_assignment_expression()
                else:
                    raise UnexpectedToken(self.token, parser=self)

        self.must_match(')', '.', regexp=False)

        if self.token.type != 'ID':
            raise UnexpectedToken(self.token, parser=self)

        method = self.token.value
        self.get_next_token(False)

        self.must_match('(', regexp=False)
        arguments = self.parse_expression_list()
        self.must_match(')', regexp=False)

        return ast.SuperCallNode(cls=cls, instance=instance,
                                 arguments=arguments, method=method)

    def parse_static_guts(self):
        if not self.enclosing_state('class'):
            raise IllegalStatic(self.token)

        node = None
        if self.tentative_match('var', regexp=False):
            node = self.parse_var_guts()
        elif self.tentative_match('function', regexp=False):
            node = self.parse_function_guts()

        if node is None:
            raise UnexpectedToken(self.token, parser=self)

        self.tentative_match(';', regexp=False)

        return ast.StaticDeclarationNode(decl=node)

    def parse_import_alias(self):
        asname = None
        name = ''
        while True:
            if self.token.type != 'ID':
                raise UnexpectedToken(self.token, ['ID'], parser=self)

            name += self.token.value
            self.get_next_token()

            if self.tentative_match('.', regexp=False):
                name += '.'
                continue

            elif self.tentative_match('as', regexp=False):
                if self.token.type != 'ID':
                    raise UnexpectedToken(self.token, ['ID'], parser=self)
                asname = self.token.value
                self.get_next_token()

            break

        return ast.ImportAliasNode(name=name, asname=asname)

    def parse_import_guts(self):
        names = []
        names.append(self.parse_import_alias())
        while self.tentative_match(',', regexp=False):
            names.append(self.parse_import_alias())
        return js_ast.StatementNode(statement=ast.ImportNode(names=names))

    def parse_from_guts(self):
        level = 0
        while self.tentative_match('.', regexp=False):
            level += 1

        module = None
        if self.token.type == 'ID':
            module = ''
            while True:
                if self.token.type != 'ID':
                    raise UnexpectedToken(self.token, ['ID'], parser=self)

                module += self.token.value
                self.get_next_token()

                if self.tentative_match('.', regexp=False):
                    module += '.'
                    continue

                elif self.tentative_match('import', regexp=False):
                    break

                else:
                    raise UnexpectedToken(self.token, parser=self)
        else:
            self.must_match('import')

        names = []
        names.append(self.parse_import_alias())
        while self.tentative_match(',', regexp=False):
            names.append(self.parse_import_alias())
        return js_ast.StatementNode(statement=ast.ImportFromNode(names=names, module=module,
                                                             level=level))
