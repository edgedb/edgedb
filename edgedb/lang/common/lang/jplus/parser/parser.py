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
        self.lexer.multiline_strings = True

    def parse(self, *args, **kwargs):
        node = super().parse(*args, **kwargs)
        assert isinstance(node, js_ast.SourceElementsNode)
        return ast.ModuleNode(body=node.code)

    def _get_operators_table(self):
        table = super()._get_operators_table()
        # (<bp>, <special type>, <token vals>, <active>)
        table.append(('rbp', 'Unary', ('@',), True));
        return table

    @stamp_state('stmt', affectslabels=True)
    def parse_try_guts(self):
        """Parse try statement."""

        self.must_match('{')
        body = self.parse_block_guts()

        handlers = []
        while self.tentative_match('catch'):
            if self.tentative_match('('):
                ex_type = []
                if self.tentative_match('['):
                    while True:
                        ex_type.append(self.parse_ID())
                        if not self.tentative_match(','):
                            break
                    self.must_match(']')
                else:
                    ex_type.append(self.parse_ID())

                if self.tentative_match('as'):
                    ex_name = self.parse_ID()

                self.must_match(')', '{')
                ex_body = self.parse_block_guts()

                handlers.append(ast.CatchNode(type=ex_type, name=ex_name, body=ex_body))
            else:
                self.must_match('{')
                ex_body = self.parse_block_guts()
                handlers.append(ast.CatchNode(type=None, name=None, body=ex_body))

        orelse = None
        if handlers and self.tentative_match('else'):
            self.must_match('{')
            orelse = self.parse_block_guts()

        finalbody = None
        if self.tentative_match('finally'):
            self.must_match('{')
            finalbody = self.parse_block_guts()

        return ast.TryNode(body=body, handlers=handlers, orelse=orelse, finalbody=finalbody)

    @stamp_state('loop', affectslabels=True)
    def parse_for_guts(self):
        """Parse foreach loop."""

        if self.tentative_match('each'):
            self.must_match('(')

            init = []
            init.append(self.parse_ID())

            if self.tentative_match(',', regexp=False):
                init.append(self.parse_ID())

            self.must_match('in')

            expr = self.parse_expression()

            self.must_match(')')
            stmt = self.parse_statement()

            return ast.ForeachNode(init=init, container=expr, statement=stmt)

        else:
            return super().parse_for_guts()

    @stamp_state('class')
    def parse_class_guts(self):
        name = self.parse_ID().name

        bases = []
        if self.tentative_match('(', regexp=False):
            bases = self.parse_expression_list()
            self.must_match(')', regexp=False)

        self.must_match('{', regexp=False)

        body = []
        while not self.tentative_match('}', consume=True):
            if self.tentative_match('@', 'function', 'static', consume=False):
                statement = self.parse_statement()

                if statement:
                    body.append(statement)

                continue

            if self.token.type == 'ID':
                id = self.parse_ID()
                self.must_match('=')
                right = self.parse_assignment_expression()
                self.tentative_match(';', consume=True)

                body.append(js_ast.AssignmentExpressionNode(
                                left=id,
                                op='=',
                                right=right))

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
        arguments = []
        if not self.tentative_match(')', regexp=False):
            arguments = self.parse_expression_list()
            self.must_match(')', regexp=False)

        return ast.SuperCallNode(cls=cls, instance=instance,
                                 arguments=arguments, method=method)

    def parse_nonlocal_guts(self):
        ids = []
        ids.append(self.parse_ID())
        while self.tentative_match(','):
            ids.append(self.parse_ID())
        return ast.NonlocalNode(ids=ids)

    def parse_static_guts(self):
        if not self.enclosing_state('class'):
            raise IllegalStatic(self.token)

        node = None
        if self.tentative_match('function', regexp=False):
            node = self.parse_function_guts()
        elif self.token.type == 'ID':
            id = self.parse_ID()
            self.must_match('=')
            right = self.parse_assignment_expression()
            self.tentative_match(';', consume=True)
            node = js_ast.AssignmentExpressionNode(
                        left=id,
                        op='=',
                        right=right)

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

    def parse_function_parameters(self):
        params = []

        defaults_mode = False
        if not self.tentative_match(')'):
            while True:
                name = self.parse_ID()

                if defaults_mode or self.tentative_match('=', consume=False):
                    defaults_mode = True
                    self.must_match('=')
                    default = self.parse_assignment_expression()

                    param = ast.FunctionParameter(name=name.name,
                                                  default=default)

                else:
                    param = ast.FunctionParameter(name=name.name)

                params.append(param)

                if self.tentative_match(')'):
                    break
                else:
                    self.must_match(',')

        return params

