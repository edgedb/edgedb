##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import copy

from metamagic.utils.lang.javascript.parser.jsparser import *
from metamagic.utils.lang.javascript import ast as js_ast

from . import keywords
from .. import ast


class IllegalStatic(SyntaxError):
    pass


class Parser(JSParser):
    keywords = keywords

    SPECIAL_NAMES = copy.copy(JSParser.SPECIAL_NAMES)
    SPECIAL_NAMES['is'] = 'Binary'
    SPECIAL_NAMES['isnt'] = 'Binary'

    def __init__(self):
        super().__init__()
        self.lexer.multiline_strings = True
        self.lexer.at_literal = True
        self.lexer.ellipsis_literal = True
        self.expansionsupport = True
        self.forofsupport = True
        self.expansionsupport = True

    def parse(self, *args, **kwargs):
        node = super().parse(*args, **kwargs)
        assert isinstance(node, js_ast.SourceElementsNode)
        return ast.ModuleNode(body=node.code)

    def _get_operators_table(self):
        table = super()._get_operators_table()
        # (<bp>, <special type>, <token vals>, <active>)
        table.append(('rbp', 'Unary', ('@',), True));
        return table

    def parse_assert_guts(self):
        test = self.parse_assignment_expression()

        failexpr = None
        if self.tentative_match(',', regexp=False):
            failexpr = self.parse_assignment_expression()

        return ast.AssertNode(test=test, failexpr=failexpr)

    @stamp_state('stmt', affectslabels=True)
    def parse_with_guts(self):
        started_at = self.prevtoken.position

        self.must_match('(')

        items = []
        while True:
            asname = None
            item_started_at = self.token.position
            expr = self.parse_assignment_expression()

            if self.tentative_match('as', regexp=False):
                asname = self.parse_ID().name

            items.append(ast.WithItemNode(expr=expr, asname=asname,
                                          position=item_started_at))

            if not self.tentative_match(',', regexp=False):
                break

        self.must_match(')', '{')
        body = self.parse_block_guts()

        return ast.WithNode(withitems=items, body=body, position=started_at)

    @stamp_state('stmt', affectslabels=True)
    def parse_try_guts(self):
        """Parse try statement."""

        started_at = self.prevtoken.position

        self.must_match('{')
        body = self.parse_block_guts()

        jscatch = None
        if self.tentative_match('catch'):
            catch_started_at = self.prevtoken.position
            old_mode = True
            self.must_match('(')
            ex_name = self.parse_ID().name
            self.must_match(')', '{')
            ex_body = self.parse_block_guts()
            jscatch = js_ast.CatchNode(catchid=ex_name, catchblock=ex_body,
                                       position=catch_started_at)

        handlers = []
        orelse = None
        if jscatch is None:
            while self.tentative_match('except'):
                except_started_at = self.prevtoken.position
                if self.tentative_match('('):
                    ex_type = []
                    ex_name = None
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

                    handlers.append(ast.ExceptNode(type=ex_type, name=ex_name, body=ex_body,
                                                   position=except_started_at))
                else:
                    self.must_match('{')
                    ex_body = self.parse_block_guts()
                    handlers.append(ast.ExceptNode(type=None, name=None, body=ex_body,
                                                   position=except_started_at))

            if handlers and self.tentative_match('else'):
                self.must_match('{')
                orelse = self.parse_block_guts()

        finalbody = None
        if self.tentative_match('finally'):
            self.must_match('{')
            finalbody = self.parse_block_guts()

        return ast.TryNode(body=body, handlers=handlers, orelse=orelse,
                           finalbody=finalbody, jscatch=jscatch,
                           position=started_at)

    @stamp_state('class')
    def parse_class_guts(self):
        started_at = self.prevtoken.position

        name = self.parse_ID().name

        metaclass = None
        bases = []
        if (self.tentative_match('(', regexp=False)
                    and not self.tentative_match(')', regexp=False)):

            while True:
                before_next_token = self.token
                next = self.parse_assignment_expression()

                if isinstance(next, js_ast.AssignmentExpressionNode):
                    if (not isinstance(next.left, js_ast.IDNode)
                                        or next.left.name != 'metaclass'):
                        raise UnexpectedToken(before_next_token, parser=self)

                    metaclass = next.right
                    self.must_match(')', regexp=False)
                    break

                bases.append(next)
                if not self.tentative_match(',', regexp=False):
                    self.must_match(')', regexp=False)
                    break

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
                                right=right,
                                position=id.position))

                continue

            raise UnexpectedToken(self.token, parser=self)

        return ast.ClassNode(name=name, bases=bases, body=body,
                             position=started_at, metaclass=metaclass)


    def nud_AT(self, token):
        # parse decorator list
        decorators = []
        at_rbp = token.rbp

        started_at = self.prevtoken.position

        while True:
            if self.token.type != 'ID':
                raise UnexpectedToken(self.token, parser=self)

            decorators.append(self.parse_assignment_expression(at_rbp))

            if not self.tentative_match('@', regexp=False):
                break

        if not self.tentative_match('class', 'function', 'static', consume=False):
            raise UnexpectedToken(self.token, parser=self)

        return ast.DecoratedNode(node=self.parse_statement(),
                                 decorators=decorators,
                                 position=started_at)

    def nud_SUPER(self, token):
        return self.parse_super_guts()

    def parse_super_guts(self):
        started_at = self.prevtoken.position

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

        if self.tentative_match('(', regexp=False):
            arguments = []
            if not self.tentative_match(')', regexp=False):
                arguments = self.parse_expression_list()
                self.must_match(')', regexp=False)

            return ast.SuperCallNode(cls=cls, instance=instance,
                                     arguments=arguments, method=method,
                                     position=started_at)
        else:
            return ast.SuperNode(cls=cls, instance=instance, method=method,
                                 position=started_at)

    def parse_nonlocal_guts(self):
        started_at = self.prevtoken.position
        ids = []
        ids.append(self.parse_ID())
        while self.tentative_match(','):
            ids.append(self.parse_ID())
        return ast.NonlocalNode(ids=ids, position=started_at)

    def parse_static_guts(self):
        started_at = self.prevtoken.position

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
                        right=right,
                        position=id.position)

        if node is None:
            raise UnexpectedToken(self.token, parser=self)

        self.tentative_match(';', regexp=False)

        return ast.StaticDeclarationNode(decl=node, position=started_at)

    def parse_import_alias(self):
        started_at = self.prevtoken.position
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

        return ast.ImportAliasNode(name=name, asname=asname, position=started_at)

    def parse_import_guts(self):
        started_at = self.prevtoken.position
        names = []
        names.append(self.parse_import_alias())
        while self.tentative_match(',', regexp=False):
            names.append(self.parse_import_alias())
        return js_ast.StatementNode(
                    statement=ast.ImportNode(names=names, position=started_at),
                    position=started_at)

    def parse_from_guts(self):
        started_at = self.prevtoken.position
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
        return js_ast.StatementNode(
                    statement=ast.ImportFromNode(
                        names=names,
                        module=module,
                        level=level,
                        position=started_at),
                    position=started_at)

    def parse_function_parameters(self):
        params = []

        defaults_mode = False
        if not self.tentative_match(')'):
            while True:
                started_at = self.token.position
                rest_param = False
                if self.tentative_match('...', regexp=False):
                    rest_param = True

                name = self.parse_ID()

                if (not rest_param) and (defaults_mode or self.tentative_match('=', consume=False)):
                    defaults_mode = True
                    self.must_match('=')
                    default = self.parse_assignment_expression()

                    param = ast.FunctionParameter(name=name.name,
                                                  default=default,
                                                  position=started_at)

                else:
                    param = ast.FunctionParameter(name=name.name,
                                                  rest=rest_param,
                                                  position=started_at)

                params.append(param)

                if rest_param:
                    self.must_match(')')
                    break
                elif self.tentative_match(')'):
                    break
                else:
                    self.must_match(',')

        return params

