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

    def __init__(self, withstatement=True):
        super().__init__(expansionsupport=True, forofsupport=True,
                         arraycompsupport=True, arrowfuncsupport=True,
                         paramdefaultsupport=True, xmlsupport=True)

        self.lexer.multiline_strings = True
        self.lexer.at_literal = True
        self.withstatement = withstatement

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

    @stamp_state('loop', affectslabels=True)
    def parse_do_guts(self):
        """Parse do loop."""

        started_at = self.token.position

        if not self.tentative_match('with', regexp=False):
            return super().parse_do_guts()

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
            if self.tentative_match('@'):
                body.append(self.nud_AT(self.prevtoken, inclass=True))
            else:
                body.append(self.parse_class_member())
                self.must_match(';')

        return ast.ClassNode(name=name, bases=bases, body=body,
                             position=started_at, metaclass=metaclass)

    def parse_class_member(self, methodonly=False):
        is_static = bool(self.tentative_match('static'))

        started_at = self.token.position
        nxt = self.parse_ID()

        if not methodonly and self.tentative_match('='):
            return ast.ClassMemberNode(
                            name=nxt.name,
                            is_static=is_static,
                            value=self.parse_assignment_expression(),
                            position=started_at)

        self.must_match('(')
        param = self.parse_function_parameters()
        self.must_match('{')
        body = self.parse_block_guts()

        return ast.ClassMethodNode(
                            name=nxt.name,
                            is_static=is_static,
                            param=param,
                            body=body,
                            isdeclaration=True,
                            position=started_at)

    def nud_AT(self, token, inclass=False):
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

        if inclass:
            return ast.DecoratedNode(node=self.parse_class_member(methodonly=True),
                                     decorators=decorators,
                                     position=started_at)

        if not self.tentative_match('class', 'function', consume=False):
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
            if level == 0:
                raise UnexpectedToken(self.token, parser=self)
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

        state = ast.POSITIONAL_ONLY

        if not self.tentative_match(')'):
            while True:
                started_at_tok = self.token

                if self.tentative_match('*', regexp=False):
                    if self.tentative_match('*', regexp=False):
                        name = self.parse_ID()
                        params.append(ast.FunctionParameter(
                                name=name.name,
                                type=ast.VAR_KEYWORD))
                        self.must_match(')')
                        break

                    if state < ast.KEYWORD_ONLY:
                        state = ast.KEYWORD_ONLY
                        if self.token.type == 'ID':
                            name = self.parse_ID()
                            params.append(ast.FunctionParameter(
                                    name=name.name,
                                    type=ast.VAR_POSITIONAL))

                        elif not self.tentative_match(',', consume=False, regexp=False):
                            raise UnexpectedToken(self.token, parser=self)
                    else:
                        raise UnexpectedToken(started_at_tok, parser=self)

                else:
                    if state == ast.POSITIONAL_ONLY:
                        name = self.parse_ID()
                        if self.tentative_match('=', regexp=False):
                            default = self.parse_assignment_expression()
                            state = ast.POSITIONAL_ONLY_DEFAULT
                            params.append(ast.FunctionParameter(
                                    name=name.name,
                                    default=default,
                                    type=ast.POSITIONAL_ONLY_DEFAULT))
                        else:
                            params.append(ast.FunctionParameter(
                                    name=name.name,
                                    type=ast.POSITIONAL_ONLY))

                    elif state == ast.POSITIONAL_ONLY_DEFAULT:
                        name = self.parse_ID()
                        self.must_match('=')
                        default = self.parse_assignment_expression()
                        params.append(ast.FunctionParameter(
                                name=name.name,
                                default=default,
                                type=ast.POSITIONAL_ONLY_DEFAULT))

                    elif state == ast.KEYWORD_ONLY:
                        name = self.parse_ID()
                        if self.tentative_match('='):
                            default = self.parse_assignment_expression()
                            params.append(ast.FunctionParameter(
                                    name=name.name,
                                    default=default,
                                    type=ast.KEYWORD_ONLY))
                        else:
                            params.append(ast.FunctionParameter(
                                    name=name.name,
                                    type=ast.KEYWORD_ONLY))

                    else:
                        raise UnexpectedToken(started_at_tok, parser=self)

                if not self.tentative_match(',', regexp=False):
                    self.must_match(')', regexp=False)
                    break

        return params

    @stamp_state('(')
    def led_LPAREN(self, left, token):
        "This is parenthesis as operator, implying callable"

        args = []
        state = ast.POSITIONAL_ONLY

        if not self.tentative_match(')', regexp=False):
            while True:
                started_at_tok = self.token

                if self.tentative_match('*', regexp=False):
                    if self.tentative_match('*', regexp=False):
                        args.append(ast.CallArgument(
                                            name=self.parse_ID(),
                                            type=ast.VAR_KEYWORD))
                        self.must_match(')', regexp=False)
                        break

                    if state < ast.KEYWORD_ONLY:
                        state = ast.KEYWORD_ONLY
                        args.append(ast.CallArgument(
                                        name=self.parse_ID(),
                                        type=ast.VAR_POSITIONAL))
                    else:
                        raise UnexpectedToken(started_at_tok, parser=self)

                else:
                    if state == ast.POSITIONAL_ONLY:
                        was_bracket = self.tentative_match('(', regexp=False, consume=False)
                        arg = self.parse_assignment_expression()
                        if not was_bracket and isinstance(arg, js_ast.AssignmentExpressionNode):
                            if not isinstance(arg.left, js_ast.IDNode):
                                raise UnexpectedToken(started_at_tok, parser=self)

                            state = ast.KEYWORD_ONLY
                            args.append(ast.CallArgument(
                                                name=arg.left.name,
                                                value=arg.right,
                                                type=ast.KEYWORD_ONLY))
                        else:
                            args.append(ast.CallArgument(
                                                value=arg,
                                                type=ast.POSITIONAL_ONLY))

                    elif (state == ast.KEYWORD_ONLY and
                                not self.tentative_match('(', regexp=False, consume=False)):
                        arg = self.parse_assignment_expression()
                        if (not isinstance(arg, js_ast.AssignmentExpressionNode) or
                                not isinstance(arg.left, js_ast.IDNode)):
                            raise UnexpectedToken(self.token, parser=self)

                        args.append(ast.CallArgument(
                                            name=arg.left.name,
                                            value=arg.right,
                                            type=state))

                    else:
                        raise UnexpectedToken(started_at_tok, parser=self)

                if not self.tentative_match(',', regexp=False):
                    self.must_match(')', regexp=False)
                    break

        return ast.CallNode(call=left, arguments=args,
                            position=token.position)
