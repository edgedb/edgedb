##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##



import sys
import tokenize
import pyggy
import os

from .. import ast as jsast
from . import keywords as keywords_module

from metamagic.exceptions import MetamagicError, _add_context
from metamagic.utils import markup


__all__ = ('JSParser', 'stamp_state', 'UnexpectedToken', 'SyntaxError')


class ExceptionContext(markup.MarkupExceptionContext):
    title = 'JavaScript Parser Context'

    def __init__(self, filename=None, lineno=None, colno=None, source=None):
        super().__init__()
        self.colno = colno
        self.filename = filename
        self.lineno = lineno
        if filename is None:
            self.source = source
            self.filename = '<string>'

    @classmethod
    def as_markup(cls, self, *, ctx):
        me = markup.elements

        tbp = me.lang.TracebackPoint(name='javascript', lineno=self.lineno,
                                     filename=self.filename, colno=self.colno)

        if self.filename == '<string>' and self.source is not None:
            tbp.load_source(window=3, lines=self.source.split('\n'))
        else:
            tbp.load_source(window=3)

        return me.lang.ExceptionContext(title=self.title, body=[tbp])


#
# Various errors
#

class ParseError(MetamagicError):
    pass


class SyntaxError(MetamagicError):
    def __init__(self, msg, line, col):
        super().__init__("Syntax error: %s" % msg)
        self.line = line
        self.col = col


class UnknownToken(SyntaxError):
    def __init__(self, token):
        super().__init__("unknown token %r (line %i, column %i)." %
                         (token.string, token.start[0], token.start[1]),
                         token.start[0], token.start[1])


class UnexpectedToken(SyntaxError):
    def __init__(self, token, expected=None, parser=None):
        if not expected:
            super().__init__("unexpected token %r (line %i, column %i)." %
                             (token.string, token.start[0], token.start[1]),
                             token.start[0], token.start[1])
        elif len(expected) == 1:
            super().__init__("unexpected token %r instead of %r (line %i, column %i)." %
                             (token.string, expected[0], token.start[0], token.start[1]),
                             token.start[0], token.start[1])
        else:
            super().__init__("unexpected token %r instead of one of %s (line %i, column %i)." %
                             (token.string, list(expected), token.start[0], token.start[1]),
                             token.start[0], token.start[1])

        if parser is not None:
            exc = ExceptionContext(lineno=token.start[0], colno=token.start[1],
                                   filename=parser.filename, source=parser.source)
            _add_context(self, exc)


class UnknownOperator(SyntaxError):
    def __init__(self, token):
        super().__init__("unknown operator %r on line %i, column %i." %
                         (token.string, token.start[0], token.start[1]),
                         token.start[0], token.start[1])


class SecondDefaultToken(SyntaxError):
    def __init__(self, token):
        super().__init__("more than one default clause in switch statement (line %i, column %i)." %
                         (token.start[0], token.start[1]),
                         token.start[0], token.start[1])


class IllegalBreak(SyntaxError):
    def __init__(self, token):
        super().__init__("illegal break statement (line %i, column %i)." %
                         (token.start[0], token.start[1]),
                         token.start[0], token.start[1])


class IllegalContinue(SyntaxError):
    def __init__(self, token):
        super().__init__("illegal continue statement (line %i, column %i)." %
                         (token.start[0], token.start[1]),
                         token.start[0], token.start[1])


class UndefinedLabel(SyntaxError):
    def __init__(self, token):
        super().__init__("undefined label %r (line %i, column %i)." %
                         (token.string, token.start[0], token.start[1]),
                         token.start[0], token.start[1])


class DuplicateLabel(SyntaxError):
    def __init__(self, token):
        super().__init__("duplicate label %r (line %i, column %i)." %
                         (token.string, token.start[0], token.start[1]),
                         token.start[0], token.start[1])


class UnexpectedNewline(SyntaxError):
    def __init__(self, token):
        super().__init__("unexpected line break after %r (line %i, column %i)." %
                         (token.string, token.end[0], token.end[1]),
                         token.end[0], token.end[1])


# decorator for certain parsing methods that need to keep track of labels
#
def stamp_state(name, affectslabels=False):
    """Add a state & label stamp to the stack before executing the function.
    Exit that state afterwards."""

    def wrapper(func):
        def callfunc(this, *args, **kwargs):
            this.enter_state(name, affectslabels=affectslabels)
            result = func(this, *args, **kwargs)
            this.exit_state()
            return result
        return callfunc
    return wrapper


class Token(tokenize.TokenInfo):
    @property
    def value(self):
        return self.string

    @property
    def position(self):
        return self.start

    def __init__(self, *args, **kwargs):
        self.parser = None

    def __repr__(self):
        return '<Token 0x{:x} {!r} {!r}>'.format(id(self), self.type, self.value)

    __str__ = __repr__

class End_Token(Token):
    'Special token for end of input stream'
    def __new__(cls, val, pos, line=''):
        tok = super().__new__(cls, '#EOF#', val, pos, pos, line)
        tok.rbp = tok.lbp = 0
        return tok


class Start_Token(Token):
    'Special token for end of input stream'
    def __new__(cls):
        tok = super().__new__(cls, '#START#', None, (0,0), (0,0), '')
        tok.rbp = tok.lbp = 0
        return tok

class JSParser:
    keywords = keywords_module

    SPECIAL_NAMES = {
        '('     : 'LPAREN',
        '['     : 'LSBRACKET',
        '{'     : 'LCBRACKET',
        '?'     : 'HOOK',
        '.'     : 'DOT',
        '=>'    : 'FatArrow',

        'function'  : 'FUNCTION',
        'instanceof': 'INSTANCEOF',
        'in'        : 'IN',
        'delete'    : 'DELETE',
        'void'      : 'VOID',
        'typeof'    : 'TYPEOF',
        'new'       : 'NEW',
        'super'     : 'SUPER',

        'true'  : 'TRUE',
        'false' : 'FALSE',
        'this'  : 'THIS',
        'null'  : 'NULL',

        #
        # extra
        #
        'let'   : 'LET',
        'for'   : 'FOR'
    }

    """ JavaScript Parser. Parsing engine with special rules for context, etc.
    parse(src) is the main method used for code parsing.
    """
    def __init__(self, lex_name=None,
                 yieldsupport=False,
                 letsupport=False,
                 expansionsupport=False,
                 catchifsupport=False,
                 arraycompsupport=False,
                 generatorexprsupport=False,
                 foreachsupport=False,
                 forofsupport=False,
                 arrowfuncsupport=False,
                 paramdefaultsupport=False,
                 paramrestsupport=False,
                 spreadsupport=False):

        super().__init__()

        if not lex_name:
            lex_name = os.path.join(os.path.dirname(__file__), "js.pyl")

        self.set_lexer(lex_name)
        self.reset()
        self.yieldsupport = yieldsupport
        self.letsupport = letsupport
        self.expansionsupport = expansionsupport
        self.catchifsupport = catchifsupport
        self.generatorexprsupport = generatorexprsupport
        self.arraycompsupport = arraycompsupport or self.generatorexprsupport
        self.forofsupport = forofsupport or self.arraycompsupport
        self.arrowfuncsupport = arrowfuncsupport
        self.paramdefaultsupport = paramdefaultsupport
        self.paramrestsupport = paramrestsupport
        self.spreadsupport = spreadsupport
        self.lexer.ellipsis_literal = paramrestsupport or spreadsupport
        self.setup_operators()

    def _get_operators_table(self):
        return [
        #   (<bp>, <special type>, <token vals>, <active>)
            ('rbp', '',         ('{', ), True),
        # XXX: '{' is right-binding because it functions like a prefix operator
        #       creating an object literal or variable unpacking expression
            ('lbp', '',         ('new', '[', '.', 'function', '=>'), True),
            ('lbp', '',         ('(', ), True),
            ('lbp', 'Unary',    ('++', '--'), True),
            ('rbp', 'Unary',    ('++', '--', '+', '-', '~', '!', 'delete', 'void', 'typeof', 'super'), True),
            ('lbp', 'Binary',   ('*', '/', '%'), True),
            ('lbp', 'Binary',   ('+', '-'), True),
            ('lbp', 'Binary',   ('<<', '>>', '>>>'), True),
            ('lbp', 'Binary',   ('<', '>', '<=', '>=', 'instanceof', 'in'), True),
            ('lbp', 'Binary',   ('==', '!=', '===', '!==', 'is', 'isnt'), True), # XXX: is, isnt - JPlus
            ('lbp', 'Binary',   ('&', ), True),
            ('lbp', 'Binary',   ('^', ), True),
            ('lbp', 'Binary',   ('|', ), True),
            ('lbp', 'Binary',   ('&&', ), True),
            ('lbp', 'Binary',   ('||', ), True),
            ('lbp', '',         ('?', ), True),
            ('lbp', 'Assign',
             ('=', '+=', '-=', '*=', '/=', '%=', '<<=', '>>=', '>>>=', '&=', '|=', '^='), True),
            ('rbp', '',         ('let', ), self.letsupport)
        ]

    def setup_operators(self):
        'Setup operators and precedence.'

        self.PREC = self._get_operators_table()

        #: this will contain mappings from values to token information for various operations
        #:
        self.OP_2_INFO = {}

        #: calculate the starting binding power
        #:
        bp_val = len(self.PREC) * 10

        for bp, special, vals, active in self.PREC:
            if active:
                for val in vals:
                    # initialize a dict entry if need be
                    #
                    if not self.OP_2_INFO.get(val, None):
                        self.OP_2_INFO[val] = {}

                    self.OP_2_INFO[val][bp] = bp_val
                    spec_id = 'nud' if bp == 'rbp' else 'led'
                    self.OP_2_INFO[val][spec_id] = special

            bp_val -= 10

    def get_bp(self, val, bp_type):
        "Returns the bp for a given token value and type of binding ('rbp' and 'lbp')."
        if val in self.OP_2_INFO:
            return self.OP_2_INFO[val].get(bp_type, 0)

        else:
            return 0


    def get_token_special_type(self, token, context):
        """Return special type or just token type based on context ('nud' or 'led') and whether
        a special type info exists for a token and the token is NOT alphabetic."""

        tok_val = token.value
        tok_type = token.type

        if tok_type in ('ID', 'OP'):
            try:
                return self.SPECIAL_NAMES[tok_val]
            except KeyError:
                pass

        if tok_type == 'OP' and tok_val in self.OP_2_INFO:
            return self.OP_2_INFO[tok_val].get(context, '')

        return tok_type

    def reset(self):
        """Reset the line & col counters, and internal state."""

        self.token = self.prevtoken = Start_Token()
        self.lexer.line, self.lexer.col = 1, 1
        self._scope = []
        self._labels = []

    @property
    def state(self):
        """parser state handling"""
        return self._scope[-1][0] if self._scope else None

    def enter_state(self, state, affectslabels=False):
        self._scope.append((state, self._labels))
        if affectslabels:
            self._labels = []

    def exit_state(self):
        self._scope.pop()

    def enclosing_state(self, *states, boundary='function'):
        "Checking for enclosing state(s)"

        scope = self._scope[:]
        scope.reverse()
        for s in scope:
            if s[0] == boundary:
                break
            elif s[0] in states:
                return True
        return False

    def enclosing_loop_labels(self):
        """parser state handling"""
        return self.eclosing_labels('loop')

    def enclosing_stmt_labels(self):
        return self.eclosing_labels('loop', 'stmt', 'switch')

    def eclosing_labels(self, *states, boundary='function'):
        "Looking for labels of enclosing state(s)"

        scope = self._scope[:]
        scope.reverse()
        labels = []
        for s in scope:
            if s[0] == boundary:
                break
            elif s[0] in states:
                labels += s[1]
        return labels

    @property
    def labels(self):
        return self._labels

    @labels.setter
    def labels(self, labels):
        self._labels = labels

    @property
    def linebreak_detected(self):
        return self.token.start[0] > self.prevtoken.end[0]


    def set_lexer(self, fname):
        "Setting up the lexer from the file spec."

        self.lexer, self.tab = pyggy.getlexer(fname, debug=0)


    def get_next_token(self, regexp=True):
        "Uses the associated lexer to grab the next token."

        self.prevtoken = self.token
        self.lexer.regexp_possible = regexp

        try:
            token = self.lexer.token()
        except Exception as ex:
            new_ex = ParseError('unhandled lexer error')
            new_ex.__cause__ = ex
            ex_ctx = ExceptionContext(lineno=self.token.start[0], colno=self.token.start[1],
                       filename=self.filename, source=self.source)
            _add_context(new_ex, ex_ctx)
            raise new_ex

        # identifier-looking tokens can be a number of things
        #
        if token == 'ID':
            val = self.lexer.value

            # could be a keyword
            #
            if val in self.keywords.js_keywords:
                token = 'KEYWORD'

                # some keywords are operators
                #
                if val in self.OP_2_INFO:
                    token = 'OP'

                # some are special values
                #
                else:
                    token = self.SPECIAL_NAMES.get(val, token)

        if token:
            tok = Token(token, self.lexer.value, self.lexer.start, self.lexer.end, '')

        else: # no token type means EOF
            pos = (self.lexer.line, self.lexer.col)
            tok = End_Token(self.lexer.value, pos, '')

        tok.rbp = self.get_bp(tok.value, 'rbp')
        tok.lbp = self.get_bp(tok.value, 'lbp')
        self.token = tok

        if tok.type == '#ERR#':
            raise UnknownToken(tok)


    def must_match(self, *tok, regexp=True, allowsemi=True):
        """Matches the current token against the specified value.
        If more than one value is given, then a sequence of tokens must match.
        Consumes the token if it is correct, raises an exception otherwise."""

        for val in tok:
            # automatic ';' insertion
            #
            if allowsemi and val ==';':
                if self.check_optional_semicolon():
                    continue

            if self.token.type == 'STRING':
                raise UnexpectedToken(self.token, tok, parser=self)

            elif self.token.string != val:
                raise UnexpectedToken(self.token, tok, parser=self)

            self.get_next_token(regexp)


    def tentative_match(self, *tok, regexp=True, consume=True, allowsemi=False):
        """Checks if the current token matches any of the provided values.
        Only checks ONE token, not a sequence, like 'must_match'.
        If it does, the token is returned and next token is processed from the lexer.
        If there is no match, None is returned and the token stays."""

        # try automatic semicolon insertion if we expected one potentially
        #
        if allowsemi and ';' in tok:
            if self.check_optional_semicolon():
                return True

        if self.token.type == 'STRING':
            return

        elif self.token.string in tok:
            if consume:
                self.get_next_token(regexp)
            return True

    def check_optional_semicolon(self):
        if (self.token.string == '}' and self.token.type == 'OP'    # [1]
            or self.linebreak_detected                              # [2]
            or self.token.type == '#EOF#'):                         # [3]
            # [1] problems parsing '}'
            # [2] there is a newline before the problematic token
            # [3] at the end of program
            #
            return True

    def check_optional_semicolon(self):
        if (self.token.string == '}' and self.token.type == 'OP'    # [1]
            or self.linebreak_detected                              # [2]
            or self.token.type == '#EOF#'):                         # [3]
            # [1] problems parsing '}'
            # [2] there is a newline before the problematic token
            # [3] at the end of program
            #
            return True

    #
    # Section: Expressions
    #


    def nud_Unary(self, token):
        operand = self.parse_assignment_expression(token.rbp)
        return jsast.PrefixExpressionNode(op=token.string, expression=operand,
                                          position=token.position)


    def led_Unary(self, left, token):
        return jsast.PostfixExpressionNode(op=token.string, expression=left,
                                           position=token.position)


    def led_Binary(self, left, token):
        right = self.parse_assignment_expression(token.lbp)
        return jsast.BinExpressionNode(left=left, op=token.string, right=right,
                                       position=left.position)


    def led_Assign(self, left, token):
        right = self.parse_assignment_expression(token.lbp - 1)
        return jsast.AssignmentExpressionNode(left=left, op=token.string, right=right,
                                              position=left.position)

    #
    # defining proper handling for some special operators
    #
    def nud_VOID(self, token):
        operand = self.parse_assignment_expression(token.rbp)
        return jsast.VoidNode(expression=operand, position=token.position)


    def nud_DELETE(self, token):
        operand = self.parse_assignment_expression(token.rbp)
        return jsast.DeleteNode(expression=operand, position=token.position)


    def nud_TYPEOF(self, token):
        operand = self.parse_assignment_expression(token.rbp)
        return jsast.TypeOfNode(expression=operand, position=token.position)


    def led_IN(self, left, token):
        right = self.parse_assignment_expression(token.lbp)
        return jsast.InNode(expression=left, container=right,
                            position=left.position)


    def led_INSTANCEOF(self, left, token):
        right = self.parse_assignment_expression(token.lbp)
        return jsast.InstanceOfNode(expression=left, type=right,
                                    position=left.position)


    def led_DOT(self, left, token):
        right = self.parse_ID(allowkeyword=True)
        return jsast.DotExpressionNode(left=left, right=right,
                                       position=left.position)

    def nud_NEW(self, token):
        # new expression, read the expression up to '('
        #
        expr = self.parse_assignment_expression(self.OP_2_INFO['(']['lbp'])
        # there may be an argument list here
        #
        args = None

        if self.tentative_match('('):
            args = [None]

            if not self.tentative_match(')', regexp=False, consume=False):
                args = self.parse_expression_list()
            self.must_match(')', regexp=False)

        return jsast.NewNode(expression=expr, arguments=args,
                             position=token.position)

    def led_FatArrow(self, left, token):
        if not self.arrowfuncsupport:
            raise UnexpectedToken(token)

        params = []
        if left is not None:
            if isinstance(left, jsast.IDNode):
                params.append(jsast.FunctionParameter(
                                name=left.name,
                                position=left.position))
            elif isinstance(left, list):
                defaults_started = False
                for sub in left:
                    if not defaults_started and isinstance(sub, jsast.IDNode):
                        params.append(jsast.FunctionParameter(
                                        name=sub.name,
                                        position=sub.position))
                        continue
                    elif isinstance(sub, jsast.FunctionParameter):
                        params.append(sub)
                        continue
                    elif isinstance(sub, jsast.AssignmentExpressionNode):
                        if sub.op == '=' and isinstance(sub.left, jsast.IDNode):
                            defaults_started = True
                            params.append(jsast.FunctionParameter(
                                                name=sub.left.name,
                                                default=sub.right,
                                                position=sub.left.position))
                            continue

                    raise UnexpectedToken(token)

        if self.tentative_match('{'):
            body = self.parse_block_guts()
        else:
            body = self.parse_assignment_expression()

        return jsast.FatArrowFunctionNode(param=params, body=body)

    def nud_FUNCTION(self, token):
        "Function expression"

        return self.parse_function_guts()

    @stamp_state('[')
    def nud_LSBRACKET(self, token):
        "Array literal"
        can_be_comprehension = self.arraycompsupport

        array = []
        while not self.tentative_match(']', regexp=False):
            spread = False
            if self.spreadsupport and self.tentative_match('...'):
                spread = True
                can_be_comprehension = False

            # take care of elision
            #
            if not spread and self.tentative_match(','):
                array.append(None)
                can_be_comprehension = False

            else: # process the next expression with its trailing comma
                expr = self.parse_assignment_expression()
                if spread:
                    expr = jsast.SpreadElement(expression=expr)
                array.append(expr)
                if self.tentative_match(','):
                    can_be_comprehension = False
                elif (can_be_comprehension and self.tentative_match('for')):
                    gen = self.parse_comprehension(array[0])
                    self.must_match(']')
                    return jsast.ArrayComprehensionNode(generator=gen)

        return jsast.ArrayLiteralNode(array=array, position=token.position)

    @stamp_state('[')
    def led_LSBRACKET(self, left, token):
        "Indexing"
        expr = self.parse_expression()
        self.must_match(']', regexp=False)
        return jsast.SBracketExpressionNode(list=left, element=expr,
                                            position=token.position)

    @stamp_state('(')
    def nud_LPAREN(self, token):
        "Parenthesis enclosed expression"

        if self.arrowfuncsupport:
            if self.tentative_match(')', regexp=False):
                # '''() => smth''' case
                self.must_match('=>')
                return self.led_FatArrow(None, self.prevtoken)

            self.enter_state('arrowfunc')
            # in 'arrowfunc' state, 'parse_expression_list' will recognize '...'
            try:
                expr = self.parse_expression(allow_generator=self.generatorexprsupport)
            finally:
                self.exit_state()

            self.must_match(')')

            if (isinstance(expr, jsast.ExpressionListNode) and expr.expressions
                    and isinstance(expr.expressions[-1], jsast.FunctionParameter)):
                # last parameter here must be 'rest'
                # and 'expr' is arrow function's parameters list
                assert expr.expressions[-1].rest
                self.must_match('=>')
                return self.led_FatArrow(expr.expressions, self.prevtoken)

            if self.tentative_match('=>', regexp=False):
                # We've got an arrow function, and 'expr' is its parameters list
                if not isinstance(expr, jsast.ExpressionListNode):
                    raise UnexpectedToken(self.prevtoken)
                return self.led_FatArrow(expr.expressions, self.prevtoken)

            return expr

        expr = self.parse_expression(allow_generator=self.generatorexprsupport)
        self.must_match(')', regexp=False)
        return expr

    @stamp_state('(')
    def led_LPAREN(self, left, token):
        "This is parenthesis as operator, implying callable"

        args = []
        if not self.tentative_match(')', regexp=False):
            args = self.parse_expression_list()
            self.must_match(')', regexp=False)
        return jsast.CallNode(call=left, arguments=args,
                              position=token.position)


    @stamp_state('{')
    def nud_LCBRACKET(self, token):
        "Object literal parsing"

        guts = []

        if not self.tentative_match('}', regexp=False):
            if self.token.type == 'ID':
                id = self.parse_ID()

                if self.tentative_match(',', regexp=False):
                    pl = self.parse_assignment_property_list()
                    pl.properties.insert(0, id)
                    return pl

                guts.append(self.parse_property(id=id))
                if not self.tentative_match('}', regexp=False, consume=False):
                    self.must_match(',')

            while not self.tentative_match('}', regexp=False):
                guts.append(self.parse_property())
                if self.tentative_match('}', regexp=False):
                    break
                else:
                    self.must_match(',')

        return jsast.ObjectLiteralNode(properties=guts, position=token.position)


    def led_HOOK(self, left, token):
        # The binding power of '?' has nothing to do with ':'.
        # Since ':' is not an operator, it will ALWAYS act as a separator.
        #
        iftrue = self.parse_assignment_expression()
        self.must_match(':')
        iffalse = self.parse_assignment_expression()
        return jsast.ConditionalExpressionNode(condition=left, true=iftrue, false=iffalse,
                                               position=left.position)

    #
    # extra features
    #
    def nud_LET(self, token, isstatement=False):
        'Process let expression (or the beginning of a let statement)'
        started_at = self.prevtoken.position

        self.must_match('(')
        var_list = self.parse_declaration_helper(statement=False, decompsupport=True)
        self.must_match(')')

        if isstatement:
            return jsast.LetStatementNode(vars=var_list, statement=self.parse_statement(),
                                          position=started_at)

        else:
            return jsast.LetExpressionNode(vars=var_list,
                                           expression=self.parse_assignment_expression(token.rbp),
                                           position=started_at)

    #
    # core methods for expression processing
    #
    def nud(self, token):
        """Null denotation.
        Used for tokens appearing at the beginning of [sub]expression."""

        tok_type = self.get_token_special_type(token, 'nud')

        if tok_type == 'ID':
            return jsast.IDNode(name=token.value, position=token.position)

        elif tok_type == 'STRING':
            return jsast.StringLiteralNode(value=token.value, position=token.position)

        elif tok_type == 'NUMBER':
            return jsast.NumericLiteralNode(value=token.value, position=token.position)

        elif tok_type == 'TRUE':
            return jsast.BooleanLiteralNode(value=True, position=token.position)

        elif tok_type == 'FALSE':
            return jsast.BooleanLiteralNode(value=False, position=token.position)

        elif tok_type == 'NULL':
            return jsast.NullNode(position=token.position)

        elif tok_type == 'THIS':
            return jsast.ThisNode(position=token.position)

        elif tok_type == 'REGEXP':
            return jsast.RegExpNode(regexp=token.value, position=token.position)

        elif tok_type == 'KEYWORD':
            nud = getattr(self, 'nud_' + self.keywords.js_keywords[token.value][0], None)
            return nud(token)

        else:
            nud = None
            if tok_type:
                nud = getattr(self, 'nud_' + tok_type, None)

            if nud:
                return nud(token)
            else:
                raise UnexpectedToken(token, parser=self)


    def led(self, left, token):
        """Left denotation.
        Used for tokens appearing inside of [sub]expression
        (to the left of some other expression)."""
        tok_type = self.get_token_special_type(token, 'led')

        led = getattr(self, 'led_' + tok_type, None)

        if led:
            return led(left, token)

        else:
            raise UnknownOperator(token)


    def parse_assignment_expression(self, rbp=0):
        """This is the basic parsing step for expressions.
        rbp - specifies Right Binding Power of the current token"""

        self.get_next_token(regexp=(self.token.type == 'OP'))

        left = self.nud(self.prevtoken)

        # don't go if lbp is weaker,
        #    or if 'in' isn't allowed,
        #    or if there's a linebreak before postfix ++/--
        #    or if 'for' is not in proper context
        #
        while (rbp < self.token.lbp
               and not (self.state == 'noin' and self.token.string == 'in')
               and not (self.linebreak_detected and
                    (self.prevtoken.lbp == 0 or
                     self.get_token_special_type(self.prevtoken, 'led') == 'Unary')
                    and self.get_token_special_type(self.token, 'led') == 'Unary')
               # 'new' and 'function' have unintuitive binding power, it is mainly dictated by
               # the need to be of the same power as '[' and '.'
               #
               # Unfortunately, due to a deficiency of bp assignment we need to exclude
               # the possibility of 'new' and 'function' to be considered OPERATORS
               # explicitly in this loop guard.
               #
               and not self.token.value in ('new', 'function')):

            self.get_next_token(regexp=(self.token.type == 'OP'))
            # it's an error to have consecutive unary postfix operators
            #
            if (self.get_token_special_type(self.prevtoken, 'led') ==
                self.get_token_special_type(self.token, 'led') == 'Unary' and
                not self.linebreak_detected):
                raise UnexpectedToken(self.token, parser=self)

            left = self.led(left, self.prevtoken)

        return left


    def parse_expression_list(self, allow_generator=False):
        """This is the parsing step for parsing lists of expressions (args or expression).

        It can also parse generator expression."""

        # Arrow functions support: see nul_LPAREN for details
        of_arrowfunc = self._scope and self._scope[-1][0] == 'arrowfunc'

        if of_arrowfunc and self.paramrestsupport and self.tentative_match('...'):
            it = jsast.FunctionParameter(name=self.parse_ID().name,
                                         rest=True)
            # no need to continue parsing, as ')' is must be the next token
            return [it]

        expr = [self.parse_assignment_expression()]

        if allow_generator and self.tentative_match('for'):
            return self.parse_comprehension(expr[0])

        while self.tentative_match(','):
            if of_arrowfunc and self.paramrestsupport and self.tentative_match('...'):
                it = jsast.FunctionParameter(name=self.parse_ID().name,
                                             rest=True)
                expr.append(it)
                # no need to continue parsing, as ')' is must be the next token
                return expr

            expr += [self.parse_assignment_expression()]

        return expr


    def parse_expression(self, allow_generator=False):
        """This is the parsing step for expression lists. Used as 'expression' in most rules.

        It can also parse generator expression."""

        started_at = self.token.position
        expr = self.parse_expression_list(allow_generator=allow_generator)

        if isinstance(expr, jsast.GeneratorExprNode):
            return expr

        elif len(expr) > 1:
            return jsast.ExpressionListNode(expressions=expr, position=started_at)

        else:
            return expr[0]


    def parse_ID(self, allowkeyword=False):
        """Parse an identifier potentially w/o converting keywords. Raise an exception if not ID."""

        tok = self.token

        if (not (tok.type == 'ID' or
                 allowkeyword and tok.value in self.keywords.js_keywords)):
            raise UnexpectedToken(self.token, parser=self)

        self.get_next_token(regexp=False)
        return jsast.IDNode(name=tok.string, position=tok.position)


    #
    # Section: Statements
    #

    def parse_statement(self, labels=[]):
        """Parse one statement as delineated by ';' or block"""

        self.labels = labels

        if self.token.type == '#ERR#':
            raise UnknownToken(self.token)

        elif self.token.type == 'KEYWORD':
            # statements start with keywords, otherwise it's an expression
            #
            return self.parse_keywords()

        elif self.tentative_match(';'):
            # empty statement
            #
            return None

        elif self.tentative_match('{'):
            # block, converts direct labels into indirect
            #
            block = self.parse_block_guts()

            eq_token = self.token
            if (self.expansionsupport and self.tentative_match('=', regexp=False)):

                right = self.parse_assignment_expression()

                if len(block.statements) == 1:
                    st = block.statements[0]

                    if isinstance(st, jsast.StatementNode):
                        if isinstance(st.statement, jsast.IDNode):
                            return jsast.StatementNode(
                                        statement=jsast.AssignmentExpressionNode(
                                            left=jsast.AssignmentPropertyList(
                                                properties=[st.statement]),
                                            op='=',
                                            right=right))

                        if isinstance(st.statement, jsast.ExpressionListNode):
                            vars = []
                            for expr in st.statement.expressions:
                                if isinstance(expr, jsast.IDNode):
                                    vars.append(expr)
                                else:
                                    break
                            else:
                                return jsast.StatementNode(
                                            statement=jsast.AssignmentExpressionNode(
                                                left=jsast.AssignmentPropertyList(
                                                    properties=vars),
                                                op='=',
                                                right=right))

                raise UnexpectedToken(eq_token, parser=self)
            else:
                return block

        elif self.tentative_match('function'):
            # function declaration
            #
            return self.parse_function_guts(is_declaration=True)

        elif self.tentative_match('for'):
            # for loop STATEMENT
            #
            return self.parse_for_guts()

        elif self.tentative_match('let'):
            # function declaration
            #
            return self.parse_let_guts()

        else:
            expr = self.parse_expression()
            errtok = self.prevtoken

            # now let's test if that was a label or expression statement
            #
            if type(expr) == jsast.IDNode and \
                self.prevtoken.type == 'ID' and \
                self.tentative_match(':'):
                # we have a label!
                #
                label = expr.name

                if label in self.enclosing_stmt_labels():
                    raise DuplicateLabel(errtok)

                return jsast.LabelNode(id=label,
                                       statement=self.parse_statement(labels=labels + [label]))
            else:
                self.must_match(';')
                return jsast.StatementNode(statement=expr, position=expr.position)


    def parse_statement_list(self, *delim, consume=True):
        """Parse a list of statements.
        The end of list delimiter must be explicitly specified."""

        statements = []

        while not self.tentative_match(*delim, consume=consume):
            statement = self.parse_statement()

            if statement:
                statements.append(statement)

        return statements


    def parse_source(self):
        """Parse source statement by statement"""

        return jsast.SourceElementsNode(code=self.parse_statement_list('<<EOF>>'))


    def parse_keywords(self):
        """Based on the current token (assumed to be keyword) parses the statement."""

        action = getattr(self, 'parse_' + self.token.string + '_guts', None)

        if action:
            self.get_next_token()
            return action()

        else:
            raise UnexpectedToken(self.token, parser=self)

    @stamp_state('stmt', affectslabels=True)
    def parse_block_guts(self):
        """Parse statements inside a block.
        The end delimiter is different from source parsing."""

        return jsast.StatementBlockNode(statements=self.parse_statement_list('}'),
                                        position=self.token.position)


    def parse_var_guts(self, statement=True):
        """Parse the VAR declaration."""
        started_at = self.prevtoken.position
        var_list = self.parse_declaration_helper(statement=statement)

        if statement:
            return jsast.StatementNode(
                        statement=jsast.VarDeclarationNode(
                            vars=var_list,
                            position=started_at),
                        position=started_at)

        else:
            return jsast.VarDeclarationNode(vars=var_list, position=started_at)

    def parse_debugger_guts(self):
        self.tentative_match(';')
        return jsast.DebuggerNode()

    def parse_let_guts(self, statement=True):
        """Parse the LET declaration."""

        started_at = self.prevtoken.position

        if not self.letsupport:
            raise UnknownToken(self.prevtoken)

        if self.token.value == '(':
            return self.nud_LET(self.token, isstatement=True)

        var_list = self.parse_declaration_helper(statement=statement, decompsupport=True)

        if statement:
            return jsast.StatementNode(
                            statement=jsast.LetDeclarationNode(
                                vars=var_list,
                                position=started_at),
                            position=started_at)

        else:
            return jsast.LetDeclarationNode(vars=var_list, position=started_at)


    def parse_declaration_helper(self, statement=True, decompsupport=False):
        """Parse the variable declaration."""

        started_at = self.token.position

        var_list = []

        while True:
            # variable name will be used a lot...
            #
            if self.expansionsupport:
                if self.tentative_match('['):
                    varname = self.parse_assignment_elements_list()
                elif self.tentative_match('{'):
                    varname = self.parse_assignment_property_list()
                else:
                    varname = self.parse_ID()
            else:
                varname = self.parse_ID()

            if self.tentative_match('='):
                var_list.append(jsast.VarInitNode(name=varname,
                                                  value=self.parse_assignment_expression(),
                                                  position=started_at))

            else:
                var_list.append(jsast.VarInitNode(name=varname, value=None,
                                                  position=started_at))

            if self.tentative_match(','):
                continue

            elif self.tentative_match('in', 'of', consume=statement):
                # this can only happen in 'noin' mode
                break

            elif self.letsupport and self.tentative_match(')', consume=False):
                # only as part of parsing 'let'
                break

            else:
                # this catches optional semicolon in statements
                #
                if statement:
                    self.must_match(';')
                    break

                # this may be an error, but what kind is better determined by whoever called us
                #
                elif self.tentative_match(';', consume=statement):
                    break

                else:
                    raise UnexpectedToken(self.token, parser=self)

        return var_list


    def parse_property(self, id=None):
        """Parse object property"""

        started_at = self.token.position

        if id is None:
            # get the property name, will use alot
            #
            prop, id = self.parse_property_name()
        else:
            prop, id = id, 'ID'

        if id == 'ID':

            if self.tentative_match(':'):
                # still a simple property definition
                #
                val = self.parse_assignment_expression()
                return jsast.SimplePropertyNode(name=prop, value=val,
                                                position=started_at)

            elif prop.name == 'get':
                prop = self.parse_property_name()[0]
                self.must_match('(', ')', '{')

                # same as function, need to clear the labels
                #
                self.labels = []
                func = self.parse_block_guts()
                return jsast.GetPropertyNode(name=prop, functionbody=func,
                                             position=started_at)

            elif prop.name == 'set':
                prop = self.parse_property_name()[0]
                self.must_match('(')
                param = self.parse_ID()
                self.must_match(')', '{')

                # same as function, need to clear the labels
                #
                self.labels = []
                func = self.parse_block_guts()
                return jsast.SetPropertyNode(name=prop, param=param,
                                             functionbody=func,
                                             position=started_at)

            else:
                # kinda hacky, but generates the right error message
                # we KNOW by this point that ':' isn't there (tried matching it earlier)
                # so forcing that match will complain appropriately
                #
                self.must_match(':')

        else:
            self.must_match(':')
            val = self.parse_assignment_expression()
            return jsast.SimplePropertyNode(name=prop, value=val, position=started_at)


    def parse_property_name(self):
        "Parse the property name"

        id = self.token.type

        if self.token.type == 'NUMBER':
            prop = jsast.NumericLiteralNode(value=self.token.string, position=self.token.position)
            self.get_next_token()

        elif self.token.type == 'STRING':
            prop = jsast.StringLiteralNode(value=self.token.string, position=self.token.position)
            self.get_next_token()

        else:
            id = 'ID'
            prop = self.parse_ID(allowkeyword=True)

        return (prop, id)


    def parse_continue_guts(self):
        """Parse the rest of the continue statement."""
        errtok = self.prevtoken

        if self.tentative_match(';', allowsemi=True):
            # must be inside a loop
            #
            if not self.enclosing_state('loop'):
                raise IllegalContinue(errtok)

            return jsast.ContinueNode(id=None, position=errtok.position)

        else:
            # must have a valid label in enclosing loop
            #
            tok = self.token
            id = self.parse_ID().name

            if id not in self.enclosing_loop_labels():
                raise UndefinedLabel(tok)

            self.must_match(';')
            return jsast.ContinueNode(id=id, position=errtok.position)


    def parse_break_guts(self):
        """Parse the rest of the break statement."""
        errtok = self.prevtoken

        if self.tentative_match(';', allowsemi=True):
            # must be inside a loop or switch
            #
            if not self.enclosing_state('loop', 'switch'):
                raise IllegalBreak(errtok)

            return jsast.BreakNode(id=None, position=errtok.position)

        else:
            # must have a valid label in enclosing stmt
            #
            tok = self.token
            id = self.parse_ID().name

            if id not in self.enclosing_stmt_labels():
                raise UndefinedLabel(tok)

            self.must_match(';')
            return jsast.BreakNode(id=id, position=errtok.position)


    def parse_return_guts(self):
        """Parse the rest of the return statement."""

        started_at = self.prevtoken.position

        if self.tentative_match(';', allowsemi=True):
            return jsast.ReturnNode(expression=None, position=started_at)
        else:
            expr = self.parse_expression()
            self.must_match(';')
            return jsast.ReturnNode(expression=expr, position=started_at)


    def parse_yield_guts(self):
        """Parse the rest of the yield statement."""

        started_at = self.prevtoken.position

        if not self.yieldsupport:
            raise UnknownToken(self.prevtoken)

        if self.tentative_match(';', allowsemi=True):
            return jsast.YieldNode(expression=None, position=started_at)
        else:
            expr = self.parse_expression()
            self.must_match(';')
            return jsast.YieldNode(expression=expr, position=started_at)

    def parse_function_parameters(self):
        params = []

        defaults_mode = False
        if not self.tentative_match(')'):
            while True:
                started_at = self.token.position
                rest_param = False
                if self.paramrestsupport and self.tentative_match('...', regexp=False):
                    rest_param = True

                name = self.parse_ID()

                if (self.paramdefaultsupport
                        and ((not rest_param)
                                and (defaults_mode or self.tentative_match('=', consume=False)))):

                    defaults_mode = True
                    self.must_match('=')
                    default = self.parse_assignment_expression()

                    param = jsast.FunctionParameter(name=name.name,
                                                    default=default,
                                                    position=started_at)

                else:
                    param = jsast.FunctionParameter(name=name.name,
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

    @stamp_state('function', affectslabels=True)
    def parse_function_guts(self, is_declaration=False):
        """Parses a function as a declaration or as an expression."""

        started_at = self.prevtoken.position

        # clear the labels since none of them matter inside
        #
        self.labels = []
        name = None

        if is_declaration:
            name = self.parse_ID().name
            self.must_match('(')

        elif not self.tentative_match('('):
            name = self.parse_ID().name
            self.must_match('(')

        # grab the arglist
        #
        param = self.parse_function_parameters()

        self.must_match('{')
        body = self.parse_block_guts()
        return jsast.FunctionNode(name=name, param=param,
                                  body=body, isdeclaration=is_declaration,
                                  position=started_at)


    @stamp_state('stmt', affectslabels=True)
    def parse_with_guts(self):
        """Parse 'with' statement."""

        started_at = self.prevtoken.position

        self.must_match('(')
        expr = self.parse_expression()
        self.must_match(')')
        stmt = self.parse_statement()
        return jsast.WithNode(expression=expr, statement=stmt,
                              position=started_at)


    def parse_throw_guts(self):
        """Parse throw statement."""

        started_at = self.prevtoken.position

        if self.linebreak_detected:
            raise UnexpectedNewline(self.prevtoken)

        expr = self.parse_expression()
        self.must_match(';')
        return jsast.ThrowNode(expression=expr, position=started_at)


    def parse_switch_guts(self):
        """Parse switch statement."""

        started_at = self.prevtoken.position

        self.must_match('(')
        expr = self.parse_expression()
        self.must_match(')')
        return jsast.SwitchNode(expression=expr, cases=self.parse_switchblock(),
                                position=started_at)


    @stamp_state('switch', affectslabels=True)
    def parse_switchblock(self):
        """Parse the switch block statements."""

        statement_started_at = self.token.position
        self.must_match('{')
        code = []
        has_default = False

        while not self.tentative_match('}'):

            if self.tentative_match('case'):
                started_at = self.prevtoken.position
                expr = self.parse_expression()
                self.must_match(':')
                stmt_list = self.parse_statement_list('case', 'default','}', consume=False)
                code.append(jsast.CaseNode(
                                    case=expr,
                                    statements=jsast.SourceElementsNode(
                                        code=stmt_list,
                                        position=self.token.position),
                                    position=started_at))

            elif not has_default and self.tentative_match('default'):
                started_at = self.prevtoken.position
                has_default = True
                self.must_match(':')
                stmt_list = self.parse_statement_list('case', 'default','}', consume=False)
                code.append(jsast.DefaultNode(
                                    statements=jsast.SourceElementsNode(
                                        code=stmt_list,
                                        position=self.token.position),
                                    position=started_at))

            elif has_default and self.token.string == 'default':
                raise SecondDefaultToken(self.token)

        return jsast.StatementBlockNode(statements=code, position=statement_started_at)


    @stamp_state('stmt', affectslabels=True)
    def parse_try_guts(self):
        """Parse try statement."""

        statement_started_at = self.token.position

        self.must_match('{')
        tryblock = self.parse_block_guts()
        finallyblock = None

        # gather the catch block info
        #
        catch = []
        while True:
            c = self.parse_catch_helper()

            if self.catchifsupport:

                if c:
                    catch.append(c)

                else:
                    break

            else: # no multiple catch blocks allowed
                catch = c
                break

        # depending on presence of catch block, finally block may or may not be optional
        #
        if not catch:
            self.must_match('finally', '{')
            finallyblock = self.parse_block_guts()

        elif self.tentative_match('finally'):
            self.must_match('{')
            finallyblock = self.parse_block_guts()

        return jsast.TryNode(tryblock=tryblock, catch=catch, finallyblock=finallyblock,
                             position=statement_started_at)


    def parse_catch_helper(self):
        'Parse the catch clause of the try/catch/finally statement.'
        catchid = condition = catchblock = None

        started_at = self.token.position

        if self.tentative_match('catch'):
            self.must_match('(')
            catchid = self.parse_ID().name

            if self.catchifsupport and self.tentative_match('if'):
                condition = self.parse_expression()

            self.must_match(')', '{')
            catchblock = self.parse_block_guts()

        # verify we have a catchblock before we return
        #
        if catchid and not self.catchifsupport:
            return jsast.CatchNode(catchid=catchid, catchblock=catchblock,
                                   position=started_at)

        elif catchid and self.catchifsupport:
            return jsast.CatchIfNode(catchid=catchid, condition=condition, catchblock=catchblock,
                                     position=started_at)

        else:
            return None


    @stamp_state('stmt', affectslabels=True)
    def parse_if_guts(self, *, as_expr=False):
        """Parse if statement."""

        started_at = self.token.position

        self.must_match('(')
        expr = self.parse_expression()
        self.must_match(')')

        if as_expr:
            return jsast.IfNode(ifclause=expr, is_expr=True)

        thenstmt = self.parse_statement()
        elsestmt = None

        if self.tentative_match('else'):
            elsestmt = self.parse_statement()

        return jsast.IfNode(ifclause=expr, thenclause=thenstmt, elseclause=elsestmt,
                            position=started_at)

    @stamp_state('loop', affectslabels=True)
    def parse_do_guts(self):
        """Parse do loop."""

        started_at = self.token.position

        stmt = self.parse_statement()
        self.must_match('while', '(')
        expr = self.parse_expression()
        self.must_match(')', ';')
        return jsast.DoNode(statement=stmt, expression=expr, position=started_at)

    @stamp_state('loop', affectslabels=True)
    def parse_while_guts(self):
        """Parse while loop."""

        started_at = self.token.position

        self.must_match('(')
        expr = self.parse_expression()
        self.must_match(')')
        stmt = self.parse_statement()
        return jsast.WhileNode(statement=stmt, expression=expr, position=started_at)

    def parse_assignment_property_list(self):
        assert self.expansionsupport

        vars = []

        while True:
            if self.tentative_match('{'):
                vars.append(self.parse_assignment_property_list())
            else:
                vars.append(self.parse_ID())

            if not self.tentative_match(',', regexp=False):
                self.must_match('}')
                break

        return jsast.AssignmentPropertyList(properties=vars)

    def parse_assignment_elements_list(self):
        assert self.expansionsupport

        vars = []

        while True:
            if self.tentative_match('['):
                vars.append(self.parse_assignment_elements_list())
            elif self.tentative_match('{'):
                vars.append(self.parse_assignment_property_list())
            else:
                vars.append(self.parse_ID())

            if not self.tentative_match(',', regexp=False):
                self.must_match(']')
                break

        return jsast.AssignmentElementList(elements=vars)

    @stamp_state('loop', affectslabels=True)
    def parse_for_guts(self, *, fors_allowed=('of', 'in', 'std'), as_expr=False):
        """Parse for loop."""

        statement_started_at = self.token.position

        self.must_match('(')
        noin_expr = None
        multiple_decl = False

        # skip classical for, without initializer
        #
        if not self.tentative_match(';', consume=False):
            self.enter_state('noin')

            if self.tentative_match('var'):
                # var declaration
                noin_expr = self.parse_var_guts(statement=False)
                multiple_decl = len(noin_expr.vars) > 1

            elif self.letsupport and self.tentative_match('let'):
                # let declaration
                noin_expr = self.parse_let_guts(statement=False)
                multiple_decl = len(noin_expr.vars) > 1

            elif (self.expansionsupport and
                        self.tentative_match('{', regexp=False)):
                noin_expr = self.parse_assignment_property_list()

            elif (self.expansionsupport and
                        self.tentative_match('[', regexp=False)):
                noin_expr = self.parse_assignment_elements_list()

            else:
                noin_expr = self.parse_expression()

            self.exit_state()

        expr = expr2 = expr3 = None

        for_type = 'std'

        if not multiple_decl:
            if self.tentative_match('in') and 'in' in fors_allowed:
                # for (x in [1,2,3]) ...
                for_type = 'in'
            elif self.forofsupport and self.tentative_match('of'):
                # for (x of [1,2,3]) ...
                for_type = 'of'

        if for_type not in fors_allowed:
            raise UnexpectedToken(self.prevtoken)

        if for_type in ('in', 'of'):
            # for-of or for-in
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

        if as_expr:
            stmt = None
        else:
            stmt = self.parse_statement()

        if for_type == 'std':
            return jsast.ForNode(part1=noin_expr, part2=expr2, part3=expr3, statement=stmt,
                                 position=statement_started_at, is_expr=as_expr)
        elif for_type == 'in':
            return jsast.ForInNode(init=noin_expr, container=expr, statement=stmt,
                                   position=statement_started_at, is_expr=as_expr)
        else:
            return jsast.ForOfNode(init=noin_expr, container=expr, statement=stmt,
                                   position=statement_started_at, is_expr=as_expr)


    def parse_comprehension(self, expr):
        """Parse array comprehension or a generator expression,
        starting with a specified expression."""
        # [ expr for - already parsed...

        comprehensions = []
        comprehensions.append(self.parse_for_guts(as_expr=True, fors_allowed=('std', 'of')))

        while True:
            if self.tentative_match('for', regexp=False):
                comprehensions.append(self.parse_for_guts(as_expr=True, fors_allowed=('std', 'of')))
            elif self.tentative_match('if', regexp=False):
                comprehensions.append(self.parse_if_guts(as_expr=True))
                break
            else:
                break

        return jsast.GeneratorExprNode(expr=expr, comprehensions=comprehensions)


    def parse(self, program, *, filename=None):
        self.filename = filename
        self.source = program
        self.lexer.setinputstr(program)
        self.reset()
        self.get_next_token()
        return self.parse_source()
