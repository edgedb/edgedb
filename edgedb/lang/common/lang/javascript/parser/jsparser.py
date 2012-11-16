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


class PP_UnexpectedToken(SyntaxError):
    def __init__(self, token):
        super().__init__("unexpected preprocessor token %r (line %i, column %i)." %
                         (token.string, token.start[0], token.start[1]),
                         token.start[0], token.start[1])


class PP_MalformedToken(SyntaxError):
    def __init__(self, token):
        super().__init__("malformed preprocessor token %r (line %i, column %i)." %
                         (token.string, token.start[0], token.start[1]),
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

        'function'  : 'FUNCTION',
        'instanceof': 'INSTANCEOF',
        'in'        : 'IN',
        'delete'    : 'DELETE',
        'void'      : 'VOID',
        'typeof'    : 'TYPEOF',
        'new'       : 'NEW',

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
                 foreachsupport=False,
                 catchifsupport=False,
                 arraycompsupport=False,
                 generatorexprsupport=False,
                 ppsupport=False):
        super().__init__()

        if not lex_name:
            lex_name = os.path.join(os.path.dirname(__file__), "js.pyl")

        self.set_lexer(lex_name)
        self.reset()
        self.ppsupport = ppsupport
        self.yieldsupport = yieldsupport
        self.letsupport = letsupport
        self.expansionsupport = expansionsupport
        self.foreachsupport = foreachsupport
        self.catchifsupport = catchifsupport
        self.arraycompsupport = arraycompsupport or generatorexprsupport
        self.generatorexprsupport = generatorexprsupport
        self.setup_operators()

    def setup_operators(self):
        'Setup operators and precedence.'

        self.PREC = [
        #   (<bp>, <special type>, <token vals>, <active>)
            ('rbp', '',         ('{', ), True),
        # XXX: '{' is right-binding because it functions like a prefix operator
        #       creating an object literal or variable unpacking expression
            ('lbp', '',         ('new', '[', '.', 'function'), True),
            ('lbp', '',         ('(', ), True),
            ('lbp', 'Unary',    ('++', '--'), True),
            ('rbp', 'Unary',    ('++', '--', '+', '-', '~', '!', 'delete', 'void', 'typeof'), True),
            ('lbp', 'Binary',   ('*', '/', '%'), True),
            ('lbp', 'Binary',   ('+', '-'), True),
            ('lbp', 'Binary',   ('<<', '>>', '>>>'), True),
            ('lbp', 'Binary',   ('<', '>', '<=', '>=', 'instanceof', 'in'), True),
            ('lbp', 'Binary',   ('==', '!=', '===', '!=='), True),
            ('lbp', 'Binary',   ('&', ), True),
            ('lbp', 'Binary',   ('^', ), True),
            ('lbp', 'Binary',   ('|', ), True),
            ('lbp', 'Binary',   ('&&', ), True),
            ('lbp', 'Binary',   ('||', ), True),
            ('lbp', '',         ('?', ), True),
            ('lbp', '',         ('for', ), self.arraycompsupport or self.generatorexprsupport),
            ('lbp', 'Assign',
             ('=', '+=', '-=', '*=', '/=', '%=', '<<=', '>>=', '>>>=', '&=', '|=', '^='), True),
            ('rbp', '',         ('let', ), self.letsupport)
        ]

        #: this will contain mappings from values to token information for various operations
        #:
        self.OP_2_INFO = {}

        #: calculate the starting binding power
        #:
        bp_val = len(self.PREC) * 10

        for bp, special, vals, active in self.PREC:

            for val in vals:
                if active:
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

        if tok.type == '#ERR#' and not self.ppsupport:
            raise UnknownToken(tok)


    def must_match(self, *tok, regexp=True, allowsemi=True):
        """Matches the current token against the specified value.
        If more than one value is given, then a sequence of tokens must match.
        Consumes the token if it is correct, raises an exception otherwise."""

        for val in tok:
            if self.token.type == 'STRING':
                raise UnexpectedToken(self.token, tok, parser=self)

            if self.token.string != val:
                # automatic ';' insertion
                #

                if allowsemi:

                    if self.token.string == '}':
                        continue # problems parsing '}'

                    elif self.linebreak_detected and val == ';':
                        continue # there is a newline before the problematic token

                    elif self.token.type == '#EOF#' and val == ';':
                        continue # at the end of program

                    else:
                        raise UnexpectedToken(self.token, tok, parser=self)

                else:
                    raise UnexpectedToken(self.token, tok, parser=self)

            self.get_next_token(regexp)


    def tentative_match(self, *tok, regexp=True, consume=True):
        """Checks if the current token matches any of the provided values.
        Only checks ONE token, not a sequence, like 'must_match'.
        If it does, the token is returned and next token is processed from the lexer.
        If there is no match, None is returned and the token stays."""

        if self.token.type == 'STRING':
            return

        if self.token.string in tok:
            t = self.token
            if consume:
                self.get_next_token(regexp)
            return t



    #
    # Section: Expressions
    #


    def nud_Unary(self, token):
        operand = self.parse_assignment_expression(token.rbp)
        return jsast.PrefixExpressionNode(op=token.string, expression=operand)


    def led_Unary(self, left, token):
        return jsast.PostfixExpressionNode(op=token.string, expression=left)


    def led_Binary(self, left, token):
        right = self.parse_assignment_expression(token.lbp)
        return jsast.BinExpressionNode(left=left, op=token.string, right=right)


    def led_Assign(self, left, token):
        right = self.parse_assignment_expression(token.lbp - 1)
        return jsast.AssignmentExpressionNode(left=left, op=token.string, right=right)

    #
    # defining proper handling for some special operators
    #
    def nud_VOID(self, token):
        operand = self.parse_assignment_expression(token.rbp)
        return jsast.VoidNode(expression=operand)


    def nud_DELETE(self, token):
        operand = self.parse_assignment_expression(token.rbp)
        return jsast.DeleteNode(expression=operand)


    def nud_TYPEOF(self, token):
        operand = self.parse_assignment_expression(token.rbp)
        return jsast.TypeOfNode(expression=operand)


    def led_IN(self, left, token):
        right = self.parse_assignment_expression(token.lbp)
        return jsast.InNode(expression=left, container=right)


    def led_INSTANCEOF(self, left, token):
        right = self.parse_assignment_expression(token.lbp)
        return jsast.InstanceOfNode(expression=left, type=right)


    def led_DOT(self, left, token):
        right = self.parse_ID(allowkeyword=True)
        return jsast.DotExpressionNode(left=left, right=right)


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

        return jsast.NewNode(expression=expr, arguments=args)


    def nud_FUNCTION(self, token):
        "Function expression"

        return self.parse_function_guts()

    @stamp_state('[')
    def nud_LSBRACKET(self, token):
        "Array literal"
        can_be_comprehension = self.arraycompsupport

        array = []
        while not self.tentative_match(']', regexp=False):
            # take care of elision
            #
            if self.tentative_match(','):
                array.append(None)
                can_be_comprehension = False

            else: # process the next expression with its trailing comma

                array.append(self.parse_assignment_expression())
                if self.tentative_match(','):
                    can_be_comprehension = False

                if (can_be_comprehension and self.tentative_match(']')):
                    return jsast.ArrayComprehensionNode(generator=array[0])

        return jsast.ArrayLiteralNode(array=array)

    @stamp_state('[')
    def led_LSBRACKET(self, left, token):
        "Indexing"
        expr = self.parse_expression()
        self.must_match(']', regexp=False)
        return jsast.SBracketExpressionNode(list=left, element=expr)


    @stamp_state('(')
    def nud_LPAREN(self, token):
        "Parenthesis enclosed expression"

        expr = self.parse_expression()
        self.must_match(')', regexp=False)
        return expr

    @stamp_state('(')
    def led_LPAREN(self, left, token):
        "This is parenthesis as operator, implying callable"

        args = []
        if not self.tentative_match(')', regexp=False):
            args = self.parse_expression_list()
            self.must_match(')', regexp=False)
        return jsast.CallNode(call=left, arguments=args)


    @stamp_state('{')
    def nud_LCBRACKET(self, token):
        "Object literal parsing"

        guts = []
        while not self.tentative_match('}', regexp=False):
            guts.append(self.parse_property())
            if self.tentative_match('}', regexp=False):
                break
            else:
                self.must_match(',')
        return jsast.ObjectLiteralNode(properties=guts)


    def led_HOOK(self, left, token):
        iftrue = self.parse_assignment_expression(token.lbp)
        self.must_match(':')
        iffalse = self.parse_assignment_expression(token.lbp)
        return jsast.ConditionalExpressionNode(condition=left, true=iftrue, false=iffalse)

    #
    # extra features
    #
    def nud_LET(self, token, isstatement=False):
        'Process let expression (or the beginning of a let statement)'
        self.must_match('(')
        var_list = self.parse_declaration_helper(statement=False, decompsupport=True)
        self.must_match(')')

        if isstatement:
            return jsast.LetStatementNode(vars=var_list, statement=self.parse_statement())

        else:
            return jsast.LetExpressionNode(vars=var_list,
                                           expression=self.parse_assignment_expression(token.rbp))

    def led_FOR(self, left, token):
        'Process generator expression.'
        if (self.generatorexprsupport or
            self.arraycompsupport and self.token.value == 'each'):
            return self.parse_comprehension(left)

        else:
            raise UnexpectedToken(self.prevtoken, parser=self)

    #
    # core methods for expression processing
    #
    def nud(self, token):
        """Null denotation.
        Used for tokens appearing at the beginning of [sub]expression."""

        tok_type = self.get_token_special_type(token, 'nud')

        if tok_type == 'ID':
            return jsast.IDNode(name=token.value)

        elif tok_type == 'STRING':
            return jsast.StringLiteralNode(value=token.value)

        elif tok_type == 'NUMBER':
            return jsast.NumericLiteralNode(value=token.value)

        elif tok_type == 'TRUE':
            return jsast.BooleanLiteralNode(value=True)

        elif tok_type == 'FALSE':
            return jsast.BooleanLiteralNode(value=False)

        elif tok_type == 'NULL':
            return jsast.NullNode()

        elif tok_type == 'THIS':
            return jsast.ThisNode()

        elif tok_type == 'REGEXP':
            return jsast.RegExpNode(regexp=token.value)

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
               and not (not self.generatorexprsupport and self.arraycompsupport and
                        not self.enclosing_state('[') and self.token.string == 'for')
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


    def parse_expression_list(self):
        """This is the parsing step for parsing lists of expressions (args or expression)."""

        expr = [self.parse_assignment_expression()]

        while self.tentative_match(','):
            expr += [self.parse_assignment_expression()]

        return expr


    def parse_expression(self):
        """This is the parsing step for expression lists. Used as 'expression' in most rules."""

        expr = self.parse_expression_list()

        if len(expr) > 1:
            return jsast.ExpressionListNode(expressions=expr)

        else:
            return expr[0]


    def parse_ID(self, allowkeyword=False):
        """Parse an identifier potentially w/o converting keywords. Raise an exception if not ID."""

        tok = self.token

        if (not (tok.type == 'ID' or
                 allowkeyword and tok.value in self.keywords.js_keywords)):
            raise UnexpectedToken(self.token, parser=self)

        self.get_next_token(regexp=False)
        return jsast.IDNode(name=tok.string)


    #
    # Section: Statements
    #

    def parse_statement(self, labels=[]):
        """Parse one statement as delineated by ';' or block"""

        self.labels = labels

        if self.token.type == '#ERR#':
            return self.pp_parse_directive()

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
            return self.parse_block_guts()

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
                return jsast.StatementNode(statement=expr)


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

        return jsast.StatementBlockNode(statements=self.parse_statement_list('}'))


    def parse_var_guts(self, statement=True):
        """Parse the VAR declaration."""
        var_list = self.parse_declaration_helper(statement=statement)

        if statement:
            return jsast.StatementNode(statement=jsast.VarDeclarationNode(vars=var_list))

        else:
            return jsast.VarDeclarationNode(vars=var_list)

    def parse_debugger_guts(self):
        self.tentative_match(';')
        return jsast.DebuggerNode()

    def parse_let_guts(self, statement=True):
        """Parse the LET declaration."""

        if not self.letsupport:
            raise UnknownToken(self.prevtoken)

        if self.token.value == '(':
            return self.nud_LET(self.token, isstatement=True)

        var_list = self.parse_declaration_helper(statement=statement, decompsupport=True)

        if statement:
            return jsast.StatementNode(statement=jsast.LetDeclarationNode(vars=var_list))

        else:
            return jsast.LetDeclarationNode(vars=var_list)


    def parse_declaration_helper(self, statement=True, decompsupport=False):
        """Parse the variable declaration."""

        var_list = []

        while True:
            # variable name will be used a lot...
            #
            if (self.letsupport or self.expansionsupport) and self.tentative_match('['):
                varname = self.nud_LSBRACKET(self.token)

            else:
                varname = self.parse_ID()

            if self.tentative_match('='):
                var_list.append(jsast.VarInitNode(name=varname,
                                                  value=self.parse_assignment_expression()))

            else:
                var_list.append(jsast.VarInitNode(name=varname, value=None))

            if self.tentative_match(','):
                continue

            elif self.tentative_match('in', consume=statement):
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


    def parse_property(self):
        """Parse object property"""

        # get the property name, will use alot
        #
        prop, id = self.parse_property_name()

        if id == 'ID':

            if self.tentative_match(':'):
                # still a simple property definition
                #
                val = self.parse_assignment_expression()
                return jsast.SimplePropertyNode(name=prop, value=val)

            elif prop.name == 'get':
                prop = self.parse_property_name()[0]
                self.must_match('(', ')', '{')

                # same as function, need to clear the labels
                #
                self.labels = []
                func = self.parse_block_guts()
                return jsast.GetPropertyNode(name=prop, functionbody=func)

            elif prop.name == 'set':
                prop = self.parse_property_name()[0]
                self.must_match('(')
                param = self.parse_ID()
                self.must_match(')', '{')

                # same as function, need to clear the labels
                #
                self.labels = []
                func = self.parse_block_guts()
                return jsast.SetPropertyNode(name=prop, param=param, functionbody=func)

            else:
                # kinda hacky, but generates the right error message
                # we KNOW by this point that ':' isn't there (tried matching it earlier)
                # so forcing that match will complain appropriately
                #
                self.must_match(':')

        else:
            self.must_match(':')
            val = self.parse_assignment_expression()
            return jsast.SimplePropertyNode(name=prop, value=val)


    def parse_property_name(self):
        "Parse the property name"

        id = self.token.type

        if self.token.type == 'NUMBER':
            prop = jsast.NumericLiteralNode(value=self.token.string)
            self.get_next_token()

        elif self.token.type == 'STRING':
            prop = jsast.StringLiteralNode(value=self.token.string)
            self.get_next_token()

        else:
            id = 'ID'
            prop = self.parse_ID(allowkeyword=True)

        return (prop, id)


    def parse_continue_guts(self):
        """Parse the rest of the continue statement."""
        errtok = self.prevtoken

        if self.tentative_match(';') or self.linebreak_detected:
            # must be inside a loop
            #
            if not self.enclosing_state('loop'):
                raise IllegalContinue(errtok)

            return jsast.ContinueNode(id=None)

        else:
            # must have a valid label in enclosing loop
            #
            tok = self.token
            id = self.parse_ID().name

            if id not in self.enclosing_loop_labels():
                raise UndefinedLabel(tok)

            self.must_match(';')
            return jsast.ContinueNode(id=id)


    def parse_break_guts(self):
        """Parse the rest of the break statement."""
        errtok = self.prevtoken

        if self.tentative_match(';') or self.linebreak_detected:
            # must be inside a loop or switch
            #
            if not self.enclosing_state('loop', 'switch'):
                raise IllegalBreak(errtok)

            return jsast.BreakNode(id=None)

        else:
            # must have a valid label in enclosing stmt
            #
            tok = self.token
            id = self.parse_ID().name

            if id not in self.enclosing_stmt_labels():
                raise UndefinedLabel(tok)

            self.must_match(';')
            return jsast.BreakNode(id=id)


    def parse_return_guts(self):
        """Parse the rest of the return statement."""

        if self.tentative_match(';') or self.linebreak_detected:
            return jsast.ReturnNode(expression=None)

        else:
            expr = self.parse_expression()
            self.must_match(';')
            return jsast.ReturnNode(expression=expr)


    def parse_yield_guts(self):
        """Parse the rest of the yield statement."""

        if not self.yieldsupport:
            raise UnknownToken(self.prevtoken)

        if self.tentative_match(';') or self.linebreak_detected:
            return jsast.YieldNode(expression=None)

        else:
            expr = self.parse_expression()
            self.must_match(';')
            return jsast.YieldNode(expression=expr)


    @stamp_state('function', affectslabels=True)
    def parse_function_guts(self, is_declaration=False):
        """Parses a function as a declaration or as an expression."""

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
        param = []

        if not self.tentative_match(')'):

            while True:
                param.append(self.parse_ID())

                if not self.tentative_match(')'):
                    self.must_match(',')

                else:
                    break

        self.must_match('{')
        body = self.parse_block_guts()
        return jsast.FunctionNode(name=name, param=param,
                                  body=body, isdeclaration=is_declaration)


    @stamp_state('stmt', affectslabels=True)
    def parse_with_guts(self):
        """Parse 'with' statement."""

        self.must_match('(')
        expr = self.parse_expression()
        self.must_match(')')
        stmt = self.parse_statement()
        return jsast.WithNode(expression=expr, statement=stmt)


    def parse_throw_guts(self):
        """Parse throw statement."""

        if self.linebreak_detected:
            raise UnexpectedNewline(self.prevtoken)

        expr = self.parse_expression()
        self.must_match(';')
        return jsast.ThrowNode(expression=expr)


    def parse_switch_guts(self):
        """Parse switch statement."""

        self.must_match('(')
        expr = self.parse_expression()
        self.must_match(')')
        return jsast.SwitchNode(expression=expr, cases=self.parse_switchblock())


    @stamp_state('switch', affectslabels=True)
    def parse_switchblock(self):
        """Parse the switch block statements."""

        self.must_match('{')
        code = []
        has_default = False

        while not self.tentative_match('}'):

            if self.tentative_match('case'):
                expr = self.parse_expression()
                self.must_match(':')
                stmt_list = self.parse_statement_list('case', 'default','}', consume=False)
                code.append(jsast.CaseNode(case=expr, statements=jsast.SourceElementsNode(code=stmt_list)))

            elif not has_default and self.tentative_match('default'):
                has_default = True
                self.must_match(':')
                stmt_list = self.parse_statement_list('case', 'default','}', consume=False)
                code.append(jsast.DefaultNode(statements=jsast.SourceElementsNode(code=stmt_list)))

            elif has_default and self.token.string == 'default':
                raise SecondDefaultToken(self.token)

        return jsast.StatementBlockNode(statements=code)


    @stamp_state('stmt', affectslabels=True)
    def parse_try_guts(self):
        """Parse try statement."""

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

        if self.catchifsupport:
            return jsast.TryNode(tryblock=tryblock, catch=catch, finallyblock=finallyblock)
        else:
            return jsast.TryNode(tryblock=tryblock, catch=catch, finallyblock=finallyblock)


    def parse_catch_helper(self):
        'Parse the catch clause of the try/catch/finally statement.'
        catchid = condition = catchblock = None

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
            return jsast.CatchNode(catchid=catchid, catchblock=catchblock)

        elif catchid and self.catchifsupport:
            return jsast.CatchIfNode(catchid=catchid, condition=condition, catchblock=catchblock)

        else:
            return None


    @stamp_state('stmt', affectslabels=True)
    def parse_if_guts(self):
        """Parse if statement."""

        self.must_match('(')
        expr = self.parse_expression()
        self.must_match(')')
        thenstmt = self.parse_statement()
        elsestmt = None

        if self.tentative_match('else'):
            elsestmt = self.parse_statement()

        return jsast.IfNode(ifclause=expr, thenclause=thenstmt, elseclause=elsestmt)

    @stamp_state('loop', affectslabels=True)
    def parse_do_guts(self):
        """Parse do loop."""

        stmt = self.parse_statement()
        self.must_match('while', '(')
        expr = self.parse_expression()
        self.must_match(')', ';')
        return jsast.DoNode(statement=stmt, expression=expr)

    @stamp_state('loop', affectslabels=True)
    def parse_while_guts(self):
        """Parse while loop."""

        self.must_match('(')
        expr = self.parse_expression()
        self.must_match(')')
        stmt = self.parse_statement()
        return jsast.WhileNode(statement=stmt, expression=expr)

    @stamp_state('loop', affectslabels=True)
    def parse_for_guts(self):
        """Parse for loop."""

        if self.foreachsupport and self.tentative_match('each'):
            return self.parse_for_each_guts()

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

            else:
                noin_expr = self.parse_expression()

            self.exit_state()

        expr = expr2 = expr3 = None

        if not multiple_decl and self.tentative_match('in'):
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

        if expr:
            return jsast.ForInNode(init=noin_expr, container=expr, statement=stmt);

        else:
            return jsast.ForNode(part1=noin_expr, part2=expr2, part3=expr3, statement=stmt);


    def parse_for_each_guts(self):
        """Parse for each loop."""
        self.must_match('(')
        var = self.parse_ID()
        self.must_match('in')
        expr = self.parse_expression()
        self.must_match(')')
        stmt = self.parse_statement()

        return jsast.ForEachNode(var=var, container=expr, statement=stmt);


    def parse_comprehension(self, expr):
        """Parse array comprehension or a generator expression,
        starting with a specified expression."""
        # [ expr for - already parsed...

        comprehensions = []

        while True:
#            if not isgeneratorexpr:
#                self.must_match('each')
            # !!! 'for' or 'for each' seem to be interchangeable here???
            #
            if self.tentative_match('each'):
                forstring = 'for each'

            else:
                forstring = 'for'

            comprehensions.append(self.parse_comprehension_chunk())

            if not self.tentative_match('for'):
                break

        return jsast.GeneratorExprNode(expr=expr, forstring=forstring,
                                       comprehensions=comprehensions)


    def parse_comprehension_chunk(self):
        "Parse the comprehension part of an array expression."
        self.must_match('(')
        var = self.parse_ID()
        self.must_match('in')
        container = self.parse_expression()
        self.must_match(')')

        condition = None
        if self.tentative_match('if'):
            self.must_match('(')
            condition = self.parse_expression()
            self.must_match(')')

        return jsast.ComprehensionNode(var=var, container=container, condition=condition)

    def parse(self, program, *, filename=None):
        self.filename = filename
        self.source = program
        self.lexer.setinputstr(program)
        self.reset()
        self.get_next_token()
        return self.parse_source()


    #
    # Section: Preprocessor
    #

    def get_tokens_to_EOL(self):
        'Gets a list of all tokens up to the end of line.'
        curline = self.prevtoken.end[0]
        tokens = []

        while self.token.start[0] == curline and self.token.type != '#EOF#':

            if self.token.value == '\\':
                curline += 1

            else:
                tokens.append(self.token)

            self.get_next_token()

        return tokens

    def expect_tokens_to_EOL(self, errtok):
        "Raise PP_MalformedToken(errtok) exception if there are NO MORE tokens on this line."
        tokens = self.get_tokens_to_EOL()

        if tokens:
            return tokens
        else:
            raise PP_MalformedToken(errtok)


    def expect_no_tokens_to_EOL(self, errtok):
        "Raise PP_MalformedToken(errtok) exception if there are ANY tokens on this line."
        tokens = self.get_tokens_to_EOL()

        if tokens:
            raise PP_MalformedToken(errtok)


    def stitch_tokens_into_string(self, tokens):
        'Given a list of tokens stitch them into a string.'
        # ignore whitespace for now, but we can recover whitespace from line, col info
        #
        return ' '.join([tok.value for tok in tokens])


    def pp_parse_directive(self):
        'Attempt to parse the directive'
        token = self.token

        # we can't have any other tokens on the same line preceeding the directives
        # and the directive must start with '#'
        #
        if self.prevtoken.end[0] == token.start[0] or token.value[0] != '#':
            raise UnknownToken(token)

        action = getattr(self, 'pp_parse_' + self.token.value[1:], None)

        if action:
            self.get_next_token()
            return action()

        else:
            raise UnknownToken(token)


    def pp_parse_error(self):
        'Grab the error message after the directive'
        message = None
        tokens = self.get_tokens_to_EOL()

        if tokens:
            message = self.stitch_tokens_into_string(tokens)

        return ppast.PP_Error(message=message)


    def pp_parse_warning(self):
        'Grab the warning message after the directive'
        message = None
        tokens = self.get_tokens_to_EOL()

        if tokens:
            message = self.stitch_tokens_into_string(tokens)

        return ppast.PP_Warning(message=message)


    def pp_parse_include(self):
        'Grab the warning message after the directive'
        errtok = self.prevtoken
        tokens = self.expect_tokens_to_EOL(errtok)

        if len(tokens) == 1 and tokens[0].type == 'STRING':
            package = tokens[0].value

        else:
            raise PP_UnexpectedToken(tokens[0])

        return ppast.PP_Include(package=package)


    def pp_parse_define(self):
        '''Grab tokens for the definition.'''
        # !!! later do special processing:
        # 1) parametrized definitions
        # 2) '#' and '##'
        #
        errtok = self.prevtoken
        param = []

        self.lexer.swallow_line_cont = True
        if self.token.type == 'ID':
            name = self.token.value
            self.get_next_token()

        else:
            raise PP_MalformedToken(errtok)

        # parse possible param list
        #
        if self.tentative_match('('):
            param = self.pp_parse_param_list()

        self.lexer.swallow_line_cont = False


        # get the rest of the tokens and aggregate them into strings, special markers, and params
        #
        curline = self.token.end[0]
        param_vals = [p.name for p in param]
        chunks = self.pp_parse_define_body(curline, param_vals)

        if param:
            return ppast.PP_DefineCallable(name=name, param=param, chunks=chunks)

        else:
            return ppast.PP_DefineName(name=name, chunks=chunks)


    def pp_parse_define_body(self, curline, param_vals):
        'Get the rest of the tokens and aggregate them into strings, special markers, and params'
        # convenience object and
        #
        chunks = []
        chunk = None
        def append_chunk(chunk, chunks=chunks):
            if chunk:
                chunks.append(ppast.PP_CodeChunk(string=str(chunk.value), token=chunk))

        while self.token.start[0] == curline and self.token.type != '#EOF#':

            if self.token.value == '\\':
                curline += 1

            elif self.token.value == '#':
                append_chunk(chunk)
                chunk = None
                chunks.append(ppast.PP_Quote())

            elif self.token.value == '##':
                append_chunk(chunk, chunks)
                chunk = None
                chunks.append(ppast.PP_Concat())

            elif self.token.value == ',':
                append_chunk(chunk, chunks)
                chunk = None
                chunks.append(ppast.PP_CodeChunk(string=',', token=self.token))

            elif self.token.type == 'ID' and self.token.value in param_vals:
                append_chunk(chunk, chunks)
                chunk = None
                chunks.append(ppast.PP_Param(name=self.token.value))

            elif self.token.value == '(':
                append_chunk(chunk, chunks)
                chunk = None

            elif self.token.value == ')':
                append_chunk(chunk, chunks)
                break

            elif not chunk:
                chunk = self.token

            else:
                chunk = Token('#chunk#', chunk.value + ' ' + self.token.value,
                              chunk.start, self.token.end, '')

            if self.token.value == '(':
                # process params
                #
                self.must_match('(')
                self.lexer.swallow_line_cont = True
                call = ppast.PP_Call(arguments=self.pp_parse_define_body(curline, param_vals))
                self.must_match(')')
                chunks.append(call)
                self.lexer.swallow_line_cont = False
                curline = self.prevtoken.end[0]

            else:
                self.get_next_token()

        append_chunk(chunk, chunks)

        return chunks

    def pp_parse_param_list(self):
        'Parse a param list.'
        param = []
        if not self.tentative_match(')'):

            while True:
                param.append(self.parse_ID())

                if not self.tentative_match(')'):
                    self.must_match(',')

                else:
                    break

        return param


    def pp_parse_if(self):
        '''Process if directive.'''
        errtok = self.prevtoken
        self.lexer.swallow_line_cont = True
        condition = self.parse_expression()
        self.lexer.swallow_line_cont = False

        self.expect_no_tokens_to_EOL(errtok)
        firstblock, elifblocks, elseblock = self.pp_parse_if_contents()

        return ppast.PP_If(condition=condition, firstblock=firstblock,
                           elifblocks=elifblocks,
                           elseblock=elseblock)

    def pp_parse_ifdef(self):
        '''Process ifdef.'''
        errtok = self.prevtoken
        name = self.pp_parse_single_name(errtok)
        firstblock, elifblocks, elseblock = self.pp_parse_if_contents()

        return ppast.PP_Ifdef(name=name, firstblock=firstblock,
                              elifblocks=elifblocks,
                              elseblock=elseblock)


    def pp_parse_ifndef(self):
        '''Process ifndef.'''
        errtok = self.prevtoken
        name = self.pp_parse_single_name(errtok)
        firstblock, elifblocks, elseblock = self.pp_parse_if_contents()

        return ppast.PP_Ifndef(name=name, firstblock=firstblock,
                               elifblocks=elifblocks,
                               elseblock=elseblock)


    def pp_parse_if_contents(self):
        '''Parse firstblock, optional elifbloks, and optional elseblock.
        Return the tuple (firstblock, elifblocks, elseblock)'''
        firstblock = self.pp_parse_if_block()

        elifblocks = []
        while self.tentative_match('#elif'):
            elifblocks.append(self.pp_parse_elif_chunk())

        elseblock = None
        if self.tentative_match('#else'):
            elseblock = self.pp_parse_else_chunk()

        self.must_match('#endif')
        errtok = self.prevtoken
        self.expect_no_tokens_to_EOL(errtok)

        return (firstblock, elifblocks, elseblock)


    def pp_parse_elif_chunk(self):
        '''Process elif directive.'''
        errtok = self.prevtoken
        self.lexer.swallow_line_cont = True
        condition = self.parse_expression()
        self.lexer.swallow_line_cont = False

        self.expect_no_tokens_to_EOL(errtok)
        block = self.pp_parse_if_block()

        return ppast.PP_Elif(condition=condition, block=block)


    def pp_parse_else_chunk(self):
        '''Process else.'''
        errtok = self.prevtoken
        self.expect_no_tokens_to_EOL(errtok)
        block = self.pp_parse_if_block()

        return ppast.PP_Else(block=block)


    def pp_parse_if_block(self):
        'Parse the source block enclosed in the #if (or similar) directive.'
        return jsast.SourceElementsNode(code=self.parse_statement_list('#else', '#elif', '#endif',
                                                                       '<<EOF>>',
                                                                       consume=False))


    def pp_parse_single_name(self, errtok):
        tokens = self.expect_tokens_to_EOL(errtok)

        if len(tokens) == 1 and tokens[0].type == 'ID':
            name = tokens[0].value

        else:
            first = tokens[0]
            raise PP_UnexpectedToken(tokens[1] if first.type == 'ID' else first)

        return name
