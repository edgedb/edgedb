##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##



import sys
import tokenize
import pyggy

from semantix.utils import parsing, ast
from semantix.utils.lang.javascript.codegen import JavascriptSourceGenerator

from .. import ast as jsast
from . import keywords


# errors
class ParseError(Exception): pass


class UnknownToken(ParseError):
    def __init__(self, val, line, col):
        super().__init__("Syntax error: unknown token %r (line %i, column %i)." %
                         (val, line, col))


class UnexpectedToken(ParseError):
    def __init__(self, token):
        super().__init__("Syntax error: unexpected token %r (line %i, column %i)." %
                         (token.value, token.start[0], token.start[1]))


class UnknownOperator(ParseError):
    def __init__(self, token):
        super().__init__("Syntax error: unknown operator %r on line %i, column %i." %
                         (token.value, token.start[0], token.start[1]))


class MissingToken(ParseError):
    def __init__(self, token, expected):
        if len(expected) == 1:
            super().__init__("Syntax error: unexpected token %r instead of %r (line %i, column %i)." %
                             (token.value, expected[0], token.start[0], token.start[1]))
        else:
            super().__init__("Syntax error: unexpected token %r instead of one of %s (line %i, column %i)." %
                             (token.value, list(expected), token.start[0], token.start[1]))


class SecondDefaultToken(ParseError):
    def __init__(self, token):
        super().__init__("Syntax error: more than one default clause in switch statement (line %i, column %i)." %
                         (token.start[0], token.start[1]))


class IllegalBreak(ParseError):
    def __init__(self, token):
        super().__init__("Syntax error: illegal break statement (line %i, column %i)." %
                         (token.start[0], token.start[1]))


class IllegalContinue(ParseError):
    def __init__(self, token):
        super().__init__("Syntax error: illegal continue statement (line %i, column %i)." %
                         (token.start[0], token.start[1]))


class UndefinedLabel(ParseError):
    def __init__(self, token):
        super().__init__("Syntax error: undefined label %r (line %i, column %i)." %
                         (token.value, token.start[0], token.start[1]))

class DuplicateLabel(ParseError):
    def __init__(self, token):
        super().__init__("Syntax error: duplicate label %r (line %i, column %i)." %
                         (token.value, token.start[0], token.start[1]))

# decorator for adding methods to tokens... ugly as hack
def method(cls):
    def bind(fn):
        setattr(cls, fn.__name__, fn)
    return bind

# decorator for certain parsing methods that need to keep track of labels
def stamp_state(name):
    "Add a state & label stamp to the stack before executing the function. Exit that state afterwards."
    def wrapper(func):
        def callfunc(this, *args, **kwargs):
            this.enter_state(name, affectslabels=True)
            result = func(this, *args, **kwargs)
            this.exit_state()
            return result
        return callfunc
    return wrapper

OP = {
    '<'     : 'LT',
    '>'     : 'GT',
    '<='    : 'LE',
    '>='    : 'GE',
    '=='    : 'EQ',
    '!='    : 'NEQ',
    '==='   : 'SEQ',
    '!=='   : 'SNEQ',
    '+'     : 'PLUS',
    '-'     : 'MINUS',
    '*'     : 'MULT',
    '%'     : 'MOD',
    '/'     : 'DIV',
    '++'    : 'PLUSPLUS',
    '--'    : 'MINUSMINUS',
    '<<'    : 'LSHIFT',
    '>>'    : 'SRSHIFT',
    '>>>'   : 'ZRSHIFT',
    '&'     : 'BITAND',
    '|'     : 'BITOR',
    '^'     : 'BITXOR',
    '!'     : 'NOT',
    '~'     : 'BITNOT',
    '&&'    : 'AND',
    '||'    : 'OR',
    '?'     : 'HOOK',
    '='     : 'ASSIGN',
    '+='    : 'PLUSASSIGN',
    '-='    : 'MINUSASSIGN',
    '*='    : 'MULTASSIGN',
    '%='    : 'MODASSIGN',
    '/='    : 'DIVASSIGN',
    '<<='   : 'LSHASSIGN',
    '>>='   : 'SRSHASSIGN',
    '>>>='  : 'ZRSHASSIGN',
    '&='    : 'ANDASSIGN',
    '|='    : 'ORASSIGN',
    '^='    : 'XORASSIGN',

    # some ops that are parsed separately, but need to be in this list
    'function'  : 'FUNCTION',
    'instanceof': 'INSTANCEOF',
    'in'    : 'IN',
    'delete': 'DELETE',
    'void'  : 'VOID',
    'typeof': 'TYPEOF',
    'new'   : 'NEW',
    '.'     : 'DOT',

    '('     : 'LPAREN',
    '['     : 'LSBRACKET',
    '{'     : 'LCBRACKET',
    }

class Token(tokenize.TokenInfo):
#    id = None # node/token type name
#    value = None # used by literals
    parser = None
    rbp, lbp = 0, 0

    @property
    def id(self): return self[0]

    @id.setter
    def id(self, val): self[0] = val

    @property
    def value(self): return self[1]

    @property
    def start(self): return self[2]

    @property
    def end(self): return self[3]


    def nud(self):
        raise UnexpectedToken(self)

    def led(self, left):
        raise UnknownOperator(self)


class UnaryRight():
    def nud(self):
        operand = self.parser.parse_assignment_expression(self.rbp)
        return jsast.PrefixExpressionNode(op=self.value, expression=operand)

class UnaryLeft():
    def led(self, left):
        return jsast.PostfixExpressionNode(op=self.value, expression=left)

class BinaryLeft():
    def led(self, left):
        right = self.parser.parse_assignment_expression(self.lbp)
        return jsast.BinExpressionNode(left=left, op=self.value, right=right)

class Assign():
    def led(self, left):
        right = self.parser.parse_assignment_expression(self.lbp)
        return jsast.BinExpressionNode(left=left, op=self.value, right=right)


PREC = [
    ((),            ('LCBRACKET', )),
    ((),            ('NEW', 'LSBRACKET', 'DOT', 'FUNCTION')),
    ((),            ('LPAREN', )),
    ((UnaryLeft,),  ('PLUSPLUS', 'MINUSMINUS')),
    ((UnaryRight,), ('PLUSPLUS', 'MINUSMINUS', 'PLUS', 'MINUS', 'BITNOT', 'NOT', 'DELETE', 'VOID', 'TYPEOF')),
    ((BinaryLeft,), ('MULT', 'DIV', 'MOD')),
    ((BinaryLeft,), ('PLUS', 'MINUS')),
    ((BinaryLeft,), ('LSHIFT', 'SRSHIFT', 'ZRSHIFT')),
    ((BinaryLeft,), ('LT', 'GT', 'LE', 'GE', 'INSTANCEOF', 'IN')),
    ((BinaryLeft,), ('EQ', 'NEQ', 'SEQ', 'SNEQ')),
    ((BinaryLeft,), ('BITAND', )),
    ((BinaryLeft,), ('BITXOR', )),
    ((BinaryLeft,), ('BITOR', )),
    ((BinaryLeft,), ('AND', )),
    ((BinaryLeft,), ('OR', )),
    ((),            ('HOOK', )),
    ((Assign,),     ('ASSIGN', 'PLUSASSIGN', 'MINUSASSIGN', 'MULTASSIGN', 'DIVASSIGN', 'MODASSIGN', 'LSHASSIGN', 'SRSHASSIGN', 'ZRSHASSIGN', 'ANDASSIGN', 'ORASSIGN'))
#    ((),            ('COMMA', 'RPAREN', 'RSBRACKET', 'RCBRACKET')),
        ]

# create the tokens for various operators
def create_Token_OP():
    # compile a list of base classes, rbp and lbp for each op

    # first populate with token names
    tokenop = {OP[val]: [(), 0, 0] for val in OP}

    # calculate the starting binding power
    bp = len(PREC) * 10
    # figure out bp and what to inherit based on precedence & associativity
    for bases, ops in PREC:
        # now for each OP token, update binding power and method
        for op in ops:
            tokenop[op][0] += bases
            if bases == (UnaryRight,):
                tokenop[op][1] = bp #rbp
            else:
                tokenop[op][2] = bp #lbp
        # decrease binding power
        bp -= 10

    for name, t in tokenop.items():
        tokenname = 'Token_' + name
        cls = type(tokenname, t[0] + (Token,), {'rbp': t[1], 'lbp': t[2]})
        setattr(sys.modules[__name__], tokenname, cls)
create_Token_OP()


# create the tokens for keywords
def create_Token_KEYWORDS():
    for val, (name, typ) in keywords.js_keywords.items():
        tokenname = 'Token_' + name
        module = sys.modules[__name__]
        if not module.__dict__.get(tokenname):
            setattr(sys.modules[__name__], tokenname, type(tokenname, (Token,), {}))
create_Token_KEYWORDS()


# make stubs for the rest of the tokens
def create_various_Tokens():
    tokens = {'ID', 'STRING', 'NUMBER', 'REGEXP', 'RCBRACKET', 'RSBRACKET', 'RPAREN',
              'DOT', 'SEMICOLON', 'COLON', 'COMMA'}
    for name in tokens:
        tokenname = 'Token_' + name
        module = sys.modules[__name__]
        if not module.__dict__.get(tokenname):
            setattr(sys.modules[__name__], tokenname, type(tokenname, (Token,), {}))

create_various_Tokens()


# tokens understood by THIS parser
@method(Token_ID)
def nud(self):
    return jsast.IDNode(name=self.value)


@method(Token_STRING)
def nud(self):
    return jsast.StringLiteralNode(value=self.value)


@method(Token_NUMBER)
def nud(self):
    return jsast.NumericLiteralNode(value=self.value)


@method(Token_TRUE)
def nud(self):
    return jsast.BooleanLiteralNode(value=True)


@method(Token_FALSE)
def nud(self):
    return jsast.BooleanLiteralNode(value=False)


@method(Token_NULL)
def nud(self):
    return jsast.NullNode()


@method(Token_THIS)
def nud(self):
    return jsast.ThisNode()


@method(Token_REGEXP)
def nud(self):
    return jsast.RegExpNode(regexp=self.value)


# defining proper handling for some special operators

@method(Token_VOID)
def nud(self):
    operand = self.parser.parse_assignment_expression(self.rbp)
    return jsast.VoidNode(expression=operand)


@method(Token_DELETE)
def nud(self):
    operand = self.parser.parse_assignment_expression(self.rbp)
    return jsast.DeleteNode(expression=operand)


@method(Token_TYPEOF)
def nud(self):
    operand = self.parser.parse_assignment_expression(self.rbp)
    return jsast.TypeOfNode(expression=operand)


@method(Token_IN)
def led(self, left):
    right = self.parser.parse_assignment_expression(self.lbp)
    return jsast.InNode(expression=left, container=right)


@method(Token_INSTANCEOF)
def led(self, left):
    right = self.parser.parse_assignment_expression(self.lbp)
    return jsast.InstanceOfNode(expression=left, type=right)


@method(Token_DOT)
def led(self, left):
    right = self.parser.parse_ID(allowkeyword=True)
    return jsast.DotExpressionNode(left=left, right=right)


@method(Token_NEW)
def nud(self):
    # new expression, read the expression up to '('
    expr = self.parser.parse_assignment_expression(Token_LPAREN.lbp)
    # there may be an argument list here
    args = None
    if self.parser.tentative_match('('):
        args = self.parser.parse_expression_list()
        self.parser.must_match(')', regexp=False)
    return jsast.NewNode(expression=expr, arguments=args)


@method(Token_FUNCTION)
def nud(self):
    # function expression
    return self.parser.parse_function_guts()


@method(Token_LSBRACKET)
def nud(self):
    # array literal
    self.parser.enter_state('[')
    array = []
    while not self.parser.tentative_match(']', regexp=False):
        if self.parser.tentative_match(','):
            # take care of elision
            array.append(None)
        else: # process the next expression with its trailing comma
            array.append(self.parser.parse_assignment_expression())
            self.parser.tentative_match(',')
    self.parser.exit_state()
    return jsast.ArrayLiteralNode(array=array)
@method(Token_LSBRACKET)
def led(self, left):
    # indexing
    self.parser.enter_state('[')
    expr = self.parser.parse_expression()
    self.parser.must_match(']', regexp=False)
    self.parser.exit_state()
    return jsast.SBracketExpressionNode(list=left, element=expr)


# parenthesis expression
@method(Token_LPAREN)
def nud(self):
    # parenthesis enclosed expression
    self.parser.enter_state('(')
    expr = self.parser.parse_expression()
    self.parser.must_match(')', regexp=False)
    self.parser.exit_state()
    return expr
@method(Token_LPAREN)
def led(self, left):
    # this is parenthesis as operator, implying callable
    self.parser.enter_state('(')
    args = []
    if not self.parser.tentative_match(')', regexp=False):
        args = self.parser.parse_expression_list()
        self.parser.must_match(')', regexp=False)
    self.parser.exit_state()
    return jsast.CallNode(call=left, arguments=args)


# object literal
@method(Token_LCBRACKET)
def nud(self):
    # parenthesis enclosed expression
    self.parser.enter_state('{')
    guts = []
    while not self.parser.tentative_match('}', regexp=False):
        guts.append(self.parser.parse_property())
        if self.parser.tentative_match('}', regexp=False):
            break
        else:
            self.parser.must_match(',')
    self.parser.exit_state()
    return jsast.ObjectLiteralNode(properties=guts)


@method(Token_HOOK)
def led(self, left):
    iftrue = self.parser.parse_assignment_expression(self.lbp)
    self.parser.must_match(':')
    iffalse = self.parser.parse_assignment_expression(self.lbp)
    return jsast.ConditionalExpressionNode(condition=left, true=iftrue, false=iffalse)


# special token for end of input stream
class End_Token(Token): pass


# change the type of some keywords to 'OP'
#Token_NEW.id = 'OP'
#Token_DELETE.id = 'OP'
#Token_VOID.id = 'OP'
#Token_TYPEOF.id = 'OP'
#Token_INSTANCEOF.id = 'OP'
#Token_IN.id = 'OP'
#Token_FUNCTION.id = 'OP'


# parsing engine with special rules for context, etc.
class JSParser:
    lexer = None
    tab = None
    token = None
    _scope = []
    _labels = []
#    _direct_labels = []
#    _indirect_labels = []

    def __init__(self, lex_name="semantix/utils/lang/javascript/parser/js2.pyl"):
        super().__init__()
        self.set_lexer(lex_name)

    # parser state handling
    @property
    def state(self):
        return self._scope[-1][0] if self._scope else None

    def enter_state(self, state, affectslabels=False):
        self._scope.append((state, self._labels))
        if affectslabels:
            self._labels = []

    # maybe the whole *verify part needs killing...
    def exit_state(self):
        self._scope.pop()

    # checking for enclosing state
    def eclosing_state(self, *states, boundary='function'):
        scope = self._scope[:]
        scope.reverse()
        for s in scope:
            if s[0] == boundary:
                break
            elif s[0] in states:
                return True
        return False

    # extracting labels from the state
    def enclosing_loop_labels(self):
        return self.eclosing_labels('loop')

    def enclosing_stmt_labels(self):
        return self.eclosing_labels('loop', 'stmt', 'switch')

    def eclosing_labels(self, *states, boundary='function'):
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

#    def add_label(self, label):
#        self._labels.append(label)

    #token lexing
    def set_lexer(self, fname):
        self.lexer, self.tab = pyggy.getlexer(fname, debug = 1)

    def get_next_token(self, regexp=True):
        "Uses the associated lexer to grab the next token."
        self.lexer.regexp_possible = regexp
        token = self.lexer.token()
        mod_dict = sys.modules[__name__].__dict__
        if token == 'OP':
            tokenname = OP[self.lexer.value]
            classname = 'Token_' + tokenname
            tok = mod_dict[classname](token, self.lexer.value, self.lexer.start, self.lexer.end, '')
        elif token == 'ID':
            tokenname = token
            if self.lexer.value in keywords.js_keywords:
                tokenname = keywords.js_keywords[self.lexer.value][0]
                # swap some topken types for 'OP' if aplicable
                token = 'OP' if self.lexer.value in OP else 'KEYWORD'
            classname = 'Token_' + tokenname
            tok = mod_dict[classname](token, self.lexer.value, self.lexer.start, self.lexer.end, '')
        elif token not in (None, '#ERR#'):
            classname = 'Token_' + token
            # swap some topken types for 'OP' if aplicable
            token = 'OP' if self.lexer.value in OP else token
            tok = mod_dict[classname](token, self.lexer.value, self.lexer.start, self.lexer.end, '')
        elif token == '#ERR#':
            raise UnknownToken(self.lexer.value, self.lexer.start[0], self.lexer.start[1])
        else:
            tok = End_Token(token, self.lexer.value, None, None, '')

        if tok:
            tok.parser = self
        return tok

    def must_match(self, *tok, regexp=True):
        """Matches the current token against the specified value.
        If more than one value is given, then a sequence of tokens must match.
        Consumes the token if it is correct, raises an exception otherwise."""
        for val in tok:
            if self.token.value != val:
                raise MissingToken(self.token, tok)
            self.token = self.get_next_token(regexp)

    def tentative_match(self, *tok, regexp=True, consume=True):
        """Checks if the current token matches any of the provided values.
        Only checks ONE token, not a sequence, like 'must_match'.
        If it does, the token is returned and next token is processed from the lexer.
        If there is no match, None is returned and the token stays."""
        if self.token.value in tok:
            t = self.token
            if consume:
                self.token = self.get_next_token(regexp)
            return t
        else:
            return None

    def parse_assignment_expression(self, rbp=0):
        """This is the basic parsing step for expressions.
        rbp - specifies Right Binding Power of the current token"""
        prevt = self.token
        self.token = self.get_next_token(regexp=(prevt.id == 'OP'))
        left = prevt.nud()
        while rbp < self.token.lbp and not (self.state == 'noin' and self.token.value == 'in'):
            prevt = self.token
            self.token = self.get_next_token(regexp=(prevt.id == 'OP'))
            # it's an error to have consecutive unary postfix operators
            if isinstance(prevt, UnaryLeft) and isinstance(self.token, UnaryLeft):
                raise UnexpectedToken(self.token)
            left = prevt.led(left)
        return left

    def parse_expression_list(self):
        """This is the parsing step for parsing lists of expressions (used in args, expression, etc.)."""
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
        if not (isinstance(tok, Token_ID) or \
            allowkeyword and tok.value in keywords.js_keywords):
            raise UnexpectedToken(self.token)
        self.token = self.get_next_token(regexp=False)
        return jsast.IDNode(name=tok.value)

    def parse_statement(self, labels=[]):
        """Parse one statement as delineated by ';' or block"""
        self.labels = labels
        if self.token.id == 'KEYWORD':
            # statements start with keywords, otherwise it's an expression
            return self.parse_keywords()
        elif self.tentative_match(';'):
            # empty statement
            return None
        elif self.tentative_match('{'):
            # block, converts direct labels into indirect
            return self.parse_block_guts()
        elif self.tentative_match('function'):
            # function declaration
            return self.parse_function_guts(is_declaration=True)
        elif self.token.id == 'ID':
            # this may be a label
            tok = self.token
            self.token = self.get_next_token(regexp=False)
            if self.tentative_match(':'):
                # this is a label
                label = tok.value
                if label in self.enclosing_stmt_labels():
                    raise DuplicateLabel(tok)
                return jsast.LabelNode(id=label,
                                       statement=self.parse_statement(labels=labels + [label]))
            else:
                # bad luck, it wasn't a label so push back last token and try parsing expression
                self.lexer.line, self.lexer.col = self.token.start
                self.lexer.PUSHBACK(self.token.value)
                self.token = tok
                expr = self.parse_expression()
                self.must_match(';')
                return jsast.StatementNode(statement=expr)
        else:
            expr = self.parse_expression()
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
        # make a dict for processing actions related to different keywords
        action = getattr(self, 'parse_' + self.token.value + '_guts', None)
#        action = {
#                  'var': self.parse_var_guts,
#                  'continue': self.parse_continue_guts,
#                  'break': self.parse_break_guts,
#                  'return': self.parse_return_guts,
#                  'debugger': lambda: (self.must_match(';'), jsast.DebuggerNode())[1]
#                  }.get(self.token.value)
        if action:
            self.token = self.get_next_token()
            return action()
        else:
            raise UnexpectedToken(self.token)
    @stamp_state('stmt')
    def parse_block_guts(self):
        """Parse statements inside a block.
        The end delimiter is different from source parsing."""
        return jsast.StatementBlockNode(statements=self.parse_statement_list('}'))

    def parse_var_guts(self, statement=True):
        """Parse the variable declaration."""
        var_list = []
        while True:
            # variable name
            varname = self.parse_ID()
            if self.tentative_match('='):
                var_list.append(jsast.VarInitNode(name=varname.name, value=self.parse_assignment_expression()))
            else:
                var_list.append(jsast.VarInitNode(name=varname.name, value=None))
            if self.tentative_match(','):
                continue
            elif self.tentative_match(';', consume=statement):
                break
            elif self.tentative_match('in', consume=statement):
                # this can only happen in 'noin' mode
                break
            else:
                raise UnexpectedToken(self.token)
        if statement:
            return jsast.StatementNode(statement=jsast.VarDeclarationNode(vars=var_list))
        else:
            return jsast.VarDeclarationNode(vars=var_list)

    def parse_property(self):
        """Parse object property"""
        # get the property name
        prop, id = self.parse_property_name()
        if id == 'ID':
            if self.tentative_match(':'):
                # still a simple property definition
                val = self.parse_assignment_expression()
                return jsast.SimplePropertyNode(name=prop, value=val)
            elif prop.name == 'get':
                prop = self.parse_property_name()[0]
                self.must_match('(', ')', '{')
                # same as function, need to clear the labels
                self.labels = []
                func = self.parse_block_guts()
                return jsast.GetPropertyNode(name=prop, functionbody=func)
            elif prop.name == 'set':
                prop = self.parse_property_name()[0]
                self.must_match('(')
                param = self.parse_ID()
                self.must_match(')', '{')
                # same as function, need to clear the labels
                self.labels = []
                func = self.parse_block_guts()
                return jsast.SetPropertyNode(name=prop, param=param, functionbody=func)
            else:
                # kinda hacky, but generates the right error message
                # we KNOW by this point that ':' isn't there (tried matching it earlier)
                # so forcing that match will complain appropriately
                self.must_match(':')
        else:
            self.must_match(':')
            val = self.parse_assignment_expression()
            return jsast.SimplePropertyNode(name=prop, value=val)

    def parse_property_name(self):
        # get the property name
        id = self.token.id
        if self.token.id == 'NUMBER':
            prop = jsast.NumericLiteralNode(value=self.token.value)
            self.token = self.get_next_token()
        elif self.token.id == 'STRING':
            prop = jsast.StringLiteralNode(value=self.token.value)
            self.token = self.get_next_token()
        else:
            id = 'ID'
            prop = self.parse_ID(allowkeyword=True)
        return (prop, id)

    def parse_continue_guts(self):
        """Parse the rest of the continue statement."""
        # !!! needs to be smarter and actually check newlines
        if self.tentative_match(';'):
            # must be inside a loop
            if not self.eclosing_state('loop'):
                raise IllegalContinue(self.token)
            return jsast.ContinueNode(id=None)
        else:
            # must have a valid label in enclosing loop
            tok = self.token
            id = self.parse_ID().name
            if id not in self.enclosing_loop_labels():
                raise UndefinedLabel(tok)
            self.must_match(';')
            return jsast.ContinueNode(id=id)

    def parse_break_guts(self):
        """Parse the rest of the continue statement."""
        # !!! needs to be smarter and actually check newlines
        if self.tentative_match(';'):
            # must be inside a loop or switch
            if not self.eclosing_state('loop', 'switch'):
                raise IllegalBreak(self.token)
            return jsast.BreakNode(id=None)
        else:
            # must have a valid label in enclosing stmt
            tok = self.token
            id = self.parse_ID().name
            if id not in self.enclosing_stmt_labels():
                raise UndefinedLabel(tok)
            self.must_match(';')
            return jsast.BreakNode(id=id)

    def parse_return_guts(self):
        """Parse the rest of the continue statement."""
        # !!! needs to be smarter and actually check labels and newlines
        if self.tentative_match(';'):
            return jsast.ReturnNode(expression=None)
        else:
            expr = self.parse_expression()
            self.must_match(';')
            return jsast.ReturnNode(expression=expr)

    @stamp_state('function')
    def parse_function_guts(self, is_declaration=False):
        """Parses a function as a declaration or as an expression."""
        # clear the labels since none of them matter inside
        self.labels = []
        name = None
        if is_declaration:
            name = self.parse_ID().name
            self.must_match('(')
        elif not self.tentative_match('('):
            name = self.parse_ID().name
            self.must_match('(')
        # grab the arglist
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
        return jsast.FunctionNode(name=name, param=param, body=body)

    @stamp_state('stmt')
    def parse_with_guts(self):
        """Parse 'with' statement."""
        self.must_match('(')
        expr = self.parse_expression()
        self.must_match(')')
        stmt = self.parse_statement()
        return jsast.WithNode(expression=expr, statement=stmt)

    def parse_throw_guts(self):
        """Parse throw statement."""
        #!!! needs to handle newline
        expr = self.parse_expression()
        self.must_match(';')
        return jsast.ThrowNode(expression=expr)

    def parse_switch_guts(self):
        """Parse switch statement."""
        self.must_match('(')
        expr = self.parse_expression()
        self.must_match(')')
        return jsast.SwitchNode(expression=expr, cases=self.parse_switchblock())

    @stamp_state('switch')
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
            elif has_default and self.token.value == 'default':
                raise SecondDefaultToken(self.token)
        return jsast.StatementBlockNode(statements=code)

    @stamp_state('stmt')
    def parse_try_guts(self):
        """Parse try statement."""
        self.must_match('{')
        tryblock = self.parse_block_guts()
        catchid = catchblock = finallyblock = None
        if self.tentative_match('catch'):
            self.must_match('(')
            catchid = self.parse_ID().name
            self.must_match(')', '{')
            catchblock = self.parse_block_guts()
        # depending on presence of catch block, finally block may or may not be optional
        if not catchid:
            self.must_match('finally', '{')
            finallyblock = self.parse_block_guts()
        else:
            if self.tentative_match('finally'):
                self.must_match('{')
                finallyblock = self.parse_block_guts()
        return jsast.TryNode(tryblock=tryblock, catchid=catchid, catchblock=catchblock, finallyblock=finallyblock)

    @stamp_state('stmt')
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

    @stamp_state('loop')
    def parse_do_guts(self):
        """Parse do loop."""
        stmt = self.parse_statement()
        self.must_match('while', '(')
        expr = self.parse_expression()
        self.must_match(')', ';')
        return jsast.DoNode(statement=stmt, expression=expr)

    @stamp_state('loop')
    def parse_while_guts(self):
        """Parse while loop."""
        self.must_match('(')
        expr = self.parse_expression()
        self.must_match(')')
        stmt = self.parse_statement()
        return jsast.WhileNode(statement=stmt, expression=expr)

    @stamp_state('loop')
    def parse_for_guts(self):
        """Parse for loop."""
        self.must_match('(')
        noin_expr = None
        multiple_decl = False
        # skip classical for, without initializer
        if not self.tentative_match(';', consume=False):
            self.enter_state('noin')
            if self.tentative_match('var'):
                # var declaration
                noin_expr = self.parse_var_guts(statement=False)
                multiple_decl = len(noin_expr.vars) > 1
            else:
                noin_expr = self.parse_expression()
            self.exit_state()
        expr = expr2 = expr3 = None
        if not multiple_decl and self.tentative_match('in'):
            # for (x in [1,2,3]) ...
            expr = self.parse_expression()
        else:
            # we've got 'classical' for
            self.must_match(';')
            if not self.tentative_match(';'):
                expr2 = self.parse_expression()
                self.must_match(';')
            if not self.tentative_match(')', consume=False):
                expr3 = self.parse_expression()
        self.must_match(')')
        stmt = self.parse_statement()
        if expr:
            return jsast.ForInNode(init=noin_expr, array=expr, statement=stmt);
        else:
            return jsast.ForNode(part1=noin_expr, part2=expr2, part3=expr3, statement=stmt);

    def reset(self):
        """Reset the line & col counters, and internal state."""
        self.token = None
        self.lexer.line, self.lexer.col = 1, 0
        self._scope = []
        self._labels = []

    def parse(self, program):
        self.lexer.setinputstr(program)
        self.reset()
        self.token = self.get_next_token()
        return self.parse_source()

## testing
#def test(src):
#    print('original src:\n', src)
#    parser.reset()
#    tree = parser.parse(src)
#    print(ast.dump.pretty_dump(tree, colorize=True))
#    processed_src = JavascriptSourceGenerator.to_source(tree)
#    print(processed_src)
#
#
#if __name__ == "__main__":
#    parser = JSParser()
#    test("a + 3;")
#    test("a + 3 * \n4;")
#    test("'www' + 0x11 / -33;")
#    test("- -33e2 * 4.23;")
#    test("4.e3 * - - -33.;")
#    test("4.e3 * - - -33.;")
#    test("3+--a;")
#    test("3+2 == 4 && 5+9<<1;")
#    test("3+true == null && 5+9<<1;")
#    test("3*(2+4);")
#    test("(2+4,a,true,3);")
#    test("2,this,3;")
#    test("('testing');")
#    test("print('testing');")
#    test("print('testing', 1,2,3);")
#    test("print([1,2,3]);")
#    test("print([1,2,3,]);")
#    test("print([,,,]);")
#    test("print([1,2,,,,]);")
#    test("print([,,1,2,,3,,]);")
#    test("print([]);")
#    test("print();")
#    test("void a, delete b, typeof c;")
#    test("1 in [1,2], a instanceof 2;")
#    test("a[b[c[d]]];")
#    test("a(b(c(d)));")
#    test("a[b(c[d])];")
#    test("a.if.else.d;")
#    test("new a.q[2](2,3,4);")
#    test("new a.q[2]+b(2,3,4);")
#    test("/ / / / /;")
