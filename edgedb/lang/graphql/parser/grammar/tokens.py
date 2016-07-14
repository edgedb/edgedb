##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import ast
import sys
import types

from edgedb.lang.common import parsing

from . import keywords
from . import precedence


class TokenMeta(parsing.TokenMeta):
    pass


class Token(parsing.Token, metaclass=TokenMeta,
            precedence_class=precedence.PrecedenceMeta):

    @property
    def type(self):
        return self.__class__.__name__[2:]

    def __repr__(self):
        position = self.context.start.line, self.context.start.column
        return '<Token 0x{:x} {!r} {!r} {}>'.format(
            id(self), self.type, self.val, position)

    __str__ = __repr__


class T_WS(Token):
    pass


class T_NL(Token):
    pass


class T_UBOM(Token):
    pass


class T_COMMA(Token):
    pass


class T_COMMENT(Token):
    pass


class T_LSBRACKET(Token):
    pass


class T_RSBRACKET(Token):
    pass


class T_LPAREN(Token):
    pass


class T_RPAREN(Token):
    pass


class T_LCBRACKET(Token):
    pass


class T_RCBRACKET(Token):
    pass


class T_COLON(Token):
    pass


class T_EQUAL(Token):
    pass


class T_AT(Token):
    pass


class T_ELLIPSIS(Token):
    pass


class T_DOLLAR(Token):
    pass


class T_BANG(Token):
    pass


class T_FLOAT(Token):
    @property
    def normalized_value(self):
        return float(self.val)


class T_INTEGER(Token):
    @property
    def normalized_value(self):
        return int(self.val)


class T_STRING(Token):
    @property
    def normalized_value(self):
        return ast.literal_eval(self.val).replace('\/', '/')


class T_IDENT(Token):
    pass


class T_VAR(Token):
    pass


def _gen_keyword_tokens():
    # Define keyword tokens

    mod = sys.modules[__name__]

    def clsexec(ns):
        ns['__module__'] = __name__
        return ns

    for val, (token, typ) in keywords.graphql_keywords.items():
        clsname = 'T_{}'.format(token)
        clskwds = dict(metaclass=parsing.TokenMeta, token=token)
        cls = types.new_class(clsname, (Token,), clskwds, clsexec)
        setattr(mod, clsname, cls)
_gen_keyword_tokens()
