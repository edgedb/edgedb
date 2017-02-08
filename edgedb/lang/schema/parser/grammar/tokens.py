##
# Copyright (c) 2016-present MagicStack Inc.
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


class T_NEWLINE(Token):
    pass


class T_DOT(Token):
    pass


class T_LBRACKET(Token):
    pass


class T_RBRACKET(Token):
    pass


class T_LPAREN(Token):
    pass


class T_RPAREN(Token):
    pass


class T_LBRACE(Token):
    pass


class T_RBRACE(Token):
    pass


class T_LANGBRACKET(Token):
    pass


class T_RANGBRACKET(Token):
    pass


class T_DOUBLECOLON(Token):
    pass


class T_COLON(Token):
    pass


class T_COMMA(Token):
    pass


class T_TURNSTILE(Token):
    pass


class T_COLONGT(Token):
    pass


class T_EQUALS(Token):
    pass


class T_ARROW(Token):
    pass


class T_STAR(Token):
    pass


class T_ICONST(Token):
    @property
    def normalized_value(self):
        return int(self.val)


class T_FCONST(Token):
    @property
    def normalized_value(self):
        return float(self.val)


class T_STRING(Token):
    @property
    def normalized_value(self):
        return ast.literal_eval(self.val)


class T_RAWSTRING(Token):
    pass


class T_RAWLEADWS(Token):
    pass


class T_MAPPING(Token):
    pass


class T_IDENT(Token):
    pass


class T_INDENT(Token):
    pass


class T_DEDENT(Token):
    pass


def _gen_keyword_tokens():
    # Define keyword tokens

    mod = sys.modules[__name__]

    def clsexec(ns):
        ns['__module__'] = __name__
        return ns

    for val, (token, typ) in keywords.edge_schema_keywords.items():
        clsname = 'T_{}'.format(token)
        clskwds = dict(metaclass=parsing.TokenMeta, token=token)
        cls = types.new_class(clsname, (Token,), clskwds, clsexec)
        setattr(mod, clsname, cls)


_gen_keyword_tokens()
