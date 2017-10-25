##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys
import types

from edgedb.lang.common import parsing

from . import keywords
from . import precedence


class TokenMeta(parsing.TokenMeta):
    pass


class Token(parsing.Token, metaclass=TokenMeta,
            precedence_class=precedence.PrecedenceMeta):
    pass


class T_DOT(Token, lextoken='.'):
    pass


class T_DOTFW(Token, lextoken='.>'):
    pass


class T_DOTBW(Token, lextoken='.<'):
    pass


class T_LBRACKET(Token, lextoken='['):
    pass


class T_RBRACKET(Token, lextoken=']'):
    pass


class T_LPAREN(Token, lextoken='('):
    pass


class T_RPAREN(Token, lextoken=')'):
    pass


class T_LBRACE(Token, lextoken='{'):
    pass


class T_RBRACE(Token, lextoken='}'):
    pass


class T_DOUBLECOLON(Token, lextoken='::'):
    pass


class T_DOUBLEQMARK(Token, lextoken='??'):
    pass


class T_COLON(Token, lextoken=':'):
    pass


class T_SEMICOLON(Token, lextoken=';'):
    pass


class T_COMMA(Token, lextoken=','):
    pass


class T_PLUS(Token, lextoken='+'):
    pass


class T_MINUS(Token, lextoken='-'):
    pass


class T_STAR(Token, lextoken='*'):
    pass


class T_SLASH(Token, lextoken='/'):
    pass


class T_PERCENT(Token, lextoken='%'):
    pass


class T_CIRCUMFLEX(Token, lextoken='^'):
    pass


class T_AT(Token, lextoken='@'):
    pass


class T_DOLLAR(Token, lextoken='$'):
    pass


class T_TURNSTILE(Token):
    pass


class T_ARROW(Token):
    pass


class T_LANGBRACKET(Token, lextoken='<'):
    pass


class T_RANGBRACKET(Token, lextoken='>'):
    pass


class T_EQUALS(Token, lextoken='='):
    pass


class T_ICONST(Token):
    pass


class T_FCONST(Token):
    pass


class T_SCONST(Token):
    pass


class T_IDENT(Token):
    pass


class T_OP(Token):
    pass


class T_LINKPROPERTY(Token):
    pass


def _gen_keyword_tokens():
    # Define keyword tokens

    mod = sys.modules[__name__]

    def clsexec(ns):
        ns['__module__'] = __name__
        return ns

    for val, (token, typ) in keywords.edgeql_keywords.items():
        clsname = 'T_{}'.format(token)
        clskwds = dict(metaclass=parsing.TokenMeta, token=token)
        cls = types.new_class(clsname, (Token,), clskwds, clsexec)
        setattr(mod, clsname, cls)


_gen_keyword_tokens()
