#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

import re
import sys
import types

from edb.common import parsing

from . import keywords
from . import precedence
from . import lexer


clean_string = re.compile(r"'(?:\s|\n)+'")
string_quote = re.compile(lexer.re_dquote)


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


class T_DOUBLEPLUS(Token, lextoken='++'):
    pass


class T_MINUS(Token, lextoken='-'):
    pass


class T_STAR(Token, lextoken='*'):
    pass


class T_SLASH(Token, lextoken='/'):
    pass


class T_DOUBLESLASH(Token, lextoken='//'):
    pass


class T_PERCENT(Token, lextoken='%'):
    pass


class T_CIRCUMFLEX(Token, lextoken='^'):
    pass


class T_AT(Token, lextoken='@'):
    pass


class T_ARGUMENT(Token):
    pass


class T_ASSIGN(Token):
    pass


class T_ADDASSIGN(Token):
    pass


class T_REMASSIGN(Token):
    pass


class T_ARROW(Token):
    pass


class T_LANGBRACKET(Token, lextoken='<'):
    pass


class T_RANGBRACKET(Token, lextoken='>'):
    pass


class T_EQUALS(Token, lextoken='='):
    pass


class T_AMPER(Token, lextoken='&'):
    pass


class T_PIPE(Token, lextoken='|'):
    pass


class T_NAMEDONLY(Token):
    pass


class T_SETANNOTATION(Token):
    pass


class T_SETTYPE(Token):
    pass


class T_ICONST(Token):
    pass


class T_NICONST(Token):
    pass


class T_FCONST(Token):
    pass


class T_NFCONST(Token):
    pass


class T_BCONST(Token):
    pass


class T_SCONST(Token):
    pass


class T_RSCONST(Token):
    pass


class T_IDENT(Token):
    pass


class T_OP(Token):
    pass


class T_EOF(Token):
    pass


def _gen_keyword_tokens():
    # Define keyword tokens

    mod = sys.modules[__name__]

    def clsexec(ns):
        ns['__module__'] = __name__
        return ns

    for token, _ in keywords.edgeql_keywords.values():
        clsname = 'T_{}'.format(token)
        clskwds = dict(metaclass=parsing.TokenMeta, token=token)
        cls = types.new_class(clsname, (Token,), clskwds, clsexec)
        setattr(mod, clsname, cls)


_gen_keyword_tokens()
