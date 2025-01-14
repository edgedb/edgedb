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
import typing

from edb.common import parsing

from . import keywords


clean_string = re.compile(r"'(?:\s|\n)+'")
string_quote = re.compile(r'\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$')


class Token(parsing.Token, is_internal=True):
    pass


class GrammarToken(Token, is_internal=True):
    """
    Instead of having different grammars, we prefix each query with a special
    grammar token which directs the parser to appropriate grammar.

    This greatly reduces the combined size of grammar specifications, since the
    overlap between grammars is substantial.
    """


class T_STARTBLOCK(GrammarToken):
    pass


class T_STARTEXTENSION(GrammarToken):
    pass


class T_STARTFRAGMENT(GrammarToken):
    pass


class T_STARTMIGRATION(GrammarToken):
    pass


class T_STARTSDLDOCUMENT(GrammarToken):
    pass


class T_STRINTERPSTART(GrammarToken):
    pass


class T_STRINTERPCONT(GrammarToken):
    pass


class T_STRINTERPEND(GrammarToken):
    pass


class T_DOT(Token, lextoken='.'):
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


class T_DOUBLESTAR(Token, lextoken='**'):
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


class T_PARAMETER(Token):
    pass


class T_PARAMETERANDTYPE(Token):
    # A special token produced by normalization
    pass


class T_ASSIGN(Token, lextoken=':='):
    pass


class T_ADDASSIGN(Token, lextoken='+='):
    pass


class T_REMASSIGN(Token, lextoken='-='):
    pass


class T_ARROW(Token, lextoken='->'):
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


class T_NAMEDONLY(Token, lextoken='named only'):
    pass


class T_SETTYPE(Token, lextoken='set type'):
    pass


class T_EXTENSIONPACKAGE(Token, lextoken='extension package'):
    pass


class T_ORDERBY(Token, lextoken='order by'):
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


class T_DISTINCTFROM(Token, lextoken="?!="):
    pass


class T_GREATEREQ(Token, lextoken=">="):
    pass


class T_LESSEQ(Token, lextoken="<="):
    pass


class T_NOTDISTINCTFROM(Token, lextoken="?="):
    pass


class T_NOTEQ(Token, lextoken="!="):
    pass


class T_IDENT(Token):
    pass


class T_EOI(Token):
    pass


# explicitly define tokens which are referenced elsewhere
T_THEN: typing.Optional[Token] = None


def _gen_keyword_tokens():
    # Define keyword tokens

    mod = sys.modules[__name__]

    def clsexec(ns):
        ns['__module__'] = __name__
        return ns

    for token, _ in keywords.edgeql_keywords.values():
        clsname = 'T_{}'.format(token)
        clskwds = dict(token=token)
        cls = types.new_class(clsname, (Token,), clskwds, clsexec)
        setattr(mod, clsname, cls)


_gen_keyword_tokens()
