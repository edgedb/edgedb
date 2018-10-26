#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


import sys
import types

from edb.lang.common import parsing

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
        return f'<Token 0x{id(self):x} {self.type!r} {self.val!r} {position}>'

    __str__ = __repr__


class T_WS(Token):
    pass


class T_NL(Token):
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
    pass


class T_INTEGER(Token):
    pass


class T_STRING(Token):
    pass


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
        clsname = f'T_{token}'
        clskwds = dict(metaclass=parsing.TokenMeta, token=token)
        cls = types.new_class(clsname, (Token,), clskwds, clsexec)
        setattr(mod, clsname, cls)


_gen_keyword_tokens()
