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


from edgedb.lang.common import parsing


class PrecedenceMeta(parsing.PrecedenceMeta):
    pass


class Precedence(parsing.Precedence, assoc='fail', metaclass=PrecedenceMeta):
    pass


class P_UNION(Precedence, assoc='left', tokens=('UNION', 'DISTINCTUNION')):
    pass


class P_IFELSE(Precedence, assoc='right', tokens=('IF', 'ELSE')):
    pass


class P_OR(Precedence, assoc='left', tokens=('OR',)):
    pass


class P_AND(Precedence, assoc='left', tokens=('AND',)):
    pass


class P_NOT(Precedence, assoc='right', tokens=('NOT',)):
    pass


class P_EQUALS(Precedence, assoc='right', tokens=('EQUALS',)):
    pass


class P_ANGBRACKET(Precedence, assoc='nonassoc',
                   tokens=('LANGBRACKET', 'RANGBRACKET')):
    pass


class P_LIKE_ILIKE(Precedence, assoc='nonassoc', tokens=('LIKE', 'ILIKE')):
    pass


class P_IN(Precedence, assoc='nonassoc', tokens=('IN',)):
    pass


class P_IDENT(Precedence, assoc='nonassoc', tokens=('IDENT', 'PARTITION')):
    pass


class P_OP(Precedence, assoc='left', tokens=('OP',)):
    pass


class P_IS(Precedence, assoc='nonassoc', tokens=('IS',)):
    pass


class P_ADD_OP(Precedence, assoc='left', tokens=('PLUS', 'MINUS')):
    pass


class P_MUL_OP(Precedence, assoc='left', tokens=('STAR', 'SLASH', 'PERCENT')):
    pass


class P_DOUBLEQMARK_OP(Precedence, assoc='right', tokens=('DOUBLEQMARK',)):
    pass


class P_BRACE(Precedence, assoc='left', tokens=('LBRACE', 'RBRACE')):
    pass


class P_UMINUS(Precedence, assoc='right'):
    pass


class P_POW_OP(Precedence, assoc='right', tokens=('CIRCUMFLEX',)):
    pass


class P_TYPECAST(Precedence, assoc='right'):
    pass


class P_BRACKET(Precedence, assoc='left', tokens=('LBRACKET', 'RBRACKET')):
    pass


class P_PAREN(Precedence, assoc='left', tokens=('LPAREN', 'RPAREN')):
    pass


class P_DOUBLECOLON(Precedence, assoc='left', tokens=('DOUBLECOLON',)):
    pass


class P_DOT(Precedence, assoc='left', tokens=('DOT', 'DOTFW', 'DOTBW')):
    pass


class P_AT(Precedence, assoc='left', tokens=('AT',)):
    pass
