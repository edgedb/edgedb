##
# Copyright (c) 2008-2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import parsing


class PrecedenceMeta(parsing.PrecedenceMeta):
    pass


class Precedence(parsing.Precedence, assoc='fail', metaclass=PrecedenceMeta):
    pass


class P_UNION_EXCEPT(Precedence, assoc='left', tokens=('UNION', 'EXCEPT')):
    pass

class P_INTERSECT(Precedence, assoc='left', tokens=('INTERSECT',)):
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

class P_POSTFIXOP(Precedence, assoc='left'):
    pass

class P_IDENT(Precedence, assoc='nonassoc', tokens=('IDENT', 'PARTITION')):
    pass

class P_OP(Precedence, assoc='left', tokens=('OPERATOR', 'OP')):
    pass

class P_IS(Precedence, assoc='nonassoc', tokens=('IS', 'NONE')):
    pass

class P_ADD_OP(Precedence, assoc='left', tokens=('PLUS', 'MINUS')):
    pass

class P_MUL_OP(Precedence, assoc='left', tokens=('STAR', 'SLASH', 'PERCENT')):
    pass

class P_POW_OP(Precedence, assoc='left', tokens=('STARSTAR',)):
    pass


class P_UMINUS(Precedence, assoc='right'):
    pass

class P_PATHSTART(Precedence, assoc='nonassoc'):
    pass

class P_BRACE(Precedence, assoc='left', tokens=('LBRACE', 'RBRACE')):
    pass

class P_BRACKET(Precedence, assoc='left', tokens=('LBRACKET', 'RBRACKET')):
    pass

class P_PAREN(Precedence, assoc='left', tokens=('LPAREN', 'RPAREN')):
    pass

class P_DOT(Precedence, assoc='left', tokens=('DOT',)):
    pass

class P_AT(Precedence, assoc='left', tokens=('AT',)):
    pass
