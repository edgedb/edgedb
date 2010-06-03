##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class Operator:
    cache = {}

    def __new__(cls, val=''):
        result = Operator.cache.get((cls, val))

        if not result:
            result = super().__new__(cls)
            Operator.cache[cls, val] = result

        return result

    def __init__(self, val):
        self.val = val

    def __str__(self):
        return self.val

    def __repr__(self):
        return '<%s.%s "%s">' % (self.__class__.__module__, self.__class__.__name__, self.val)

class ComparisonOperator(Operator):
    pass

EQ = ComparisonOperator('=')
NE = ComparisonOperator('!=')
GT = ComparisonOperator('>')
GE = ComparisonOperator('>=')
LT = ComparisonOperator('<')
LE = ComparisonOperator('<=')


class ArithmeticOperator(Operator):
    pass


class BinaryArithmeticOperator(ArithmeticOperator):
    pass


ADD = BinaryArithmeticOperator('+')
SUB = BinaryArithmeticOperator('-')
MUL = BinaryArithmeticOperator('*')
DIV = BinaryArithmeticOperator('/')
POW = BinaryArithmeticOperator('^')
MOD = BinaryArithmeticOperator('%')


class UnaryArithmeticOperator(ArithmeticOperator):
    pass


UPLUS = UnaryArithmeticOperator('+')
UMINUS = UnaryArithmeticOperator('-')


class BooleanOperator(Operator):
    pass

OR = BooleanOperator('or')
AND = BooleanOperator('and')
NOT = BooleanOperator('not')

class EquivalenceOperator(Operator):
    pass

IS = EquivalenceOperator('is')
IS_NOT = EquivalenceOperator('is not')


class MembershipOperator(Operator):
    pass

IN = MembershipOperator('in')
NOT_IN = MembershipOperator('not in')
