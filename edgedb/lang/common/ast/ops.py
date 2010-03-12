##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class Operator:
    cache = {}

    def __new__(cls, val=''):
        result = Operator.cache.get(val)

        if not result:
            result = super(Operator, cls).__new__(cls)
            Operator.cache[val] = result

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

ADD = ArithmeticOperator('+')
SUB = ArithmeticOperator('-')
MUL = ArithmeticOperator('*')
DIV = ArithmeticOperator('/')
POW = ArithmeticOperator('^')
MOD = ArithmeticOperator('%')

class BooleanOperator(Operator):
    pass

OR = BooleanOperator('or')
AND = BooleanOperator('and')
NOT = BooleanOperator('not')

class EquivalenceOperator(Operator):
    pass

IS = EquivalenceOperator('is')
IS_NOT = EquivalenceOperator('is not')
