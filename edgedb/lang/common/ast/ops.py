##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class Operator:
    cache = {}
    funcmap = {}

    def __new__(cls, val='', *, funcname=None):
        result = Operator.cache.get((cls, val))

        if not result:
            result = super().__new__(cls)
            Operator.cache[cls, val] = result

            if funcname:
                Operator.funcmap[funcname] = result

        return result

    def __init__(self, val, *, funcname=None):
        self.val = val

    def __str__(self):
        return self.val

    def __repr__(self):
        return '<%s.%s "%s">' % (self.__class__.__module__, self.__class__.__name__, self.val)

    @classmethod
    def funcname_to_op(cls, funcname):
        return cls.funcmap.get(funcname)


class ComparisonOperator(Operator):
    pass

EQ = ComparisonOperator('=', funcname='__eq__')
NE = ComparisonOperator('!=', funcname='__ne__')
GT = ComparisonOperator('>', funcname='__gt__')
GE = ComparisonOperator('>=', funcname='__ge__')
LT = ComparisonOperator('<', funcname='__lt__')
LE = ComparisonOperator('<=', funcname='__le__')


class ArithmeticOperator(Operator):
    pass


class BinaryArithmeticOperator(ArithmeticOperator):
    pass


ADD = BinaryArithmeticOperator('+', funcname='__add__')
SUB = BinaryArithmeticOperator('-', funcname='__sub__')
MUL = BinaryArithmeticOperator('*', funcname='__mul__')
DIV = BinaryArithmeticOperator('/', funcname='__truediv__')
POW = BinaryArithmeticOperator('^', funcname='__pow__')
MOD = BinaryArithmeticOperator('%', funcname='__mod__')


class UnaryArithmeticOperator(ArithmeticOperator):
    pass


UPLUS = UnaryArithmeticOperator('+', funcname='__pos__')
UMINUS = UnaryArithmeticOperator('-', funcname='__neg__')


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
