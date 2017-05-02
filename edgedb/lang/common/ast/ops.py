##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing


class Operator(str):
    cache: typing.Any = {}
    funcmap: typing.Any = {}

    def __new__(
            cls, val='', *, funcname=None, rfuncname=None, commutative=None):
        result = Operator.cache.get((cls, val))

        if not result:
            result = super().__new__(cls, val)
            Operator.cache[cls, val] = result

            if funcname:
                Operator.funcmap[funcname] = result

            if rfuncname:
                Operator.funcmap[rfuncname] = (result, 'reversed')

        return result

    def __init__(
            self, val, *, funcname=None, rfuncname=None, commutative=None):
        self.val = val
        self.commutative = commutative

    def __repr__(self):
        return '<%s.%s "%s">' % (
            self.__class__.__module__, self.__class__.__name__, self.val)

    def __hash__(self):
        return object.__hash__(self)

    @classmethod
    def funcname_to_op(cls, funcname):
        return cls.funcmap.get(funcname)


class ComparisonOperator(Operator):
    pass


EQ = ComparisonOperator('=', funcname='__eq__', commutative=True)
NE = ComparisonOperator('!=', funcname='__ne__', commutative=True)
GT = ComparisonOperator('>', funcname='__gt__')
GE = ComparisonOperator('>=', funcname='__ge__')
LT = ComparisonOperator('<', funcname='__lt__')
LE = ComparisonOperator('<=', funcname='__le__')


class ArithmeticOperator(Operator):
    pass


class BinaryArithmeticOperator(ArithmeticOperator):
    pass


ADD = BinaryArithmeticOperator(
    '+', funcname='__add__', rfuncname='__radd__', commutative=True)
SUB = BinaryArithmeticOperator(
    '-', funcname='__sub__', rfuncname='__rsub__')
MUL = BinaryArithmeticOperator(
    '*', funcname='__mul__', rfuncname='__rmul__', commutative=True)
DIV = BinaryArithmeticOperator(
    '/', funcname='__truediv__', rfuncname='__rtruediv__')
POW = BinaryArithmeticOperator('^', funcname='__pow__', rfuncname='__rpow__')
MOD = BinaryArithmeticOperator('%', funcname='__mod__', rfuncname='__rmod__')


class UnaryArithmeticOperator(ArithmeticOperator):
    pass


UPLUS = UnaryArithmeticOperator('+', funcname='__pos__')
UMINUS = UnaryArithmeticOperator('-', funcname='__neg__')


class BooleanOperator(Operator):
    pass


OR = BooleanOperator('or')
AND = BooleanOperator('and')
NOT = BooleanOperator('not')


class TypeCheckOperator(Operator):
    pass


IS = TypeCheckOperator('is')
IS_NOT = TypeCheckOperator('is not')


class MembershipOperator(Operator):
    pass


IN = MembershipOperator('in')
NOT_IN = MembershipOperator('not in')
