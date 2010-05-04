##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import ast


class Base(ast.AST):
    pass


class BinOp(Base):
    __fields = ['op', 'left', 'right']


class UnaryOp(Base):
    __fields = ['op', 'operand']


class FunctionCall(Base):
    __fields = ['name', ('args', list)]


class Constant(Base):
    __fields = ['value']
