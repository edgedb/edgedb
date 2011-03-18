##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from semantix.utils import ast

class Base(ast.AST):
    pass

class ProgramNode(Base):
    __fields = [('code', list)]

class StatementNode(Base):
    __fields = ['statement']

class VarDeclarationNode(Base):
    __fields = [('vars', list)]

class VarInitNode(Base):
    __fields = ['name', 'value']

# expressions

class LiteralNode(Base):
    __fields = ['value']

class ArrayLiteralNode(Base):
    __fields = [('array', list)]

class ObjectLiteralNode(Base):
    __fields = [('properties', list)]

class IDNode(Base):
    __fields = ['name']

class ThisNode(Base):
    pass

class NullNode(Base):
    pass

class ExpressionListNode(Base):
    __fields = [('expressions', list)]

class BinExpressionNode(Base):
    __fields = ['left', 'op', 'right']

# object property definitions

class SimplePropertyNode(Base):
    __fields = ['name', 'value']

class GetPropertyNode(Base):
    __fields = ['name', 'function']

class SetPropertyNode(Base):
    __fields = ['name', 'param', 'function']

#class ArgListNode(Base):
#    __fields = ['name', ('args', list)]

