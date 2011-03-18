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

class StringLiteralNode(LiteralNode): pass

class NumericLiteralNode(LiteralNode): pass

class BooleanLiteralNode(LiteralNode): pass

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

class ParenthesisNode(Base):
    __fields = ['expression']

class ExpressionListNode(Base):
    __fields = [('expressions', list)]

class PrefixExpressionNode(Base):
    __fields = ['expression', 'op']

class PostfixExpressionNode(Base):
    __fields = ['expression', 'op']

class BinExpressionNode(Base):
    __fields = ['left', 'op', 'right']

class CallNode(Base):
    __fields = ['call', ('arguments', list)]

class NewNode(Base):
    __fields = ['expression', ('arguments', list)]

class SBracketExpressionNode(Base):
    __fields = ['list', 'element']

class DeleteNode(Base):
    __fields = ['expression']

class VoidNode(Base):
    __fields = ['expression']

class TypeOfNode(Base):
    __fields = ['expression']

# object property definitions

class SimplePropertyNode(Base):
    __fields = ['name', 'value']

class GetPropertyNode(Base):
    __fields = ['name', 'function']

class SetPropertyNode(Base):
    __fields = ['name', 'param', 'function']

# function
class FunctionBodyNode(Base):
    __fields = ['body']
