##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import ast


class Base(ast.AST):
    pass

class StatementNode(Base):
    __fields = ['statement']

class BlockNode(Base):
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

class RegExpNode(Base):
    __fields = ['regexp']

class IDNode(Base):
    __fields = ['name']

class ThisNode(Base):
    pass

class NullNode(Base):
    pass

class ExpressionListNode(Base):
    __fields = [('expressions', list)]

class PrefixExpressionNode(Base):
    __fields = ['expression', 'op']

class PostfixExpressionNode(Base):
    __fields = ['expression', 'op']

class BinExpressionNode(Base):
    __fields = ['left', 'op', 'right']

class AssignmentExpressionNode(BinExpressionNode):
    pass

class DotExpressionNode(Base):
    __fields = ['left', 'right']

class ConditionalExpressionNode(Base):
    __fields = ['condition', 'true', 'false']

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

class InstanceOfNode(Base):
    __fields = ['expression', 'type']

class InNode(Base):
    __fields = ['expression', 'container']

# object property definitions

class SimplePropertyNode(Base):
    __fields = ['name', 'value']

class GetPropertyNode(Base):
    __fields = ['name', 'functionbody']

class SetPropertyNode(Base):
    __fields = ['name', 'param', 'functionbody']

# function
class FunctionNode(Base):
    __fields = ['name', ('param', list), 'body']

#class FunctionBodyNode(Base):
#    __fields = ['body']

# Statements

class SourceElementsNode(Base):
    __fields = [('code', list)]

class StatementBlockNode(Base):
    __fields = [('statements', list)]

class IfNode(Base):
    __fields = ['ifclause', 'thenclause', 'elseclause']

class DoNode(Base):
    __fields = ['statement', 'expression']

class WhileNode(Base):
    __fields = ['expression', 'statement']

class ForNode(Base):
    __fields = ['part1', 'part2', 'part3', 'statement']

class ForInNode(Base):
    __fields = ['init', 'array', 'statement']

class WithNode(Base):
    __fields = ['expression', 'statement']

class ContinueNode(Base):
    __fields = ['id']

class BreakNode(Base):
    __fields = ['id']

class ReturnNode(Base):
    __fields = ['expression']

class LabelNode(Base):
    __fields = ['id', 'statement']

class SwitchNode(Base):
    __fields = ['expression', 'cases']

class CaseNode(Base):
    __fields = ['case', 'statements']

class DefaultNode(Base):
    __fields = ['statements']

class ThrowNode(Base):
    __fields = ['expression']

class TryNode(Base):
    __fields = ['tryblock', 'catchid', 'catchblock', 'finallyblock']

class DebuggerNode(Base):
    pass
