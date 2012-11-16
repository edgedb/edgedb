##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import ast


class Base(ast.AST): __fields = [('position', tuple, None)]


class Expression(Base): pass


class StatementNode(Base):
    __fields = ['statement']


class VarDeclarationNode(Base):
    __fields = [('vars', list)]


class VarInitNode(Base):
    __fields = ['name', 'value']


# expressions
#
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


class ThisNode(Base): pass


class NullNode(Base): pass


class ExpressionListNode(Base):
    __fields = [('expressions', list)]

class PrefixExpressionNode(Expression):
    __fields = ['expression', 'op']


class PostfixExpressionNode(Expression):
    __fields = ['expression', 'op']


class BinExpressionNode(Expression):
    __fields = ['left', 'op', 'right']


class AssignmentExpressionNode(BinExpressionNode): pass


class DotExpressionNode(Expression):
    __fields = ['left', 'right']


class ConditionalExpressionNode(Expression):
    __fields = ['condition', 'true', 'false']


class CallNode(Expression):
    __fields = ['call', ('arguments', list)]


class NewNode(Expression):
    __fields = ['expression', ('arguments', list)]


class SBracketExpressionNode(Expression):
    __fields = ['list', 'element']


class DeleteNode(Expression):
    __fields = ['expression']


class VoidNode(Expression):
    __fields = ['expression']


class TypeOfNode(Expression):
    __fields = ['expression']


class InstanceOfNode(Expression):
    __fields = ['expression', 'type']


class InNode(Expression):
    __fields = ['expression', 'container']


# object property definitions
#
class SimplePropertyNode(Base):
    __fields = ['name', 'value']


class GetPropertyNode(Base):
    __fields = ['name', 'functionbody']


class SetPropertyNode(Base):
    __fields = ['name', 'param', 'functionbody']


# function
#
class FunctionNode(Base):
    __fields = ['name', ('param', list), 'body', ('isdeclaration', bool)]


# Statements
#
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
    __fields = ['init', 'container', 'statement']


class ForOfNode(Base):
    __fields = ['init', 'container', 'statement']


class WithNode(Base):
    __fields = ['expression', 'statement']


class ContinueNode(Base):
    __fields = ['id']


class BreakNode(Base):
    __fields = ['id']


class ReturnNode(Base):
    __fields = ['expression']


class YieldNode(Base):
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
    __fields = ['tryblock', 'catch', 'finallyblock']


class CatchNode(Base):
    __fields = ['catchid', 'catchblock']


class DebuggerNode(Base): pass

#
# Features
#

class LetDeclarationNode(Base):
    __fields = [('vars', list)]


class LetExpressionNode(Expression):
    __fields = [('vars', list), 'expression']


class LetStatementNode(Base):
    __fields = [('vars', list), 'statement']


class ForEachNode(Base):
    __fields = ['var', 'container', 'statement']


#class TryCatchIfNode(Base):
#    __fields = ['tryblock', ('catch', list), 'finallyblock']


class CatchIfNode(Base):
    __fields = ['catchid', 'condition', 'catchblock']


#class ArrayComprehensionNode(Expression):
#    __fields = ['expr', ('comprehensions', list)]
#
#!!! seems that syntactically, there's just a generator expression inside
#
class ArrayComprehensionNode(Expression):
    __fields = ['generator']


class ComprehensionNode(Base):
    __fields = ['var', 'container', 'condition']


class GeneratorExprNode(Expression):
    __fields = ['expr', ('forstring', str, 'for'), ('comprehensions', list)]


class AssignmentPropertyList(Base):
    __fields = [('properties', list)]


class AssignmentElementList(Base):
    __fields = [('elements', list)]
