##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import ast


class Base(ast.AST):
#    __fields = [('inparen', bool, False)]
    def __init__(self, *args, **kwargs):
        self.inparen = False

    def countIN(self):
        count = 0
        if not self.inparen:
            print("!!!!!!!! not inparen", type(self), self.__fields)
            # any IN enclosed in parenthesis doesn't count
            for f in self.__fields:
                # just make sure you get the field name
                if type(f) != str:
                    f = f[0]

                print("!!!!!!!!!!!! field:", f)

                attr = getattr(self, f)
                if isinstance(attr, Base):
                    count += attr.countIN()
                elif isinstance(attr, list):
                    count += sum([x.count() for x in attr if isinstance(x, Base)])
        return count

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

    def countIN(self):
        count = super().countIN()
        print("!!!!!!!!!!!!!!!!! counted", count)
        return count

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
    def countIN(self):
        if not self.inparen:
            return 1 + self.expression.countIN() + self.container.countIN()
        else:
            return 0


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
