##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import ast


class Base(ast.LanguageAST): pass
class Context(ast.LanguageAST): __fields = ['context']

class Module(Base, ast.ASTBlockNode): pass


class Function(Base, ast.ASTBlockNode):
    __fields = ['name', ('args', list), 'returns_annotation']


class FunctionArgument(Base): __fields = ['id', 'annotation', 'kwonly', 'default']


NAME_CONTEXT_LOAD = 'L'
NAME_CONTEXT_STORE = 'S'
NAME_CONTEXT_DELETE = 'D'
class Name(Base, Context): __fields = ['id']


class ReservedConstant(Base): pass
class NoneConst(ReservedConstant): pass
class TrueConst(ReservedConstant): pass
class FalseConst(ReservedConstant): pass
class EllipsisConst(ReservedConstant): pass


class Literal(Base): __fields = ['value']
class Number(Literal): pass
class String(Literal): pass
class Bytes(Literal): pass


class Return(Base): __fields = ['value']


class Assert(Base): __fields = ['test', 'message']


class Call(Base): __fields = ['target', ('args', list)]


class Assign(Base): __fields = ['target', 'value']


class BinaryOperation(Base): __fields = ['left', 'op', 'right']
class BooleanOperation(BinaryOperation): pass
class UnaryOperation(Base): __fields = ['op', 'operand']
class Compare(Base): __fields = ['left', 'op', 'right']


class BaseTypes(Base): pass
class List(BaseTypes, Context): __fields = [('elements', list)]
class Tuple(BaseTypes, Context): __fields = [('elements', list)]
class Dict(BaseTypes): __fields = [('keys', list), ('values', list)]
class Set(BaseTypes): __fields = [('elements', list)]


class GetItem(Base, Context): __fields = ['value', 'slice']
class Slice(Base): __fields = ['lower', 'upper', 'step']
class Index(Base): __fields = ['value']


class And(Base): pass
class Or(Base): pass


class Add(Base): pass
class Sub(Base): pass
class Mult(Base): pass
class Div(Base): pass
class Mod(Base): pass
class Pow(Base): pass
class LShift(Base): pass
class RShift(Base): pass
class BitOr(Base): pass
class BitXor(Base): pass
class BitAnd(Base): pass
class FloorDiv(Base): pass


class Invert(Base): pass
class Not(Base): pass
class UAdd(Base): pass
class USub(Base): pass


class Eq(Base): pass
class NotEq(Base): pass
class Lt(Base): pass
class LtE(Base): pass
class Gt(Base): pass
class GtE(Base): pass
class Is(Base): pass
class IsNot(Base): pass
class In(Base): pass
class NotIn(Base): pass
