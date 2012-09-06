##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.ast import *


class PyAST(LanguageAST): pass


#{
#    mod = Module(stmt* body)
#        | Interactive(stmt* body)
#        | Expression(expr body)

class PyModule(PyAST, ASTBlockNode): __fields = [('body', list)]
class PyInteractive(PyAST): __fields = [('body', list)]
class PyExpression(PyAST): __fields = [('body', list)]

#          | Import(alias* names)
#          | ImportFrom(identifier? module, alias* names, int? level)

class PyImport(PyAST): __fields = [('names', list)]
class PyImportFrom(PyAST): __fields = [('level', int, 0), ('names', list), ('module', str, None)]

#    stmt = FunctionDef(identifier name, arguments args,
#                           stmt* body, expr* decorator_list, expr? returns)
#          | ClassDef(identifier name,
#             expr* bases,
#             keyword* keywords,
#             expr? starargs,
#             expr? kwargs,
#             stmt* body,
#             expr *decorator_list)
#          | Return(expr? value)

class PyFunctionDef(PyAST): __fields = ['name', 'args', ('body', list),
                                        ('decorator_list', list), 'returns']

class PyClassDef(PyAST): __fields = ['name', ('keywords', list), ('decorator_list', list),
                                     ('bases', list), 'starargs', 'kwargs', ('body', list)]

class PyReturn(PyAST): __fields = ['value']

#          | Delete(expr* targets)
#          | Assign(expr* targets, expr value)
#          | AugAssign(expr target, operator op, expr value)

class PyDelete(PyAST): __fields = [('targets', list)]
class PyAssign(PyAST): __fields = [('targets', list), 'value']
class PyAugAssign(PyAST): __fields = ['target', 'op', 'value']

#          -- use 'orelse' because else is a keyword in target languages
#          | For(expr target, expr iter, stmt* body, stmt* orelse)
#          | While(expr test, stmt* body, stmt* orelse)
#          | If(expr test, stmt* body, stmt* orelse)
#          | With(expr context_expr, expr? optional_vars, stmt* body)

class PyFor(PyAST, ASTBlockNode): __fields = ['target', 'iter', ('body', list), ('orelse', list)]
class PyWhile(PyAST): __fields = ['test', ('body', list), ('orelse', list)]
class PyIf(PyAST, ASTBlockNode): __fields = ['test', ('body', list), ('orelse', list)]
class PyWith(PyAST): __fields = ['context_expr', 'optional_vars', ('body', list)]

#          | Raise(expr? exc, expr? cause)
#          | TryExcept(stmt* body, excepthandler* handlers, stmt* orelse)
#          | TryFinally(stmt* body, stmt* finalbody)
#          | Assert(expr test, expr? msg)

class PyRaise(PyAST): __fields = ['exc', 'cause']
class PyTryExcept(PyAST): __fields = [('body', list), ('handlers', list), ('orelse', list)]
class PyTryFinally(PyAST): __fields = [('body', list), ('finalbody', list)]
class PyAssert(PyAST): __fields = ['test', 'msg']

#          | Import(alias* names)
#          | ImportFrom(identifier module, alias* names, int? level)

class PyImport(PyAST): __fields = [('names', list)]
class PyImportFrom(PyAST): __fields = ['module', ('names', list), 'level']

#          | Global(identifier* names)
#          | Nonlocal(identifier* names)
#          | Expr(expr value)

class PyGlobal(PyAST): __fields = [('names', list)]
class PyNonlocal(PyAST): __fields = [('names', list)]
class PyExpr(PyAST): __fields = ['value']

#          | Pass | Break | Continue

class PyPass(PyAST): pass
class PyBreak(PyAST): pass
class PyContinue(PyAST): pass


#    expr = BoolOp(boolop op, expr* values)
#         | BinOp(expr left, operator op, expr right)
#         | UnaryOp(unaryop op, expr operand)

class PyBoolOp(PyAST): __fields = ['op', ('values', list)]
class PyBinOp(PyAST): __fields = ['left', 'op', 'right']
class PyUnaryOp(PyAST): __fields = ['op', 'operand']

#         | Lambda(arguments args, expr body)
#         | IfExp(expr test, expr body, expr orelse)
#         | Dict(expr* keys, expr* values)
#         | Set(expr* elts)

class PyLambda(PyAST): __fields = ['args', 'body']
class PyIfExp(PyAST): __fields = ['test', 'body', 'orelse']
class PyDict(PyAST): __fields = [('keys', list), ('values', list)]
class PySet(PyAST): __fields = ['elts']

#         | ListComp(expr elt, comprehension* generators)
#         | SetComp(expr elt, comprehension* generators)
#         | DictComp(expr key, expr value, comprehension* generators)
#         | GeneratorExp(expr elt, comprehension* generators)

class PyListComp(PyAST): __fields = ['elt', ('generators', list)]
class PySetComp(PyAST): __fields = ['elt', ('generators', list)]
class PyDictComp(PyAST): __fields = ['key', 'value', ('generators', list)]
class PyGeneratorExp(PyAST): __fields = ['elt', ('generators', list)]

#         -- the grammar constrains where yield expressions can occur
#         | Yield(expr? value)
#         -- need sequences for compare to distinguish between
#         -- x < 4 < 3 and (x < 4) < 3
#         | Compare(expr left, cmpop* ops, expr* comparators)
#         | Call(expr func, expr* args, keyword* keywords,
#             expr? starargs, expr? kwargs)

class PyYield(PyAST): __fields = ['value']
class PyCompare(PyAST): __fields = ['left', ('ops', list), ('comparators', list)]
class PyCall(PyAST): __fields = ['func', ('args', list), ('keywords', list), 'starargs', 'kwargs']

#         | Num(object n) -- a number as a PyObject.
#         | Str(string s) -- need to specify raw, unicode, etc?
#         | Bytes(string s)
#         | Ellipsis

class PyNum(PyAST): __fields = ['n']
class PyStr(PyAST): __fields = ['s']
class PyBytes(PyAST): __fields = ['s']
class PyEllipsis(PyAST): pass

#         -- other literals? bools?
#
#         -- the following expression can appear in assignment context
#         | Attribute(expr value, identifier attr, expr_context ctx)
#         | Subscript(expr value, slice slice, expr_context ctx)
#         | Starred(expr value, expr_context ctx)

class PyAttribute(PyAST): __fields = ['value', 'attr', 'ctx']
class PySubscript(PyAST): __fields = ['value', 'slice', 'ctx']
class PyStarred(PyAST): __fields = ['value', 'ctx']

#         | Name(identifier id, expr_context ctx)
#         | List(expr* elts, expr_context ctx)
#         | Tuple(expr* elts, expr_context ctx)

class PyName(PyAST): __fields = ['id', 'ctx']
class PyList(PyAST): __fields = [('elts', list), 'ctx']
class PyTuple(PyAST): __fields = [('elts', list), 'ctx']

#          -- col_offset is the byte offset in the utf8 string the parser uses
#          attributes (int lineno, int col_offset)
#
#    expr_context = Load | Store | Del | AugLoad | AugStore | Param
#
#    slice = Slice(expr? lower, expr? upper, expr? step)
#          | ExtSlice(slice* dims)
#          | Index(expr value)

class PySlice(PyAST): __fields = ['lower', 'upper', 'step']
class PyExtSlice(PyAST): __fields = [('dims', list)]
class PyIndex(PyAST): __fields = ['value']

#    boolop = And | Or
#
#    operator = Add | Sub | Mult | Div | Mod | Pow | LShift
#                 | RShift | BitOr | BitXor | BitAnd | FloorDiv

class PyAnd(PyAST): pass
class PyOr(PyAST): pass

class PyAdd(PyAST): pass
class PySub(PyAST): pass
class PyMult(PyAST): pass
class PyDiv(PyAST): pass
class PyMod(PyAST): pass
class PyPow(PyAST): pass
class PyLShift(PyAST): pass
class PyRShift(PyAST): pass
class PyBitOr(PyAST): pass
class PyBitXor(PyAST): pass
class PyBitAnd(PyAST): pass
class PyFloorDiv(PyAST): pass

#    unaryop = Invert | Not | UAdd | USub

class PyInvert(PyAST): pass
class PyNot(PyAST): pass
class PyUAdd(PyAST): pass
class PyUSub(PyAST): pass


#    cmpop = Eq | NotEq | Lt | LtE | Gt | GtE | Is | IsNot | In | NotIn

class PyEq(PyAST): pass
class PyNotEq(PyAST): pass
class PyLt(PyAST): pass
class PyLtE(PyAST): pass
class PyGt(PyAST): pass
class PyGtE(PyAST): pass
class PyIs(PyAST): pass
class PyIsNot(PyAST): pass
class PyIn(PyAST): pass
class PyNotIn(PyAST): pass

#    comprehension = (expr target, expr iter, expr* ifs)

class Pycomprehension(PyAST): __fields = ['target', 'iter', ('ifs', list)]

#    -- not sure what to call the first argument for raise and except
#    excepthandler = ExceptHandler(expr? type, identifier? name, stmt* body)
#                    attributes (int lineno, int col_offset)

class Pyexcepthandler(PyAST): __fields = ['type', 'name', ('body', list)]

#    arguments = (arg* args, identifier? vararg, expr? varargannotation,
#                     arg* kwonlyargs, identifier? kwarg,
#                     expr? kwargannotation, expr* defaults,
#                     expr* kw_defaults)
#    arg = (identifier arg, expr? annotation)

class Pyarguments(PyAST): __fields = [('args', list), 'vararg', 'varargannotation',
                                      ('kwonlyargs', list), 'kwarg', 'kwargannotation',
                                      ('defaults', list), ('kw_defaults', list)]

class Pyarg(PyAST): __fields = ['arg', 'annotation']

#        -- keyword arguments supplied to call
#        keyword = (identifier arg, expr value)
#
#        -- import name with optional 'as' alias.
#        alias = (identifier name, identifier? asname)

class Pykeyword(PyAST): __fields = ['arg', 'value']
class Pyalias(PyAST): __fields = ['name', 'asname']


class PyLoad(PyAST): pass
class PyStore(PyAST): pass
class PyDel(PyAST): pass

#}
