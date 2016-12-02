##
# Copyright (c) 2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast

from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import types as s_types

from . import ast as irast


class PathIndex(dict):
    """Graph path mapping path identifiers to AST nodes."""

    def update(self, other):
        for k, v in other.items():
            if k in self:
                super().__getitem__(k).update(v)
            else:
                self[k] = v

    def __setitem__(self, key, value):
        if not isinstance(key, (LinearPath, str)):
            raise TypeError('Invalid key type for PathIndex: %s' % key)

        if not isinstance(value, set):
            value = {value}

        super().__setitem__(key, value)


def infer_arg_types(ir, schema):
    def flt(n):
        if isinstance(n, irast.BinOp):
            return (isinstance(n.left, irast.Constant) or
                    isinstance(n.right, irast.Constant))

    ops = ast.find_children(ir, flt)

    arg_types = {}

    for binop in ops:
        typ = None

        if isinstance(binop.right, irast.Constant):
            expr = binop.left
            arg = binop.right
            reversed = False
        else:
            expr = binop.right
            arg = binop.left
            reversed = True

        if arg.index is None:
            continue

        if isinstance(binop.op, irast.EdgeDBMatchOperator):
            typ = schema.get('std::str')

        elif isinstance(binop.op, (ast.ops.ComparisonOperator,
                                   ast.ops.ArithmeticOperator)):
            typ = infer_type(expr, schema)

        elif isinstance(binop.op, ast.ops.MembershipOperator) and not reversed:
            from edgedb.lang.schema import objects as s_obj

            elem_type = infer_type(expr, schema)
            typ = s_obj.Set(element_type=elem_type)

        elif isinstance(binop.op, ast.ops.BooleanOperator):
            typ = schema.get('std::bool')

        else:
            msg = 'cannot infer expr type: unsupported ' \
                  'operator: {!r}'.format(binop.op)
            raise ValueError(msg)

        if typ is None:
            msg = 'cannot infer expr type'
            raise ValueError(msg)

        try:
            existing = arg_types[arg.index]
        except KeyError:
            arg_types[arg.index] = typ
        else:
            if existing != typ:
                msg = 'cannot infer expr type: ambiguous resolution: ' + \
                      '{!r} and {!r}'
                raise ValueError(msg.format(existing, typ))

    return arg_types


def infer_type(ir, schema):
    if isinstance(ir, (irast.Set, irast.Shape)):
        result = ir.scls

    elif isinstance(ir, irast.FunctionCall):
        func_obj = schema.get(ir.name)
        result = func_obj.returntype

    elif isinstance(ir, irast.Constant):
        if ir.expr:
            result = infer_type(ir.expr, schema)
        else:
            result = ir.type

    elif isinstance(ir, irast.BinOp):
        if isinstance(ir.op, (ast.ops.ComparisonOperator,
                              ast.ops.TypeCheckOperator,
                              ast.ops.MembershipOperator)):
            result = schema.get('std::bool')
        else:
            left_type = infer_type(ir.left, schema)
            right_type = infer_type(ir.right, schema)

            result = s_types.TypeRules.get_result(
                ir.op, (left_type, right_type), schema)
            if result is None:
                result = s_types.TypeRules.get_result(
                    (ir.op, 'reversed'), (right_type, left_type), schema)

    elif isinstance(ir, irast.UnaryOp):
        if ir.op == ast.ops.NOT:
            result = schema.get('std::bool')
        else:
            operand_type = infer_type(ir.expr, schema)
            result = s_types.TypeRules.get_result(
                ir.op, (operand_type,), schema)

    elif isinstance(ir, irast.TypeCast):
        if ir.type.subtypes:
            coll = s_obj.Collection.get_class(ir.type.maintype)
            result = coll.from_subtypes(
                [schema.get(t) for t in ir.type.subtypes])
        else:
            result = schema.get(ir.type.maintype)

    elif isinstance(ir, irast.Stmt):
        result = infer_type(ir.result, schema)

    elif isinstance(ir, irast.SubstmtRef):
        result = infer_type(ir.stmt, schema)

    elif isinstance(ir, irast.ExistPred):
        result = schema.get('std::bool')

    elif isinstance(ir, irast.SliceIndirection):
        result = infer_type(ir.expr, schema)

    elif isinstance(ir, irast.IndexIndirection):
        arg = infer_type(ir.expr, schema)

        if arg is None:
            result = None
        else:
            str_t = schema.get('std::str')
            if arg.issubclass(str_t):
                result = arg
            else:
                result = None

    else:
        result = None

    if result is not None:
        allowed = (s_obj.Class, s_obj.MetaClass)
        if not (isinstance(result, allowed) or
                (isinstance(result, (tuple, list)) and
                 isinstance(result[1], allowed))):
            raise RuntimeError(
                f'infer_type({ir!r}) retured {result!r} instead of a Class')

    return result


def get_source_references(ir):
    result = set()

    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    for ir_set in ir_sets:
        result.add(ir_set.scls)

    return result


def get_terminal_references(ir):
    result = set()
    parents = set()

    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    for ir_set in ir_sets:
        result.add(ir_set)
        if ir_set.rptr:
            parents.add(ir_set.rptr.source)

    return result - parents


def get_variables(ir):
    result = set()

    flt = lambda n: isinstance(n, irast.Constant) and n.index is not None
    result.update(ast.find_children(ir, flt))

    return result


def is_const(ir):
    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    variables = get_variables(ir)
    return not ir_sets and not variables


class LinearPath(list):
    """Denotes a linear path in the graph.

    The path is considered linear if it
    does not have branches and is in the form
    <concept> <link> <concept> <link> ... <concept>
    """

    def __eq__(self, other):
        if not isinstance(other, LinearPath):
            return NotImplemented

        if len(other) != len(self):
            return False
        elif len(self) == 0:
            return True

        if self[0] != other[0]:
            return False

        for i in range(1, len(self) - 1, 2):
            if self[i] != other[i]:
                break
            if self[i + 1] != other[i + 1]:
                break
        else:
            return True
        return False

    def add(self, link, direction, target):
        if not link.generic():
            link = link.bases[0]
        self.append((link, direction))
        self.append(target)

    def rptr(self):
        if len(self) > 1:
            return self[-2][0]
        else:
            return None

    def iter_prefixes(self):
        yield self[:1]

        for i in range(1, len(self) - 1, 2):
            if self[i + 1]:
                yield self[:i + 2]
            else:
                break

    def __hash__(self):
        return hash(tuple(self))

    def __str__(self):
        if not self:
            return ''

        result = '({})'.format(self[0].name)

        for i in range(1, len(self) - 1, 2):
            link = self[i][0].name
            if self[i + 1]:
                lexpr = '({} TO {})'.format(link, self[i + 1].name)
            else:
                lexpr = '({})'.format(link)
            result += '.{}{}'.format(self[i][1], lexpr)
        return result

    __repr__ = __str__


def extend_path(self, schema, source_set, ptr):
    scls = source_set.scls

    if isinstance(ptr, str):
        ptrcls = scls.resolve_pointer(schema, ptr)
    else:
        ptrcls = ptr

    path_id = LinearPath(source_set.path_id)
    path_id.add(ptrcls, s_pointers.PointerDirection.Outbound, ptrcls.target)

    target_set = irast.Set()
    target_set.scls = ptrcls.target
    target_set.path_id = path_id

    ptr = irast.Pointer(
        source=source_set,
        target=target_set,
        ptrcls=ptrcls,
        direction=s_pointers.PointerDirection.Outbound
    )

    target_set.rptr = ptr

    return target_set
