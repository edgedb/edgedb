##
# Copyright (c) 2015-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import functools
import typing

from edgedb.lang.common import ast

from edgedb.lang.schema import inheriting as s_inh
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import sources as s_src
from edgedb.lang.schema import types as s_types
from edgedb.lang.schema import utils as s_utils

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors as ql_errors

from . import ast as irast


class PathIndex(collections.OrderedDict):
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
            return (isinstance(n.left, irast.Parameter) or
                    isinstance(n.right, irast.Parameter))

    ops = ast.find_children(ir, flt)

    arg_types = {}

    for binop in ops:
        typ = None

        if isinstance(binop.right, irast.Parameter):
            expr = binop.left
            arg = binop.right
            reversed = False
        else:
            expr = binop.right
            arg = binop.left
            reversed = True

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
            existing = arg_types[arg.name]
        except KeyError:
            arg_types[arg.name] = typ
        else:
            if existing != typ:
                msg = 'cannot infer expr type: ambiguous resolution: ' + \
                      '{!r} and {!r}'
                raise ValueError(msg.format(existing, typ))

    return arg_types


def _infer_common_type(irs: typing.List[irast.Base], schema):
    arg_types = []
    for arg in irs:
        if isinstance(arg, irast.Constant) and arg.value is None:
            continue

        arg_type = infer_type(arg, schema)
        if arg_type.name == 'std::null':
            continue

        arg_types.append(arg_type)

    if not arg_types:
        # all std::null
        result = schema.get('std::null')
    else:
        result = s_utils.get_class_nearest_common_ancestor(arg_types)

    return result


@functools.singledispatch
def _infer_type(ir, schema):
    return


@_infer_type.register(type(None))
def __infer_none(ir, schema):
    # Here for debugging purposes.
    raise ValueError('invalid infer_type(None, schema) call')


@_infer_type.register(irast.Set)
@_infer_type.register(irast.Shape)
def __infer_set_or_shape(ir, schema):
    return ir.scls


@_infer_type.register(irast.FunctionCall)
def __infer_func(ir, schema):
    result = ir.func.returntype

    def is_polymorphic(t):
        if isinstance(t, s_obj.Collection):
            t = t.get_element_type()

        return t.name == 'std::any'

    if is_polymorphic(result):
        # Polymorhic function, determine the result type from
        # the argument type.
        for i, arg in enumerate(ir.args):
            if is_polymorphic(ir.func.paramtypes[i]):
                result = infer_type(arg, schema)
                break

    return result


@_infer_type.register(irast.Constant)
@_infer_type.register(irast.Parameter)
def __infer_const_or_param(ir, schema):
    return ir.type


@_infer_type.register(irast.Coalesce)
def __infer_coalesce(ir, schema):
    result = _infer_common_type(ir.args, schema)
    if result is None:
        raise ql_errors.EdgeQLError(
            'coalescing operator must have operands of related types',
            context=ir.context)

    return result


@_infer_type.register(irast.BinOp)
def __infer_binop(ir, schema):
    if isinstance(ir.op, (ast.ops.ComparisonOperator,
                          ast.ops.TypeCheckOperator,
                          ast.ops.MembershipOperator,
                          irast.TextSearchOperator)):
        return schema.get('std::bool')

    left_type = infer_type(ir.left, schema)
    right_type = infer_type(ir.right, schema)

    if left_type.name == 'std::null':
        result = right_type
    elif right_type.name == 'std::null':
        result = left_type
    else:
        result = s_types.TypeRules.get_result(
            ir.op, (left_type, right_type), schema)
        if result is None:
            result = s_types.TypeRules.get_result(
                (ir.op, 'reversed'), (right_type, left_type), schema)

    if result is None:
        raise ql_errors.EdgeQLError(
            'operator does not exist: {} {} {}'.format(
                left_type.name, ir.op, right_type.name),
            context=ir.left.context)

    return result


@_infer_type.register(irast.UnaryOp)
def __infer_unaryop(ir, schema):
    if ir.op == ast.ops.NOT:
        result = schema.get('std::bool')
    else:
        operand_type = infer_type(ir.expr, schema)
        if operand_type.name == 'std::null':
            result = operand_type
        else:
            result = s_types.TypeRules.get_result(
                ir.op, (operand_type,), schema)

    return result


@_infer_type.register(irast.IfElseExpr)
def __infer_ifelse(ir, schema):
    if_expr_type = infer_type(ir.if_expr, schema)
    else_expr_type = infer_type(ir.else_expr, schema)

    result = s_utils.get_class_nearest_common_ancestor(
        [if_expr_type, else_expr_type])

    if result is None:
        raise ql_errors.EdgeQLError(
            'if/else clauses must be of related types, got: {}/{}'.format(
                if_expr_type.name, else_expr_type.name),
            context=ir.if_expr.context)

    return result


@_infer_type.register(irast.TypeCast)
@_infer_type.register(irast.TypeFilter)
def __infer_typecast(ir, schema):
    if ir.type.subtypes:
        coll = s_obj.Collection.get_class(ir.type.maintype)
        result = coll.from_subtypes(
            [schema.get(t) for t in ir.type.subtypes])
    else:
        result = schema.get(ir.type.maintype)
    return result


@_infer_type.register(irast.Stmt)
def __infer_stmt(ir, schema):
    return infer_type(ir.result, schema)


@_infer_type.register(irast.SelectStmt)
def __infer_select_stmt(ir, schema):
    if ir.set_op is not None:
        if ir.set_op == qlast.UNION:
            ltype = infer_type(ir.set_op_larg, schema)
            rtype = infer_type(ir.set_op_rarg, schema)

            if ltype.issubclass(rtype):
                result = ltype
            elif rtype.issubclass(ltype):
                result = rtype
            else:
                result = s_inh.create_virtual_parent(
                    schema, [ltype, rtype])

        else:
            result = infer_type(ir.set_op_larg, schema)
    else:
        result = infer_type(ir.result, schema)

    return result


@_infer_type.register(irast.ExistPred)
def __infer_exist(ir, schema):
    return schema.get('std::bool')


@_infer_type.register(irast.SliceIndirection)
def __infer_slice(ir, schema):
    return infer_type(ir.expr, schema)


@_infer_type.register(irast.IndexIndirection)
def __infer_index(ir, schema):
    node_type = infer_type(ir.expr, schema)
    index_type = infer_type(ir.index, schema)

    str_t = schema.get('std::str')
    int_t = schema.get('std::int')

    result = None

    if node_type.issubclass(str_t):

        if not index_type.issubclass(int_t):
            raise ql_errors.EdgeQLError(
                f'cannot index string by {index_type.name}, '
                f'{int_t.name} was expected',
                context=ir.index.context)

        result = node_type

    elif isinstance(node_type, s_obj.Map):

        if not index_type.issubclass(node_type.key_type):
            raise ql_errors.EdgeQLError(
                f'cannot index {node_type.name} by {index_type.name}, '
                f'{node_type.key_type.name} was expected',
                context=ir.index.context)

        result = node_type.element_type

    elif isinstance(node_type, s_obj.Array):

        if not index_type.issubclass(int_t):
            raise ql_errors.EdgeQLError(
                f'cannot index array by {index_type.name}, '
                f'{int_t.name} was expected',
                context=ir.index.context)

        result = node_type.element_type

    return result


@_infer_type.register(irast.Mapping)
def __infer_map(ir, schema):
    key_type = _infer_common_type(ir.keys, schema)
    if key_type is None:
        raise ql_errors.EdgeQLError('could not determine map keys type',
                                    context=ir.context)

    element_type = _infer_common_type(ir.values, schema)
    if element_type is None:
        raise ql_errors.EdgeQLError('could not determine map values type',
                                    context=ir.context)

    return s_obj.Map(key_type=key_type, element_type=element_type)


@_infer_type.register(irast.Sequence)
def __infer_seq(ir, schema):
    if ir.is_array:
        if ir.elements:
            element_type = _infer_common_type(ir.elements, schema)
            if element_type is None:
                raise ql_errors.EdgeQLError('could not determine array type',
                                            context=ir.context)
        else:
            element_type = schema.get('std::any')

        result = s_obj.Array(element_type=element_type)
    else:
        result = s_obj.Tuple(element_type=schema.get('std::any'))

    return result


def infer_type(ir, schema):
    try:
        return ir._inferred_type_
    except AttributeError:
        pass

    result = _infer_type(ir, schema)

    if (result is not None and
            not isinstance(result, (s_obj.Class, s_obj.MetaClass))):

        raise ql_errors.EdgeQLError(
            f'infer_type({ir!r}) retured {result!r} instead of a Class',
            context=ir.context)

    if result is None or result.name == 'std::any':
        raise ql_errors.EdgeQLError('could not determine expression type',
                                    context=ir.context)

    ir._inferred_type_ = result
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
    flt = lambda n: isinstance(n, irast.Parameter)
    result.update(ast.find_children(ir, flt))
    return result


def is_const(ir):
    flt = lambda n: isinstance(n, irast.Set) and n.expr is None
    ir_sets = ast.find_children(ir, flt)
    variables = get_variables(ir)
    return not ir_sets and not variables


def is_aggregated_expr(ir):
    def flt(n):
        if isinstance(n, irast.FunctionCall):
            return n.func.aggregate
        elif isinstance(n, irast.Stmt):
            # Make sure we don't dip into subqueries
            raise ast.SkipNode()

    return bool(set(ast.find_children(ir, flt)))


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
            genptr = self[-2][0]
            direction = self[-2][1]
            if direction == s_pointers.PointerDirection.Outbound:
                src = self[-3]
            else:
                src = self[-1]

            if isinstance(src, s_src.Source):
                return src.pointers.get(genptr.name)
            else:
                return None
        else:
            return None

    def rptr_dir(self):
        if len(self) > 1:
            return self[-2][1]
        else:
            return None

    def iter_prefixes(self):
        yield self.__class__(self[:1])

        for i in range(1, len(self) - 1, 2):
            if self[i + 1]:
                yield self.__class__(self[:i + 2])
            else:
                break

    def __hash__(self):
        return hash(tuple(self))

    def __str__(self):
        if not self:
            return ''

        result = f'({self[0].name})'

        for i in range(1, len(self) - 1, 2):
            ptr = self[i][0]
            ptrdir = self[i][1]
            tgt = self[i + 1]

            if tgt:
                lexpr = f'({ptr.name} [IS {tgt.name}])'
            else:
                lexpr = f'({ptr.name})'

            if isinstance(ptr, s_lprops.LinkProperty):
                step = '@'
            else:
                step = f'.{ptrdir}'

            result += f'{step}{lexpr}'

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
