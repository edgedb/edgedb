#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import functools
import typing

from edb.lang.common import ast
from edb.lang.edgeql import ast as qlast

from . import attributes
from . import constraints
from . import delta as sd
from . import expr
from . import inheriting
from . import name as sn
from . import named
from . import nodes
from . import objects as so
from . import schema as s_schema
from . import types as s_types


class ScalarType(nodes.Node, constraints.ConsistencySubject,
                 attributes.AttributeSubject):

    default = so.SchemaField(
        expr.ExpressionText, default=None,
        coerce=True, compcoef=0.909)

    def _get_deps(self, schema):
        deps = super()._get_deps(schema)

        consts = self.get_constraints(schema)
        if consts:
            N = sn.Name

            # Add dependency on all built-in scalars unconditionally
            deps.add(N(module='std', name='str'))
            deps.add(N(module='std', name='bytes'))
            deps.add(N(module='std', name='int16'))
            deps.add(N(module='std', name='int32'))
            deps.add(N(module='std', name='int64'))
            deps.add(N(module='std', name='float32'))
            deps.add(N(module='std', name='float64'))
            deps.add(N(module='std', name='decimal'))
            deps.add(N(module='std', name='bool'))
            deps.add(N(module='std', name='uuid'))

            for constraint in consts.objects(schema):
                c_params = constraint.get_params(schema).objects(schema)
                ptypes = [p.get_type(schema) for p in c_params]
                if ptypes:
                    for ptype in ptypes:
                        if isinstance(ptype, s_types.Collection):
                            subtypes = ptype.get_subtypes()
                        else:
                            subtypes = [ptype]

                        for subtype in subtypes:
                            if subtype is not self:
                                deps.add(subtype.get_name(schema))

        return deps

    def is_scalar(self):
        return True

    def is_polymorphic(self, schema):
        return self.get_is_abstract(schema)

    def _resolve_polymorphic(self, schema, concrete_type: s_types.Type):
        if (self.is_polymorphic(schema) and
                concrete_type.is_scalar() and
                not concrete_type.is_polymorphic(schema)):
            return concrete_type

    def _to_nonpolymorphic(self, schema, concrete_type: s_types.Type):
        if (not concrete_type.is_polymorphic(schema) and
                concrete_type.issubclass(schema, self)):
            return concrete_type
        raise TypeError(
            f'cannot interpret {concrete_type.get_name(schema)} '
            f'as {self.get_name(schema)}')

    def _test_polymorphic(self, schema, other: s_types.Type):
        if other.is_any():
            return True
        else:
            return self.issubclass(schema, other)

    def assignment_castable_to(self, target: s_types.Type, schema) -> bool:
        if self.implicitly_castable_to(target, schema):
            return True

        source = str(self.get_topmost_concrete_base(schema).get_name(schema))
        target = str(target.get_topmost_concrete_base(schema).get_name(schema))

        return _is_assignment_castable_impl(source, target)

    def implicitly_castable_to(self, other: s_types.Type, schema) -> bool:
        if not isinstance(other, ScalarType):
            return False
        left = str(self.get_topmost_concrete_base(schema).get_name(schema))
        right = str(other.get_topmost_concrete_base(schema).get_name(schema))
        return _is_implicitly_castable_impl(left, right)

    def find_common_implicitly_castable_type(
            self, other: s_types.Type,
            schema) -> typing.Optional[s_types.Type]:

        if not isinstance(other, ScalarType):
            return

        if self.is_polymorphic(schema) and other.is_polymorphic(schema):
            return self

        left = str(self.get_topmost_concrete_base(schema).get_name(schema))
        right = str(other.get_topmost_concrete_base(schema).get_name(schema))

        if left == right:
            return schema.get(left)

        result = _find_common_castable_type_impl(left, right)
        if result is not None:
            return schema.get(result)


# source -> target   (source can be implicitly casted into target)
_implicit_numeric_cast_map = {
    'std::int16': 'std::int32',
    'std::int32': 'std::int64',
    'std::int64': {'std::float64', 'std::decimal'},
    'std::float32': 'std::float64',
    'std::float64': None,
    'std::decimal': None,
}


def _is_reachable(graph, source: str, target: str) -> bool:
    if source == target:
        return True

    while True:
        source = graph.get(source)
        if source is None:
            return False
        elif source == target:
            return True
        elif isinstance(source, set):
            return any(_is_reachable(graph, s, target) for s in source)


@functools.lru_cache()
def _is_implicitly_castable_impl(source: str, target: str) -> bool:
    return _is_reachable(_implicit_numeric_cast_map, source, target)


@functools.lru_cache()
def _find_common_castable_type_impl(
        source: str, target: str) -> typing.Optional[str]:

    if _is_implicitly_castable_impl(target, source):
        return source
    if _is_implicitly_castable_impl(source, target):
        return target

    # Elevate target in the castability ladder, and check if
    # source is castable to it on each step.
    while True:
        target = _implicit_numeric_cast_map.get(target)
        if target is None:
            return None
        elif isinstance(target, set):
            for t in target:
                candidate = _find_common_castable_type_impl(source, t)
                if candidate is not None:
                    return candidate
            else:
                return None
        elif _is_implicitly_castable_impl(source, target):
            return target


# target -> source   (source can be casted into target in assignment)
_assignment_numeric_cast_map = {
    'std::int16': 'std::int32',
    'std::int32': 'std::int64',
    'std::float32': 'std::float64',
    'std::float64': 'std::decimal',
}


@functools.lru_cache()
def _is_assignment_castable_impl(source: str, target: str) -> bool:
    return _is_reachable(_assignment_numeric_cast_map, target, source)


# Operator type rules: {operator -> [(operand_type, ..., result_type)]}
# The list of operator variants MUST be defined in the order of
# decreasing specificity in the implicit cast tower, i.e small int
# variants must be specified before the large int variants.
_operator_map = {
    ast.ops.ADD: [
        ('std::int16', 'std::int16', 'std::int16'),
        ('std::int32', 'std::int32', 'std::int32'),
        ('std::int64', 'std::int64', 'std::int64'),
        ('std::float32', 'std::float32', 'std::float32'),
        ('std::float64', 'std::float64', 'std::float64'),
        ('std::decimal', 'std::decimal', 'std::decimal'),
        ('std::str', 'std::str', 'std::str'),
        ('std::bytes', 'std::bytes', 'std::bytes'),
        ('std::datetime', 'std::timedelta', 'std::datetime'),
        ('std::naive_datetime', 'std::timedelta', 'std::naive_datetime'),
        ('std::naive_date', 'std::timedelta', 'std::naive_date'),
        ('std::naive_time', 'std::timedelta', 'std::naive_time'),
        ('std::timedelta', 'std::timedelta', 'std::timedelta'),
    ],

    ast.ops.SUB: [
        ('std::int16', 'std::int16', 'std::int16'),
        ('std::int32', 'std::int32', 'std::int32'),
        ('std::int64', 'std::int64', 'std::int64'),
        ('std::float32', 'std::float32', 'std::float32'),
        ('std::float64', 'std::float64', 'std::float64'),
        ('std::decimal', 'std::decimal', 'std::decimal'),
        ('std::datetime', 'std::datetime', 'std::timedelta'),
        ('std::naive_datetime', 'std::naive_datetime', 'std::timedelta'),
        ('std::naive_date', 'std::naive_date', 'std::timedelta'),
        ('std::naive_time', 'std::naive_time', 'std::timedelta'),
        ('std::datetime', 'std::timedelta', 'std::datetime'),
        ('std::naive_datetime', 'std::timedelta', 'std::naive_datetime'),
        ('std::naive_date', 'std::timedelta', 'std::naive_date'),
        ('std::naive_time', 'std::timedelta', 'std::naive_time'),
        ('std::timedelta', 'std::timedelta', 'std::timedelta'),
    ],

    ast.ops.MUL: [
        ('std::int16', 'std::int16', 'std::int16'),
        ('std::int32', 'std::int32', 'std::int32'),
        ('std::int64', 'std::int64', 'std::int64'),
        ('std::float32', 'std::float32', 'std::float32'),
        ('std::float64', 'std::float64', 'std::float64'),
        ('std::decimal', 'std::decimal', 'std::decimal'),
    ],

    ast.ops.DIV: [
        ('std::int64', 'std::int64', 'std::float64'),
        ('std::float32', 'std::float32', 'std::float32'),
        ('std::float64', 'std::float64', 'std::float64'),
        ('std::decimal', 'std::decimal', 'std::decimal'),
        ('std::decimal', 'std::int64', 'std::decimal'),
    ],

    ast.ops.FLOORDIV: [
        ('std::int64', 'std::int64', 'std::int64'),
        ('std::float32', 'std::float32', 'std::float32'),
        ('std::float64', 'std::float64', 'std::float64'),
        ('std::decimal', 'std::decimal', 'std::decimal'),
    ],

    ast.ops.MOD: [
        ('std::int16', 'std::int16', 'std::int16'),
        ('std::int32', 'std::int32', 'std::int32'),
        ('std::int64', 'std::int64', 'std::int64'),
        ('std::decimal', 'std::decimal', 'std::decimal'),
    ],

    ast.ops.POW: [
        # Non-float numerics use decimal upcast, like std::sum,
        # floats use float64.
        ('std::decimal', 'std::decimal', 'std::decimal'),
        ('std::float64', 'std::float64', 'std::float64'),
    ],

    ast.ops.OR: [
        ('std::bool', 'std::bool', 'std::bool'),
    ],

    ast.ops.AND: [
        ('std::bool', 'std::bool', 'std::bool'),
    ],

    ast.ops.NOT: [
        ('std::bool', 'std::bool'),
    ],

    ast.ops.UMINUS: [
        ('std::int16', 'std::int16'),
        ('std::int32', 'std::int32'),
        ('std::int64', 'std::int64'),
        ('std::float32', 'std::float32'),
        ('std::float64', 'std::float64'),
        ('std::decimal', 'std::decimal'),
        ('std::timedelta', 'std::timedelta'),
    ],

    ast.ops.UPLUS: [
        ('std::int16', 'std::int16'),
        ('std::int32', 'std::int32'),
        ('std::int64', 'std::int64'),
        ('std::float32', 'std::float32'),
        ('std::float64', 'std::float64'),
        ('std::decimal', 'std::decimal'),
        ('std::timedelta', 'std::timedelta'),
    ],
}


_commutative_ops = (ast.ops.ADD, ast.ops.MUL, ast.ops.OR, ast.ops.AND)


def _get_op_type(op: ast.ops.Operator,
                 *operands: s_types.Type,
                 schema: s_schema.Schema) -> typing.Optional[s_types.Type]:
    candidates = _operator_map.get(op)
    if not candidates:
        return None

    operand_count = len(operands)
    shortlist = []

    for candidate in candidates:
        if len(candidate) != operand_count + 1:
            # Skip candidates with non-matching operand count.
            continue

        cast_count = 0

        for def_opr_name, passed_opr in zip(candidate, operands):
            def_opr = schema.get(def_opr_name)

            if passed_opr.issubclass(schema, def_opr):
                pass
            elif passed_opr.implicitly_castable_to(def_opr, schema):
                cast_count += 1
            else:
                break
        else:
            shortlist.append((candidate[-1], cast_count))

    if shortlist:
        shortlist.sort(key=lambda c: c[1])
        return schema.get(shortlist[0][0])
    else:
        return None


@functools.lru_cache()
def get_op_type(op: ast.ops.Operator,
                *operands: s_types.Type,
                schema: s_schema.Schema) -> typing.Optional[s_types.Type]:
    restype = _get_op_type(op, *operands, schema=schema)
    if restype is None:
        if op in _commutative_ops and len(operands) == 2:
            restype = _get_op_type(op, operands[1], operands[0], schema=schema)

    return restype


class ScalarTypeCommandContext(sd.ObjectCommandContext,
                               attributes.AttributeSubjectCommandContext,
                               constraints.ConsistencySubjectCommandContext,
                               nodes.NodeCommandContext):
    pass


class ScalarTypeCommand(constraints.ConsistencySubjectCommand,
                        attributes.AttributeSubjectCommand,
                        nodes.NodeCommand,
                        schema_metaclass=ScalarType,
                        context_class=ScalarTypeCommandContext):
    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cmd = cls._handle_view_op(schema, cmd, astnode, context)
        return cmd


class CreateScalarType(ScalarTypeCommand, inheriting.CreateInheritingObject):
    astnode = qlast.CreateScalarType

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        for sub in cmd.get_subcommands(type=sd.AlterObjectProperty):
            if sub.property == 'default':
                sub.new_value = [sub.new_value]

        return cmd

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'default':
            if op.new_value:
                op.new_value = op.new_value[0]
                super()._apply_field_ast(schema, context, node, op)
        else:
            super()._apply_field_ast(schema, context, node, op)


class RenameScalarType(ScalarTypeCommand, named.RenameNamedObject):
    pass


class RebaseScalarType(ScalarTypeCommand, inheriting.RebaseNamedObject):
    pass


class AlterScalarType(ScalarTypeCommand, inheriting.AlterInheritingObject):
    astnode = qlast.AlterScalarType


class DeleteScalarType(ScalarTypeCommand, inheriting.DeleteInheritingObject):
    astnode = qlast.DropScalarType
