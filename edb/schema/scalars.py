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


from __future__ import annotations

import typing

from edb import errors

from edb.common import typed
from edb.edgeql import ast as qlast

from . import abc as s_abc
from . import annos as s_anno
from . import casts as s_casts
from . import constraints
from . import delta as sd
from . import expr
from . import inheriting
from . import name as sn
from . import objects as so
from . import types as s_types


class FrozenStrList(typed.FrozenTypedList, type=str):
    pass


class ScalarType(s_types.Type, constraints.ConsistencySubject,
                 s_anno.AnnotationSubject, s_abc.ScalarType):

    default = so.SchemaField(
        expr.Expression, default=None,
        coerce=True, compcoef=0.909,
    )

    enum_values = so.SchemaField(
        FrozenStrList, default=None,
        coerce=True, compcoef=0.8,
    )

    @classmethod
    def get_schema_class_displayname(cls):
        return 'scalar type'

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
                        if isinstance(ptype, s_abc.Collection):
                            subtypes = ptype.get_subtypes(schema)
                        else:
                            subtypes = [ptype]

                        for subtype in subtypes:
                            if subtype is not self:
                                deps.add(subtype.get_name(schema))

        return deps

    def is_scalar(self):
        return True

    def is_enum(self, schema) -> bool:
        return bool(self.get_enum_values(schema))

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

    def assignment_castable_to(self, other: s_types.Type, schema) -> bool:
        left = self.get_base_for_cast(schema)
        right = other.get_base_for_cast(schema)
        return s_casts.is_assignment_castable(schema, left, right)

    def implicitly_castable_to(self, other: s_types.Type, schema) -> bool:
        if not isinstance(other, ScalarType):
            return False
        left = self.get_topmost_concrete_base(schema)
        right = other.get_topmost_concrete_base(schema)
        return s_casts.is_implicitly_castable(schema, left, right)

    def get_implicit_cast_distance(self, other: s_types.Type, schema) -> int:
        if not isinstance(other, ScalarType):
            return -1
        left = self.get_topmost_concrete_base(schema)
        right = other.get_topmost_concrete_base(schema)
        return s_casts.get_implicit_cast_distance(schema, left, right)

    def find_common_implicitly_castable_type(
            self, other: s_types.Type,
            schema) -> typing.Optional[s_types.Type]:

        if not isinstance(other, ScalarType):
            return

        if self.is_polymorphic(schema) and other.is_polymorphic(schema):
            return self

        left = self.get_topmost_concrete_base(schema)
        right = other.get_topmost_concrete_base(schema)

        if left == right:
            return left
        else:
            return s_casts.find_common_castable_type(schema, left, right)


class AnonymousEnumTypeRef(so.ObjectRef):

    def __init__(self, *, name: str, elements: typing.List[str]):
        super().__init__(name=name)
        self.__dict__['elements'] = elements


class ScalarTypeCommandContext(sd.ObjectCommandContext,
                               s_anno.AnnotationSubjectCommandContext,
                               constraints.ConsistencySubjectCommandContext):
    pass


class ScalarTypeCommand(constraints.ConsistencySubjectCommand,
                        s_anno.AnnotationSubjectCommand,
                        s_types.TypeCommand,
                        schema_metaclass=ScalarType,
                        context_class=ScalarTypeCommandContext):
    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cmd = cls._handle_view_op(schema, cmd, astnode, context)
        return cmd

    @classmethod
    def _validate_base_refs(cls, schema, base_refs, astnode, context):
        has_enums = any(isinstance(br, AnonymousEnumTypeRef)
                        for br in base_refs)

        if has_enums:
            if len(base_refs) > 1:
                raise errors.SchemaError(
                    f'invalid scalar type definition, enumeration must be the '
                    f'only supertype specified',
                    context=astnode.bases[0].context,
                )

        return super()._validate_base_refs(schema, base_refs, astnode, context)


class CreateScalarType(ScalarTypeCommand, inheriting.CreateInheritingObject):
    astnode = qlast.CreateScalarType

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(cmd, sd.CommandGroup):
            for subcmd in cmd.get_subcommands():
                if isinstance(subcmd, cls):
                    create_cmd = subcmd
                    break
            else:
                raise errors.InternalServerError(
                    'scalar view definition did not return CreateScalarType'
                )
        else:
            create_cmd = cmd

        bases = create_cmd.get_attribute_value('bases')
        is_enum = False
        if len(bases) == 1 and isinstance(bases._ids[0], AnonymousEnumTypeRef):
            elements = bases._ids[0].elements
            create_cmd.set_attribute_value('enum_values', elements)
            create_cmd.set_attribute_value('is_final', True)
            is_enum = True

        for sub in create_cmd.get_subcommands(type=sd.AlterObjectProperty):
            if sub.property == 'default':
                if is_enum:
                    raise errors.UnsupportedFeatureError(
                        f'enumerated types do not support defaults'
                    )
                else:
                    sub.new_value = [sub.new_value]

        return cmd

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'default':
            if op.new_value:
                op.new_value = op.new_value[0]
                super()._apply_field_ast(schema, context, node, op)
        else:
            super()._apply_field_ast(schema, context, node, op)


class RenameScalarType(ScalarTypeCommand, sd.RenameObject):
    pass


class RebaseScalarType(ScalarTypeCommand, inheriting.RebaseInheritingObject):

    def apply(self, schema, context):
        scls = self.get_object(schema, context)
        self.scls = scls

        enum_values = scls.get_enum_values(schema)
        if enum_values:
            raise errors.UnsupportedFeatureError(
                f'altering enum composition is not supported')

            if self.removed_bases and not self.added_bases:
                raise errors.SchemaError(
                    f'cannot DROP EXTENDING enum')

            all_bases = []

            for bases, pos in self.added_bases:
                if pos:
                    raise errors.SchemaError(
                        f'cannot add another enum as supertype '
                        f'use EXTENDING without position qualification')

                all_bases.extend(bases)

            if len(all_bases) > 1:
                raise errors.SchemaError(
                    f'cannot set more than one enum as supertype ')

            new_base = all_bases[0]
            new_values = new_base.elements

            schema = self._validate_enum_change(
                scls, enum_values, new_values, schema, context)

            return schema, scls
        else:
            return super().apply(self, schema, context)

    def _validate_enum_change(self, stype, cur_labels, new_labels,
                              schema, context):
        if len(set(new_labels)) != len(new_labels):
            raise errors.SchemaError(
                f'enum labels are not unique')

        cur_set = set(cur_labels)

        if cur_set - set(new_labels):
            raise errors.SchemaError(
                f'cannot remove labels from an enumeration type')

        existing = [l for l in new_labels if l in cur_set]
        if existing != cur_labels:
            raise errors.SchemaError(
                f'cannot change the relative order of existing labels '
                f'in an enumeration type')

        self.set_attribute_value('enum_values', new_labels)
        schema = stype.set_field_value(schema, 'enum_values', new_labels)
        return schema


class AlterScalarType(ScalarTypeCommand, inheriting.AlterInheritingObject):
    astnode = qlast.AlterScalarType


class DeleteScalarType(ScalarTypeCommand, inheriting.DeleteInheritingObject):
    astnode = qlast.DropScalarType
