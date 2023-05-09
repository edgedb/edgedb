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

from typing import *

from edb import errors

from edb.common import checked
from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import abc as s_abc
from . import annos as s_anno
from . import casts as s_casts
from . import constraints
from . import delta as sd
from . import expr as s_expr
from . import inheriting
from . import name as s_name
from . import objects as so
from . import schema as s_schema
from . import types as s_types
from . import utils as s_utils


class ScalarType(
    s_types.InheritingType,
    constraints.ConsistencySubject,
    s_abc.ScalarType,
    qlkind=qltypes.SchemaObjectClass.SCALAR_TYPE,
    data_safe=True,
):

    default = so.SchemaField(
        s_expr.Expression, default=None,
        coerce=True, compcoef=0.909,
    )

    enum_values = so.SchemaField(
        checked.FrozenCheckedList[str], default=None,
        coerce=True, compcoef=0.8,
    )

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'scalar type'

    def is_scalar(self) -> bool:
        return True

    def is_concrete_enum(self, schema: s_schema.Schema) -> bool:
        return any(
            str(base.get_name(schema)) == 'std::anyenum'
            for base in self.get_bases(schema).objects(schema)
        )

    def is_base_type(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        """Returns true of the type has only abstract bases"""
        bases: Sequence[s_types.Type] = self.get_bases(schema).objects(schema)
        return all(b.get_abstract(schema) for b in bases)

    def is_enum(self, schema: s_schema.Schema) -> bool:
        return bool(self.get_enum_values(schema))

    def is_sequence(self, schema: s_schema.Schema) -> bool:
        seq = schema.get('std::sequence', type=ScalarType)
        return self.issubclass(schema, seq)

    def is_polymorphic(self, schema: s_schema.Schema) -> bool:
        return self.get_abstract(schema)

    def is_json(self, schema: s_schema.Schema) -> bool:
        return self.issubclass(
            schema,
            schema.get(s_name.QualName('std', 'json'), type=ScalarType),
        )

    def can_accept_constraints(self, schema: s_schema.Schema) -> bool:
        return not self.is_enum(schema)

    def _resolve_polymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: s_types.Type,
    ) -> Optional[s_types.Type]:
        if (self.is_polymorphic(schema) and
                concrete_type.is_scalar() and
                not concrete_type.is_polymorphic(schema)):
            return concrete_type
        return None

    def _to_nonpolymorphic(
        self,
        schema: s_schema.Schema,
        concrete_type: s_types.Type,
    ) -> Tuple[s_schema.Schema, s_types.Type]:
        if (not concrete_type.is_polymorphic(schema) and
                concrete_type.issubclass(schema, self)):
            return schema, concrete_type
        raise TypeError(
            f'cannot interpret {concrete_type.get_name(schema)} '
            f'as {self.get_name(schema)}')

    def _test_polymorphic(
        self,
        schema: s_schema.Schema,
        other: s_types.Type,
    ) -> bool:
        if other.is_any(schema):
            return True
        else:
            return self.issubclass(schema, other)

    def assignment_castable_to(
        self,
        other: s_types.Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, ScalarType):
            return False
        if self.is_polymorphic(schema) or other.is_polymorphic(schema):
            return False
        left = self.get_base_for_cast(schema)
        right = other.get_base_for_cast(schema)
        return s_casts.is_assignment_castable(schema, left, right)

    def implicitly_castable_to(
        self,
        other: s_types.Type,
        schema: s_schema.Schema,
    ) -> bool:
        if not isinstance(other, ScalarType):
            return False
        if self.is_polymorphic(schema) or other.is_polymorphic(schema):
            return False
        left = self.get_topmost_concrete_base(schema)
        right = other.get_topmost_concrete_base(schema)
        assert isinstance(left, s_types.Type)
        assert isinstance(right, s_types.Type)
        return s_casts.is_implicitly_castable(schema, left, right)

    def castable_to(
        self,
        other: s_types.Type,
        schema: s_schema.Schema,
    ) -> bool:
        """Determine if any cast exists between self and *other*."""
        if not isinstance(other, ScalarType):
            return False
        if self.is_polymorphic(schema) or other.is_polymorphic(schema):
            return False
        left = self.get_topmost_concrete_base(schema)
        right = other.get_topmost_concrete_base(schema)
        assert isinstance(left, s_types.Type)
        assert isinstance(right, s_types.Type)
        return s_casts.is_castable(schema, left, right)

    def get_implicit_cast_distance(
        self,
        other: s_types.Type,
        schema: s_schema.Schema,
    ) -> int:
        if not isinstance(other, ScalarType):
            return -1
        if self.is_polymorphic(schema) or other.is_polymorphic(schema):
            return -1
        left = self.get_topmost_concrete_base(schema)
        right = other.get_topmost_concrete_base(schema)
        return s_casts.get_implicit_cast_distance(schema, left, right)

    def find_common_implicitly_castable_type(
        self,
        other: s_types.Type,
        schema: s_schema.Schema,
    ) -> Tuple[s_schema.Schema, Optional[ScalarType]]:

        if not isinstance(other, ScalarType):
            return schema, None

        if self.is_polymorphic(schema) and other.is_polymorphic(schema):
            return schema, self

        left = self.get_topmost_concrete_base(schema)
        right = other.get_topmost_concrete_base(schema)

        if left == right:
            return schema, left
        else:
            return (
                schema,
                cast(
                    Optional[ScalarType],
                    s_casts.find_common_castable_type(schema, left, right),
                )
            )

    def get_base_for_cast(self, schema: s_schema.Schema) -> so.Object:
        if self.is_enum(schema):
            # all enums have to use std::anyenum as base type for casts
            return schema.get('std::anyenum')
        else:
            return super().get_base_for_cast(schema)

    def get_verbosename(
        self, schema: s_schema.Schema, *, with_parent: bool = False
    ) -> str:
        if self.is_enum(schema):
            clsname = 'enumerated type'
        else:
            clsname = self.get_schema_class_displayname()
        dname = self.get_displayname(schema)
        return f"{clsname} '{dname}'"

    def as_alter_delta(
        self,
        other: ScalarType,
        *,
        self_schema: s_schema.Schema,
        other_schema: s_schema.Schema,
        confidence: float,
        context: so.ComparisonContext,
    ) -> sd.ObjectCommand[ScalarType]:
        alter = super().as_alter_delta(
            other,
            self_schema=self_schema,
            other_schema=other_schema,
            confidence=confidence,
            context=context,
        )

        # If this is an enum and enum_values changed, we need to
        # generate a rebase.
        enum_values = alter.get_local_attribute_value('enum_values')
        if enum_values is not None:
            assert isinstance(alter.classname, s_name.QualName)
            rebase = RebaseScalarType(
                classname=alter.classname,
                removed_bases=(),
                added_bases=(
                    ([AnonymousEnumTypeShell(elements=enum_values)], ''),
                ),
            )
            alter.add(rebase)

        return alter


class AnonymousEnumTypeShell(s_types.TypeShell[ScalarType]):

    elements: Sequence[str]

    def __init__(
        self,
        *,
        name: s_name.Name = s_name.QualName(module='std', name='anyenum'),
        elements: Iterable[str],
    ) -> None:
        super().__init__(name=name, schemaclass=ScalarType)
        self.elements = list(elements)

    def resolve(self, schema: s_schema.Schema) -> ScalarType:
        raise errors.InvalidPropertyDefinitionError(
            'this type cannot be anonymous',
            details=(
                'you may want define this enum first:\n\n'
                '  scalar type MyEnum extending enum<...>;'
            ),
        )


class ScalarTypeCommandContext(sd.ObjectCommandContext[ScalarType],
                               s_anno.AnnotationSubjectCommandContext,
                               constraints.ConsistencySubjectCommandContext):
    pass


class ScalarTypeCommand(
    s_types.InheritingTypeCommand[ScalarType],
    constraints.ConsistencySubjectCommand[ScalarType],
    s_anno.AnnotationSubjectCommand[ScalarType],
    context_class=ScalarTypeCommandContext,
):
    def validate_scalar_ancestors(
        self,
        ancestors: Sequence[so.SubclassableObject],
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        real_concrete_ancestors = {
            ancestor for ancestor in ancestors
            if not ancestor.get_abstract(schema)
        }
        # Filter out anything that has a subclass relation with
        # every other concrete ancestor. This lets us allow chains
        # of concrete scalar types while prohibiting diamonds (for
        # example if X <: A, B <: int64 where A, B are concrete).
        # (If we wanted to allow diamonds, we could instead filter out
        # anything that has concrete bases.)
        concrete_ancestors = {
            c1 for c1 in real_concrete_ancestors
            if not all(c1 == c2 or c1.issubclass(schema, c2)
                       or c2.issubclass(schema, c1)
                       for c2 in real_concrete_ancestors)
        }

        if len(concrete_ancestors) > 1:
            raise errors.SchemaError(
                f'scalar type may not have more than '
                f'one concrete base type',
                context=self.source_context,
            )
        abstract = self.get_attribute_value('abstract')
        enum = self.get_attribute_value('enum_values')
        if (
            len(real_concrete_ancestors) < 1
            and not context.stdmode
            and not abstract
            and not enum
        ):
            if not ancestors:
                hint = (
                    f'\nFor example: scalar type {self.classname.name} '
                    f'extending str'
                )
            else:
                hint = 'Bases were specified but no concrete bases were found'

            raise errors.SchemaError(
                f'scalar type must have a concrete base type',
                context=self.source_context,
                hint=hint,
            )

    def validate_scalar_bases(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        bases = self.get_resolved_attribute_value(
            'bases', schema=schema, context=context)

        if bases is not None:
            ancestors = []
            for base in bases.objects(schema):
                ancestors.append(base)
                ancestors.extend(base.get_ancestors(schema).objects(schema))

            self.validate_scalar_ancestors(ancestors, schema, context)

    def get_dummy_expr_field_value(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: Any,
    ) -> Optional[s_expr.Expression]:
        if field.name == 'expr':
            return s_expr.Expression(text=f'0')
        else:
            raise NotImplementedError(f'unhandled field {field.name!r}')


class CreateScalarType(
    ScalarTypeCommand,
    s_types.CreateInheritingType[ScalarType],
):
    astnode = qlast.CreateScalarType

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(cmd, sd.CommandGroup):
            for subcmd in cmd.get_subcommands():
                if isinstance(subcmd, cls):
                    create_cmd: sd.Command = subcmd
                    break
            else:
                raise errors.InternalServerError(
                    'scalar alias definition did not return CreateScalarType'
                )
        else:
            create_cmd = cmd

        if isinstance(astnode, qlast.CreateScalarType):
            bases = [
                s_utils.ast_to_type_shell(
                    b,
                    metaclass=ScalarType,
                    modaliases=context.modaliases,
                    schema=schema,
                )
                for b in (astnode.bases or [])
            ]
            is_enum = any(
                isinstance(br, AnonymousEnumTypeShell) for br in bases)
            for ab, b in zip(astnode.bases, bases):
                if isinstance(b, s_types.CollectionTypeShell):
                    raise errors.SchemaError(
                        f'scalar type may not have a collection base type',
                        context=ab.context,
                    )

            # We don't support FINAL, but old dumps and migrations specify
            # it on enum CREATE SCALAR TYPEs, so we need to permit it in those
            # cases.
            if not is_enum and astnode.final:
                raise errors.UnsupportedFeatureError(
                    f'FINAL is not supported',
                    context=astnode.context,
                )

            if is_enum:
                # This is an enumerated type.
                if len(bases) > 1:
                    assert isinstance(astnode, qlast.BasedOnTuple)
                    raise errors.SchemaError(
                        f'invalid scalar type definition, enumeration must be'
                        f' the only supertype specified',
                        context=astnode.bases[0].context,
                    )
                if create_cmd.has_attribute_value('default'):
                    raise errors.UnsupportedFeatureError(
                        f'enumerated types do not support defaults',
                        context=(
                            create_cmd.get_attribute_source_context('default')
                        ),
                    )

                shell = bases[0]
                assert isinstance(shell, AnonymousEnumTypeShell)
                if len(set(shell.elements)) != len(shell.elements):
                    raise errors.SchemaDefinitionError(
                        f'enums cannot contain duplicate values',
                        context=astnode.bases[0].context,
                    )
                create_cmd.set_attribute_value('enum_values', shell.elements)
                create_cmd.set_attribute_value(
                    'bases',
                    so.ObjectCollectionShell(
                        [s_utils.ast_objref_to_object_shell(
                            s_utils.name_to_ast_ref(
                                s_name.QualName('std', 'anyenum'),
                            ),
                            schema=schema,
                            metaclass=ScalarType,
                            modaliases={},
                        )],
                        collection_type=so.ObjectList,
                    )
                )

        return cmd

    def validate_create(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super().validate_create(schema, context)
        self.validate_scalar_bases(schema, context)

    def _get_ast_node(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> Type[qlast.DDLOperation]:
        if self.get_attribute_value('expr'):
            return qlast.CreateAlias
        else:
            return super()._get_ast_node(schema, context)

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if op.property == 'default':
            if op.new_value:
                assert isinstance(op.new_value, list)
                op.new_value = op.new_value[0]
                super()._apply_field_ast(schema, context, node, op)

        elif op.property == 'bases':
            enum_values = self.get_local_attribute_value('enum_values')
            if enum_values:
                assert isinstance(node, qlast.BasedOnTuple)
                node.bases = [
                    qlast.TypeName(
                        maintype=qlast.ObjectRef(name='enum'),
                        subtypes=[
                            qlast.TypeName(maintype=qlast.ObjectRef(name=v))
                            for v in enum_values
                        ]
                    )
                ]
            else:
                super()._apply_field_ast(schema, context, node, op)
        else:
            super()._apply_field_ast(schema, context, node, op)


class RenameScalarType(
    ScalarTypeCommand,
    s_types.RenameInheritingType[ScalarType],
):
    pass


class RebaseScalarType(
    ScalarTypeCommand,
    inheriting.RebaseInheritingObject[ScalarType],
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        scls = self.get_object(schema, context)
        self.scls = scls
        assert isinstance(scls, ScalarType)

        cur_labels = scls.get_enum_values(schema)

        if cur_labels:

            if self.removed_bases and not self.added_bases:
                raise errors.SchemaError(
                    f'cannot DROP EXTENDING enum')

            all_bases = []

            for bases, pos in self.added_bases:
                # Check that there aren't any non-enum bases.
                for base in bases:
                    if isinstance(base, AnonymousEnumTypeShell):
                        is_enum_base = True
                    elif isinstance(base, s_types.TypeShell):
                        is_enum_base = base.resolve(schema).is_enum(schema)
                    else:
                        is_enum_base = base.is_enum(schema)

                    if not is_enum_base:
                        raise errors.SchemaError(
                            f'cannot add another type as supertype, '
                            f'enumeration must be the only supertype specified'
                        )

                # Since all bases are enums at this point, error
                # messages only mention about enums.
                if pos:
                    raise errors.SchemaError(
                        f'cannot add another enum as supertype, '
                        f'use EXTENDING without position qualification')

                all_bases.extend(bases)

            if len(all_bases) > 1:
                raise errors.SchemaError(
                    f'cannot set more than one enum as supertype')

            new_base = all_bases[0]
            new_labels = new_base.elements

            schema = self._validate_enum_change(
                scls, cur_labels, new_labels, schema, context)

            self.validate_scalar_bases(schema, context)

            schema = super().apply(schema, context)

        else:
            old_concrete = self.scls.maybe_get_topmost_concrete_base(schema)

            for b in [b for bs, _ in self.added_bases for b in bs]:
                if isinstance(b, s_types.CollectionTypeShell):
                    raise errors.SchemaError(
                        f'scalar type may not have a collection base type',
                        context=self.source_context,
                    )

            schema = super().apply(schema, context)

            self.validate_scalar_bases(schema, context)

            new_concrete = self.scls.maybe_get_topmost_concrete_base(schema)
            if old_concrete != new_concrete and not scls.is_view(schema):
                raise errors.SchemaError(
                    f'cannot change concrete base of a scalar type')

        return schema

    def validate_scalar_bases(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super().validate_scalar_bases(schema, context)

        bases = self.get_resolved_attribute_value(
            'bases', schema=schema, context=context)
        if bases:
            obj = self.scls
            # For each descendant, compute its new ancestors and check
            # that they are valid for a scalar type.
            new_schema = obj.set_field_value(schema, 'bases', bases)
            for desc in obj.descendants(schema):
                ancestors = so.compute_ancestors(new_schema, desc)
                self.validate_scalar_ancestors(ancestors, schema, context)

    def _validate_enum_change(
        self,
        stype: s_types.Type,
        cur_labels: Sequence[str],
        new_labels: Sequence[str],
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        new_set = set(new_labels)
        if len(new_set) != len(new_labels):
            raise errors.SchemaError(
                f'enums cannot contain duplicate values')

        self.set_attribute_value('enum_values', new_labels)
        schema = stype.set_field_value(schema, 'enum_values', new_labels)
        return schema


class AlterScalarType(
    ScalarTypeCommand,
    inheriting.AlterInheritingObject[ScalarType],
):
    astnode = qlast.AlterScalarType


class DeleteScalarType(
    ScalarTypeCommand,
    s_types.DeleteInheritingType[ScalarType],
):
    astnode = qlast.DropScalarType

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if self.get_orig_attribute_value('expr_type'):
            # This is an alias type, appropriate DDL would be generated
            # from the corresponding DeleteAlias node.
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)
