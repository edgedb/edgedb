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

from typing import Any, Optional, Tuple, Type, Dict, TYPE_CHECKING

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb import errors

from . import abc as s_abc
from . import constraints
from . import delta as sd
from . import inheriting
from . import name as sn
from . import objects as so
from . import pointers
from . import referencing
from . import rewrites as s_rewrites
from . import sources
from . import types as s_types
from . import utils
from . import expr as s_expr

if TYPE_CHECKING:
    from . import schema as s_schema


class Property(
    pointers.Pointer,
    s_abc.Property,
    qlkind=qltypes.SchemaObjectClass.PROPERTY,
    data_safe=False,
):

    def derive_ref(
        self,
        schema: s_schema.Schema,
        referrer: so.QualifiedObject,
        *qualifiers: str,
        target: Optional[s_types.Type] = None,
        attrs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Tuple[s_schema.Schema, Property]:
        from . import links as s_links
        if target is None:
            target = self.get_target(schema)

        schema, ptr = super().derive_ref(
            schema, referrer, target=target, attrs=attrs, **kwargs)

        ptr_sn = str(ptr.get_shortname(schema))

        if ptr_sn == 'std::source':
            assert isinstance(referrer, s_links.Link)
            schema = ptr.set_field_value(
                schema, 'target', referrer.get_source(schema))
        elif ptr_sn == 'std::target':
            schema = ptr.set_field_value(
                schema, 'target', referrer.get_field_value(schema, 'target'))

        assert isinstance(ptr, Property)
        return schema, ptr

    def compare(
        self,
        other: so.Object,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> float:
        if not isinstance(other, Property):
            if isinstance(other, pointers.Pointer):
                return 0.0
            else:
                raise NotImplementedError

        similarity = super().compare(
            other, our_schema=our_schema,
            their_schema=their_schema, context=context)

        if (
            not self.is_non_concrete(our_schema)
            and not other.is_non_concrete(their_schema)
            and self.issubclass(
                our_schema, our_schema.get('std::source', type=Property)
            )
            and other.issubclass(
                their_schema, their_schema.get('std::source', type=Property)
            )
        ):
            # Make std::source link property ignore differences in its target.
            # This is consistent with skipping the comparison on Pointer.source
            # in general.
            field = self.__class__.get_field('target')
            target_coef = field.type.compare_values(
                self.get_target(our_schema),
                other.get_target(their_schema),
                our_schema=our_schema,
                their_schema=their_schema,
                context=context,
                compcoef=field.compcoef)
            if target_coef < 1:
                similarity *= target_coef
        return similarity

    def should_propagate(self, schema: s_schema.Schema) -> bool:
        # @source and @target link props don't propagate to children
        # because we create new properties with distinct types.
        return not self.is_endpoint_pointer(schema)

    def is_property(self, schema: s_schema.Schema) -> bool:
        return True

    def scalar(self) -> bool:
        return True

    def has_user_defined_properties(self, schema: s_schema.Schema) -> bool:
        return False

    def is_link_property(self, schema: s_schema.Schema) -> bool:
        source = self.get_source(schema)
        if source is None:
            return False
        return isinstance(source, pointers.Pointer)

    def allow_ref_propagation(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> bool:
        source = self.get_source(schema)
        if isinstance(source, pointers.Pointer):
            if source.is_non_concrete(schema):
                return True
            else:
                source = source.get_source(schema)
                assert isinstance(source, s_types.Type)
                return not source.is_view(schema)
        else:
            assert isinstance(source, s_types.Type)
            return not source.is_view(schema)

    @classmethod
    def get_root_classes(cls) -> Tuple[sn.QualName, ...]:
        return (
            sn.QualName(module='std', name='property'),
        )

    @classmethod
    def get_default_base_name(self) -> sn.QualName:
        return sn.QualName('std', 'property')

    def is_blocking_ref(
        self,
        schema: s_schema.Schema,
        reference: so.Object,
    ) -> bool:
        return not self.is_endpoint_pointer(schema)

    def init_delta_command(
        self,
        schema: s_schema.Schema,
        cmdtype: Type[sd.ObjectCommand_T],
        *,
        classname: Optional[sn.Name] = None,
        **kwargs: Any,
    ) -> sd.ObjectCommand_T:
        delta = super().init_delta_command(
            schema=schema,
            cmdtype=cmdtype,
            classname=classname,
            **kwargs,
        )
        assert isinstance(delta, referencing.ReferencedObjectCommandBase)
        delta.is_strong_ref = self.is_special_pointer(schema)
        return delta  # type: ignore


class PropertySourceContext(sources.SourceCommandContext[sources.Source_T]):
    pass


class PropertySourceCommand(
    inheriting.InheritingObjectCommand[sources.Source_T],
):
    pass


class PropertyCommandContext(
    pointers.PointerCommandContext[Property],
    constraints.ConsistencySubjectCommandContext,
    s_rewrites.RewriteCommandContext,
):
    pass


class PropertyCommand(
    pointers.PointerCommand[Property],
    context_class=PropertyCommandContext,
    referrer_context_class=PropertySourceContext,
):

    def validate_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        """Check that property definition is sound."""
        super().validate_object(schema, context)

        scls = self.scls
        if not scls.get_owned(schema):
            return

        if scls.is_special_pointer(schema):
            return

        if (
            scls.is_link_property(schema)
            and not scls.is_pure_computable(schema)
        ):
            # link properties cannot be required or multi
            if self.get_attribute_value('required'):
                raise errors.InvalidPropertyDefinitionError(
                    'link properties cannot be required',
                    span=self.span,
                )
            if (self.get_attribute_value('cardinality')
                    is qltypes.SchemaCardinality.Many):
                raise errors.InvalidPropertyDefinitionError(
                    "multi properties aren't supported for links",
                    span=self.span,
                )

        target_type = scls.get_target(schema)
        if target_type is None:
            raise TypeError(f'missing target type in scls {scls}')

        if target_type.is_polymorphic(schema):
            span = self.get_attribute_span('target')
            raise errors.InvalidPropertyTargetError(
                f'invalid property type: '
                f'{target_type.get_verbosename(schema)} '
                f'is a generic type',
                span=span,
            )

        if (target_type.is_object_type()
                or (isinstance(target_type, s_types.Collection)
                    and target_type.contains_object(schema))):
            span = self.get_attribute_span('target')
            raise errors.InvalidPropertyTargetError(
                f'invalid property type: expected a scalar type, '
                f'or a scalar collection, got '
                f'{target_type.get_verbosename(schema)}',
                span=span,
            )

    def _check_field_errors(self, node: qlast.DDLOperation) -> None:
        for sub in node.commands:
            # do not allow link property on properties
            if isinstance(sub, qlast.CreateConcretePointer):
                raise errors.InvalidDefinitionError(
                    f'cannot place a link property on a property',
                    span=node.span,
                    hint=(
                        'Link properties can only be placed on links, whose '
                        'target types are object types.'
                    ),
                )
            # do not allow on source/target delete on properties
            if isinstance(sub, (qlast.OnSourceDelete, qlast.OnTargetDelete)):
                raise errors.InvalidDefinitionError(
                    f'cannot place a deletion policy on a property',
                    span=node.span,
                    hint=(
                        'Deletion policies can only be placed on links, whose '
                        'target types are object types.'
                    ),
                )


class CreateProperty(
    PropertyCommand,
    pointers.CreatePointer[Property],
):
    astnode = [qlast.CreateConcreteProperty,
               qlast.CreateProperty]

    referenced_astnode = qlast.CreateConcreteProperty

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.CreateConcreteProperty):
            assert isinstance(cmd, PropertyCommand)
            cmd._process_create_or_alter_ast(schema, astnode, context)
            cmd._check_field_errors(astnode)

        return cmd

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if (
            field == 'required'
            and issubclass(astnode, qlast.CreateConcreteProperty)
        ):
            return 'is_required'
        elif (
            field == 'cardinality'
            and issubclass(astnode, qlast.CreateConcreteProperty)
        ):
            return 'cardinality'
        else:
            return super().get_ast_attr_for_field(field, astnode)

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        link = context.get(PropertySourceContext)

        if op.property == 'target' and link:
            if isinstance(node, qlast.CreateConcreteProperty):
                expr: Optional[s_expr.Expression] = (
                    self.get_attribute_value('expr')
                )
                if expr is not None:
                    node.target = expr.parse()
                else:
                    ref = op.new_value
                    assert isinstance(ref, (so.Object, so.ObjectShell))
                    node.target = utils.typeref_to_ast(schema, ref)
            else:
                ref = op.new_value
                assert isinstance(ref, (so.Object, so.ObjectShell))
                node.commands.append(
                    qlast.SetPointerType(
                        value=utils.typeref_to_ast(schema, ref)
                    )
                )
        else:
            super()._apply_field_ast(schema, context, node, op)


class RenameProperty(
    PropertyCommand,
    referencing.RenameReferencedInheritingObject[Property],
):
    pass


class RebaseProperty(
    PropertyCommand,
    referencing.RebaseReferencedInheritingObject[Property],
):
    pass


class SetPropertyType(
    pointers.SetPointerType[Property],
    referrer_context_class=PropertySourceContext,
    field='target',
):
    pass


class AlterPropertyUpperCardinality(
    pointers.AlterPointerUpperCardinality[Property],
    referrer_context_class=PropertySourceContext,
    field='cardinality',
):
    pass


class AlterPropertyLowerCardinality(
    pointers.AlterPointerLowerCardinality[Property],
    referrer_context_class=PropertySourceContext,
    field='required',
):
    pass


class AlterPropertyOwned(
    referencing.AlterOwned[Property],
    pointers.PointerCommandOrFragment[Property],
    referrer_context_class=PropertySourceContext,
    field='owned',
):
    pass


class AlterProperty(
    PropertyCommand,
    pointers.AlterPointer[Property],
):
    astnode = [qlast.AlterConcreteProperty,
               qlast.AlterProperty]

    referenced_astnode = qlast.AlterConcreteProperty

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> AlterProperty:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, AlterProperty)
        if isinstance(astnode, qlast.CreateConcreteProperty):
            cmd._process_create_or_alter_ast(schema, astnode, context)
        else:
            cmd._process_alter_ast(schema, astnode, context)
        cmd._check_field_errors(astnode)
        return cmd

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if op.property == 'target':
            if op.new_value:
                assert isinstance(op.new_value, so.ObjectShell)
                node.commands.append(
                    qlast.SetPointerType(
                        value=utils.typeref_to_ast(schema, op.new_value),
                    ),
                )
        else:
            super()._apply_field_ast(schema, context, node, op)

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if self.maybe_get_object_aux_data('from_alias'):
            # This is an alias type, appropriate DDL would be generated
            # from the corresponding Alter/DeleteAlias node.
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)


class DeleteProperty(
    PropertyCommand,
    pointers.DeletePointer[Property],
):
    astnode = [qlast.DropConcreteProperty,
               qlast.DropProperty]

    referenced_astnode = qlast.DropConcreteProperty

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if self.maybe_get_object_aux_data('from_alias'):
            # This is an alias type, appropriate DDL would be generated
            # from the corresponding Alter/DeleteAlias node.
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)
