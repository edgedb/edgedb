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
from . import sources
from . import types as s_types
from . import utils

if TYPE_CHECKING:
    from . import schema as s_schema


class Property(pointers.Pointer, s_abc.Property,
               qlkind=qltypes.SchemaObjectClass.PROPERTY):

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

        ptr_sn = ptr.get_shortname(schema)

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

        if (not self.generic(our_schema) and
                not other.generic(their_schema) and
                self.issubclass(
                    our_schema, our_schema.get('std::source')) and
                other.issubclass(
                    their_schema, their_schema.get('std::source'))):
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
                similarity /= target_coef
        return similarity

    def is_property(self, schema: s_schema.Schema) -> bool:
        return True

    @classmethod
    def merge_targets(
        cls,
        schema: s_schema.Schema,
        ptr: pointers.Pointer,
        t1: s_types.Type,
        t2: s_types.Type,
        *,
        allow_contravariant: bool = False,
    ) -> Tuple[s_schema.Schema, Optional[s_types.Type]]:
        if ptr.is_endpoint_pointer(schema):
            return schema, t1
        else:
            return super().merge_targets(
                schema,
                ptr,
                t1,
                t2,
                allow_contravariant=allow_contravariant
            )

    def scalar(self) -> bool:
        return True

    def has_user_defined_properties(self, schema: s_schema.Schema) -> bool:
        return False

    def is_link_property(self, schema: s_schema.Schema) -> bool:
        source = self.get_source(schema)
        if source is None:
            raise ValueError(f'{self.get_verbosename(schema)} is abstract')
        return isinstance(source, pointers.Pointer)

    def allow_ref_propagation(
        self,
        schema: s_schema.Schema,
        constext: sd.CommandContext,
        refdict: so.RefDict,
    ) -> bool:
        source = self.get_source(schema)
        if isinstance(source, pointers.Pointer):
            if source.generic(schema):
                return True
            else:
                source = source.get_source(schema)
                assert isinstance(source, s_types.Type)
                return not source.is_view(schema)
        else:
            assert isinstance(source, s_types.Type)
            return not source.is_view(schema)

    @classmethod
    def get_root_classes(cls) -> Tuple[sn.Name, ...]:
        return (
            sn.Name(module='std', name='property'),
        )

    @classmethod
    def get_default_base_name(self) -> sn.Name:
        return sn.Name('std::property')

    def is_blocking_ref(
        self,
        schema: s_schema.Schema,
        reference: so.Object,
    ) -> bool:
        return not self.is_endpoint_pointer(schema)


class PropertySourceContext(sources.SourceCommandContext):
    pass


class PropertySourceCommand(inheriting.InheritingObjectCommand[Property]):
    pass


class PropertyCommandContext(pointers.PointerCommandContext,
                             constraints.ConsistencySubjectCommandContext):
    pass


class PropertyCommand(pointers.PointerCommand,
                      schema_metaclass=Property,
                      context_class=PropertyCommandContext,
                      referrer_context_class=PropertySourceContext):

    def _set_pointer_type(
        self,
        schema: s_schema.Schema,
        astnode: qlast.CreateConcretePointer,
        context: sd.CommandContext,
        target_ref: Union[so.Object, so.ObjectShell],
    ) -> None:
        assert astnode.target is not None
        spt = SetPropertyType(classname=self.classname, type=target_ref)
        spt.set_attribute_value(
            'target', target_ref, source_context=astnode.target.context)
        self.add(spt)

    def _validate_pointer_def(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        """Check that property definition is sound."""
        super()._validate_pointer_def(schema, context)

        scls = self.scls
        if not scls.get_is_local(schema):
            return

        if scls.is_special_pointer(schema):
            return

        if scls.is_link_property(schema):
            # link properties cannot be required or multi
            if self.get_attribute_value('required'):
                raise errors.InvalidPropertyDefinitionError(
                    'link properties cannot be required',
                    context=self.source_context,
                )
            if (self.get_attribute_value('cardinality')
                    is qltypes.SchemaCardinality.MANY):
                raise errors.InvalidPropertyDefinitionError(
                    "multi properties aren't supported for links",
                    context=self.source_context,
                )

        target_type = scls.get_target(schema)
        if target_type is None:
            raise TypeError(f'missing target type in scls {scls}')

        if target_type.is_polymorphic(schema):
            srcctx = self.get_attribute_source_context('target')
            raise errors.InvalidPropertyTargetError(
                f'invalid property type: '
                f'{target_type.get_verbosename(schema)} '
                f'is a generic type',
                context=srcctx,
            )

        if (target_type.is_object_type()
                or (isinstance(target_type, s_types.Collection)
                    and target_type.contains_object(schema))):
            srcctx = self.get_attribute_source_context('target')
            raise errors.InvalidPropertyTargetError(
                f'invalid property type: expected a scalar type, '
                f'or a scalar collection, got '
                f'{target_type.get_verbosename(schema)}',
                context=srcctx,
            )


class CreateProperty(
    PropertyCommand,
    referencing.CreateReferencedInheritingObject[Property],
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
            assert isinstance(cmd, pointers.PointerCommand)
            cmd._process_create_or_alter_ast(schema, astnode, context)
        else:
            # this is an abstract property then
            if cmd.get_attribute_value('default') is not None:
                raise errors.SchemaDefinitionError(
                    f"'default' is not a valid field for an abstract property",
                    context=astnode.context)

        return cmd

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        # type ignore below, because the class is used as mixin
        link = context.get(PropertySourceContext)  # type: ignore

        if op.property == 'required':
            if isinstance(node, qlast.CreateConcreteProperty):
                node.is_required = bool(op.new_value)
            else:
                node.commands.append(
                    qlast.SetSpecialField(
                        name='required',
                        value=op.new_value,
                    ),
                )
        elif op.property == 'cardinality':
            node.cardinality = op.new_value
        elif op.property == 'target' and link:
            if isinstance(node, qlast.CreateConcreteProperty):
                expr = self.get_attribute_value('expr')
                if expr is not None:
                    node.target = expr.qlast
                else:
                    ref = op.new_value
                    assert isinstance(ref, (so.Object, so.ObjectShell))
                    node.target = utils.typeref_to_ast(schema, ref)
            else:
                ref = op.new_value
                assert isinstance(ref, (so.Object, so.ObjectShell))
                node.commands.append(
                    qlast.SetPropertyType(
                        type=utils.typeref_to_ast(schema, ref)
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


class SetPropertyType(pointers.SetPointerType,
                      schema_metaclass=Property,
                      referrer_context_class=PropertySourceContext):

    astnode = qlast.SetPropertyType


class AlterProperty(
    PropertyCommand,
    referencing.AlterReferencedInheritingObject[Property],
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
    ) -> referencing.AlterReferencedInheritingObject[Property]:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.CreateConcreteProperty):
            assert isinstance(cmd, pointers.PointerCommand)
            cmd._process_create_or_alter_ast(schema, astnode, context)

        assert isinstance(cmd, referencing.AlterReferencedInheritingObject)
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
                node.commands.append(qlast.SetLinkType(
                    type=qlast.ObjectRef(
                        name=op.new_value.classname.name,  # type: ignore
                        module=op.new_value.classname.module  # type: ignore
                    )
                ))
        else:
            super()._apply_field_ast(schema, context, node, op)


class DeleteProperty(
    PropertyCommand,
    referencing.DeleteReferencedInheritingObject[Property],
):
    astnode = [qlast.DropConcreteProperty,
               qlast.DropProperty]

    referenced_astnode = qlast.DropConcreteProperty

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.DropConcreteProperty):
            prop = schema.get(cmd.classname, type=Property)
            target = prop.get_target(schema)

            if target is not None and target.is_collection():
                s_types.cleanup_schema_collection(
                    schema, target, prop, cmd, context=context,
                    src_context=astnode.context)

        return cmd
