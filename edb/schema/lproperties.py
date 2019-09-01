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


class Property(pointers.Pointer, s_abc.Property):

    def derive(self, schema, source, target=None, attrs=None, **kwargs):
        if target is None:
            target = self.get_target(schema)

        schema, ptr = super().derive(
            schema, source, target, attrs=attrs, **kwargs)

        ptr_sn = ptr.get_shortname(schema)

        if ptr_sn == 'std::source':
            schema = ptr.set_field_value(
                schema, 'target', source.get_source(schema))
        elif ptr_sn == 'std::target':
            schema = ptr.set_field_value(
                schema, 'target', source.get_field_value(schema, 'target'))

        return schema, ptr

    def compare(self, other, *, our_schema, their_schema, context=None):
        if not isinstance(other, Property):
            if isinstance(other, pointers.Pointer):
                return 0.0
            else:
                return NotImplemented

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

    def is_property(self, schema):
        return True

    @classmethod
    def merge_targets(cls, schema, ptr, t1, t2):
        if ptr.is_endpoint_pointer(schema):
            return schema, t1
        else:
            return super().merge_targets(schema, ptr, t1, t2)

    def scalar(self):
        return True

    def has_user_defined_properties(self, schema):
        return False

    def is_link_property(self, schema):
        source = self.get_source(schema)
        if source is None:
            raise ValueError(f'{self.get_verbosename(schema)} is abstract')
        return isinstance(source, pointers.Pointer)

    @classmethod
    def get_root_classes(cls):
        return (
            sn.Name(module='std', name='property'),
        )

    @classmethod
    def get_default_base_name(self):
        return sn.Name('std::property')

    def is_blocking_ref(self, schema, reference):
        return not self.is_endpoint_pointer(schema)


class PropertySourceContext(sources.SourceCommandContext):
    pass


class PropertySourceCommand(inheriting.InheritingObjectCommand):
    pass


class PropertyCommandContext(pointers.PointerCommandContext,
                             constraints.ConsistencySubjectCommandContext):
    pass


class PropertyCommand(pointers.PointerCommand,
                      schema_metaclass=Property,
                      context_class=PropertyCommandContext,
                      referrer_context_class=PropertySourceContext):
    pass


class CreateProperty(PropertyCommand,
                     referencing.CreateReferencedInheritingObject):
    astnode = [qlast.CreateConcreteProperty,
               qlast.CreateProperty]

    referenced_astnode = qlast.CreateConcreteProperty

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        modaliases = context.modaliases

        if isinstance(astnode, qlast.CreateConcreteProperty):
            target = getattr(astnode, 'target', None)

            cmd.add(
                sd.AlterObjectProperty(
                    property='required',
                    new_value=astnode.is_required or False
                )
            )

            cmd.add(
                sd.AlterObjectProperty(
                    property='cardinality',
                    new_value=astnode.cardinality or qltypes.Cardinality.ONE
                )
            )

            parent_ctx = context.get(PropertySourceContext)
            source_name = parent_ctx.op.classname

            cmd.add(
                sd.AlterObjectProperty(
                    property='source',
                    new_value=so.ObjectRef(
                        name=source_name
                    )
                )
            )

            if isinstance(target, qlast.TypeName):
                target_ref = utils.ast_to_typeref(
                    target, modaliases=modaliases, schema=schema,
                    metaclass=s_types.Type)
            else:
                # computable
                target_ref, base = cmd._parse_computable(
                    target, schema, context)

                if base is not None:
                    cmd.set_attribute_value(
                        'bases', so.ObjectList.create(schema, [base]),
                    )

                    cmd.set_attribute_value(
                        'derived_from', base
                    )

                    cmd.set_attribute_value(
                        'is_derived', True
                    )

                    if context.declarative:
                        cmd.set_attribute_value(
                            'declared_inherited', True
                        )

            target_type = utils.resolve_typeref(target_ref, schema=schema)

            if target_type.is_polymorphic(schema):
                raise errors.InvalidPropertyTargetError(
                    f'invalid property type: '
                    f'{target_type.get_displayname(schema)!r} '
                    f'is a generic type',
                    context=target.context
                )

            if (target_type.is_object_type()
                    or (target_type.is_collection()
                        and target_type.contains_object(schema))):
                raise errors.InvalidPropertyTargetError(
                    f'invalid property type: expected a scalar type, '
                    f'or a scalar collection, got '
                    f'{target_type.get_displayname(schema)!r}',
                    context=target.context
                )

            if target_type.is_collection():
                sd.ensure_schema_collection(
                    schema, target_type, cmd,
                    src_context=target.context,
                    context=context,
                )

            cmd.add(
                sd.AlterObjectProperty(
                    property='target',
                    new_value=target_ref,
                )
            )

            cls._parse_default(cmd)

        else:
            # this is an abstract property then
            if cmd.get_attribute_value('default') is not None:
                raise errors.SchemaDefinitionError(
                    f"'default' is not a valid field for an abstact property",
                    context=astnode.context)

        return cmd

    def _get_ast_node(self, context):
        link = context.get(PropertySourceContext)
        if link:
            return qlast.CreateConcreteProperty
        else:
            return qlast.CreateProperty

    def _apply_fields_ast(self, schema, context, node):
        super()._apply_fields_ast(schema, context, node)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(schema, node, op, context)

    def _apply_field_ast(self, schema, context, node, op):
        link = context.get(PropertySourceContext)

        if op.property == 'is_derived':
            pass
        elif op.property == 'derived_from':
            pass
        elif op.property == 'default':
            self._encode_default(schema, context, node, op)
        elif op.property == 'required':
            node.is_required = op.new_value
        elif op.property == 'cardinality':
            node.cardinality = op.new_value
        elif op.property == 'source':
            pass
        elif op.property == 'target' and link:
            node.target = utils.typeref_to_ast(schema, op.new_value)
        else:
            super()._apply_field_ast(schema, context, node, op)


class RenameProperty(PropertyCommand,
                     referencing.RenameReferencedInheritingObject):
    pass


class RebaseProperty(PropertyCommand, inheriting.RebaseInheritingObject):
    pass


class SetPropertyType(pointers.SetPointerType,
                      schema_metaclass=Property,
                      referrer_context_class=PropertySourceContext):

    astnode = qlast.SetPropertyType


class AlterProperty(PropertyCommand,
                    referencing.AlterReferencedInheritingObject):
    astnode = [qlast.AlterConcreteProperty,
               qlast.AlterProperty]

    referenced_astnode = qlast.AlterConcreteProperty

    def _get_ast_node(self, context):
        objtype = context.get(PropertySourceContext)

        if objtype:
            return qlast.AlterConcreteProperty
        else:
            return qlast.AlterProperty

    def _apply_fields_ast(self, schema, context, node):
        super()._apply_fields_ast(schema, context, node)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(schema, node, op, context)

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'target':
            if op.new_value:
                node.commands.append(qlast.SetType(
                    type=qlast.ObjectRef(
                        name=op.new_value.classname.name,
                        module=op.new_value.classname.module
                    )
                ))
        elif op.property == 'source':
            pass
        else:
            super()._apply_field_ast(schema, context, node, op)


class DeleteProperty(PropertyCommand, inheriting.DeleteInheritingObject):
    astnode = [qlast.DropConcreteProperty,
               qlast.DropProperty]

    referenced_astnode = qlast.DropConcreteProperty

    def _get_ast_node(self, context):
        objtype = context.get(PropertySourceContext)

        if objtype:
            return qlast.DropConcreteProperty
        else:
            return qlast.DropProperty

    def _apply_fields_ast(self, schema, context, node):
        super()._apply_fields_ast(schema, context, node)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(schema, node, op, context)

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if isinstance(astnode, qlast.DropConcreteProperty):
            prop = schema.get(cmd.classname)
            target = prop.get_target(schema)

            if target.is_collection():
                sd.cleanup_schema_collection(
                    schema, target, prop, cmd, context=context,
                    src_context=astnode.context)

        return cmd
