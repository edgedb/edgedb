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


from edb.lang.edgeql import ast as qlast

from . import constraints
from . import delta as sd
from . import error as s_err
from . import inheriting
from . import name as sn
from . import named
from . import objects as so
from . import pointers
from . import policy
from . import referencing
from . import scalars
from . import sources
from . import types
from . import utils


class Property(pointers.Pointer):
    _type = 'link_property'

    def derive(self, schema, source, target=None, attrs=None, **kwargs):
        if target is None:
            target = self.target

        ptr = super().derive(schema, source, target, attrs=attrs, **kwargs)

        if ptr.shortname == 'std::source':
            ptr.target = source.source

        if ptr.shortname == 'std::target':
            ptr.target = source.target

        return ptr

    def compare(self, other, context):
        if not isinstance(other, Property):
            if isinstance(other, pointers.Pointer):
                return 0.0
            else:
                return NotImplemented

        similarity = super().compare(other, context)
        if (not self.generic() and not other.generic() and
                self.shortname == 'std::source' and
                other.shortname == 'std::source'):
            # Make std::source link property ignore differences in its target.
            # This is consistent with skipping the comparison on Pointer.source
            # in general.
            field = self.__class__.get_field('target')
            target_coef = field.type[0].compare_values(
                self.target, other.target, context, field.compcoef)
            if target_coef < 1:
                similarity /= target_coef
        return similarity

    @classmethod
    def merge_targets(cls, schema, ptr, t1, t2):
        if ptr.is_endpoint_pointer():
            return t1
        else:
            return super().merge_targets(schema, ptr, t1, t2)

    def scalar(self):
        return True

    def has_user_defined_properties(self):
        return False

    def is_link_property(self):
        if self.source is None:
            raise ValueError(f'{self.name} is abstract')
        return isinstance(self.source, pointers.Pointer)

    def copy(self):
        result = super().copy()
        result.source = self.source
        result.target = self.target
        result.default = self.default
        return result

    def finalize(self, schema, bases=None, *, apply_defaults=True, dctx=None):
        super().finalize(schema, bases=bases, apply_defaults=apply_defaults,
                         dctx=dctx)

        if not self.generic() and apply_defaults:
            if self.cardinality is None:
                self.cardinality = pointers.PointerCardinality.OneToOne

                if dctx is not None:
                    from . import delta as sd

                    dctx.current().op.add(sd.AlterObjectProperty(
                        property='cardinality',
                        new_value=self.cardinality,
                        source='default'
                    ))

    @classmethod
    def get_root_classes(cls):
        return (
            sn.Name(module='std', name='property'),
        )

    @classmethod
    def get_default_base_name(self):
        return 'std::property'


class PropertySourceContext(sources.SourceCommandContext):
    pass


class PropertySourceCommand(referencing.ReferencingObjectCommand):
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
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        modaliases = context.modaliases

        if isinstance(astnode, qlast.CreateConcreteProperty):
            target = getattr(astnode, 'target', None)

            cmd.add(
                sd.AlterObjectProperty(
                    property='required',
                    new_value=astnode.is_required
                )
            )

            parent_ctx = context.get(PropertySourceContext)
            source_name = parent_ctx.op.classname

            cmd.add(
                sd.AlterObjectProperty(
                    property='source',
                    new_value=so.ObjectRef(
                        classname=source_name
                    )
                )
            )

            if isinstance(target, qlast.TypeName):
                target_ref = utils.ast_to_typeref(
                    target, modaliases=modaliases, schema=schema)
            else:
                # computable
                target_ref = cmd._parse_computable(
                    target, schema, context)

            target_type = utils.resolve_typeref(target_ref, schema=schema)

            if not isinstance(target_type, (scalars.ScalarType,
                                            types.Collection)):
                raise s_err.SchemaDefinitionError(
                    f'invalid property target, expected primitive type, '
                    f'got {target_type.displayname!r}',
                    context=target.context
                )

            cmd.add(
                sd.AlterObjectProperty(
                    property='target',
                    new_value=target_ref,
                )
            )

            cls._parse_default(cmd)

        return cmd

    def _get_ast_node(self, context):
        link = context.get(PropertySourceContext)
        if link:
            return qlast.CreateConcreteProperty
        else:
            return qlast.CreateProperty

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)

    def _apply_field_ast(self, context, node, op):
        link = context.get(PropertySourceContext)

        if op.property == 'is_derived':
            pass
        elif op.property == 'default':
            self._encode_default(context, node, op)
        elif op.property == 'required':
            node.is_required = op.new_value
        elif op.property == 'source':
            pass
        elif op.property == 'target' and link:
            node.target = utils.typeref_to_ast(op.new_value)
        else:
            super()._apply_field_ast(context, node, op)


class RenameProperty(PropertyCommand, named.RenameNamedObject):
    pass


class RebaseProperty(PropertyCommand, inheriting.RebaseNamedObject):
    pass


class AlterProperty(PropertyCommand, inheriting.AlterInheritingObject):
    astnode = [qlast.AlterConcreteProperty,
               qlast.AlterProperty]

    def _get_ast_node(self, context):
        objtype = context.get(PropertySourceContext)

        if objtype:
            return qlast.AlterConcreteProperty
        else:
            return qlast.AlterProperty

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)

    def _apply_field_ast(self, context, node, op):
        if op.property == 'target':
            if op.new_value:
                node.commands.append(qlast.AlterTarget(
                    targets=[
                        qlast.ObjectRef(
                            name=op.new_value.classname.name,
                            module=op.new_value.classname.module)
                    ]
                ))
        elif op.property == 'source':
            pass
        else:
            super()._apply_field_ast(context, node, op)


class DeleteProperty(PropertyCommand, inheriting.DeleteInheritingObject):
    astnode = [qlast.DropConcreteProperty,
               qlast.DropProperty]

    def _get_ast_node(self, context):
        objtype = context.get(PropertySourceContext)

        if objtype:
            return qlast.DropConcreteProperty
        else:
            return qlast.DropProperty

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)

        for op in self.get_subcommands(type=constraints.ConstraintCommand):
            self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=policy.PolicyCommand):
            self._append_subcmd_ast(node, op, context)
