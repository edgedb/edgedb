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


import base64

from edb.lang.common.persistent_hash import persistent_hash

from edb.lang.edgeql import ast as qlast

from . import delta as sd
from . import expr
from . import objects as so
from . import name as sn
from . import error as s_err


NamedObject = so.NamedObject


class NamedObjectList(so.ObjectList, type=NamedObject):
    def get_names(self):
        return tuple(ref.name for ref in self)

    def persistent_hash(self):
        return persistent_hash(self.get_names())

    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
        our_names = ours.get_names() if ours else tuple()
        their_names = theirs.get_names() if theirs else tuple()

        if frozenset(our_names) != frozenset(their_names):
            return compcoef
        else:
            return 1.0


class NamedObjectSet(so.ObjectSet, type=NamedObject):
    def get_names(self):
        return frozenset(ref.name for ref in self)

    def persistent_hash(self):
        return persistent_hash(self.get_names())

    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
        our_names = ours.get_names() if ours else frozenset()
        their_names = theirs.get_names() if theirs else frozenset()

        if our_names != their_names:
            return compcoef
        else:
            return 1.0


class NamedObjectCommand(sd.ObjectCommand):
    classname = so.Field(sn.Name)

    @classmethod
    def _get_ast_name(cls, astnode, context, schema):
        return astnode.name.name

    @classmethod
    def _classname_from_ast(cls, astnode, context, schema):
        nqname = cls._get_ast_name(astnode, context, schema)
        module = context.modaliases.get(astnode.name.module,
                                        astnode.name.module)
        if module is None:
            raise s_err.SchemaDefinitionError(
                f'unqualified name and no default module set',
                context=astnode.name.context
            )

        return sn.Name(module=module, name=nqname)

    @classmethod
    def _cmd_from_ast(cls, astnode, context, schema):
        classname = cls._classname_from_ast(astnode, context, schema)
        return cls(classname=classname)

    def _append_subcmd_ast(cls, node, subcmd, context):
        subnode = subcmd.get_ast(context)
        if subnode is not None:
            node.commands.append(subnode)

    def _get_ast_node(self, context):
        return self.__class__.astnode

    def _get_ast(self, context):
        metaclass = self.get_schema_metaclass()
        astnode = self._get_ast_node(context)
        if isinstance(self.classname, sn.Name):
            if hasattr(metaclass, 'get_shortname'):
                nname = metaclass.get_shortname(self.classname)
            else:
                nname = self.classname
            name = qlast.ObjectRef(module=nname.module, name=nname.name)
        else:
            name = qlast.ObjectRef(module='', name=self.classname)

        if astnode.get_field('name'):
            op = astnode(name=name)
        else:
            op = astnode()

        self._apply_fields_ast(context, op)

        return op

    def _set_attribute_ast(self, context, node, name, value):
        if isinstance(value, expr.ExpressionText):
            value = qlast.ExpressionText(expr=str(value))

        as_expr = isinstance(value, qlast.ExpressionText)
        name_ref = qlast.ObjectRef(
            name=name, module='')
        node.commands.append(qlast.CreateAttributeValue(
            name=name_ref, value=value, as_expr=as_expr))

    def _drop_attribute_ast(self, context, node, name):
        name_ref = qlast.ObjectRef(name=name, module='')
        node.commands.append(qlast.DropAttributeValue(name=name_ref))

    def _apply_fields_ast(self, context, node):
        for op in self.get_subcommands(type=RenameNamedObject):
            self._append_subcmd_ast(node, op, context)

        for op in self.get_subcommands(type=sd.AlterObjectProperty):
            self._apply_field_ast(context, node, op)

    def _apply_field_ast(self, context, node, op):
        if op.property == 'name':
            pass
        else:
            subnode = op._get_ast(context)
            if subnode is not None:
                node.commands.append(subnode)

    def _add_to_schema(self, schema):
        metaclass = self.get_schema_metaclass()
        if schema.get(self.classname, default=None, type=metaclass):
            raise ValueError(f'{self.classname!r} already exists in schema')
        schema = schema.add(self.scls)
        return schema

    def _create_begin(self, schema, context):
        schema = super()._create_begin(schema, context)
        return self._add_to_schema(schema)


class CreateOrAlterNamedObject(NamedObjectCommand):
    pass


class CreateNamedObject(CreateOrAlterNamedObject, sd.CreateObject):
    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        cmd.add(
            sd.AlterObjectProperty(
                property='name',
                new_value=cmd.classname
            )
        )

        return cmd

    def _apply_field_ast(self, context, node, op):
        if op.property == 'name':
            pass
        elif op.property == 'bases':
            node.bases = [
                qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name=b.classname.name,
                        module=b.classname.module
                    )
                )
                for b in op.new_value
            ]
        elif op.property == 'mro':
            pass
        elif op.property == 'is_abstract':
            node.is_abstract = op.new_value
        elif op.property == 'is_final':
            node.is_final = op.new_value
        else:
            super()._apply_field_ast(context, node, op)

    def apply(self, schema, context):
        metaclass = self.get_schema_metaclass()
        if schema.get(self.classname, default=None, type=metaclass):
            raise ValueError(f'{self.classname!r} already exists in schema')

        # apply will add to the schema
        return sd.CreateObject.apply(self, schema, context)

    def __repr__(self):
        return '<%s.%s "%s">' % (self.__class__.__module__,
                                 self.__class__.__name__,
                                 self.classname)


class RenameNamedObject(NamedObjectCommand):
    _delta_action = 'rename'

    astnode = qlast.Rename

    new_name = so.Field(sn.Name)

    def __repr__(self):
        return '<%s.%s "%s" to "%s">' % (self.__class__.__module__,
                                         self.__class__.__name__,
                                         self.classname, self.new_name)

    def _rename_begin(self, schema, context, scls):
        schema = schema.drop_inheritance_cache(scls)
        schema = schema.drop_inheritance_cache_for_child(scls)

        self.old_name = self.classname
        schema = schema.delete(scls)
        scls.name = self.new_name
        schema = schema.add(scls)

        parent_ctx = context.get(sd.CommandContextToken)
        for subop in parent_ctx.op.get_subcommands(type=NamedObjectCommand):
            if subop is not self and subop.classname == self.old_name:
                subop.classname = self.new_name

        return schema

    def _rename_innards(self, schema, context, scls):
        return schema

    def _rename_finalize(self, schema, context, scls):
        return schema

    def apply(self, schema, context):
        metaclass = self.get_schema_metaclass()
        scls = schema.get(self.classname, type=metaclass)
        self.scls = scls

        schema = self._rename_begin(schema, context, scls)
        schema = self._rename_innards(schema, context, scls)
        schema = self._rename_finalize(schema, context, scls)

        return schema, scls

    def _get_ast(self, context):
        astnode = self._get_ast_node(context)
        metaclass = self.get_schema_metaclass()

        if hasattr(metaclass, 'get_shortname'):
            new_name = metaclass.get_shortname(self.new_name)
        else:
            new_name = self.new_name

        if new_name != self.new_name:
            # Derived name
            name_b32 = base64.b32encode(self.new_name.name.encode()).decode()
            new_nname = '__b32_' + name_b32.replace('=', '_')

            new_name = sn.Name(module=self.new_name.module, name=new_nname)
        else:
            new_name = self.new_name

        ref = qlast.ObjectRef(
            name=new_name.name, module=new_name.module)
        return astnode(new_name=ref)

    @classmethod
    def _cmd_from_ast(cls, astnode, context, schema):
        parent_ctx = context.get(sd.CommandContextToken)
        parent_class = parent_ctx.op.get_schema_metaclass()
        rename_class = sd.ObjectCommandMeta.get_command_class(
            RenameNamedObject, parent_class)
        return rename_class._rename_cmd_from_ast(astnode, context)

    @classmethod
    def _rename_cmd_from_ast(cls, astnode, context):
        parent_ctx = context.get(sd.CommandContextToken)
        parent_class = parent_ctx.op.get_schema_metaclass()
        rename_class = sd.ObjectCommandMeta.get_command_class(
            RenameNamedObject, parent_class)

        new_name = astnode.new_name
        if new_name.name.startswith('__b32_'):
            name_b32 = new_name.name[6:].replace('_', '=')
            new_nname = base64.b32decode(name_b32).decode()
            new_name = sn.Name(module=new_name.module, name=new_nname)

        return rename_class(
            metaclass=parent_class,
            classname=parent_ctx.op.classname,
            new_name=sn.Name(
                module=new_name.module,
                name=new_name.name
            )
        )


class AlterNamedObject(CreateOrAlterNamedObject, sd.AlterObject):
    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        added_bases = []
        dropped_bases = []

        if getattr(astnode, 'commands', None):
            for astcmd in astnode.commands:
                if isinstance(astcmd, qlast.AlterDropInherit):
                    dropped_bases.extend(
                        so.ObjectRef(
                            classname=sn.Name(
                                module=b.module,
                                name=b.name
                            )
                        )
                        for b in astcmd.bases
                    )

                elif isinstance(astcmd, qlast.AlterAddInherit):
                    bases = [
                        so.ObjectRef(
                            classname=sn.Name(
                                module=b.module, name=b.name))
                        for b in astcmd.bases
                    ]

                    pos_node = astcmd.position
                    if pos_node.ref is not None:
                        ref = pos_node.ref.module + '::' + pos_node.ref.name
                        pos = (pos_node.position, ref)
                    else:
                        pos = pos_node.position

                    added_bases.append((bases, pos))

        if added_bases or dropped_bases:
            from . import inheriting

            parent_class = cmd.get_schema_metaclass()
            rebase_class = sd.ObjectCommandMeta.get_command_class(
                inheriting.RebaseNamedObject, parent_class)

            cmd.add(
                rebase_class(
                    metaclass=parent_class,
                    classname=cmd.classname,
                    removed_bases=tuple(dropped_bases),
                    added_bases=tuple(added_bases)
                )
            )

        return cmd

    def _apply_rebase_ast(self, context, node, op):
        from . import inheriting

        parent_ctx = context.get(sd.CommandContextToken)
        parent_op = parent_ctx.op
        rebase = next(iter(parent_op.get_subcommands(
            type=inheriting.RebaseNamedObject)))

        dropped = rebase.removed_bases
        added = rebase.added_bases

        if dropped:
            node.commands.append(
                qlast.AlterDropInherit(
                    bases=[
                        qlast.ObjectRef(
                            module=b.classname.module,
                            name=b.classname.name
                        )
                        for b in dropped
                    ]
                )
            )

        for bases, pos in added:
            if isinstance(pos, tuple):
                pos_node = qlast.Position(
                    position=pos[0],
                    ref=qlast.ObjectRef(
                        module=pos[1].classname.module,
                        name=pos[1].classname.name))
            else:
                pos_node = qlast.Position(position=pos)

            node.commands.append(
                qlast.AlterAddInherit(
                    bases=[
                        qlast.ObjectRef(
                            module=b.classname.module,
                            name=b.classname.name
                        )
                        for b in bases
                    ],
                    position=pos_node
                )
            )

    def _apply_field_ast(self, context, node, op):
        if op.property in {'is_abstract', 'is_final'}:
            node.commands.append(
                qlast.SetSpecialField(
                    name=op.property,
                    value=op.new_value
                )
            )
        elif op.property == 'bases':
            self._apply_rebase_ast(context, node, op)
        else:
            super()._apply_field_ast(context, node, op)

    def _get_ast(self, context):
        node = super()._get_ast(context)
        if (node is not None and hasattr(node, 'commands') and
                not node.commands):
            # Alter node without subcommands.  Occurs when all
            # subcommands have been filtered out of DDL stream,
            # so filter it out as well.
            node = None
        return node

    def _alter_begin(self, schema, context, scls):
        for op in self.get_subcommands(type=RenameNamedObject):
            schema, _ = op.apply(schema, context)

        props = self.get_struct_properties(schema)
        for name, value in props.items():
            setattr(scls, name, value)

        return schema

    def _alter_innards(self, schema, context, scls):
        return schema

    def _alter_finalize(self, schema, context, scls):
        return schema

    def apply(self, schema, context):
        metaclass = self.get_schema_metaclass()
        scls = schema.get(self.classname, type=metaclass)
        self.scls = scls

        with self.new_context(context) as ctx:
            ctx.original_class = \
                scls.__class__.get_canonical_class().copy(scls)

            schema = self._alter_begin(schema, context, scls)
            schema = self._alter_innards(schema, context, scls)
            schema = self._alter_finalize(schema, context, scls)

        return schema, scls


class DeleteNamedObject(NamedObjectCommand, sd.DeleteObject):
    def _delete_begin(self, schema, context, scls):
        return schema

    def _delete_innards(self, schema, context, scls):
        return schema

    def _delete_finalize(self, schema, context, scls):
        schema = schema.delete(scls)
        return schema

    def apply(self, schema, context=None):
        metaclass = self.get_schema_metaclass()
        scls = schema.get(self.classname, type=metaclass)
        self.scls = scls
        self.old_class = scls

        with self.new_context(context) as ctx:
            ctx.original_class = scls

            schema = self._delete_begin(schema, context, scls)
            schema = self._delete_innards(schema, context, scls)
            schema = self._delete_finalize(schema, context, scls)

        return schema, scls
