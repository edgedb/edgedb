##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import base64

from edgedb.lang.common.algos.persistent_hash import persistent_hash

from edgedb.lang.edgeql import ast as qlast

from . import delta as sd
from . import expr
from . import objects as so
from . import name as sn


class NamedPrototypeCommand(sd.PrototypeCommand):
    prototype_name = so.Field(sn.Name)

    @classmethod
    def _protoname_from_ast(cls, astnode, context):
        prototype_name = sn.Name(module=astnode.name.module,
                                 name=astnode.name.name)
        return prototype_name

    @classmethod
    def _protobases_from_ast(cls, astnode, context):
        bases = so.PrototypeList(
            so.PrototypeRef(prototype_name=sn.Name(
                name=b.name, module=b.module
            ))
            for b in getattr(astnode, 'bases', None) or []
        )
        return bases

    @classmethod
    def _cmd_from_ast(cls, astnode, context):
        gpc = getattr(cls, '_get_prototype_class', None)
        if not callable(gpc):
            msg = 'cannot determine prototype_class for {}' \
                        .format(cls.__name__)
            raise NotImplementedError(msg)

        prototype_name = cls._protoname_from_ast(astnode, context)
        prototype_class = gpc()
        cmd = cls(prototype_name=prototype_name,
                  prototype_class=prototype_class)

        return cmd

    def _append_subcmd_ast(cls, node, subcmd, context):
        subnode = subcmd.get_ast(context)
        if subnode is not None:
            node.commands.append(subnode)

    def _get_ast_node(self, context):
        return self.__class__.astnode

    def _get_ast(self, context):
        astnode = self._get_ast_node(context)
        if isinstance(self.prototype_name, sn.Name):
            if hasattr(self.prototype_class, 'normalize_name'):
                nname = self.prototype_class.normalize_name(
                            self.prototype_name)
            else:
                nname = self.prototype_name
            name = qlast.PrototypeRefNode(module=nname.module, name=nname.name)
        else:
            name = qlast.PrototypeRefNode(module='', name=self.prototype_name)

        if astnode.get_field('name'):
            op = astnode(name=name)
        else:
            op = astnode()

        self._apply_fields_ast(context, op)

        return op

    def _set_attribute_ast(self, context, node, name, value):
        if isinstance(value, expr.ExpressionText):
            value = qlast.ExpressionTextNode(expr=str(value))

        as_expr = isinstance(value, qlast.ExpressionTextNode)
        name_ref = qlast.PrototypeRefNode(
            name=name, module='')
        node.commands.append(qlast.CreateAttributeValueNode(
            name=name_ref, value=value, as_expr=as_expr))

    def _drop_attribute_ast(self, context, node, name):
        name_ref = qlast.PrototypeRefNode(name=name, module='')
        node.commands.append(qlast.DropAttributeValueNode(name=name_ref))

    def _apply_fields_ast(self, context, node):
        for op in self(RenameNamedPrototype):
            self._append_subcmd_ast(node, op, context)

        for op in self(sd.AlterPrototypeProperty):
            self._apply_field_ast(context, node, op)

    def _apply_field_ast(self, context, node, op):
        if op.property == 'name':
            pass
        else:
            subnode = op._get_ast(context)
            if subnode is not None:
                node.commands.append(subnode)


class CreateOrAlterNamedPrototype(NamedPrototypeCommand):
    pass


class CreateNamedPrototype(CreateOrAlterNamedPrototype, sd.CreatePrototype):
    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

        cmd.add(
            sd.AlterPrototypeProperty(
                property='name',
                new_value=cmd.prototype_name
            )
        )

        bases = cls._protobases_from_ast(astnode, context)
        cmd.add(
            sd.AlterPrototypeProperty(
                property='bases',
                new_value=bases
            )
        )

        return cmd

    def _apply_field_ast(self, context, node, op):
        if op.property == 'name':
            pass
        elif op.property == 'bases':
            node.bases = [
                qlast.PrototypeRefNode(name=b.prototype_name.name,
                                       module=b.prototype_name.module)
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
        if schema.get(self.prototype_name, default=None,
                      type=self.prototype_class):
            raise ValueError('{!r} already exists in schema'
                                .format(self.prototype_name))

        proto = sd.CreatePrototype.apply(self, schema, context)
        schema.add(proto)

        self._resolve_refs(schema, context, proto)

        return proto

    def __repr__(self):
        return '<%s.%s "%s">' % (self.__class__.__module__,
                                 self.__class__.__name__,
                                 self.prototype_name)


class RenameNamedPrototype(NamedPrototypeCommand):
    astnode = qlast.RenameNode

    new_name = so.Field(sn.Name)

    def __repr__(self):
        return '<%s.%s "%s" to "%s">' % (self.__class__.__module__,
                                         self.__class__.__name__,
                                         self.prototype_name, self.new_name)

    def apply(self, schema, context):
        old_name = self.prototype_name
        prototype = schema.get(self.prototype_name, type=self.prototype_class)
        schema.delete(prototype)
        prototype.name = self.new_name
        schema.add(prototype)

        # Update context
        context_token = context.pop()
        assert context_token
        context_token.proto = prototype
        context.push(context_token)

        self._resolve_refs(schema, context, prototype)

        parent_ctx = context.get(sd.CommandContextToken)
        for subop in parent_ctx.op(NamedPrototypeCommand):
            if subop is not self and subop.prototype_name == old_name:
                subop.prototype_name = self.new_name

        return prototype

    def _get_ast(self, context):
        astnode = self._get_ast_node(context)

        if hasattr(self.prototype_class, 'normalize_name'):
            new_name = self.prototype_class.normalize_name(self.new_name)
        else:
            new_name = self.new_name

        if new_name != self.new_name:
            # Derived name
            name_b32 = base64.b32encode(self.new_name.name.encode()).decode()
            new_nname = '__b32_' + name_b32.replace('=', '_')

            new_name = sn.Name(module=self.new_name.module, name=new_nname)
        else:
            new_name = self.new_name

        ref = qlast.PrototypeRefNode(
            name=new_name.name, module=new_name.module)
        return astnode(new_name=ref)

    @classmethod
    def _cmd_from_ast(cls, astnode, context):
        parent_ctx = context.get(sd.CommandContextToken)
        parent_class = parent_ctx.op.prototype_class
        rename_class = NamedPrototypeMeta.get_rename_command(parent_class)
        return rename_class._rename_cmd_from_ast(astnode, context)

    @classmethod
    def _rename_cmd_from_ast(cls, astnode, context):
        parent_ctx = context.get(sd.CommandContextToken)
        parent_class = parent_ctx.op.prototype_class
        rename_class = NamedPrototypeMeta.get_rename_command(parent_class)

        new_name = astnode.new_name
        if new_name.name.startswith('__b32_'):
            name_b32 = new_name.name[6:].replace('_', '=')
            new_nname = base64.b32decode(name_b32).decode()
            new_name = sn.Name(module=new_name.module, name=new_nname)

        return rename_class(
            prototype_class=parent_class,
            prototype_name=parent_ctx.op.prototype_name,
            new_name=sn.Name(
                module=new_name.module,
                name=new_name.name
            )
        )


class AlterNamedPrototype(CreateOrAlterNamedPrototype):
    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

        added_bases = []
        dropped_bases = []

        if getattr(astnode, 'commands', None):
            for astcmd in astnode.commands:
                if isinstance(astcmd, qlast.AlterDropInheritNode):
                    dropped_bases.extend(
                        so.PrototypeRef(
                            prototype_name=sn.Name(
                                module=b.module,
                                name=b.name
                            )
                        )
                        for b in astcmd.bases
                    )

                elif isinstance(astcmd, qlast.AlterAddInheritNode):
                    bases = [
                        so.PrototypeRef(
                            prototype_name=sn.Name(
                                module=b.module, name=b.name))
                        for b in astcmd.bases
                    ]

                    pos_node = astcmd.position
                    if pos_node.ref is not None:
                        ref = pos_node.ref.module + '::' + \
                                pos_node.ref.name
                        pos = (pos_node.position, ref)
                    else:
                        pos = pos_node.position

                    added_bases.append((bases, pos))

        if added_bases or dropped_bases:
            parent_class = cmd.prototype_class
            rebase_class = NamedPrototypeMeta.get_rebase_command(parent_class)

            cmd.add(
                rebase_class(
                    prototype_class=parent_class,
                    prototype_name=cmd.prototype_name,
                    removed_bases=tuple(dropped_bases),
                    added_bases=tuple(added_bases)
                )
            )

        return cmd

    def _apply_rebase_ast(self, context, node, op):
        from . import inheriting

        parent_ctx = context.get(sd.CommandContextToken)
        parent_op = parent_ctx.op
        rebase = next(iter(parent_op(inheriting.RebaseNamedPrototype)))

        dropped = rebase.removed_bases
        added = rebase.added_bases

        if dropped:
            node.commands.append(
                qlast.AlterDropInheritNode(
                    bases=[
                        qlast.PrototypeRefNode(
                            module=b.prototype_name.module,
                            name=b.prototype_name.name
                        )
                        for b in dropped
                    ]
                )
            )

        for bases, pos in added:
            if isinstance(pos, tuple):
                pos_node = qlast.PositionNode(
                    position=pos[0],
                    ref=qlast.PrototypeRefNode(
                        module=pos[1].prototype_name.module,
                        name=pos[1].prototype_name.name))
            else:
                pos_node = qlast.PositionNode(position=pos)

            node.commands.append(
                qlast.AlterAddInheritNode(
                    bases=[
                        qlast.PrototypeRefNode(
                            module=b.prototype_name.module,
                            name=b.prototype_name.name
                        )
                        for b in bases
                    ],
                    position=pos_node
                )
            )

    def _apply_field_ast(self, context, node, op):
        if op.property in {'is_abstract', 'is_final'}:
            node.commands.append(
                qlast.SetSpecialFieldNode(
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

    def get_context_token(self):
        return self.context_class(self, None)

    def apply(self, schema, context):
        prototype = schema.get(self.prototype_name, type=self.prototype_class)
        self.prototype = prototype

        context_token = context.pop()

        try:
            context_token.original_proto = \
                prototype.__class__.get_canonical_class().copy(prototype)
            context_token.proto = prototype
            self.original_proto = context_token.original_proto
        finally:
            context.push(context_token)

        for op in self(RenameNamedPrototype):
            op.apply(schema, context)

        props = self.get_struct_properties(schema)
        for name, value in props.items():
            setattr(prototype, name, value)

        return prototype


class DeleteNamedPrototype(NamedPrototypeCommand):
    def apply(self, schema, context=None):
        prototype = schema.get(self.prototype_name, type=self.prototype_class)
        self.prototype = prototype
        self.old_prototype = prototype
        schema.delete(prototype)
        schema.drop_inheritance_cache_for_child(prototype)
        return prototype


class NamedPrototypeMeta(type(so.BasePrototype)):
    _rename_map = {}
    _rebase_map = {}

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)
        dd = dct.get('delta_driver')
        if dd is not None:
            rename_cmd = dd.rename
            if rename_cmd:
                ccls = cls.get_canonical_class()
                if ccls not in mcls._rename_map:
                    mcls._rename_map[ccls] = rename_cmd

            rebase_cmd = dd.rebase
            if rebase_cmd:
                ccls = cls.get_canonical_class()
                if ccls not in mcls._rebase_map:
                    mcls._rebase_map[ccls] = rebase_cmd

        return cls

    @classmethod
    def get_rename_command(mcls, objcls):
        cobjcls = objcls.get_canonical_class()
        return mcls._rename_map.get(cobjcls)

    @classmethod
    def get_rebase_command(mcls, objcls):
        cobjcls = objcls.get_canonical_class()
        return mcls._rebase_map.get(cobjcls)


class NamedPrototype(so.BasePrototype, metaclass=NamedPrototypeMeta):
    name = so.Field(sn.Name, private=True, compcoef=0.640)

    def delta_properties(self, delta, other, reverse=False, context=None):
        old, new = (other, self) if not reverse else (self, other)

        if old and new:
            if old.name != new.name:
                delta.add(old.delta_rename(new.name))

        super().delta_properties(delta, other, reverse, context)

    def delta_rename(self, new_name):
        try:
            delta_driver = self.delta_driver
        except AttributeError:
            msg = 'missing required delta driver info for {}'.format(
                    self.__class__.__name__)
            raise AttributeError(msg) from None

        return delta_driver.rename(prototype_name=self.name,
                                   new_name=new_name,
                                   prototype_class=self.get_canonical_class())

    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
        similarity = 1.0

        if (ours is None) != (theirs is None):
            similarity /= 1.2
        elif ours is not None:
            if ours.__class__.get_canonical_class() != \
                        theirs.__class__.get_canonical_class():
                similarity /= 1.4
            elif ours.name != theirs.name:
                similarity /= 1.2

        return similarity

    def __repr__(self):
        cls = self.__class__
        return '<{}.{} "{}" at 0x{:x}>'.format(
                    cls.__module__, cls.__name__, self.name, id(self))

    __str__ = __repr__


class NamedPrototypeList(so.PrototypeList, type=NamedPrototype):
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


class NamedPrototypeSet(so.PrototypeSet, type=NamedPrototype):
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
