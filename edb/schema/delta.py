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
from typing import *  # NoQA

import base64
import collections
import collections.abc
import uuid

import immutables as immu

from edb import errors

from edb.common import adapter
from edb.edgeql import ast as qlast

from edb.common import checked, markup, ordered, struct

from . import expr as s_expr
from . import name as sn
from . import objects as so
from . import schema as s_schema
from . import utils


if TYPE_CHECKING:
    from . import types as s_types


class CommandMeta(adapter.Adapter, struct.MixedStructMeta,
                  markup.MarkupCapableMeta):

    _astnode_map = {}

    def __new__(mcls, name, bases, dct, *, context_class=None, **kwargs):
        cls = super().__new__(mcls, name, bases, dct, **kwargs)

        if context_class is not None:
            cls._context_class = context_class

        return cls

    def __init__(cls, name, bases, clsdict, *, adapts=None, **kwargs):
        adapter.Adapter.__init__(cls, name, bases, clsdict, adapts=adapts)
        struct.MixedStructMeta.__init__(cls, name, bases, clsdict)
        astnodes = clsdict.get('astnode')
        if astnodes and not isinstance(astnodes, (list, tuple)):
            astnodes = [astnodes]
        if astnodes:
            cls.register_astnodes(astnodes)

    def register_astnodes(cls, astnodes):
        mapping = type(cls)._astnode_map

        for astnode in astnodes:
            existing = mapping.get(astnode)
            if existing:
                msg = ('duplicate EdgeQL AST node to command mapping: ' +
                       '{!r} is already declared for {!r}')
                raise TypeError(msg.format(astnode, existing))

            mapping[astnode] = cls


_void = object()


class Command(struct.MixedStruct, metaclass=CommandMeta):
    """Abstract base class for all delta commands."""

    source_context = struct.Field(object, default=None)
    canonical = struct.Field(bool, default=False)

    _context_class = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ops = ordered.OrderedSet()
        self.before_ops = ordered.OrderedSet()

    def copy(self):
        result = super().copy()
        result.ops = ordered.OrderedSet(
            op.copy() for op in self.ops)
        result.before_ops = ordered.OrderedSet(
            op.copy() for op in self.before_ops)
        return result

    @classmethod
    def adapt(cls, obj):
        result = obj.copy_with_class(cls)
        for op in obj.get_subcommands():
            result.ops.add(type(cls).adapt(op))
        return result

    def _resolve_type_ref(self, ref, schema):
        return utils.resolve_typeref(ref, schema)

    def _resolve_attr_value(self, value, fname, field, schema):
        ftype = field.type

        if isinstance(ftype, so.ObjectMeta):
            value = self._resolve_type_ref(value, schema)

        elif issubclass(ftype, checked.CheckedDict):
            if issubclass(ftype.valuetype, so.Object):
                vals = {}

                for k, val in value.items():
                    vals[k] = self._resolve_type_ref(val, schema)

                value = ftype(vals)
            else:
                value = field.coerce_value(schema, value)

        elif issubclass(ftype, (checked.AbstractCheckedList,
                                checked.AbstractCheckedSet)):
            if issubclass(ftype.type, so.Object):
                vals = []

                for val in value:
                    vals.append(self._resolve_type_ref(val, schema))

                value = ftype(vals)
            else:
                value = field.coerce_value(schema, value)

        elif issubclass(ftype, so.ObjectDict):
            value = ftype.create(schema, dict(value.items(schema)))

        elif issubclass(ftype, so.ObjectCollection):
            value = ftype.create(schema, value.objects(schema))

        elif issubclass(ftype, s_expr.Expression):
            if value is not None:
                value = ftype.from_expr(value, schema)

        else:
            value = field.coerce_value(schema, value)

        return value

    def get_struct_properties(self, schema):
        result = {}
        metaclass = self.get_schema_metaclass()

        for op in self.get_subcommands(type=AlterObjectProperty):
            field = metaclass.get_field(op.property)
            if field is None:
                raise errors.SchemaDefinitionError(
                    f'got AlterObjectProperty command for '
                    f'invalid field: {metaclass.__name__}.{op.property}')

            val = self._resolve_attr_value(
                op.new_value, op.property, field, schema)

            result[op.property] = val

        return result

    def has_attribute_value(self, attr_name):
        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.property == attr_name:
                return True
        return False

    def get_attribute_set_cmd(self, attr_name):
        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.property == attr_name:
                return op

    def get_attribute_value(self, attr_name):
        op = self.get_attribute_set_cmd(attr_name)
        if op is not None:
            return op.new_value
        else:
            return None

    def get_local_attribute_value(self, attr_name):
        """Return the new value of field, if not inherited."""
        op = self.get_attribute_set_cmd(attr_name)
        if op is not None and op.source != 'inheritance':
            return op.new_value
        else:
            return None

    def get_attribute_source_context(self, attr_name):
        op = self.get_attribute_set_cmd(attr_name)
        if op is not None:
            return op.source_context
        else:
            return None

    def set_attribute_value(self, attr_name, value, *, inherited=False,
                            source_context=None):
        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.property == attr_name:
                op.new_value = value
                if inherited:
                    op.source = 'inheritance'
                if source_context is not None:
                    op.source_context = source_context
                break
        else:
            op = AlterObjectProperty(property=attr_name, new_value=value)
            if inherited:
                op.source = 'inheritance'
            if source_context is not None:
                op.source_context = source_context

            self.add(op)

    def discard_attribute(self, attr_name):
        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.property == attr_name:
                self.discard(op)
                return

    def __iter__(self):
        for op in self.ops:
            yield from op.before_ops
            yield op

    def get_subcommands(self, *, type=None):
        if type is not None:
            return filter(lambda i: isinstance(i, type), self)
        else:
            return list(self)

    def has_subcommands(self):
        return bool(self.ops)

    def after(self, command):
        if isinstance(command, CommandGroup):
            for op in command:
                self.before_ops.add(op)
        else:
            self.before_ops.add(command)

    def prepend(self, command):
        if isinstance(command, CommandGroup):
            for op in reversed(command.get_subcommands()):
                self.ops.add(op, last=False)
        else:
            self.ops.add(command, last=False)

    def add(self, command):
        if isinstance(command, CommandGroup):
            self.update(command)
        else:
            self.ops.add(command)

    def update(self, commands):
        for command in commands:
            self.add(command)

    def replace(self, commands):
        self.ops.clear()
        self.ops.update(commands)

    def discard(self, command):
        self.ops.discard(command)

    def apply(self, schema, context):
        return schema, None

    def get_ast(self, schema, context):
        with self.new_context(schema, context):
            return self._get_ast(schema, context)

    @classmethod
    def command_for_ast_node(cls, astnode, schema, context):
        cmdcls = type(cls)._astnode_map.get(type(astnode))
        if hasattr(cmdcls, '_command_for_ast_node'):
            # Delegate the choice of command class to the specific command.
            cmdcls = cmdcls._command_for_ast_node(astnode, schema, context)

        return cmdcls

    @classmethod
    def from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.Base,
        *,
        context: Optional[CommandContext]=None,
    ) -> Command:

        if context is None:
            context = CommandContext()

        cmdcls = cls.command_for_ast_node(
            astnode, schema=schema, context=context)

        if cmdcls is None:
            msg = 'cannot find command for ast node {!r}'.format(astnode)
            raise TypeError(msg)

        context_class = cmdcls.get_context_class()
        if context_class is not None:
            modaliases = cmdcls._modaliases_from_ast(schema, astnode, context)
            with context(context_class(schema, op=None,
                                       modaliases=modaliases)):
                cmd = cmdcls._cmd_tree_from_ast(schema, astnode, context)
        else:
            cmd = cmdcls._cmd_tree_from_ast(schema, astnode, context)

        return cmd

    @classmethod
    def _modaliases_from_ast(cls, schema, astnode, context):
        modaliases = {}
        for alias in astnode.aliases:
            if isinstance(alias, qlast.ModuleAliasDecl):
                modaliases[alias.alias] = alias.module

        return modaliases

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = cls._cmd_from_ast(schema, astnode, context)
        cmd.source_context = astnode.context
        ctx = context.current()
        if ctx is not None and type(ctx) is cls.get_context_class():
            ctx.op = cmd

        if getattr(astnode, 'commands', None):
            for subastnode in astnode.commands:
                subcmd = Command.from_ast(schema, subastnode, context=context)
                if subcmd is not None:
                    cmd.add(subcmd)

        return cmd

    @classmethod
    def _cmd_from_ast(cls, schema, astnode, context):
        return cls()

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = markup.elements.lang.TreeNode(name=str(self))

        for dd in self:
            if isinstance(dd, AlterObjectProperty):
                diff = markup.elements.doc.ValueDiff(
                    before=repr(dd.old_value), after=repr(dd.new_value))

                if dd.source == 'inheritance':
                    diff.comment = 'inherited'

                node.add_child(label=dd.property, node=diff)
            else:
                node.add_child(node=markup.serialize(dd, ctx=ctx))

        return node

    @classmethod
    def get_context_class(cls):
        return cls._context_class

    def new_context(self, schema, context, scls=_void):
        if context is None:
            context = CommandContext()

        if scls is _void:
            scls = getattr(self, 'scls', None)

        context_class = self.get_context_class()
        return context(context_class(schema, self, scls))

    def __str__(self) -> str:
        return struct.MixedStruct.__str__(self)

    def __repr__(self) -> str:
        flds = struct.MixedStruct.__repr__(self)
        return '<{}.{}{}>'.format(self.__class__.__module__,
                                  self.__class__.__name__,
                                  (' ' + flds) if flds else '')


CommandList = checked.CheckedList[Command]


class CommandGroup(Command):
    def apply(self, schema, context=None):
        for op in self:
            schema, _ = op.apply(schema, context)
        return schema, None


class CommandContextToken:
    def __init__(self, schema, op=None, *, modaliases=None):
        self.original_schema = schema
        self.op = op
        self.unresolved_refs = {}
        self.modaliases = modaliases if modaliases is not None else {}
        self.inheritance_merge = None
        self.inheritance_refdicts = None
        self.mark_derived = None
        self.preserve_path_id = None
        self.enable_recursion = None


class CommandContextWrapper:
    def __init__(self, context, token):
        self.context = context
        self.token = token

    def __enter__(self):
        self.context.push(self.token)
        return self.token

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.context.pop()


class CommandContext:
    def __init__(self, *, declarative=False, modaliases=None,
                 schema=None, stdmode=False, testmode=False,
                 disable_dep_verification=False, descriptive_mode=False,
                 emit_oids=False):
        self.stack = []
        self._cache = {}
        self.declarative = declarative
        self.schema = schema
        self._modaliases = modaliases if modaliases is not None else {}
        self.stdmode = stdmode
        self.testmode = testmode
        self.descriptive_mode = descriptive_mode
        self.disable_dep_verification = disable_dep_verification
        self.emit_oids = emit_oids
        self.renames = {}
        self.renamed_objs = set()
        self.altered_targets = set()

    @property
    def modaliases(self) -> Mapping[Optional[str], str]:
        maps = [t.modaliases for t in reversed(self.stack)]
        maps.append(self._modaliases)
        return collections.ChainMap(*maps)

    @property
    def inheritance_merge(self):
        for ctx in reversed(self.stack):
            if ctx.inheritance_merge is not None:
                return ctx.inheritance_merge

    @property
    def mark_derived(self):
        for ctx in reversed(self.stack):
            if ctx.mark_derived is not None:
                return ctx.mark_derived

    @property
    def preserve_path_id(self):
        for ctx in reversed(self.stack):
            if ctx.preserve_path_id is not None:
                return ctx.preserve_path_id

    @property
    def inheritance_refdicts(self):
        for ctx in reversed(self.stack):
            if ctx.inheritance_refdicts is not None:
                return ctx.inheritance_refdicts

    @property
    def enable_recursion(self):
        for ctx in reversed(self.stack):
            if ctx.enable_recursion is not None:
                return ctx.enable_recursion

        return True

    @property
    def canonical(self):
        return any(ctx.op.canonical for ctx in self.stack)

    def in_deletion(self, offset: int = 0) -> bool:
        """Return True if any object is being deleted in this context.

        :param offset:
            The offset in the context stack to start looking at.

        :returns:
            True if any object is being deleted in this context starting
            from *offset* in the stack.
        """
        return any(isinstance(ctx.op, DeleteObject)
                   for ctx in self.stack[:-offset])

    def is_deleting(self, obj: so.Object) -> bool:
        """Return True if *obj* is being deleted in this context.

        :param obj:
            The object in question.

        :returns:
            True if *obj* is being deleted in this context.
        """
        return any(isinstance(ctx.op, DeleteObject)
                   and ctx.op.scls is obj for ctx in self.stack)

    def push(self, token):
        self.stack.append(token)

    def pop(self):
        return self.stack.pop()

    def get(self, cls):
        if issubclass(cls, Command):
            cls = cls.get_context_class()

        for item in reversed(self.stack):
            if isinstance(item, cls):
                return item

    def get_ancestor(self, cls, op=None):
        if issubclass(cls, Command):
            cls = cls.get_context_class()

        if op is not None:
            for item in list(reversed(self.stack)):
                if isinstance(item, cls) and item.op is not op:
                    return item
        else:
            for item in list(reversed(self.stack))[1:]:
                if isinstance(item, cls):
                    return item

    def top(self):
        if self.stack:
            return self.stack[0]
        else:
            return None

    def current(self):
        if self.stack:
            return self.stack[-1]
        else:
            return None

    def parent(self):
        if len(self.stack) > 1:
            return self.stack[-2]
        else:
            return None

    def copy(self):
        ctx = CommandContext()
        ctx.stack = self.stack[:]
        return ctx

    def at_top(self):
        ctx = CommandContext()
        ctx.stack = ctx.stack[:1]
        return ctx

    def cache_value(self, key, value):
        self._cache[key] = value

    def get_cached(self, key):
        return self._cache.get(key)

    def drop_cache(self, key):
        self._cache.pop(key, None)

    def __call__(self, token):
        return CommandContextWrapper(self, token)


class DeltaRootContext(CommandContextToken):
    pass


class DeltaRoot(CommandGroup):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.new_types = set()
        self.deleted_types = {}

    def apply(self, schema, context=None):
        from . import modules
        from . import types as s_types

        context = context or CommandContext()

        with context(DeltaRootContext(schema=schema, op=self)):
            mods = []

            for op in self.get_subcommands(type=modules.CreateModule):
                schema, mod = op.apply(schema, context)
                mods.append(mod)

            for op in self.get_subcommands(type=modules.AlterModule):
                schema, mod = op.apply(schema, context)
                mods.append(mod)

            for op in self:
                if not isinstance(op, (modules.CreateModule,
                                       modules.AlterModule,
                                       s_types.DeleteCollectionType)):
                    schema, _ = op.apply(schema, context)

            for op in self.get_subcommands(type=s_types.DeleteCollectionType):
                schema, _ = op.apply(schema, context)

        return schema, None


class ObjectCommandMeta(CommandMeta):
    _transparent_adapter_subclass = True
    _schema_metaclasses: Dict[Tuple[str, type], ObjectCommandMeta] = {}

    def __new__(mcls, name, bases, dct, *, schema_metaclass=None, **kwargs):
        cls = super().__new__(mcls, name, bases, dct, **kwargs)
        if cls.get_adaptee() is not None:
            # This is a command adapter rather than the actual
            # command, so skip the registrations.
            return cls

        if (schema_metaclass is not None or
                not hasattr(cls, '_schema_metaclass')):
            cls._schema_metaclass = schema_metaclass

        delta_action = getattr(cls, '_delta_action', None)
        if cls._schema_metaclass is not None and delta_action is not None:
            key = delta_action, cls._schema_metaclass
            cmdcls = mcls._schema_metaclasses.get(key)
            if cmdcls is not None:
                raise TypeError(
                    f'Action {cls._delta_action!r} for '
                    f'{cls._schema_metaclass} is already claimed by {cmdcls}'
                )
            mcls._schema_metaclasses[key] = cls

        return cls

    @classmethod
    def get_command_class(mcls, cmdtype, schema_metaclass):
        return mcls._schema_metaclasses.get(
            (cmdtype._delta_action, schema_metaclass))

    @classmethod
    def get_command_class_or_die(mcls, cmdtype, schema_metaclass):
        cmdcls = mcls.get_command_class(cmdtype, schema_metaclass)
        if cmdcls is None:
            raise TypeError(f'missing {cmdtype.__name__} implementation '
                            f'for {schema_metaclass.__name__}')
        return cmdcls


class ObjectCommand(Command, metaclass=ObjectCommandMeta):
    """Base class for all Object-related commands."""
    classname = struct.Field(sn.Name)

    scls: s_types.Type

    @classmethod
    def _get_ast_name(cls, schema, astnode, context):
        return astnode.name.name

    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        nqname = cls._get_ast_name(schema, astnode, context)
        module = context.modaliases.get(astnode.name.module,
                                        astnode.name.module)
        if module is None:
            raise errors.SchemaDefinitionError(
                f'unqualified name and no default module set',
                context=astnode.name.context
            )

        return sn.Name(module=module, name=nqname)

    @classmethod
    def _cmd_from_ast(cls, schema, astnode, context):
        classname = cls._classname_from_ast(schema, astnode, context)
        return cls(classname=classname)

    def _build_alter_cmd_stack(self, schema, context, scls, *, referrer=None):
        root = DeltaRoot()
        return root, root

    def _prohibit_if_expr_refs(self, schema, context, action):
        scls = self.scls
        expr_refs = s_expr.get_expr_referrers(schema, scls)

        if expr_refs:
            ref_desc = []
            for ref, fn in expr_refs.items():
                if fn == 'expr':
                    fdesc = 'expression'
                else:
                    fdesc = f"{fn.replace('_', ' ')} expression"

                vn = ref.get_verbosename(schema, with_parent=True)

                ref_desc.append(f'{fdesc} of {vn}')

            expr_s = 'an expression' if len(ref_desc) == 1 else 'expressions'
            ref_desc_s = "\n - " + "\n - ".join(ref_desc)

            raise errors.SchemaDefinitionError(
                f'cannot {action} because it is used in {expr_s}',
                details=(
                    f'{scls.get_verbosename(schema)} is used in:{ref_desc_s}'
                )
            )

    def _append_subcmd_ast(cls, schema, node, subcmd, context):
        subnode = subcmd.get_ast(schema, context)
        if subnode is not None:
            node.commands.append(subnode)

    def _get_ast_node(self, schema, context):
        return self.__class__.astnode

    def _get_ast(self, schema, context):
        astnode = self._get_ast_node(schema, context)
        qlclass = self.get_schema_metaclass().get_ql_class()
        if isinstance(self.classname, sn.Name):
            nname = sn.shortname_from_fullname(self.classname)
            name = qlast.ObjectRef(module=nname.module, name=nname.name,
                                   itemclass=qlclass)
        else:
            name = qlast.ObjectRef(module='', name=self.classname,
                                   itemclass=qlclass)

        if astnode.get_field('name'):
            op = astnode(name=name)
        else:
            op = astnode()

        self._apply_fields_ast(schema, context, op)

        return op

    def _apply_fields_ast(self, schema, context, node):
        for op in self.get_subcommands(type=RenameObject):
            self._append_subcmd_ast(schema, node, op, context)

        mcls = self.get_schema_metaclass()

        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.source != 'inheritance' or context.descriptive_mode:
                self._apply_field_ast(schema, context, node, op)

        for refdict in mcls.get_refdicts():
            self._apply_refs_fields_ast(schema, context, node, refdict)

    def _apply_refs_fields_ast(self, schema, context, node, refdict):
        for op in self.get_subcommands(metaclass=refdict.ref_cls):
            self._append_subcmd_ast(schema, node, op, context)

    def _apply_field_ast(self, schema, context, node, op):
        if op.property != 'name':
            subnode = op._get_ast(schema, context)
            if subnode is not None:
                node.commands.append(subnode)

    @classmethod
    def get_schema_metaclass(cls):
        if cls._schema_metaclass is None:
            raise TypeError(f'schema metaclass not set for {cls}')
        return cls._schema_metaclass

    def get_subcommands(self, *, type=None, metaclass=None):
        if metaclass is not None:
            return filter(
                lambda i: (isinstance(i, ObjectCommand) and
                           issubclass(i.get_schema_metaclass(), metaclass)),
                self)
        else:
            return super().get_subcommands(type=type)

    def _validate_legal_command(self, schema, context):
        from . import functions as s_functions

        if (not context.stdmode and not context.testmode and
                not isinstance(self, s_functions.ParameterCommand)):

            if isinstance(self.classname, sn.Name):
                shortname = sn.shortname_from_fullname(self.classname)
                modname = self.classname.module
            else:
                # modules have classname as simple strings
                shortname = modname = self.classname

            if modname in s_schema.STD_MODULES:
                raise errors.SchemaDefinitionError(
                    f'cannot {self._delta_action} `{shortname}`: '
                    f'module {modname} is read-only',
                    context=self.source_context)

    def get_object(self, schema, context, *, name=None):
        if name is None:
            name = self.classname
            rename = context.renames.get(name)
            if rename is not None:
                name = rename
        metaclass = self.get_schema_metaclass()
        return schema.get(name, type=(metaclass,))

    def compute_inherited_fields(self, schema, context):
        result = {}
        mcls = self.get_schema_metaclass()
        for op in self.get_subcommands(type=AlterObjectProperty):
            field = mcls.get_field(op.property)
            if field.inheritable:
                result[op.property] = op.source == 'inheritance'

        return immu.Map(result)

    def _prepare_field_updates(self, schema, context):
        result = {}
        metaclass = self.get_schema_metaclass()

        for op in self.get_subcommands(type=AlterObjectProperty):
            field = metaclass.get_field(op.property)
            if field is None:
                raise errors.SchemaDefinitionError(
                    f'got AlterObjectProperty command for '
                    f'invalid field: {metaclass.__name__}.{op.property}')

            val = self._resolve_attr_value(
                op.new_value, op.property, field, schema)

            if isinstance(val, s_expr.Expression) and not val.is_compiled():
                val = self.compile_expr_field(schema, context, field, val)

            result[op.property] = val

        return schema, result

    def _get_field_updates(self, schema, context):
        field_updates = context.get_cached((self, 'field_updates'))
        if field_updates is None or True:
            schema, field_updates = self._prepare_field_updates(
                schema, context)
            context.cache_value((self, 'field_updates'), field_updates)

        return schema, field_updates

    def compile_expr_field(self, schema, context, field, value):
        cdn = self.get_schema_metaclass().get_schema_class_displayname()
        raise errors.InternalServerError(
            f'uncompiled expression in the field {field.name!r} of '
            f'{cdn} {self.classname!r}'
        )

    def _create_begin(
        self, schema: s_schema.Schema, context: CommandContext
    ) -> s_schema.Schema:
        raise NotImplementedError


class ObjectCommandContext(CommandContextToken):
    def __init__(self, schema, op, scls=None, *, modaliases=None):
        super().__init__(schema, op, modaliases=modaliases)
        self.scls = scls


class UnqualifiedObjectCommand(ObjectCommand):

    classname = struct.Field(str)

    @classmethod
    def _classname_from_ast(cls, schema, astnode, context):
        return astnode.name.name

    def get_object(self, schema, context, *, name=None):
        metaclass = self.get_schema_metaclass()
        if name is None:
            name = self.classname
            rename = context.renames.get(name)
            if rename is not None:
                name = rename
        return schema.get_global(metaclass, name)


class GlobalObjectCommand(UnqualifiedObjectCommand):
    pass


class CreateObject(ObjectCommand):
    _delta_action = 'create'

    # If the command is conditioned with IF NOT EXISTS
    if_not_exists = struct.Field(bool, default=False)

    @classmethod
    def _command_for_ast_node(cls, astnode, schema, context):
        if astnode.sdl_alter_if_exists:
            modaliases = cls._modaliases_from_ast(schema, astnode, context)
            with context(CommandContextToken(schema, modaliases=modaliases)):
                classname = cls._classname_from_ast(schema, astnode, context)
            mcls = cls.get_schema_metaclass()
            if schema.get(classname, default=None) is not None:
                return ObjectCommandMeta.get_command_class(AlterObject, mcls)

        return cls

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        cmd.if_not_exists = astnode.create_if_not_exists

        cmd.add(
            AlterObjectProperty(
                property='name',
                new_value=cmd.classname
            )
        )

        if getattr(astnode, 'is_abstract', False):
            cmd.add(AlterObjectProperty(
                property='is_abstract',
                new_value=True
            ))

        return cmd

    def _create_begin(self, schema, context):
        self._validate_legal_command(schema, context)

        schema, props = self._get_create_fields(schema, context)

        metaclass = self.get_schema_metaclass()
        schema, self.scls = metaclass.create_in_schema(schema, **props)

        context.current().scls = self.scls

        if not props.get('id'):
            # Record the generated ID.
            self.add(AlterObjectProperty(
                property='id', new_value=self.scls.id))

        return schema

    def _get_ast(self, schema, context):
        node = super()._get_ast(schema, context)
        if self.if_not_exists:
            node.create_if_not_exists = True
        return node

    def _prepare_create_fields(self, schema, context):
        return self._prepare_field_updates(schema, context)

    def _get_create_fields(self, schema, context):
        field_updates = context.get_cached((self, 'create_fields'))
        if field_updates is None or True:
            schema, field_updates = self._prepare_create_fields(
                schema, context)
            context.cache_value((self, 'create_fields'), field_updates)

        return schema, field_updates

    def _create_innards(self, schema, context):
        from . import types as s_types

        mcls = self.get_schema_metaclass()

        for op in self.get_subcommands(type=s_types.CollectionTypeCommand):
            schema, _ = op.apply(schema, context)

        for refdict in mcls.get_refdicts():
            schema = self._create_refs(schema, context, self.scls, refdict)

        return schema

    def _create_finalize(self, schema, context):
        return schema

    def _create_refs(self, schema, context, scls, refdict):
        for op in self.get_subcommands(metaclass=refdict.ref_cls):
            schema, _ = op.apply(schema, context=context)
        return schema

    def apply(self, schema, context):
        with self.new_context(schema, context, None):
            if self.if_not_exists:
                try:
                    obj = self.get_object(schema, context)
                except errors.InvalidReferenceError:
                    pass
                else:
                    return schema, obj

            schema = self._create_begin(schema, context)
            context.current().scls = self.scls
            schema = self._create_innards(schema, context)
            schema = self._create_finalize(schema, context)
        return schema, self.scls

    def __repr__(self):
        return '<%s.%s "%s">' % (self.__class__.__module__,
                                 self.__class__.__name__,
                                 self.classname)


class AlterObjectFragment(ObjectCommand):

    def apply(self, schema, context):
        # AlterObjectFragment must be executed in the context
        # of a parent AlterObject command.
        scls = context.current().op.scls
        self.scls = scls
        schema = self._alter_begin(schema, context, scls)
        schema = self._alter_innards(schema, context, scls)
        schema = self._alter_finalize(schema, context, scls)

        return schema, scls

    def _alter_begin(self, schema, context, scls):
        schema, props = self._get_field_updates(schema, context)
        schema = scls.update(schema, props)
        return schema

    def _alter_innards(self, schema, context, scls):
        return schema

    def _alter_finalize(self, schema, context, scls):
        return schema


class RenameObject(AlterObjectFragment):
    _delta_action = 'rename'

    astnode = qlast.Rename

    new_name = struct.Field(sn.Name)

    def __repr__(self) -> str:
        return '<%s.%s "%s" to "%s">' % (self.__class__.__module__,
                                         self.__class__.__name__,
                                         self.classname, self.new_name)

    def _rename_begin(self, schema, context, scls):
        self._validate_legal_command(schema, context)

        # Renames of schema objects used in expressions is
        # not supported yet.  Eventually we'll add support
        # for transparent recompilation.
        vn = scls.get_verbosename(schema)
        self._prohibit_if_expr_refs(schema, context, action=f'rename {vn}')

        self.old_name = self.classname
        schema = scls.set_field_value(schema, 'name', self.new_name)

        return schema

    def _rename_innards(self, schema, context, scls):
        return schema

    def _rename_finalize(self, schema, context, scls):
        return schema

    def apply(self, schema, context):
        parent_ctx = context.current()
        scls = self.scls = parent_ctx.op.scls

        context.renames[self.classname] = self.new_name
        context.renamed_objs.add(scls)

        schema = self._rename_begin(schema, context, scls)
        schema = self._rename_innards(schema, context, scls)
        schema = self._rename_finalize(schema, context, scls)

        return schema, scls

    def _get_ast(self, schema, context):
        astnode = self._get_ast_node(schema, context)

        new_name = sn.shortname_from_fullname(self.new_name)

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
    def _cmd_from_ast(cls, schema, astnode, context):
        parent_ctx = context.current()
        parent_class = parent_ctx.op.get_schema_metaclass()
        rename_class = ObjectCommandMeta.get_command_class(
            RenameObject, parent_class)
        return rename_class._rename_cmd_from_ast(schema, astnode, context)

    @classmethod
    def _rename_cmd_from_ast(cls, schema, astnode, context):
        parent_ctx = context.current()
        parent_class = parent_ctx.op.get_schema_metaclass()
        rename_class = ObjectCommandMeta.get_command_class(
            RenameObject, parent_class)

        new_name = astnode.new_name
        if new_name.name.startswith('__b32_'):
            name_b32 = new_name.name[6:].replace('_', '=')
            new_nname = base64.b32decode(name_b32).decode()
            new_name = sn.Name(module=new_name.module, name=new_nname)
        else:
            new_name = cls._classname_from_ast(schema, astnode, context)

        return rename_class(
            metaclass=parent_class,
            classname=parent_ctx.op.classname,
            new_name=sn.Name(
                module=new_name.module,
                name=new_name.name
            )
        )


class AlterObject(ObjectCommand):
    _delta_action = 'alter'

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        if getattr(astnode, 'is_abstract', False):
            cmd.add(AlterObjectProperty(
                property='is_abstract',
                new_value=True
            ))

        added_bases = []
        dropped_bases = []

        if getattr(astnode, 'commands', None):
            for astcmd in astnode.commands:
                if isinstance(astcmd, qlast.AlterDropInherit):
                    dropped_bases.extend(
                        utils.ast_to_typeref(
                            b,
                            metaclass=cls.get_schema_metaclass(),
                            modaliases=context.modaliases,
                            schema=schema,
                        )
                        for b in astcmd.bases
                    )

                elif isinstance(astcmd, qlast.AlterAddInherit):
                    bases = [
                        utils.ast_to_typeref(
                            b,
                            metaclass=cls.get_schema_metaclass(),
                            modaliases=context.modaliases,
                            schema=schema,
                        )
                        for b in astcmd.bases
                    ]

                    pos_node = astcmd.position
                    if pos_node is not None:
                        if pos_node.ref is not None:
                            ref = f'{pos_node.ref.module}::{pos_node.ref.name}'
                            pos = (pos_node.position, ref)
                        else:
                            pos = pos_node.position
                    else:
                        pos = None

                    added_bases.append((bases, pos))

        if added_bases or dropped_bases:
            from . import inheriting

            parent_class = cmd.get_schema_metaclass()
            rebase_class = ObjectCommandMeta.get_command_class_or_die(
                inheriting.RebaseInheritingObject, parent_class)

            cmd.add(
                rebase_class(
                    metaclass=parent_class,
                    classname=cmd.classname,
                    removed_bases=tuple(dropped_bases),
                    added_bases=tuple(added_bases)
                )
            )

        return cmd

    def _apply_field_ast(self, schema, context, node, op):
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
            super()._apply_field_ast(schema, context, node, op)

    def _get_ast(self, schema, context):
        node = super()._get_ast(schema, context)
        if (node is not None and hasattr(node, 'commands') and
                not node.commands):
            # Alter node without subcommands.  Occurs when all
            # subcommands have been filtered out of DDL stream,
            # so filter it out as well.
            node = None
        return node

    def _alter_begin(self, schema, context, scls):
        from . import types as s_types

        self._validate_legal_command(schema, context)

        for op in self.get_subcommands(type=AlterObjectFragment):
            schema, _ = op.apply(schema, context)

        for op in self.get_subcommands(type=s_types.CollectionTypeCommand):
            schema, _ = op.apply(schema, context)

        schema, props = self._get_field_updates(schema, context)
        schema = scls.update(schema, props)
        return schema

    def _alter_innards(self, schema, context, scls):
        for op in self.get_subcommands():
            if not isinstance(op, (AlterObjectFragment, AlterObjectProperty)):
                schema, _ = op.apply(schema, context=context)

        return schema

    def _alter_finalize(self, schema, context, scls):
        return schema

    def apply(self, schema, context):
        scls = self.get_object(schema, context)
        self.scls = scls

        with self.new_context(schema, context, scls):
            schema = self._alter_begin(schema, context, scls)
            schema = self._alter_innards(schema, context, scls)
            schema = self._alter_finalize(schema, context, scls)

        return schema, scls


class DeleteObject(ObjectCommand):
    _delta_action = 'delete'

    def _delete_begin(self, schema, context, scls):
        from . import ordering

        self._validate_legal_command(schema, context)

        if not context.canonical:
            self._canonicalize(schema, context, scls)
            ordering.linearize_delta(self, schema, schema)

        return schema

    def _canonicalize(self, schema, context, scls):
        mcls = self.get_schema_metaclass()

        for refdict in mcls.get_refdicts():
            deleted_refs = set()

            all_refs = set(
                scls.get_field_value(schema, refdict.attr).objects(schema)
            )

            for op in self.get_subcommands(metaclass=refdict.ref_cls):
                deleted_ref = schema.get(op.classname)
                deleted_refs.add(deleted_ref)

            # Add implicit Delete commands for any local refs not
            # deleted explicitly.
            for ref in all_refs - deleted_refs:
                del_cmd = ObjectCommandMeta.get_command_class(
                    DeleteObject, type(ref))

                op = del_cmd(classname=ref.get_name(schema))
                op._canonicalize(schema, context, ref)
                self.add(op)

    def _delete_innards(self, schema, context, scls):
        for op in self.get_subcommands(metaclass=so.Object):
            schema, _ = op.apply(schema, context=context)

        return schema

    def _delete_finalize(self, schema, context, scls):
        ref_strs = []

        if not context.canonical:
            refs = schema.get_referrers(self.scls)
            if refs:
                for ref in refs:
                    if (not context.is_deleting(ref)
                            and ref.is_blocking_ref(schema, scls)):
                        ref_strs.append(
                            ref.get_verbosename(schema, with_parent=True))

            if ref_strs:
                vn = self.scls.get_verbosename(schema, with_parent=True)
                dn = self.scls.get_displayname(schema)
                detail = '; '.join(f'{ref_str} depends on {dn}'
                                   for ref_str in ref_strs)
                raise errors.SchemaError(
                    f'cannot drop {vn} because '
                    f'other objects in the schema depend on it',
                    details=detail,
                )

        schema = schema.delete(scls)
        return schema

    def apply(self, schema, context=None):
        scls = self.get_object(schema, context)
        self.scls = scls

        with self.new_context(schema, context):
            schema = self._delete_begin(schema, context, scls)
            schema = self._delete_innards(schema, context, scls)
            schema = self._delete_finalize(schema, context, scls)

        return schema, scls


class AlterSpecialObjectProperty(Command):
    astnode = qlast.SetSpecialField

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        propname = astnode.name
        parent_ctx = context.current()
        parent_op = parent_ctx.op
        field = parent_op.get_schema_metaclass().get_field(propname)

        new_value = astnode.value

        if field.type is s_expr.Expression and field.name == 'expr':
            new_value = s_expr.Expression.from_ast(
                astnode.value,
                schema,
                context.modaliases,
            )

        return AlterObjectProperty(
            property=astnode.name,
            new_value=new_value
        )


class AlterObjectProperty(Command):
    astnode = qlast.SetField

    property = struct.Field(str)
    old_value = struct.Field(object, None)
    new_value = struct.Field(object, None)
    source = struct.Field(str, None)

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        from edb.edgeql import compiler as qlcompiler

        propname = astnode.name

        parent_ctx = context.current()
        parent_op = parent_ctx.op
        field = parent_op.get_schema_metaclass().get_field(propname)
        if field is None:
            raise errors.SchemaDefinitionError(
                f'{propname!r} is not a valid field',
                context=astnode.context)

        if not (field.allow_ddl_set
                or context.stdmode
                or context.testmode):
            raise errors.SchemaDefinitionError(
                f'{propname!r} is not a valid field',
                context=astnode.context)

        if field.name == 'id' and not isinstance(parent_op, CreateObject):
            raise errors.SchemaDefinitionError(
                f'cannot alter object id',
                context=astnode.context)

        if field.type is s_expr.Expression:
            new_value = s_expr.Expression.from_ast(
                astnode.value,
                schema,
                context.modaliases,
            )
        else:
            if isinstance(astnode.value, qlast.Tuple):
                new_value = tuple(
                    qlcompiler.evaluate_ast_to_python_val(
                        el.value, schema=schema)
                    for el in astnode.value.elements
                )

            elif isinstance(astnode.value, qlast.ObjectRef):

                new_value = utils.ast_objref_to_objref(
                    astnode.value, modaliases=context.modaliases,
                    schema=schema)

            elif (isinstance(astnode.value, qlast.Set)
                    and not astnode.value.elements):
                # empty set
                new_value = None

            else:
                new_value = qlcompiler.evaluate_ast_to_python_val(
                    astnode.value, schema=schema)

        return cls(property=propname, new_value=new_value,
                   source_context=astnode.context)

    def _get_ast(self, schema, context):
        value = self.new_value
        astcls = qlast.SetField

        new_value_empty = \
            (value is None or
                (isinstance(value, collections.abc.Container) and not value))

        parent_ctx = context.current()
        parent_op = parent_ctx.op
        field = parent_op.get_schema_metaclass().get_field(self.property)
        if field is None:
            raise errors.SchemaDefinitionError(
                f'{self.property!r} is not a valid field',
                context=self.context)

        if ((not field.allow_ddl_set and self.property != 'expr') or
                (self.property == 'id' and not context.emit_oids)):
            # Don't produce any AST if:
            #
            # * a field does not have the "allow_ddl_set" option, unless
            #   it's an 'expr' field.
            #
            #   'expr' fields come from the "USING" clause and are specially
            #   treated in parser and codegen.
            #
            # * an 'id' field unless we asked for it by setting
            #   the "emit_iods" option in the context.  This is used
            #   for dumping the schema (for later restore.)
            return

        if self.source == 'inheritance':
            # We don't want to show inherited properties unless
            # we are in "descriptive_mode" and ...

            if not (
                context.descriptive_mode and
                self.property in {'default', 'readonly'}
            ):
                # If property isn't 'default' or 'readonly' --
                # skip the AST for it.
                return

            parentop_sn = sn.shortname_from_fullname(parent_op.classname).name
            if self.property == 'default' and parentop_sn == 'id':
                # If it's 'default' for the 'id' property --
                # skip the AST for it.
                return

        if new_value_empty:
            return

        if isinstance(value, s_expr.Expression):
            value = value.qlast
            if self.property == 'expr':
                astcls = qlast.SetSpecialField
        elif utils.is_nontrivial_container(value):
            value = qlast.Tuple(elements=[
                qlast.BaseConstant.from_python(el) for el in value
            ])
        elif isinstance(value, uuid.UUID):
            value = qlast.TypeCast(
                expr=qlast.BaseConstant.from_python(str(value)),
                type=qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='uuid',
                        module='std',
                    )
                )
            )
        else:
            value = qlast.BaseConstant.from_python(value)

        op = astcls(name=self.property, value=value)
        return op

    def __repr__(self):
        return '<%s.%s "%s":"%s"->"%s">' % (
            self.__class__.__module__, self.__class__.__name__,
            self.property, self.old_value, self.new_value)


def ensure_schema_collection(schema, coll_type, parent_cmd, *,
                             src_context=None, context):
    if not coll_type.is_collection():
        raise ValueError(
            f'{coll_type.get_displayname(schema)} is not a collection')

    if coll_type.contains_array_of_tuples(schema):
        raise errors.UnsupportedFeatureError(
            'arrays of tuples are not supported at the schema level',
            context=src_context,
        )

    delta_root = context.top().op

    if (schema.get_by_id(coll_type.id, None) is None
            and coll_type.id not in delta_root.new_types):
        parent_cmd.add(coll_type.as_create_delta(schema))
        delta_root.new_types.add(coll_type.id)

    if coll_type.id in delta_root.deleted_types:
        # Revert the deletion decision.
        del_cmd = delta_root.deleted_types.pop(coll_type.id)
        delta_root.discard(del_cmd)


def cleanup_schema_collection(schema, coll_type, parent, parent_cmd, *,
                              src_context=None, context):
    if not coll_type.is_collection():
        raise ValueError(
            f'{coll_type.get_displayname(schema)} is not a collection')

    delta_root = context.top().op

    refs = schema.get_referrers(coll_type)
    if (len(refs) == 1 and list(refs)[0].id == parent.id
            and coll_type.id not in delta_root.deleted_types):
        # The parent is the last user of this collection, drop it.
        del_cmd = coll_type.as_delete_delta(schema)
        delta_root.deleted_types[coll_type.id] = del_cmd
        delta_root.add(del_cmd)
