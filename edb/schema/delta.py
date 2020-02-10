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

import collections
import collections.abc
import itertools
import uuid

import immutables as immu

from edb import errors

from edb.common import adapter
from edb.common import parsing
from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.common import checked, markup, ordered, struct

from . import expr as s_expr
from . import name as sn
from . import objects as so
from . import schema as s_schema
from . import utils


CommandMeta_T = TypeVar("CommandMeta_T", bound="CommandMeta")


class CommandMeta(
    adapter.Adapter,
    struct.MixedStructMeta,
    markup.MarkupCapableMeta,
):

    _astnode_map: Dict[Type[qlast.DDLOperation], Type[Command]] = {}

    def __new__(
        mcls: Type[CommandMeta_T],
        name: str,
        bases: Tuple[type, ...],
        dct: Dict[str, Any],
        *,
        context_class: Optional[Type[CommandContextToken[Command]]] = None,
        **kwargs: Any,
    ) -> CommandMeta_T:
        cls = super().__new__(mcls, name, bases, dct, **kwargs)

        if context_class is not None:
            cast(Command, cls)._context_class = context_class

        return cls

    def __init__(
        cls,
        name: str,
        bases: Tuple[type, ...],
        clsdict: Dict[str, Any],
        *,
        adapts: Optional[type] = None,
        **kwargs: Any,
    ) -> None:
        adapter.Adapter.__init__(cls, name, bases, clsdict, adapts=adapts)
        struct.MixedStructMeta.__init__(cls, name, bases, clsdict)
        astnodes = clsdict.get('astnode')
        if astnodes and not isinstance(astnodes, (list, tuple)):
            astnodes = [astnodes]
        if astnodes:
            cls.register_astnodes(astnodes)

    def register_astnodes(
        cls,
        astnodes: Iterable[Type[qlast.DDLCommand]],
    ) -> None:
        mapping = type(cls)._astnode_map

        for astnode in astnodes:
            existing = mapping.get(astnode)
            if existing:
                msg = ('duplicate EdgeQL AST node to command mapping: ' +
                       '{!r} is already declared for {!r}')
                raise TypeError(msg.format(astnode, existing))

            mapping[astnode] = cast(Type["Command"], cls)


_void = object()

# We use _DummyObject for contexts where an instance of an object is
# required by type signatures, and the actual reference will be quickly
# replaced by a real object.
_dummy_object = so.Object(_private_init=True)


Command_T = TypeVar("Command_T", bound="Command")


class Command(struct.MixedStruct, metaclass=CommandMeta):
    """Abstract base class for all delta commands."""

    source_context = struct.Field(parsing.ParserContext, default=None)
    canonical = struct.Field(bool, default=False)

    _context_class: Optional[Type[CommandContextToken[Command]]] = None

    ops: ordered.OrderedSet[Command]
    before_ops: ordered.OrderedSet[Command]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.ops = ordered.OrderedSet()
        self.before_ops = ordered.OrderedSet()
        self.qlast: qlast.DDLOperation

    def copy(self: Command_T) -> Command_T:
        result = super().copy()
        result.ops = ordered.OrderedSet(
            op.copy() for op in self.ops)
        result.before_ops = ordered.OrderedSet(
            op.copy() for op in self.before_ops)
        return result

    @classmethod
    def adapt(cls: Type[Command_T], obj: Command) -> Command_T:
        result = obj.copy_with_class(cls)
        mcls = cast(CommandMeta, type(cls))
        for op in obj.get_prerequisites():
            result.add_prerequisite(mcls.adapt(op))
        for op in obj.get_subcommands(include_prerequisites=False):
            result.add(mcls.adapt(op))
        return result

    def _resolve_type_ref(
        self,
        ref: so.Object,
        schema: s_schema.Schema,
    ) -> so.Object:
        return utils.resolve_typeref(ref, schema)

    def _resolve_attr_value(
        self,
        value: Any,
        fname: str,
        field: so.Field[Any],
        schema: s_schema.Schema,
    ) -> Any:
        ftype = field.type

        if isinstance(ftype, so.ObjectMeta):
            value = self._resolve_type_ref(value, schema)

        elif issubclass(ftype, checked.CheckedDict):
            if issubclass(ftype.valuetype, so.Object):
                dct = {}

                for k, val in value.items():
                    dct[k] = self._resolve_type_ref(val, schema)

                value = ftype(dct)
            else:
                value = field.coerce_value(schema, value)

        elif issubclass(ftype, (checked.AbstractCheckedList,
                                checked.AbstractCheckedSet)):
            if issubclass(ftype.type, so.Object):
                lst = []

                for val in value:
                    lst.append(self._resolve_type_ref(val, schema))

                value = ftype(lst)
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

    def has_attribute_value(self, attr_name: str) -> bool:
        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.property == attr_name:
                return True
        return False

    def get_attribute_set_cmd(
        self,
        attr_name: str,
    ) -> Optional[AlterObjectProperty]:
        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.property == attr_name:
                return op
        return None

    def get_attribute_value(
        self,
        attr_name: str,
    ) -> Any:
        op = self.get_attribute_set_cmd(attr_name)
        if op is not None:
            return op.new_value
        else:
            return None

    def get_local_attribute_value(
        self,
        attr_name: str,
    ) -> Any:
        """Return the new value of field, if not inherited."""
        op = self.get_attribute_set_cmd(attr_name)
        if op is not None and op.source != 'inheritance':
            return op.new_value
        else:
            return None

    def get_attribute_source_context(
        self,
        attr_name: str,
    ) -> Optional[parsing.ParserContext]:
        op = self.get_attribute_set_cmd(attr_name)
        if op is not None:
            return op.source_context
        else:
            return None

    def set_attribute_value(
        self,
        attr_name: str,
        value: Any,
        *,
        inherited: bool = False,
        source_context: Optional[parsing.ParserContext] = None,
    ) -> None:
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

    def discard_attribute(self, attr_name: str) -> None:
        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.property == attr_name:
                self.discard(op)
                return

    def __iter__(self) -> NoReturn:
        raise TypeError(f'{type(self)} object is not iterable')

    @overload
    def get_subcommands(
        self,
        *,
        type: Type[Command_T],
        metaclass: Optional[Type[so.Object]] = None,
        include_prerequisites: bool = True,
    ) -> Tuple[Command_T, ...]:
        ...

    @overload
    def get_subcommands(  # NoQA: F811
        self,
        *,
        type: None = None,
        metaclass: Optional[Type[so.Object]] = None,
        include_prerequisites: bool = True,
    ) -> Tuple[Command, ...]:
        ...

    def get_subcommands(  # NoQA: F811
        self,
        *,
        type: Union[Type[Command_T], None] = None,
        metaclass: Optional[Type[so.Object]] = None,
        include_prerequisites: bool = True,
    ) -> Tuple[Command, ...]:
        ops: Iterable[Command] = self.ops
        if include_prerequisites:
            ops = itertools.chain(ops, self.before_ops)

        filters = []

        if type is not None:
            t = type
            filters.append(lambda i: isinstance(i, t))

        if metaclass is not None:
            mcls = metaclass
            filters.append(
                lambda i: (
                    isinstance(i, ObjectCommand)
                    and issubclass(i.get_schema_metaclass(), mcls)
                )
            )

        if filters:
            return tuple(filter(lambda i: all(f(i) for f in filters), ops))
        else:
            return tuple(ops)

    @overload
    def get_prerequisites(
        self,
        *,
        type: Type[Command_T],
        include_prerequisites: bool = True,
    ) -> Tuple[Command_T, ...]:
        ...

    @overload
    def get_prerequisites(  # NoQA: F811
        self,
        *,
        type: None = None,
    ) -> Tuple[Command, ...]:
        ...

    def get_prerequisites(  # NoQA: F811
        self,
        *,
        type: Union[Type[Command_T], None] = None,
        include_prerequisites: bool = True,
    ) -> Tuple[Command, ...]:
        if type is not None:
            t = type
            return tuple(filter(lambda i: isinstance(i, t), self.before_ops))
        else:
            return tuple(self.before_ops)

    def has_subcommands(self) -> bool:
        return bool(self.ops)

    def add_prerequisite(self, command: Command) -> None:
        if isinstance(command, CommandGroup):
            self.before_ops.update(command.get_subcommands())  # type: ignore
        else:
            self.before_ops.add(command)

    def prepend(self, command: Command) -> None:
        if isinstance(command, CommandGroup):
            for op in reversed(command.get_subcommands()):
                self.ops.add(op, last=False)
        else:
            self.ops.add(command, last=False)

    def add(self, command: Command) -> None:
        if isinstance(command, CommandGroup):
            self.ops.update(command.get_subcommands())  # type: ignore
        else:
            self.ops.add(command)

    def update(self, commands: Iterable[Command]) -> None:  # type: ignore
        for command in commands:
            self.add(command)

    def replace(self, commands: Iterable[Command]) -> None:  # type: ignore
        self.ops.clear()
        self.ops.update(commands)  # type: ignore

    def discard(self, command: Command) -> None:
        self.ops.discard(command)

    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        return schema

    def get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        context_class = type(self).get_context_class()
        assert context_class is not None
        with context(context_class(schema=schema, op=self)):
            return self._get_ast(schema, context, parent_node=parent_node)

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        raise NotImplementedError

    @classmethod
    def get_orig_expr_text(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        name: str,
    ) -> Optional[str]:
        from edb.edgeql import compiler as qlcompiler

        orig_text_expr = qlast.get_ddl_field_value(astnode, f'orig_{name}')
        if orig_text_expr:
            orig_text = qlcompiler.evaluate_ast_to_python_val(
                orig_text_expr, schema=schema)
        else:
            orig_text = None

        return orig_text  # type: ignore

    @classmethod
    def command_for_ast_node(
        cls,
        astnode: qlast.DDLOperation,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Type[Command]:
        return cls

    @classmethod
    def _modaliases_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> Dict[Optional[str], str]:
        modaliases = {}
        if isinstance(astnode, qlast.DDLCommand):
            for alias in astnode.aliases:
                if isinstance(alias, qlast.ModuleAliasDecl):
                    modaliases[alias.alias] = alias.module

        return modaliases

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> Command:
        cmd = cls._cmd_from_ast(schema, astnode, context)
        cmd.source_context = astnode.context
        cmd.qlast = astnode
        ctx = context.current()
        if ctx is not None and type(ctx) is cls.get_context_class():
            ctx.op = cmd

        if astnode.commands:
            for subastnode in astnode.commands:
                subcmd = compile_ddl(schema, subastnode, context=context)
                if subcmd is not None:
                    cmd.add(subcmd)

        return cmd

    @classmethod
    def _cmd_from_ast(
        cls: Type[Command_T],
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> Command_T:
        return cls()

    @classmethod
    def as_markup(cls, self: Command, *, ctx: markup.Context) -> markup.Markup:
        node = markup.elements.lang.TreeNode(name=str(self))

        for dd in self.get_subcommands():
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
    def get_context_class(
        cls: Type[Command_T],
    ) -> Optional[Type[CommandContextToken[Command_T]]]:
        return cast(
            Optional[Type[CommandContextToken[Command_T]]],
            cls._context_class,
        )

    def __str__(self) -> str:
        return struct.MixedStruct.__str__(self)

    def __repr__(self) -> str:
        flds = struct.MixedStruct.__repr__(self)
        return '<{}.{}{}>'.format(self.__class__.__module__,
                                  self.__class__.__name__,
                                  (' ' + flds) if flds else '')


# Similarly to _dummy_object, we use _dummy_command for places where
# the typing requires an object, but we don't have it just yet.
_dummy_command = Command()


CommandList = checked.CheckedList[Command]


class CommandGroup(Command):
    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        for op in self.get_subcommands():
            schema = op.apply(schema, context)
        return schema


CommandContextToken_T = TypeVar(
    "CommandContextToken_T",
    bound="CommandContextToken[Command]",
)


class CommandContextToken(Generic[Command_T]):
    original_schema: s_schema.Schema
    op: Command_T
    modaliases: Mapping[Optional[str], str]
    inheritance_merge: Optional[bool]
    inheritance_refdicts: Optional[AbstractSet[str]]
    mark_derived: Optional[bool]
    preserve_path_id: Optional[bool]
    enable_recursion: Optional[bool]

    def __init__(
        self,
        schema: s_schema.Schema,
        op: Command_T,
        *,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
    ):
        self.original_schema = schema
        self.op = op
        self.modaliases = modaliases if modaliases is not None else {}
        self.inheritance_merge = None
        self.inheritance_refdicts = None
        self.mark_derived = None
        self.preserve_path_id = None
        self.enable_recursion = None


class CommandContextWrapper(Generic[Command_T]):
    def __init__(
        self,
        context: CommandContext,
        token: CommandContextToken[Command_T],
    ) -> None:
        self.context = context
        self.token = token

    def __enter__(self) -> CommandContextToken[Command_T]:
        self.context.push(self.token)  # type: ignore
        return self.token

    def __exit__(
        self,
        exc_type: Type[Exception],
        exc_value: Exception,
        traceback: Any,
    ) -> None:
        self.context.pop()


class CommandContext:
    def __init__(
        self,
        *,
        schema: Optional[s_schema.Schema] = None,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
        declarative: bool = False,
        stdmode: bool = False,
        testmode: bool = False,
        disable_dep_verification: bool = False,
        descriptive_mode: bool = False,
        schema_object_ids: Optional[
            Mapping[Tuple[str, Optional[str]], uuid.UUID]
        ] = None
    ) -> None:
        self.stack: List[CommandContextToken[Command]] = []
        self._cache: Dict[Hashable, Any] = {}
        self._values: Dict[Hashable, Any] = {}
        self.declarative = declarative
        self.schema = schema
        self._modaliases = modaliases if modaliases is not None else {}
        self.stdmode = stdmode
        self.testmode = testmode
        self.descriptive_mode = descriptive_mode
        self.disable_dep_verification = disable_dep_verification
        self.renames: Dict[str, str] = {}
        self.renamed_objs: Set[so.Object] = set()
        self.altered_targets: Set[so.Object] = set()
        self.schema_object_ids = schema_object_ids

    @property
    def modaliases(self) -> Mapping[Optional[str], str]:
        maps = [t.modaliases for t in reversed(self.stack)]
        maps.append(self._modaliases)
        return collections.ChainMap(*maps)

    @property
    def inheritance_merge(self) -> Optional[bool]:
        for ctx in reversed(self.stack):
            if ctx.inheritance_merge is not None:
                return ctx.inheritance_merge
        return None

    @property
    def mark_derived(self) -> Optional[bool]:
        for ctx in reversed(self.stack):
            if ctx.mark_derived is not None:
                return ctx.mark_derived
        return None

    @property
    def preserve_path_id(self) -> Optional[bool]:
        for ctx in reversed(self.stack):
            if ctx.preserve_path_id is not None:
                return ctx.preserve_path_id
        return None

    @property
    def inheritance_refdicts(self) -> Optional[AbstractSet[str]]:
        for ctx in reversed(self.stack):
            if ctx.inheritance_refdicts is not None:
                return ctx.inheritance_refdicts
        return None

    @property
    def enable_recursion(self) -> bool:
        for ctx in reversed(self.stack):
            if ctx.enable_recursion is not None:
                return ctx.enable_recursion

        return True

    @property
    def canonical(self) -> bool:
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

    def push(self, token: CommandContextToken[Command]) -> None:
        self.stack.append(token)

    def pop(self) -> CommandContextToken[Command]:
        return self.stack.pop()

    def get(
        self,
        cls: Type[CommandContextToken_T],
    ) -> Optional[CommandContextToken_T]:
        if issubclass(cls, Command):
            cls = cls.get_context_class()

        for item in reversed(self.stack):
            if isinstance(item, cls):
                return item

        return None

    def get_ancestor(
        self,
        cls: Union[Type[Command], Type[CommandContextToken[Command]]],
        op: Optional[Command] = None,
    ) -> Optional[CommandContextToken[Command]]:
        if issubclass(cls, Command):
            ctxcls = cls.get_context_class()
            assert ctxcls is not None
        else:
            ctxcls = cls

        if op is not None:
            for item in list(reversed(self.stack)):
                if isinstance(item, ctxcls) and item.op is not op:
                    return item
        else:
            for item in list(reversed(self.stack))[1:]:
                if isinstance(item, ctxcls):
                    return item

        return None

    def top(self) -> CommandContextToken[Command]:
        if self.stack:
            return self.stack[0]
        else:
            raise KeyError('command context stack is empty')

    def current(self) -> CommandContextToken[Command]:
        if self.stack:
            return self.stack[-1]
        else:
            raise KeyError('command context stack is empty')

    def parent(self) -> Optional[CommandContextToken[Command]]:
        if len(self.stack) > 1:
            return self.stack[-2]
        else:
            return None

    def copy(self) -> CommandContext:
        ctx = CommandContext()
        ctx.stack = self.stack[:]
        return ctx

    def at_top(self) -> CommandContext:
        ctx = CommandContext()
        ctx.stack = ctx.stack[:1]
        return ctx

    def cache_value(self, key: Hashable, value: Any) -> None:
        self._cache[key] = value

    def get_cached(self, key: Hashable) -> Any:
        return self._cache.get(key)

    def drop_cache(self, key: Hashable) -> None:
        self._cache.pop(key, None)

    def store_value(self, key: Hashable, value: Any) -> None:
        self._values[key] = value

    def get_value(self, key: Hashable) -> Any:
        return self._values.get(key)

    def __call__(
        self,
        token: CommandContextToken[Command_T],
    ) -> CommandContextWrapper[Command_T]:
        return CommandContextWrapper(self, token)


class DeltaRootContext(CommandContextToken["DeltaRoot"]):
    pass


class DeltaRoot(CommandGroup):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.new_types: Set[uuid.UUID] = set()
        self.deleted_types: Dict[uuid.UUID, Command] = {}

    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        from . import modules
        from . import types as s_types

        context = context or CommandContext()

        with context(DeltaRootContext(schema=schema, op=self)):
            mods = []

            for cmop in self.get_subcommands(type=modules.CreateModule):
                schema = cmop.apply(schema, context)
                mods.append(cmop.scls)

            for amop in self.get_subcommands(type=modules.AlterModule):
                schema = amop.apply(schema, context)
                mods.append(amop.scls)

            for objop in self.get_subcommands():
                if not isinstance(objop, (modules.CreateModule,
                                          modules.AlterModule,
                                          s_types.DeleteCollectionType)):
                    schema = objop.apply(schema, context)

            for cop in self.get_subcommands(type=s_types.DeleteCollectionType):
                schema = cop.apply(schema, context)

        return schema


class ObjectCommandMeta(CommandMeta):
    _transparent_adapter_subclass: ClassVar[bool] = True
    _schema_metaclasses: ClassVar[
        Dict[Tuple[str, Type[so.Object]], Type[ObjectCommand[so.Object]]]
    ] = {}

    def __new__(
        mcls,
        name: str,
        bases: Tuple[type, ...],
        dct: Dict[str, Any],
        *,
        schema_metaclass: Optional[Type[so.Object]] = None,
        **kwargs: Any,
    ) -> ObjectCommandMeta:
        cls = cast(
            Type["ObjectCommand[so.Object]"],
            super().__new__(mcls, name, bases, dct, **kwargs),
        )
        if cls.has_adaptee():
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
    def get_command_class(
        mcls,
        cmdtype: Type[Command_T],
        schema_metaclass: Type[so.Object],
    ) -> Optional[Type[Command_T]]:
        assert issubclass(cmdtype, ObjectCommand)
        return cast(
            Optional[Type[Command_T]],
            mcls._schema_metaclasses.get(
                (cmdtype._delta_action, schema_metaclass)),
        )

    @classmethod
    def get_command_class_or_die(
        mcls,
        cmdtype: Type[Command_T],
        schema_metaclass: Type[so.Object],
    ) -> Type[Command_T]:
        cmdcls = mcls.get_command_class(cmdtype, schema_metaclass)
        if cmdcls is None:
            raise TypeError(f'missing {cmdtype.__name__} implementation '
                            f'for {schema_metaclass.__name__}')
        return cmdcls


class ObjectCommand(
    Command,
    Generic[so.Object_T],
    metaclass=ObjectCommandMeta,
):
    """Base class for all Object-related commands."""
    classname = struct.Field(sn.Name)

    scls: so.Object_T
    _delta_action: ClassVar[str]
    _schema_metaclass: ClassVar[Optional[Type[so.Object_T]]]
    astnode: ClassVar[Type[qlast.DDLOperation]]

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: CommandContext,
    ) -> str:
        objref = astnode.name
        module = context.modaliases.get(objref.module, objref.module)
        if module is None:
            raise errors.SchemaDefinitionError(
                f'unqualified name and no default module set',
                context=objref.context,
            )

        return sn.Name(module=module, name=objref.name)

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> ObjectCommand[so.Object_T]:
        assert isinstance(astnode, qlast.ObjectDDL), 'expected ObjectDDL'
        classname = cls._classname_from_ast(schema, astnode, context)
        return cls(classname=classname)

    def _build_alter_cmd_stack(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        scls: so.Object,
        *,
        referrer: Optional[so.Object] = None,
    ) -> Tuple[DeltaRoot, Command]:
        root = DeltaRoot()
        return root, root

    def _prohibit_if_expr_refs(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        action: str,
    ) -> None:
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

    def _append_subcmd_ast(
        self,
        schema: s_schema.Schema,
        node: qlast.DDLOperation,
        subcmd: Command,
        context: CommandContext,
    ) -> None:
        subnode = subcmd.get_ast(schema, context, parent_node=node)
        if subnode is not None:
            node.commands.append(subnode)

    def _get_ast_node(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Type[qlast.DDLOperation]:
        return type(self).astnode

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
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

    def _apply_fields_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        node: qlast.DDLOperation,
    ) -> None:
        for op in self.get_subcommands(type=RenameObject):
            self._append_subcmd_ast(schema, node, op, context)

        mcls = self.get_schema_metaclass()

        for fop in self.get_subcommands(type=AlterObjectProperty):
            if fop.source != 'inheritance' or context.descriptive_mode:
                self._apply_field_ast(schema, context, node, fop)

        for refdict in mcls.get_refdicts():
            self._apply_refs_fields_ast(schema, context, node, refdict)

    def _apply_refs_fields_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        node: qlast.DDLOperation,
        refdict: so.RefDict,
    ) -> None:
        for op in self.get_subcommands(metaclass=refdict.ref_cls):
            self._append_subcmd_ast(schema, node, op, context)

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        node: qlast.DDLOperation,
        op: AlterObjectProperty,
    ) -> None:
        if op.property != 'name':
            subnode = op._get_ast(schema, context, parent_node=node)
            if subnode is not None:
                node.commands.append(subnode)

    def get_ast_attr_for_field(self, field: so.Field[Any]) -> Optional[str]:
        return None

    @classmethod
    def get_schema_metaclass(cls) -> Type[so.Object_T]:
        if cls._schema_metaclass is None:
            raise TypeError(f'schema metaclass not set for {cls}')
        return cls._schema_metaclass

    def get_struct_properties(self, schema: s_schema.Schema) -> Dict[str, Any]:
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

    def _validate_legal_command(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> None:
        from . import functions as s_functions
        from . import modules as s_mod

        if (not context.stdmode and not context.testmode and
                not isinstance(self, s_functions.ParameterCommand)):

            if isinstance(self.classname, sn.Name):
                shortname = sn.shortname_from_fullname(self.classname)
                modname = self.classname.module
            elif issubclass(self.get_schema_metaclass(), s_mod.Module):
                # modules have classname as simple strings
                shortname = modname = self.classname
            else:
                modname = None

            if modname is not None and modname in s_schema.STD_MODULES:
                raise errors.SchemaDefinitionError(
                    f'cannot {self._delta_action} `{shortname}`: '
                    f'module {modname} is read-only',
                    context=self.source_context)

    @overload
    def get_object(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[str] = None,
        default: Union[so.Object_T, so.NoDefaultT] = so.NoDefault,
    ) -> so.Object_T:
        ...

    @overload
    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[str] = None,
        default: None = None,
    ) -> Optional[so.Object_T]:
        ...

    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[str] = None,
        default: Union[so.Object_T, so.NoDefaultT, None] = so.NoDefault,
    ) -> Optional[so.Object_T]:
        if name is None:
            name = self.classname
            rename = context.renames.get(name)
            if rename is not None:
                name = rename
        metaclass = self.get_schema_metaclass()
        return schema.get(name, type=metaclass, default=default)

    def compute_inherited_fields(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> immu.Map[str, bool]:
        result = {}
        mcls = self.get_schema_metaclass()
        for op in self.get_subcommands(type=AlterObjectProperty):
            field = mcls.get_field(op.property)
            if field.inheritable:
                result[op.property] = op.source == 'inheritance'

        return immu.Map(result)

    def _prepare_field_updates(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Tuple[s_schema.Schema, Dict[str, Any]]:
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

    def _get_field_updates(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Tuple[s_schema.Schema, Dict[str, Any]]:
        field_updates = context.get_cached((self, 'field_updates'))
        if field_updates is None or True:
            schema, field_updates = self._prepare_field_updates(
                schema, context)
            context.cache_value((self, 'field_updates'), field_updates)

        return schema, field_updates

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        field: so.Field[Any],
        value: Any,
    ) -> s_expr.Expression:
        cdn = self.get_schema_metaclass().get_schema_class_displayname()
        raise errors.InternalServerError(
            f'uncompiled expression in the field {field.name!r} of '
            f'{cdn} {self.classname!r}'
        )

    def _create_begin(
        self, schema: s_schema.Schema, context: CommandContext
    ) -> s_schema.Schema:
        raise NotImplementedError

    def new_context(
        self: ObjectCommand[so.Object_T],
        schema: s_schema.Schema,
        context: CommandContext,
        scls: so.Object_T,
    ) -> CommandContextWrapper[ObjectCommand[so.Object_T]]:
        ctxcls = type(self).get_context_class()
        assert ctxcls is not None
        obj_ctxcls = cast(
            Type[ObjectCommandContext[so.Object_T]],
            ctxcls,
        )
        return context(obj_ctxcls(schema=schema, op=self, scls=scls))

    def get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        dummy = cast(so.Object_T, _dummy_object)
        with self.new_context(schema, context, dummy):
            return self._get_ast(schema, context, parent_node=parent_node)


class ObjectCommandContext(CommandContextToken[ObjectCommand[so.Object_T]]):

    def __init__(
        self,
        schema: s_schema.Schema,
        op: ObjectCommand[so.Object_T],
        scls: Optional[so.Object_T] = None,
        *,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
    ) -> None:
        super().__init__(schema, op, modaliases=modaliases)
        self.scls = scls


class UnqualifiedObjectCommand(ObjectCommand[so.UnqualifiedObject]):

    classname = struct.Field(str)  # type: ignore

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: CommandContext,
    ) -> str:
        return astnode.name.name

    @overload
    def get_object(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[str] = None,
        default: Union[so.UnqualifiedObject, so.NoDefaultT] = so.NoDefault,
    ) -> so.UnqualifiedObject:
        ...

    @overload
    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[str] = None,
        default: None = None,
    ) -> Optional[so.UnqualifiedObject]:
        ...

    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[str] = None,
        default: Union[
            so.UnqualifiedObject, so.NoDefaultT, None] = so.NoDefault,
    ) -> Optional[so.UnqualifiedObject]:
        metaclass = self.get_schema_metaclass()
        if name is None:
            name = self.classname
            rename = context.renames.get(name)
            if rename is not None:
                name = rename
        return schema.get_global(metaclass, name, default=default)


class GlobalObjectCommand(UnqualifiedObjectCommand):
    pass


class CreateObject(ObjectCommand[so.Object_T], Generic[so.Object_T]):
    _delta_action = 'create'

    # If the command is conditioned with IF NOT EXISTS
    if_not_exists = struct.Field(bool, default=False)

    @classmethod
    def command_for_ast_node(
        cls,
        astnode: qlast.DDLOperation,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Type[ObjectCommand[so.Object_T]]:
        assert isinstance(astnode, qlast.CreateObject), "expected CreateObject"

        if astnode.sdl_alter_if_exists:
            modaliases = cls._modaliases_from_ast(schema, astnode, context)
            dummy_op = cls(classname=sn.Name('placeholder::placeholder'))
            ctxcls = cls.get_context_class()
            assert ctxcls is not None
            with context(ctxcls(schema, op=dummy_op, modaliases=modaliases)):
                classname = cls._classname_from_ast(schema, astnode, context)
            mcls = cls.get_schema_metaclass()
            if schema.get(classname, default=None) is not None:
                return ObjectCommandMeta.get_command_class_or_die(
                    AlterObject, mcls)

        return cls

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(astnode, qlast.CreateObject)
        assert isinstance(cmd, CreateObject)

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

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        self._validate_legal_command(schema, context)

        for op in self.get_prerequisites():
            schema = op.apply(schema, context)

        for op in self.get_subcommands(type=CreateObjectFragment):
            schema = op.apply(schema, context)

        if context.schema_object_ids is not None:
            mcls = self.get_schema_metaclass()
            qlclass: Optional[qltypes.SchemaObjectClass]
            if issubclass(mcls, so.UnqualifiedObject):
                qlclass = mcls.get_ql_class_or_die()
            else:
                qlclass = None
            key = (self.classname, qlclass)
            specified_id = context.schema_object_ids.get(key)
            if specified_id is not None:
                self.set_attribute_value('id', specified_id)

        schema, props = self._get_create_fields(schema, context)
        metaclass = self.get_schema_metaclass()
        schema, self.scls = metaclass.create_in_schema(schema, **props)

        if not props.get('id'):
            # Record the generated ID.
            self.set_attribute_value('id', self.scls.id)

        return schema

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        node = super()._get_ast(schema, context, parent_node=parent_node)
        if node is not None and self.if_not_exists:
            node.create_if_not_exists = True
        return node

    def _prepare_create_fields(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Tuple[s_schema.Schema, Dict[str, Any]]:
        return self._prepare_field_updates(schema, context)

    def _get_create_fields(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Tuple[s_schema.Schema, Dict[str, Any]]:
        field_updates = context.get_cached((self, 'create_fields'))
        if field_updates is None or True:
            schema, field_updates = self._prepare_create_fields(
                schema, context)
            context.cache_value((self, 'create_fields'), field_updates)

        return schema, field_updates

    def _create_innards(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        from . import types as s_types

        for cop in self.get_subcommands(type=s_types.CollectionTypeCommand):
            schema = cop.apply(schema, context)

        for op in self.get_subcommands(include_prerequisites=False):
            if not isinstance(op, (s_types.CollectionTypeCommand,
                                   CreateObjectFragment)):
                schema = op.apply(schema, context=context)

        return schema

    def _create_finalize(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        return schema

    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        dummy = cast(so.Object_T, _dummy_object)
        with self.new_context(schema, context, dummy):
            if self.if_not_exists:
                try:
                    self.scls = self.get_object(schema, context)
                except errors.InvalidReferenceError:
                    pass
                else:
                    return schema

            schema = self._create_begin(schema, context)
            ctx = context.current()
            objctx = cast(ObjectCommandContext[so.Object_T], ctx)
            objctx.scls = self.scls
            schema = self._create_innards(schema, context)
            schema = self._create_finalize(schema, context)
        return schema

    def __repr__(self) -> str:
        return '<%s.%s "%s">' % (self.__class__.__module__,
                                 self.__class__.__name__,
                                 self.classname)


class CreateObjectFragment(ObjectCommand[so.Object]):
    pass


class AlterObjectFragment(ObjectCommand[so.Object]):

    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        # AlterObjectFragment must be executed in the context
        # of a parent AlterObject command.
        op = context.current().op
        assert isinstance(op, ObjectCommand)
        scls = op.scls
        self.scls = scls
        schema = self._alter_begin(schema, context)
        schema = self._alter_innards(schema, context)
        schema = self._alter_finalize(schema, context)

        return schema

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        schema, props = self._get_field_updates(schema, context)
        schema = self.scls.update(schema, props)
        return schema

    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        return schema

    def _alter_finalize(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        return schema


class RenameObject(AlterObjectFragment):
    _delta_action = 'rename'

    astnode = qlast.Rename

    new_name = struct.Field(sn.Name)

    def __repr__(self) -> str:
        return '<%s.%s "%s" to "%s">' % (self.__class__.__module__,
                                         self.__class__.__name__,
                                         self.classname, self.new_name)

    def _rename_begin(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        self._validate_legal_command(schema, context)
        scls = self.scls

        # Renames of schema objects used in expressions is
        # not supported yet.  Eventually we'll add support
        # for transparent recompilation.
        vn = scls.get_verbosename(schema)
        self._prohibit_if_expr_refs(schema, context, action=f'rename {vn}')

        self.old_name = self.classname
        schema = scls.set_field_value(schema, 'name', self.new_name)

        return schema

    def _rename_innards(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        return schema

    def _rename_finalize(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        return schema

    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        parent_ctx = context.current()
        parent_op = parent_ctx.op
        assert isinstance(parent_op, ObjectCommand)
        scls = self.scls = parent_op.scls

        context.renames[self.classname] = self.new_name
        context.renamed_objs.add(scls)

        schema = self._rename_begin(schema, context)
        schema = self._rename_innards(schema, context)
        schema = self._rename_finalize(schema, context)

        return schema

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        astnode = self._get_ast_node(schema, context)
        new_name = self.new_name

        ref = qlast.ObjectRef(
            name=new_name.name, module=new_name.module)
        return astnode(new_name=ref)

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> RenameObject:
        parent_ctx = context.current()
        parent_op = parent_ctx.op
        assert isinstance(parent_op, ObjectCommand)
        parent_class = parent_op.get_schema_metaclass()
        rename_class = ObjectCommandMeta.get_command_class_or_die(
            RenameObject, parent_class)
        return rename_class._rename_cmd_from_ast(schema, astnode, context)

    @classmethod
    def _rename_cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> RenameObject:
        assert isinstance(astnode, qlast.Rename)

        parent_ctx = context.current()
        parent_op = parent_ctx.op
        assert isinstance(parent_op, ObjectCommand)
        parent_class = parent_op.get_schema_metaclass()
        rename_class = ObjectCommandMeta.get_command_class_or_die(
            RenameObject, parent_class)

        new_name = cls._classname_from_ast(schema, astnode, context)
        assert isinstance(new_name, sn.Name)

        return rename_class(
            metaclass=parent_class,
            classname=parent_op.classname,
            new_name=sn.Name(
                module=new_name.module,
                name=new_name.name
            )
        )


class AlterObject(ObjectCommand[so.Object_T], Generic[so.Object_T]):
    _delta_action = 'alter'

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, AlterObject)

        if getattr(astnode, 'is_abstract', False):
            cmd.add(AlterObjectProperty(
                property='is_abstract',
                new_value=True
            ))

        added_bases = []
        dropped_bases: List[so.Object] = []

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

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        node = super()._get_ast(schema, context, parent_node=parent_node)
        if (node is not None and hasattr(node, 'commands') and
                not node.commands):
            # Alter node without subcommands.  Occurs when all
            # subcommands have been filtered out of DDL stream,
            # so filter it out as well.
            node = None
        return node

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        from . import types as s_types

        self._validate_legal_command(schema, context)

        for op in self.get_prerequisites():
            schema = op.apply(schema, context)

        for op in self.get_subcommands(type=AlterObjectFragment):
            schema = op.apply(schema, context)

        for op in self.get_subcommands(type=s_types.CollectionTypeCommand):
            schema = op.apply(schema, context)

        schema, props = self._get_field_updates(schema, context)
        schema = self.scls.update(schema, props)
        return schema

    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        for op in self.get_subcommands(include_prerequisites=False):
            if not isinstance(op, (AlterObjectFragment, AlterObjectProperty)):
                schema = op.apply(schema, context=context)

        return schema

    def _alter_finalize(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        return schema

    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        scls = self.get_object(schema, context)
        self.scls = scls

        with self.new_context(schema, context, scls):
            schema = self._alter_begin(schema, context)
            schema = self._alter_innards(schema, context)
            schema = self._alter_finalize(schema, context)

        return schema


class DeleteObject(ObjectCommand[so.Object_T], Generic[so.Object_T]):
    _delta_action = 'delete'

    def _delete_begin(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        from . import ordering

        self._validate_legal_command(schema, context)

        if (not context.canonical
                and not context.get_value(('delcanon', self.scls))):
            commands = self._canonicalize(schema, context, self.scls)
            root = DeltaRoot()
            root.update(commands)
            root = ordering.linearize_delta(root, schema, schema)
            self.update(root.get_subcommands())

        return schema

    def _canonicalize(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        scls: so.Object,
    ) -> Sequence[Command]:
        mcls = self.get_schema_metaclass()
        commands = []

        for refdict in mcls.get_refdicts():
            deleted_refs = set()

            all_refs = set(
                scls.get_field_value(schema, refdict.attr).objects(schema)
            )

            refcmds = cast(
                Tuple[ObjectCommand[so.Object], ...],
                self.get_subcommands(metaclass=refdict.ref_cls),
            )

            for op in refcmds:
                deleted_ref: so.Object = schema.get(op.classname)
                deleted_refs.add(deleted_ref)

            # Add implicit Delete commands for any local refs not
            # deleted explicitly.
            for ref in all_refs - deleted_refs:
                del_cmd = ObjectCommandMeta.get_command_class_or_die(
                    DeleteObject, type(ref))

                op = del_cmd(classname=ref.get_name(schema))
                subcmds = op._canonicalize(schema, context, ref)
                op.update(subcmds)
                commands.append(op)

        # Record the fact that DeleteObject._canonicalize
        # was called on this object to guard against possible
        # duplicate calls.
        context.store_value(('delcanon', scls), True)

        return commands

    def _delete_innards(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        for op in self.get_subcommands(metaclass=so.Object):
            schema = op.apply(schema, context=context)

        return schema

    def _delete_finalize(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        ref_strs = []

        if not context.canonical:
            refs = schema.get_referrers(self.scls)
            if refs:
                for ref in refs:
                    if (not context.is_deleting(ref)
                            and ref.is_blocking_ref(schema, self.scls)):
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

        schema = schema.delete(self.scls)
        return schema

    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        scls = self.get_object(schema, context)
        self.scls = scls

        with self.new_context(schema, context, scls):
            schema = self._delete_begin(schema, context)
            schema = self._delete_innards(schema, context)
            schema = self._delete_finalize(schema, context)

        return schema


class AlterSpecialObjectProperty(Command):
    astnode = qlast.SetSpecialField

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> AlterObjectProperty:
        assert isinstance(astnode, qlast.BaseSetField)

        propname = astnode.name
        parent_ctx = context.current()
        parent_op = parent_ctx.op
        assert isinstance(parent_op, ObjectCommand)
        parent_cls = parent_op.get_schema_metaclass()
        field = parent_cls.get_field(propname)

        new_value: Any = astnode.value

        if field.type is s_expr.Expression:
            orig_expr_field = parent_cls.get_field(f'orig_{field.name}')
            if orig_expr_field:
                orig_text = cls.get_orig_expr_text(
                    schema, parent_op.qlast, field.name)
            else:
                orig_text = None
            new_value = s_expr.Expression.from_ast(
                astnode.value,
                schema,
                context.modaliases,
                orig_text=orig_text,
            )
        elif field.name == 'required' and not new_value:
            # disallow dropping required that is not locally set
            parent_obj = parent_op.get_object(schema, context, default=None)
            errmsg = None

            if parent_obj:
                local_required = parent_obj.get_explicit_local_field_value(
                    schema, 'required', None)

                if not local_required:
                    parent_repr = parent_obj.get_verbosename(
                        schema, with_parent=True)
                    errmsg = (
                        f'cannot drop required qualifier because it is not '
                        f'defined directly on {parent_repr}'
                    )
            else:
                # We don't have a parent object which means we're in
                # the process of creating it.
                shortname = sn.shortname_from_fullname(
                    parent_op.classname).name

                parent_classname = parent_cls.get_schema_class_displayname()

                errmsg = (
                    f'cannot drop required qualifier of an '
                    f'inherited {parent_classname} {shortname!r}'
                )

            if errmsg:
                raise errors.SchemaError(errmsg, context=astnode.context)

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
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> AlterObjectProperty:
        from edb.edgeql import compiler as qlcompiler
        assert isinstance(astnode, qlast.BaseSetField)

        propname = astnode.name

        parent_ctx = context.current()
        parent_op = parent_ctx.op
        assert isinstance(parent_op, ObjectCommand)
        parent_cls = parent_op.get_schema_metaclass()
        field = parent_cls.get_field(propname)
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

        new_value: Any

        if field.type is s_expr.Expression:
            orig_expr_field = parent_cls.get_field(f'orig_{field.name}')
            if orig_expr_field:
                orig_text = cls.get_orig_expr_text(
                    schema, parent_op.qlast, field.name)
            else:
                orig_text = None
            new_value = s_expr.Expression.from_ast(
                astnode.value,
                schema,
                context.modaliases,
                orig_text=orig_text,
            )
        else:
            if isinstance(astnode.value, qlast.Tuple):
                new_value = tuple(
                    qlcompiler.evaluate_ast_to_python_val(
                        el.val, schema=schema)
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

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        value = self.new_value
        astcls = qlast.SetField

        new_value_empty = \
            (value is None or
                (isinstance(value, collections.abc.Container) and not value))

        parent_ctx = context.current()
        parent_op = parent_ctx.op
        assert isinstance(parent_op, ObjectCommand)
        assert parent_node is not None
        parent_cls = parent_op.get_schema_metaclass()
        field = parent_cls.get_field(self.property)
        parent_node_attr = parent_op.get_ast_attr_for_field(field.name)
        if field is None:
            raise errors.SchemaDefinitionError(
                f'{self.property!r} is not a valid field',
                context=self.source_context)

        if self.property == 'id':
            return None

        if (not field.allow_ddl_set
                and self.property != 'expr'
                and parent_node_attr is None):
            # Don't produce any AST if:
            #
            # * a field does not have the "allow_ddl_set" option, unless
            #   it's an 'expr' field.
            #
            #   'expr' fields come from the "USING" clause and are specially
            #   treated in parser and codegen.
            return None

        if self.source == 'inheritance':
            # We don't want to show inherited properties unless
            # we are in "descriptive_mode" and ...

            if ((not context.descriptive_mode
                    or self.property not in {'default', 'readonly'})
                    and parent_node_attr is None):
                # If property isn't 'default' or 'readonly' --
                # skip the AST for it.
                return None

            parentop_sn = sn.shortname_from_fullname(parent_op.classname).name
            if self.property == 'default' and parentop_sn == 'id':
                # If it's 'default' for the 'id' property --
                # skip the AST for it.
                return None

        if new_value_empty:
            return None

        if issubclass(field.type, s_expr.Expression):
            return self._get_expr_field_ast(
                schema,
                context,
                parent_op=parent_op,
                field=field,
                parent_node=parent_node,
                parent_node_attr=parent_node_attr,
            )
        elif (v := utils.is_nontrivial_container(value)) and v is not None:
            value = qlast.Tuple(elements=[
                utils.const_ast_from_python(el) for el in v
            ])
        elif isinstance(value, uuid.UUID):
            value = qlast.TypeCast(
                expr=qlast.StringConstant.from_python(str(value)),
                type=qlast.TypeName(
                    maintype=qlast.ObjectRef(
                        name='uuid',
                        module='std',
                    )
                )
            )
        else:
            value = utils.const_ast_from_python(value)

        return astcls(name=self.property, value=value)

    def _get_expr_field_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_op: ObjectCommand[so.Object],
        field: so.Field[Any],
        parent_node: qlast.DDLOperation,
        parent_node_attr: Optional[str],
    ) -> Optional[qlast.DDLOperation]:
        from edb import edgeql

        astcls: Type[qlast.BaseSetField]

        assert isinstance(self.new_value, s_expr.Expression)

        if self.property == 'expr':
            astcls = qlast.SetSpecialField
        else:
            astcls = qlast.SetField

        parent_cls = parent_op.get_schema_metaclass()
        has_shadow = parent_cls.get_field(f'orig_{field.name}') is not None

        if context.descriptive_mode:
            # When generating AST for DESCRIBE AS TEXT, we want
            # to use the original user-specified and unmangled
            # expression to render the object definition.
            expr_ql = edgeql.parse_fragment(self.new_value.origtext)
        else:
            # In all other DESCRIBE modes we want the original expression
            # to be there as a 'SET orig_<expr> := ...' command.
            # The mangled expression should be the main expression that
            # the object is defined with.
            expr_ql = self.new_value.qlast
            orig_fname = f'orig_{field.name}'
            if (has_shadow
                    and not qlast.get_ddl_field_value(
                        parent_node, orig_fname)):
                assert self.new_value.origtext is not None
                parent_node.commands.append(
                    qlast.SetField(
                        name=orig_fname,
                        value=qlast.StringConstant.from_python(
                            self.new_value.origtext),
                    )
                )

        if parent_node is not None and parent_node_attr is not None:
            setattr(parent_node, parent_node_attr, expr_ql)
            return None
        else:
            return astcls(name=self.property, value=expr_ql)

    def __repr__(self) -> str:
        return '<%s.%s "%s":"%s"->"%s">' % (
            self.__class__.__module__, self.__class__.__name__,
            self.property, self.old_value, self.new_value)


def compile_ddl(
    schema: s_schema.Schema,
    astnode: qlast.DDLOperation,
    *,
    context: Optional[CommandContext]=None,
) -> Command:

    if context is None:
        context = CommandContext()

    primary_cmdcls = CommandMeta._astnode_map.get(type(astnode))
    if primary_cmdcls is None:
        raise LookupError(f'no delta command class for AST node {astnode!r}')

    cmdcls = primary_cmdcls.command_for_ast_node(astnode, schema, context)

    context_class = cmdcls.get_context_class()
    if context_class is not None:
        modaliases = cmdcls._modaliases_from_ast(schema, astnode, context)
        ctx = context_class(schema, op=_dummy_command, modaliases=modaliases)
        with context(ctx):
            cmd = cmdcls._cmd_tree_from_ast(schema, astnode, context)
    else:
        cmd = cmdcls._cmd_tree_from_ast(schema, astnode, context)

    return cmd
