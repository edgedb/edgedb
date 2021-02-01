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
import contextlib
import functools
import itertools
import uuid

import typing_inspect

from edb import errors

from edb.common import adapter
from edb.common import checked
from edb.common import markup
from edb.common import ordered
from edb.common import parsing
from edb.common import struct
from edb.common import topological
from edb.common import verutils

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes
from edb.edgeql import quote as qlquote

from . import expr as s_expr
from . import name as sn
from . import objects as so
from . import schema as s_schema
from . import utils


def delta_objects(
    old: Iterable[so.Object_T],
    new: Iterable[so.Object_T],
    sclass: Type[so.Object_T],
    *,
    parent_confidence: Optional[float] = None,
    context: so.ComparisonContext,
    old_schema: s_schema.Schema,
    new_schema: s_schema.Schema,
) -> DeltaRoot:

    delta = DeltaRoot()

    oldkeys = {o: o.hash_criteria(old_schema) for o in old}
    newkeys = {o: o.hash_criteria(new_schema) for o in new}

    unchanged = set(oldkeys.values()) & set(newkeys.values())

    old = ordered.OrderedSet[so.Object_T](
        o for o, checksum in oldkeys.items()
        if checksum not in unchanged
    )
    new = ordered.OrderedSet[so.Object_T](
        o for o, checksum in newkeys.items()
        if checksum not in unchanged
    )

    oldnames = {o.get_name(old_schema) for o in old}
    newnames = {o.get_name(new_schema) for o in new}
    common_names = oldnames & newnames

    pairs = sorted(
        itertools.product(new, old),
        key=lambda pair: pair[0].get_name(new_schema) not in common_names,
    )

    full_matrix: List[Tuple[so.Object_T, so.Object_T, float]] = []

    # If there are any renames that are already decided on, honor those first
    renames_x: Set[sn.Name] = set()
    renames_y: Set[sn.Name] = set()
    for y in old:
        rename = context.renames.get((type(y), y.get_name(old_schema)))
        if rename:
            renames_x.add(rename.new_name)
            renames_y.add(rename.classname)

    if context.guidance is not None:
        guidance = context.guidance

        def can_create(name: sn.Name) -> bool:
            return (sclass, name) not in guidance.banned_creations

        def can_alter(old_name: sn.Name, new_name: sn.Name) -> bool:
            return (sclass, (old_name, new_name)) not in guidance.banned_alters

        def can_delete(name: sn.Name) -> bool:
            return (sclass, name) not in guidance.banned_deletions
    else:
        def can_create(name: sn.Name) -> bool:
            return True

        def can_alter(old_name: sn.Name, new_name: sn.Name) -> bool:
            return True

        def can_delete(name: sn.Name) -> bool:
            return True

    for x, y in pairs:
        x_name = x.get_name(new_schema)
        y_name = y.get_name(old_schema)

        if can_alter(y_name, x_name):
            similarity = y.compare(
                x,
                our_schema=old_schema,
                their_schema=new_schema,
                context=context,
            )
        else:
            similarity = 0.0

        full_matrix.append((x, y, similarity))

    full_matrix.sort(
        key=lambda v: (
            1.0 - v[2],
            str(v[0].get_name(new_schema)),
            str(v[1].get_name(old_schema)),
        ),
    )

    full_matrix_x = {}
    full_matrix_y = {}

    seen_x = set()
    seen_y = set()
    x_alter_variants: Dict[so.Object_T, int] = collections.defaultdict(int)
    y_alter_variants: Dict[so.Object_T, int] = collections.defaultdict(int)
    comparison_map: Dict[so.Object_T, Tuple[float, so.Object_T]] = {}
    comparison_map_y: Dict[so.Object_T, Tuple[float, so.Object_T]] = {}

    # Find the top similarity pairs
    for x, y, similarity in full_matrix:
        if x not in seen_x and y not in seen_y:
            comparison_map[x] = (similarity, y)
            comparison_map_y[y] = (similarity, x)
            seen_x.add(x)
            seen_y.add(y)

        if x not in full_matrix_x:
            full_matrix_x[x] = (similarity, y)

        if y not in full_matrix_y:
            full_matrix_y[y] = (similarity, x)

        if (
            can_alter(y.get_name(old_schema), x.get_name(new_schema))
            and full_matrix_x[x][0] != 1.0
            and full_matrix_y[y][0] != 1.0
        ):
            x_alter_variants[x] += 1
            y_alter_variants[y] += 1

    alters = []

    if comparison_map:
        if issubclass(sclass, so.InheritingObject):
            # Generate the diff from the top of the inheritance
            # hierarchy, since changes to parent objects may inform
            # how the delta in child objects is treated.
            order_x = cast(
                Iterable[so.Object_T],
                _sort_by_inheritance(
                    new_schema,
                    cast(Iterable[so.InheritingObject], comparison_map),
                ),
            )
        else:
            order_x = comparison_map

        for x in order_x:
            s, y = comparison_map[x]
            x_name = x.get_name(new_schema)
            y_name = y.get_name(old_schema)
            if (
                0.6 < s < 1.0
                or (
                    (not can_create(x_name) or not can_delete(y_name))
                    and can_alter(y_name, x_name)
                )
                or x_name in renames_x
            ):
                if (
                    (x_alter_variants[x] > 1 or can_create(x_name))
                    and parent_confidence != 1.0
                ):
                    confidence = s
                else:
                    # TODO: investigate how parent confidence should be
                    # correlated with child confidence in cases of explicit
                    # nested ALTER.
                    confidence = 1.0

                alter = y.as_alter_delta(
                    other=x,
                    context=context,
                    self_schema=old_schema,
                    other_schema=new_schema,
                    confidence=confidence,
                )

                alter.set_annotation('confidence', confidence)
                alters.append(alter)

    created = new - {x for x, (s, _) in comparison_map.items() if s > 0.6}

    for x in created:
        x_name = x.get_name(new_schema)
        if can_create(x_name) and x_name not in renames_x:
            create = x.as_create_delta(schema=new_schema, context=context)
            if x_alter_variants[x] > 0 and parent_confidence != 1.0:
                confidence = full_matrix_x[x][0]
            else:
                confidence = 1.0
            create.set_annotation('confidence', confidence)
            delta.add(create)

    delta.update(alters)

    deleted_order: Iterable[so.Object_T]
    deleted = old - {y for _, (s, y) in comparison_map.items() if s > 0.6}

    if issubclass(sclass, so.InheritingObject):
        deleted_order = _sort_by_inheritance(  # type: ignore
            old_schema,
            cast(Iterable[so.InheritingObject], deleted),
        )
    else:
        deleted_order = deleted

    for y in deleted_order:
        y_name = y.get_name(old_schema)
        if can_delete(y_name) and y_name not in renames_y:
            delete = y.as_delete_delta(schema=old_schema, context=context)
            if y_alter_variants[y] > 0 and parent_confidence != 1.0:
                confidence = full_matrix_y[y][0]
            else:
                confidence = 1.0
            delete.set_annotation('confidence', confidence)
            delta.add(delete)

    return delta


def _sort_by_inheritance(
    schema: s_schema.Schema,
    objs: Iterable[so.InheritingObjectT],
) -> Iterable[so.InheritingObjectT]:
    graph = {}
    for x in objs:
        graph[x] = topological.DepGraphEntry(
            item=x,
            deps=ordered.OrderedSet(x.get_bases(schema).objects(schema)),
            extra=False,
        )

    return topological.sort(graph, allow_unresolved=True)


def sort_by_cross_refs(
    schema: s_schema.Schema,
    objs: Iterable[so.Object_T],
) -> Tuple[so.Object_T, ...]:
    """Sort an iterable of objects according to cross-references between them.

    Return a toplogical ordering of a graph of objects joined by references.
    It is assumed that the graph has no cycles.
    """
    graph = {}
    for x in objs:
        graph[x] = topological.DepGraphEntry(
            item=x,
            deps=set(schema.get_referrers(x)),
            extra=False,
        )

    return topological.sort(graph, allow_unresolved=True)  # type: ignore


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


# We use _DummyObject for contexts where an instance of an object is
# required by type signatures, and the actual reference will be quickly
# replaced by a real object.
_dummy_object = so.Object(
    _private_id=uuid.UUID('C0FFEE00-C0DE-0000-0000-000000000000'),
)


Command_T = TypeVar("Command_T", bound="Command")
Command_T_co = TypeVar("Command_T_co", bound="Command", covariant=True)


class Command(struct.MixedStruct, metaclass=CommandMeta):
    source_context = struct.Field(parsing.ParserContext, default=None)
    canonical = struct.Field(bool, default=False)

    _context_class: Optional[Type[CommandContextToken[Command]]] = None

    ops: List[Command]
    before_ops: List[Command]

    #: AlterObjectProperty lookup table for get|set_attribute_value
    _attrs: Dict[str, AlterObjectProperty]
    #: AlterSpecialObjectField lookup table
    _special_attrs: Dict[str, AlterSpecialObjectField[so.Object]]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.ops = []
        self.before_ops = []
        self.qlast: qlast.DDLOperation
        self._attrs = {}
        self._special_attrs = {}

    def copy(self: Command_T) -> Command_T:
        result = super().copy()
        result.ops = [op.copy() for op in self.ops]
        result.before_ops = [op.copy() for op in self.before_ops]
        return result

    def get_verb(self) -> str:
        """Return a verb representing this command in infinitive form."""
        raise NotImplementedError

    def get_friendly_description(
        self,
        *,
        parent_op: Optional[Command] = None,
        schema: Optional[s_schema.Schema] = None,
        object: Any = None,
        object_desc: Optional[str] = None,
    ) -> str:
        """Return a friendly description of this command in imperative mood.

        The result is used in error messages and other user-facing renderings
        of the command.
        """
        raise NotImplementedError

    @classmethod
    def adapt(cls: Type[Command_T], obj: Command) -> Command_T:
        result = obj.copy_with_class(cls)
        mcls = cast(CommandMeta, type(cls))
        for op in obj.get_prerequisites():
            result.add_prerequisite(mcls.adapt(op))
        for op in obj.get_subcommands(include_prerequisites=False):
            result.add(mcls.adapt(op))
        return result

    def is_data_safe(self) -> bool:
        return False

    def get_required_user_input(self) -> Dict[str, str]:
        return {}

    def record_diff_annotations(
        self,
        schema: s_schema.Schema,
        orig_schema: Optional[s_schema.Schema],
        context: so.ComparisonContext,
    ) -> None:
        """Record extra information on a delta obtained by diffing schemas.

        This provides an apportunity for a delta command to annotate itself
        in schema diff schenarios (i.e. migrations).

        Args:
            schema:
                Final schema of a migration.

            orig_schema:
                Original schema of a migration.

            context:
                Schema comparison context.
        """
        pass

    def resolve_obj_collection(
        self,
        value: Any,
        schema: s_schema.Schema,
    ) -> Sequence[so.Object]:
        sequence: Sequence[so.Object]
        if isinstance(value, so.ObjectCollection):
            sequence = value.objects(schema)
        else:
            sequence = []
            for v in value:
                if isinstance(v, so.Shell):
                    val = v.resolve(schema)
                else:
                    val = v
                sequence.append(val)
        return sequence

    def _resolve_attr_value(
        self,
        value: Any,
        fname: str,
        field: so.Field[Any],
        schema: s_schema.Schema,
    ) -> Any:
        ftype = field.type

        if isinstance(value, so.Shell):
            value = value.resolve(schema)
        else:
            if issubclass(ftype, so.ObjectDict):
                if isinstance(value, so.ObjectDict):
                    items = dict(value.items(schema))
                elif isinstance(value, collections.abc.Mapping):
                    items = {}
                    for k, v in value.items():
                        if isinstance(v, so.Shell):
                            val = v.resolve(schema)
                        else:
                            val = v
                        items[k] = val

                value = ftype.create(schema, items)

            elif issubclass(ftype, so.ObjectCollection):
                sequence = self.resolve_obj_collection(value, schema)
                value = ftype.create(schema, sequence)

            elif issubclass(ftype, s_expr.Expression):
                if value is not None:
                    value = ftype.from_expr(value, schema)

            else:
                value = field.coerce_value(schema, value)

        return value

    def enumerate_attributes(self) -> Tuple[str, ...]:
        return tuple(self._attrs)

    def _enumerate_attribute_cmds(self) -> Tuple[AlterObjectProperty, ...]:
        return tuple(self._attrs.values())

    def has_attribute_value(self, attr_name: str) -> bool:
        return attr_name in self._attrs or attr_name in self._special_attrs

    def _get_simple_attribute_set_cmd(
        self,
        attr_name: str,
    ) -> Optional[AlterObjectProperty]:
        return self._attrs.get(attr_name)

    def _get_attribute_set_cmd(
        self,
        attr_name: str,
    ) -> Optional[AlterObjectProperty]:
        cmd = self._get_simple_attribute_set_cmd(attr_name)
        if cmd is None:
            special_cmd = self._special_attrs.get(attr_name)
            if special_cmd is not None:
                cmd = special_cmd._get_attribute_set_cmd(attr_name)
        return cmd

    def get_attribute_value(
        self,
        attr_name: str,
    ) -> Any:
        op = self._get_attribute_set_cmd(attr_name)
        if op is not None:
            return op.new_value
        else:
            return None

    def get_local_attribute_value(
        self,
        attr_name: str,
    ) -> Any:
        """Return the new value of field, if not inherited."""
        op = self._get_attribute_set_cmd(attr_name)
        if op is not None and not op.new_inherited:
            return op.new_value
        else:
            return None

    def get_orig_attribute_value(
        self,
        attr_name: str,
    ) -> Any:
        op = self._get_attribute_set_cmd(attr_name)
        if op is not None:
            return op.old_value
        else:
            return None

    def is_attribute_inherited(
        self,
        attr_name: str,
    ) -> bool:
        op = self._get_attribute_set_cmd(attr_name)
        if op is not None:
            return op.new_inherited
        else:
            return False

    def is_attribute_computed(
        self,
        attr_name: str,
    ) -> bool:
        op = self._get_attribute_set_cmd(attr_name)
        if op is not None:
            return op.new_computed
        else:
            return False

    def get_attribute_source_context(
        self,
        attr_name: str,
    ) -> Optional[parsing.ParserContext]:
        op = self._get_attribute_set_cmd(attr_name)
        if op is not None:
            return op.source_context
        else:
            return None

    def set_attribute_value(
        self,
        attr_name: str,
        value: Any,
        *,
        orig_value: Any = None,
        inherited: bool = False,
        orig_inherited: Optional[bool] = None,
        computed: bool = False,
        from_default: bool = False,
        orig_computed: Optional[bool] = None,
        source_context: Optional[parsing.ParserContext] = None,
    ) -> Command:
        orig_op = op = self._get_simple_attribute_set_cmd(attr_name)
        if op is None:
            op = AlterObjectProperty(property=attr_name, new_value=value)
        else:
            op.new_value = value

        if orig_inherited is None:
            orig_inherited = inherited
        op.new_inherited = inherited
        op.old_inherited = orig_inherited

        if orig_computed is None:
            orig_computed = computed
        op.new_computed = computed
        op.old_computed = orig_computed
        op.from_default = from_default

        if source_context is not None:
            op.source_context = source_context
        if orig_value is not None:
            op.old_value = orig_value

        if orig_op is None:
            self.add(op)

        return op

    def discard_attribute(self, attr_name: str) -> None:
        op = self._get_attribute_set_cmd(attr_name)
        if op is not None:
            self.discard(op)

    def __iter__(self) -> NoReturn:
        raise TypeError(f'{type(self)} object is not iterable')

    @overload
    def get_subcommands(
        self,
        *,
        type: Type[Command_T],
        metaclass: Optional[Type[so.Object]] = None,
        exclude: Union[Type[Command], Tuple[Type[Command], ...], None] = None,
        include_prerequisites: bool = True,
    ) -> Tuple[Command_T, ...]:
        ...

    @overload
    def get_subcommands(  # NoQA: F811
        self,
        *,
        type: None = None,
        metaclass: Optional[Type[so.Object]] = None,
        exclude: Union[Type[Command], Tuple[Type[Command], ...], None] = None,
        include_prerequisites: bool = True,
    ) -> Tuple[Command, ...]:
        ...

    def get_subcommands(  # NoQA: F811
        self,
        *,
        type: Union[Type[Command_T], None] = None,
        metaclass: Optional[Type[so.Object]] = None,
        exclude: Union[Type[Command], Tuple[Type[Command], ...], None] = None,
        include_prerequisites: bool = True,
    ) -> Tuple[Command, ...]:
        ops: Iterable[Command]
        if include_prerequisites:
            ops = itertools.chain(self.before_ops, self.ops)
        else:
            ops = self.ops

        filters = []

        if type is not None:
            t = type
            filters.append(lambda i: isinstance(i, t))

        if exclude is not None:
            ex = exclude
            filters.append(lambda i: not isinstance(i, ex))

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
        return bool(self.ops) or bool(self.before_ops)

    def get_nonattr_subcommand_count(self) -> int:
        count = 0
        attr_cmds = (AlterObjectProperty, AlterSpecialObjectField)
        for op in self.ops:
            if not isinstance(op, attr_cmds):
                count += 1
        for op in self.before_ops:
            if not isinstance(op, attr_cmds):
                count += 1
        return count

    def prepend_prerequisite(self, command: Command) -> None:
        if isinstance(command, CommandGroup):
            for op in reversed(command.get_subcommands()):
                self.prepend_prerequisite(op)
        else:
            self.before_ops.insert(0, command)

    def add_prerequisite(self, command: Command) -> None:
        if isinstance(command, CommandGroup):
            self.before_ops.extend(command.get_subcommands())
        else:
            self.before_ops.append(command)

    def prepend(self, command: Command) -> None:
        if isinstance(command, CommandGroup):
            for op in reversed(command.get_subcommands()):
                self.prepend(op)
        else:
            if isinstance(command, AlterObjectProperty):
                self._attrs[command.property] = command
            elif isinstance(command, AlterSpecialObjectField):
                self._special_attrs[command._field] = command
            self.ops.insert(0, command)

    def add(self, command: Command) -> None:
        if isinstance(command, CommandGroup):
            self.update(command.get_subcommands())
        else:
            if isinstance(command, AlterObjectProperty):
                self._attrs[command.property] = command
            elif isinstance(command, AlterSpecialObjectField):
                self._special_attrs[command._field] = command
            self.ops.append(command)

    def update(self, commands: Iterable[Command]) -> None:  # type: ignore
        for command in commands:
            self.add(command)

    def replace(self, existing: Command, new: Command) -> None:  # type: ignore
        i = self.ops.index(existing)
        self.ops[i] = new

    def replace_all(self, commands: Iterable[Command]) -> None:
        self.ops.clear()
        self._attrs.clear()
        self._special_attrs.clear()
        self.update(commands)

    def discard(self, command: Command) -> None:
        try:
            self.ops.remove(command)
        except ValueError:
            pass
        try:
            self.before_ops.remove(command)
        except ValueError:
            pass
        if isinstance(command, AlterObjectProperty):
            self._attrs.pop(command.property)
        elif isinstance(command, AlterSpecialObjectField):
            self._special_attrs.pop(command._field)

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
    def localnames_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> Set[str]:
        localnames: Set[str] = set()
        if isinstance(astnode, qlast.DDLCommand):
            for alias in astnode.aliases:
                if isinstance(alias, qlast.AliasedExpr):
                    localnames.add(alias.alias)

        return localnames

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
    ) -> Command:
        return cls()

    @classmethod
    def as_markup(cls, self: Command, *, ctx: markup.Context) -> markup.Markup:
        node = markup.elements.lang.TreeNode(name=str(self))

        for dd in self.get_subcommands():
            if isinstance(dd, AlterObjectProperty):
                diff = markup.elements.doc.ValueDiff(
                    before=repr(dd.old_value), after=repr(dd.new_value))

                if dd.new_inherited:
                    diff.comment = 'inherited'
                elif dd.new_computed:
                    diff.comment = 'computed'

                node.add_child(label=dd.property, node=diff)
            else:
                node.add_child(node=markup.serialize(dd, ctx=ctx))

        return node

    @classmethod
    def get_context_class(
        cls: Type[Command_T],
    ) -> Optional[Type[CommandContextToken[Command_T]]]:
        return cls._context_class  # type: ignore

    @classmethod
    def get_context_class_or_die(
        cls: Type[Command_T],
    ) -> Type[CommandContextToken[Command_T]]:
        ctxcls = cls.get_context_class()
        if ctxcls is None:
            raise RuntimeError(f'context class not defined for {cls}')
        return ctxcls

    def formatfields(
        self,
        formatter: str = 'str',
    ) -> Iterator[Tuple[str, str]]:
        """Return an iterator over fields formatted using `formatter`."""
        for name, field in self.__class__._fields.items():
            value = getattr(self, name)
            default = field.default
            formatter_obj = field.formatters.get(formatter)
            if formatter_obj and value != default:
                yield (name, formatter_obj(value))


class Nop(Command):
    pass


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


class CommandContextToken(Generic[Command_T]):
    original_schema: s_schema.Schema
    op: Command_T
    modaliases: Mapping[Optional[str], str]
    localnames: AbstractSet[str]
    inheritance_merge: Optional[bool]
    inheritance_refdicts: Optional[AbstractSet[str]]
    mark_derived: Optional[bool]
    preserve_path_id: Optional[bool]
    enable_recursion: Optional[bool]
    transient_derivation: Optional[bool]

    def __init__(
        self,
        schema: s_schema.Schema,
        op: Command_T,
        *,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
        # localnames are the names defined locally via with block or
        # as function parameters and should not be fully-qualified
        localnames: AbstractSet[str] = frozenset(),
    ) -> None:
        self.original_schema = schema
        self.op = op
        self.modaliases = modaliases if modaliases is not None else {}
        self.localnames = localnames
        self.inheritance_merge = None
        self.inheritance_refdicts = None
        self.mark_derived = None
        self.preserve_path_id = None
        self.enable_recursion = None
        self.transient_derivation = None


class CommandContextWrapper(Generic[Command_T_co]):
    def __init__(
        self,
        context: CommandContext,
        token: CommandContextToken[Command_T_co],
    ) -> None:
        self.context = context
        self.token = token

    def __enter__(self) -> CommandContextToken[Command_T_co]:
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
        localnames: AbstractSet[str] = frozenset(),
        declarative: bool = False,
        stdmode: bool = False,
        testmode: bool = False,
        internal_schema_mode: bool = False,
        disable_dep_verification: bool = False,
        descriptive_mode: bool = False,
        schema_object_ids: Optional[
            Mapping[Tuple[sn.Name, Optional[str]], uuid.UUID]
        ] = None,
        backend_runtime_params: Optional[Any] = None,
        compat_ver: Optional[verutils.Version] = None,
    ) -> None:
        self.stack: List[CommandContextToken[Command]] = []
        self._cache: Dict[Hashable, Any] = {}
        self._values: Dict[Hashable, Any] = {}
        self.declarative = declarative
        self.schema = schema
        self._modaliases = modaliases if modaliases is not None else {}
        self._localnames = localnames
        self.stdmode = stdmode
        self.internal_schema_mode = internal_schema_mode
        self.testmode = testmode
        self.descriptive_mode = descriptive_mode
        self.disable_dep_verification = disable_dep_verification
        self.renames: Dict[sn.Name, sn.Name] = {}
        self.early_renames: Dict[sn.Name, sn.Name] = {}
        self.renamed_objs: Set[so.Object] = set()
        self.change_log: Dict[Tuple[Type[so.Object], str], Set[so.Object]] = (
            collections.defaultdict(set))
        self.schema_object_ids = schema_object_ids
        self.backend_runtime_params = backend_runtime_params
        self.affected_finalization: Dict[
            Command,
            List[Tuple[Command, Command, List[str]]],
        ] = collections.defaultdict(list)
        self.compat_ver = compat_ver

    @property
    def modaliases(self) -> Mapping[Optional[str], str]:
        maps = [t.modaliases for t in reversed(self.stack)]
        maps.append(self._modaliases)
        return collections.ChainMap(*maps)

    @property
    def localnames(self) -> Set[str]:
        ign: Set[str] = set()
        for ctx in reversed(self.stack):
            ign.update(ctx.localnames)
        ign.update(self._localnames)
        return ign

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
    def transient_derivation(self) -> bool:
        for ctx in reversed(self.stack):
            if ctx.transient_derivation is not None:
                return ctx.transient_derivation

        return False

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
                   and ctx.op.scls == obj for ctx in self.stack)

    def push(self, token: CommandContextToken[Command]) -> None:
        self.stack.append(token)

    def pop(self) -> CommandContextToken[Command]:
        return self.stack.pop()

    def get_referrer_name(
        self, referrer_ctx: CommandContextToken[ObjectCommand[so.Object]],
    ) -> sn.QualName:
        referrer_name = referrer_ctx.op.classname
        renamed = self.early_renames.get(referrer_name)
        if renamed:
            referrer_name = renamed
        else:
            renamed = self.renames.get(referrer_name)
            if renamed:
                referrer_name = renamed
        assert isinstance(referrer_name, sn.QualName)
        return referrer_name

    def get(
        self,
        cls: Union[Type[Command], Type[CommandContextToken[Command]]],
    ) -> Optional[CommandContextToken[Command]]:
        if issubclass(cls, Command):
            ctxcls = cls.get_context_class()
            assert ctxcls is not None
        else:
            ctxcls = cls

        for item in reversed(self.stack):
            if isinstance(item, ctxcls):
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

    @contextlib.contextmanager
    def suspend_dep_verification(self) -> Iterator[CommandContext]:
        dep_ver = self.disable_dep_verification
        self.disable_dep_verification = True
        try:
            yield self
        finally:
            self.disable_dep_verification = dep_ver

    def __call__(
        self,
        token: CommandContextToken[Command_T],
    ) -> CommandContextWrapper[Command_T]:
        return CommandContextWrapper(self, token)

    def compat_ver_is_before(
        self,
        ver: Tuple[int, int, verutils.VersionStage, int],
    ) -> bool:
        return self.compat_ver is not None and self.compat_ver < ver


class ContextStack:

    def __init__(
        self,
        contexts: Iterable[CommandContextWrapper[Command]],
    ) -> None:
        self._contexts = list(contexts)

    def push(self, ctx: CommandContextWrapper[Command]) -> None:
        self._contexts.append(ctx)

    def pop(self) -> None:
        self._contexts.pop()

    @contextlib.contextmanager
    def __call__(self) -> Generator[None, None, None]:
        with contextlib.ExitStack() as stack:
            for ctx in self._contexts:
                stack.enter_context(ctx)  # type: ignore
            yield


class DeltaRootContext(CommandContextToken["DeltaRoot"]):
    pass


class DeltaRoot(CommandGroup, context_class=DeltaRootContext):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.new_types: Set[uuid.UUID] = set()

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


class Query(Command):
    """A special delta command representing a non-DDL query.

    These are found in migrations.
    """

    astnode = qlast.Query

    expr = struct.Field(s_expr.Expression)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> Command:
        return cls(
            source_context=astnode.context,
            qlast=astnode,
            expr=s_expr.Expression.from_ast(
                astnode,
                schema=schema,
                modaliases=context.modaliases,
                localnames=context.localnames,
            ),
        )

    @classmethod
    def as_markup(cls, self: Command, *, ctx: markup.Context) -> markup.Markup:
        node = super().as_markup(self, ctx=ctx)
        assert isinstance(node, markup.elements.lang.TreeNode)
        assert isinstance(self, Query)
        qltext = self.expr.text
        node.add_child(node=markup.elements.lang.MultilineString(str=qltext))
        return node

    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        if not self.expr.is_compiled():
            self.expr = self.expr.compiled(
                self.expr,
                schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                )
            )
        return schema


_command_registry: Dict[
    Tuple[str, Type[so.Object]],
    Type[ObjectCommand[so.Object]]
] = {}


def get_object_command_class(
    cmdtype: Type[Command_T],
    schema_metaclass: Type[so.Object],
) -> Optional[Type[Command_T]]:
    assert issubclass(cmdtype, ObjectCommand)
    return _command_registry.get(  # type: ignore
        (cmdtype._delta_action, schema_metaclass),
    )


def get_object_command_class_or_die(
    cmdtype: Type[Command_T],
    schema_metaclass: Type[so.Object],
) -> Type[Command_T]:
    cmdcls = get_object_command_class(cmdtype, schema_metaclass)
    if cmdcls is None:
        raise TypeError(f'missing {cmdtype.__name__} implementation '
                        f'for {schema_metaclass.__name__}')
    return cmdcls


ObjectCommand_T = TypeVar("ObjectCommand_T", bound='ObjectCommand[so.Object]')


class ObjectCommand(Command, Generic[so.Object_T]):
    """Base class for all Object-related commands."""

    #: Full name of the object this command operates on.
    classname = struct.Field(sn.Name)
    #: An optional set of values neceessary to render the command in DDL.
    ddl_identity = struct.Field(
        dict,  # type: ignore
        default=None,
    )
    #: An optional dict of metadata annotations for this command.
    annotations = struct.Field(
        dict,  # type: ignore
        default=None,
    )
    #: Auxiliary object information that might be necessary to process
    #: this command, derived from object fields.
    aux_object_data = struct.Field(
        dict,  # type: ignore
        default=None,
    )
    #: When this command is produced by a breakup of a larger command
    #: subtree, *orig_cmd_type* would contain the type of the original
    #: command.
    orig_cmd_type = struct.Field(
        CommandMeta,
        default=None,
    )

    scls: so.Object_T
    _delta_action: ClassVar[str]
    _schema_metaclass: ClassVar[Optional[Type[so.Object_T]]] = None
    astnode: ClassVar[Union[Type[qlast.DDLOperation],
                            List[Type[qlast.DDLOperation]]]]

    def __init_subclass__(cls, *args: Any, **kwargs: Any) -> None:
        # Check if the command subclass has been parametrized with
        # a concrete schema object class, and if so, record the
        # argument to be made available via get_schema_metaclass().
        super().__init_subclass__(*args, **kwargs)  # type: ignore
        generic_bases = typing_inspect.get_generic_bases(cls)
        mcls: Optional[Type[so.Object]] = None
        for gb in generic_bases:
            base_origin = typing_inspect.get_origin(gb)
            # Find the <ObjectCommand>[Type] base, where ObjectCommand
            # is any ObjectCommand subclass.
            if (
                base_origin is not None
                and issubclass(base_origin, ObjectCommand)
            ):
                args = typing_inspect.get_args(gb)
                if len(args) != 1:
                    raise AssertionError(
                        'expected only one argument to ObjectCommand generic')
                arg_0 = args[0]
                if not typing_inspect.is_typevar(arg_0):
                    assert issubclass(arg_0, so.Object)
                    if not arg_0.is_abstract():
                        mcls = arg_0
                        break

        if mcls is not None:
            existing = getattr(cls, '_schema_metaclass', None)
            if existing is not None and existing is not mcls:
                raise TypeError(
                    f'cannot redefine schema class of {cls.__name__} to '
                    f'{mcls.__name__}: a superclass has already defined it as '
                    f'{existing.__name__}'
                )
            cls._schema_metaclass = mcls

        # If this is a command adapter rather than the actual
        # command, skip the command class registration.
        if not cls.has_adaptee():
            delta_action = getattr(cls, '_delta_action', None)
            schema_metaclass = getattr(cls, '_schema_metaclass', None)
            if schema_metaclass is not None and delta_action is not None:
                key = delta_action, schema_metaclass
                cmdcls = _command_registry.get(key)
                if cmdcls is not None:
                    raise TypeError(
                        f'Action {cls._delta_action!r} for '
                        f'{schema_metaclass} is already claimed by {cmdcls}'
                    )
                _command_registry[key] = cls  # type: ignore

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: CommandContext,
    ) -> sn.Name:
        return sn.UnqualName(astnode.name.name)

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

    def is_data_safe(self) -> bool:
        if self.get_schema_metaclass()._data_safe:
            return True
        else:
            return all(
                subcmd.is_data_safe()
                for subcmd in self.get_subcommands()
            )

    def get_required_user_input(self) -> Dict[str, str]:
        result: Dict[str, str] = self.get_annotation('required_input')
        if result is None:
            result = {}
        for cmd in self.get_subcommands():
            subresult = cmd.get_required_user_input()
            if subresult:
                result.update(subresult)
        return result

    def get_friendly_description(
        self,
        *,
        parent_op: Optional[Command] = None,
        schema: Optional[s_schema.Schema] = None,
        object: Any = None,
        object_desc: Optional[str] = None,
    ) -> str:
        """Return a friendly description of this command in imperative mood.

        The result is used in error messages and other user-facing renderings
        of the command.
        """
        object_desc = self.get_friendly_object_name_for_description(
            parent_op=parent_op,
            schema=schema,
            object=object,
            object_desc=object_desc,
        )
        return f'{self.get_verb()} {object_desc}'

    def get_user_prompt(
        self,
        *,
        parent_op: Optional[Command] = None,
    ) -> Tuple[str, str]:
        """Return a human-friendly prompt describing this operation."""

        # The prompt is determined by the *innermost* subcommand as
        # long as all its parents have exactly one child.  The tree
        # traversal stops on fragments and CreateObject commands,
        # since there is no point to prompt about the creation of
        # object innards.
        if (
            not isinstance(self, AlterObjectFragment)
            and (
                not isinstance(self, CreateObject)
                and (
                    self.orig_cmd_type is None
                    or not issubclass(
                        self.orig_cmd_type, CreateObject
                    )
                )
            )
        ):
            subcommands = self.get_subcommands(
                type=ObjectCommand,
                exclude=AlterObjectProperty,
            )
            if len(subcommands) == 1:
                subcommand = subcommands[0]
                if isinstance(subcommand, AlterObjectFragment):
                    return subcommand.get_user_prompt(parent_op=parent_op)
                else:
                    return subcommand.get_user_prompt(parent_op=self)

        desc = self.get_friendly_description(parent_op=parent_op)
        prompt_text = f'did you {desc}?'
        prompt_id = get_object_command_id(self)
        assert prompt_id is not None
        return prompt_id, prompt_text

    def get_parent_op(
        self,
        context: CommandContext,
    ) -> ObjectCommand[so.Object]:
        parent = context.parent()
        if parent is None:
            raise AssertionError(f'{self!r} has no parent context')
        op = parent.op
        assert isinstance(op, ObjectCommand)
        return op

    @classmethod
    @functools.lru_cache()
    def _get_special_handler(
        cls,
        field_name: str,
    ) -> Optional[Type[AlterSpecialObjectField[so.Object]]]:
        if (
            issubclass(cls, AlterObjectOrFragment)
            and not issubclass(cls, AlterSpecialObjectField)
        ):
            schema_cls = cls.get_schema_metaclass()
            return get_special_field_alter_handler(field_name, schema_cls)
        else:
            return None

    def set_attribute_value(
        self,
        attr_name: str,
        value: Any,
        *,
        orig_value: Any = None,
        inherited: bool = False,
        orig_inherited: Optional[bool] = None,
        computed: bool = False,
        orig_computed: Optional[bool] = None,
        from_default: bool = False,
        source_context: Optional[parsing.ParserContext] = None,
    ) -> Command:
        special = type(self)._get_special_handler(attr_name)
        op = self._get_attribute_set_cmd(attr_name)
        top_op: Optional[Command] = None

        if orig_inherited is None:
            orig_inherited = inherited

        if orig_computed is None:
            orig_computed = computed

        if op is None:
            op = AlterObjectProperty(
                property=attr_name,
                new_value=value,
                old_value=orig_value,
                new_inherited=inherited,
                old_inherited=orig_inherited,
                new_computed=computed,
                old_computed=orig_computed,
                from_default=from_default,
                source_context=source_context,
            )

            if special is not None:
                top_op = special(classname=self.classname)
                top_op.add(op)
            else:
                top_op = op
        else:
            op.new_value = value
            op.new_inherited = inherited
            op.old_inherited = orig_inherited

            op.new_computed = computed
            op.old_computed = orig_computed
            op.from_default = from_default

            if source_context is not None:
                op.source_context = source_context
            if orig_value is not None:
                op.old_value = orig_value

        if top_op is not None:
            self.add(top_op)
            return top_op
        else:
            return op

    def _propagate_if_expr_refs(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        action: str,
        fixer: Optional[
            Callable[[s_schema.Schema, ObjectCommand[so.Object], str,
                      CommandContext, s_expr.Expression],
                     s_expr.Expression]
        ]=None,
        metadata_only: bool=True,
    ) -> s_schema.Schema:
        scls = self.scls
        expr_refs = s_expr.get_expr_referrers(schema, scls)

        if expr_refs:
            ref_desc = []
            for ref, fns in expr_refs.items():
                from . import functions as s_func
                from . import indexes as s_indexes
                from . import pointers as s_pointers
                from . import constraints as s_cnstr
                from . import expraliases as s_alias
                from . import types as s_types

                cmd_drop: Command
                cmd_create: Command

                this_ref_desc = []
                for fn in fns:
                    if fn == 'expr':
                        fdesc = 'expression'
                    else:
                        fdesc = f"{fn.replace('_', ' ')} expression"

                    vn = ref.get_verbosename(schema, with_parent=True)

                    this_ref_desc.append(f'{fdesc} of {vn}')

                if isinstance(
                    ref,
                    (
                        s_indexes.Index,
                        s_pointers.Pointer,
                        s_func.Function,
                        s_cnstr.Constraint,
                        s_alias.Alias,
                        s_types.Type,
                    ),
                ):
                    # Alter the affected entity to change the body to
                    # a dummy version (removing the dependency) and
                    # then reset the body to original expression.
                    delta_drop, cmd_drop, _ = ref.init_delta_branch(
                        schema, context, cmdtype=AlterObject)
                    delta_create, cmd_create, _ = ref.init_delta_branch(
                        schema, context, cmdtype=AlterObject)
                    cmd_create.scls = ref
                    # Mark it metadata_only so that if it actually gets
                    # applied, only the metadata is changed but not
                    # the real underlying schema.
                    if metadata_only:
                        cmd_drop.metadata_only = True
                        cmd_create.metadata_only = True

                    # Compute a dummy value
                    dummy = None
                    if isinstance(ref, s_indexes.Index):
                        dummy = s_expr.Expression(text='0')
                    elif isinstance(ref, s_cnstr.Constraint):
                        dummy = s_expr.Expression(text='SELECT false')
                    elif isinstance(ref, s_func.Function):
                        dummy = ref.get_dummy_body(schema)
                    elif isinstance(ref, (s_alias.Alias, s_types.Type)):
                        dummy = s_expr.Expression(text='std::Object')
                    elif isinstance(
                        ref, (s_pointers.Pointer, s_alias.Alias, s_types.Type)
                    ):
                        dummy = None

                    # We need to extract the command on whatever the
                    # enclosing object of our referrer is, since we
                    # need to put that in the context so that
                    # compile_expr_field calls in the fixer can find
                    # the subject.
                    obj_cmd = next(iter(delta_create.ops))
                    assert isinstance(obj_cmd, ObjectCommand)
                    obj = obj_cmd.get_object(schema, context)

                    for fn in fns:
                        # Do the switcheroos
                        value = ref.get_explicit_field_value(schema, fn, None)
                        if value is None:
                            continue
                        assert isinstance(value, s_expr.Expression)
                        # Strip the "compiled" out of the expression
                        value = s_expr.Expression.not_compiled(value)
                        if fixer:
                            with obj_cmd.new_context(schema, context, obj):
                                value = fixer(
                                    schema, cmd_create, fn, context, value)

                        cmd_drop.set_attribute_value(fn, dummy)
                        cmd_create.set_attribute_value(fn, value)

                    context.affected_finalization[self].append(
                        (delta_create, cmd_create, this_ref_desc)
                    )
                    schema = delta_drop.apply(schema, context)
                    continue

                ref_desc.extend(this_ref_desc)

            if ref_desc:
                expr_s = (
                    'an expression' if len(ref_desc) == 1 else 'expressions')
                ref_desc_s = "\n - " + "\n - ".join(ref_desc)

                raise errors.SchemaDefinitionError(
                    f'cannot {action} because it is used in {expr_s}',
                    details=(
                        f'{scls.get_verbosename(schema)} is used in:'
                        f'{ref_desc_s}'
                    )
                )

        return schema

    def _finalize_affected_refs_specialize(
        self,
        cmd: Command,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> None:
        # Handle some special cases in affected ref handling below
        from . import lproperties as s_props
        from . import links as s_links
        from . import pointers as s_pointers
        from . import types as s_types
        from . import constraints as s_cnstr
        from . import functions as s_func
        from . import indexes as s_indexes
        from edb.ir import ast as irast

        ast: qlast.ObjectDDL
        # if the delta involves re-setting a computable
        # expression, then we also need to change the type to the
        # new expression type
        if isinstance(cmd, (s_props.AlterProperty, s_links.AlterLink)):
            for cm in cmd.get_subcommands(type=AlterObjectProperty):
                if cm.property == 'expr':
                    assert isinstance(cm.new_value, s_expr.Expression)
                    pointer = cast(
                        s_pointers.Pointer, schema.get(cmd.classname))
                    source = cast(s_types.Type, pointer.get_source(schema))
                    expression = s_expr.Expression.compiled(
                        cm.new_value,
                        schema=schema,
                        options=qlcompiler.CompilerOptions(
                            modaliases=context.modaliases,
                            anchors={qlast.Source().name: source},
                            path_prefix_anchor=qlast.Source().name,
                            singletons=frozenset([source]),
                            apply_query_rewrites=not context.stdmode,
                        ),
                    )

                    assert isinstance(expression.irast, irast.Statement)
                    target = expression.irast.stype
                    cmd.set_attribute_value('target', target)
                    break
        # If it involves changing the subjectexpr of a constraint,
        # we unfortunately need to compute a new name and rename
        # the constraint.
        elif (
            isinstance(cmd, s_cnstr.AlterConstraint)
            and not cmd.get_attribute_value('abstract')
            and (subjectexpr :=
                 cmd.get_attribute_value('subjectexpr')) is not None
        ):
            # To compute the new name, we construct an AST of the
            # constraint, since that is the infrastructure we have for
            # computing the classname.
            name = sn.shortname_from_fullname(cmd.classname)
            assert isinstance(name, sn.QualName), \
                "expected qualified name"
            ast = qlast.CreateConcreteConstraint(
                name=qlast.ObjectRef(name=name.name, module=name.module),
                subjectexpr=subjectexpr.qlast,
            )
            quals = sn.quals_from_fullname(cmd.classname)
            new_name = cmd._classname_from_ast_and_referrer(
                schema, sn.QualName.from_string(quals[0]), ast, context)
            if new_name == cmd.classname:
                return

            rename = cmd.scls.init_delta_command(
                schema, RenameObject, new_name=new_name)
            rename.set_attribute_value(
                'name', value=new_name, orig_value=cmd.classname)
            cmd.add(rename)

        # Also indexes.
        elif (
            isinstance(cmd, s_indexes.AlterIndex)
            and not cmd.get_attribute_value('abstract')
            and (indexexpr :=
                 cmd.get_attribute_value('expr')) is not None
        ):
            # To compute the new name, we construct an AST of the
            # index, since that is the infrastructure we have for
            # computing the classname.
            name = sn.shortname_from_fullname(cmd.classname)
            ast = qlast.CreateIndex(
                name=qlast.ObjectRef(name="idx", module="__"),
                expr=indexexpr.qlast,
            )
            quals = sn.quals_from_fullname(cmd.classname)
            new_name = cmd._classname_from_ast_and_referrer(
                schema, sn.QualName.from_string(quals[0]), ast, context)
            if new_name == cmd.classname:
                return

            rename = cmd.scls.init_delta_command(
                schema, RenameObject, new_name=new_name)
            rename.set_attribute_value(
                'name', value=new_name, orig_value=cmd.classname)
            cmd.add(rename)

        # For functions, we need to update the internal function name and
        # the internal param names if a type name has changed.
        elif isinstance(cmd, s_func.AlterFunction):
            # Produce a param desc list which we use to find a new name.
            param_list = cmd.scls.get_params(schema)
            params = s_func.CallableCommand._get_param_desc_from_params_ast(
                schema, context.modaliases, param_list.get_ast(schema))
            name = sn.shortname_from_fullname(cmd.classname)
            assert isinstance(name, sn.QualName), \
                "expected qualified name"
            new_fname = s_func.CallableObject.get_fqname(schema, name, params)
            if new_fname == cmd.classname:
                return

            # Do the rename
            rename = cmd.scls.init_delta_command(
                schema, RenameObject, new_name=new_fname)
            rename.set_attribute_value(
                'name', value=new_fname, orig_value=cmd.classname)
            cmd.add(rename)

    def _finalize_affected_refs(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        for delta, cmd, refdesc in context.affected_finalization.get(self, []):
            try:
                self._finalize_affected_refs_specialize(cmd, schema, context)

                schema = delta.apply(schema, context)

                if not context.canonical and delta:
                    # We need to force the attributes to be resolved so
                    # that expressions get compiled *now* under a schema
                    # where they are correct, and not later, when more
                    # renames may have broken them.
                    assert isinstance(cmd, ObjectCommand)
                    for key, value in cmd.get_resolved_attributes(
                            schema, context).items():
                        cmd.set_attribute_value(key, value)
                    self.add(delta)
            except errors.QueryError as e:
                desc = self.get_friendly_description(schema=schema)
                raise errors.SchemaDefinitionError(
                    f'cannot {desc} because this affects'
                    f' {" and ".join(refdesc)}',
                    details=e.args[0],
                ) from e

        return schema

    def _get_computed_status_of_fields(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Dict[str, bool]:
        result = {}
        mcls = self.get_schema_metaclass()
        for op in self._enumerate_attribute_cmds():
            field = mcls.get_field(op.property)
            if not field.ephemeral:
                result[op.property] = op.new_computed

        return result

    def _update_computed_fields(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        update: Mapping[str, bool],
    ) -> None:
        cur_comp_fields = self.scls.get_computed_fields(schema)
        comp_fields = set(cur_comp_fields)
        for fn, computed in update.items():
            if computed:
                comp_fields.add(fn)
            else:
                comp_fields.discard(fn)

        if cur_comp_fields != comp_fields:
            if comp_fields:
                self.set_attribute_value(
                    'computed_fields',
                    frozenset(comp_fields),
                    orig_value=cur_comp_fields if cur_comp_fields else None,
                )
            else:
                self.set_attribute_value(
                    'computed_fields',
                    None,
                    orig_value=cur_comp_fields if cur_comp_fields else None,
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
        # TODO: how to handle the following type: ignore?
        # in this class, astnode is always a Type[DDLOperation],
        # but the current design of constraints handles it as
        # a List[Type[DDLOperation]]
        return type(self).astnode  # type: ignore

    def _deparse_name(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        name: sn.Name,
    ) -> qlast.ObjectRef:
        qlclass = self.get_schema_metaclass().get_ql_class()

        if isinstance(name, sn.QualName):
            nname = sn.shortname_from_fullname(name)
            assert isinstance(nname, sn.QualName), \
                "expected qualified name"
            ref = qlast.ObjectRef(
                module=nname.module, name=nname.name, itemclass=qlclass)
        else:
            ref = qlast.ObjectRef(module='', name=str(name), itemclass=qlclass)

        return ref

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        astnode = self._get_ast_node(schema, context)

        if astnode.get_field('name'):
            op = astnode(
                name=self._deparse_name(schema, context, self.classname),
            )
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
        mcls = self.get_schema_metaclass()

        if not isinstance(self, DeleteObject):
            fops = self.get_subcommands(type=AlterObjectProperty)
            for fop in sorted(fops, key=lambda f: f.property):
                field = mcls.get_field(fop.property)
                if fop.new_value is not None:
                    new_value = fop.new_value
                else:
                    new_value = field.get_default()

                if (
                    (
                        # Only include fields that are not inherited
                        # and that have their value actually changed.
                        not fop.new_inherited
                        or context.descriptive_mode
                    )
                    and (
                        fop.old_value != new_value
                        or fop.old_inherited != fop.new_inherited
                        or fop.old_computed != fop.new_computed
                    )
                ):
                    self._apply_field_ast(schema, context, node, fop)

        if not isinstance(self, AlterObjectFragment):
            for field in self.get_ddl_identity_fields(context):
                ast_attr = self.get_ast_attr_for_field(field.name, type(node))
                if (
                    ast_attr is not None
                    and not getattr(node, ast_attr, None)
                    and (
                        field.required
                        or self.has_ddl_identity(field.name)
                    )
                ):
                    ddl_id = self.get_ddl_identity(field.name)
                    if issubclass(field.type, s_expr.Expression):
                        attr_val = ddl_id.qlast
                    elif issubclass(field.type, s_expr.ExpressionList):
                        attr_val = [e.qlast for e in ddl_id]
                    else:
                        raise AssertionError(
                            f'unexpected type of ddl_identity'
                            f' field: {field.type!r}'
                        )

                    setattr(node, ast_attr, attr_val)

            # Keep subcommands from refdicts and alter fragments (like
            # rename, rebase) in order when producing DDL asts
            refdicts = tuple(x.ref_cls for x in mcls.get_refdicts())
            for op in self.get_subcommands():
                if (
                    isinstance(op, AlterObjectFragment)
                    or (isinstance(op, ObjectCommand) and
                        issubclass(op.get_schema_metaclass(), refdicts))
                ):
                    self._append_subcmd_ast(schema, node, op, context)

        else:
            for op in self.get_subcommands(type=AlterObjectFragment):
                self._append_subcmd_ast(schema, node, op, context)

        if isinstance(node, qlast.DropObject):
            # Deletes in the AST shouldn't have subcommands, so we
            # drop them.  To try to make sure we aren't papering
            # over bugs by dropping things we dont expect, make
            # sure every subcommand was also a delete.
            assert all(
                isinstance(sub, qlast.DropObject) for sub in node.commands)
            node.commands = []

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

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        return None

    def get_ddl_identity_fields(
        self,
        context: CommandContext,
    ) -> Tuple[so.Field[Any], ...]:
        mcls = self.get_schema_metaclass()
        return tuple(f for f in mcls.get_fields().values() if f.ddl_identity)

    @classmethod
    def maybe_get_schema_metaclass(cls) -> Optional[Type[so.Object_T]]:
        return cls._schema_metaclass

    @classmethod
    def get_schema_metaclass(cls) -> Type[so.Object_T]:
        if cls._schema_metaclass is None:
            raise TypeError(f'schema metaclass not set for {cls}')
        return cls._schema_metaclass

    @classmethod
    def get_other_command_class(
        cls,
        cmdtype: Type[ObjectCommand_T],
    ) -> Type[ObjectCommand_T]:
        mcls = cls.get_schema_metaclass()
        return get_object_command_class_or_die(cmdtype, mcls)

    def _validate_legal_command(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> None:
        from . import functions as s_func

        if (not context.stdmode and not context.testmode and
                not isinstance(self, s_func.ParameterCommand)):

            if (
                isinstance(self.classname, sn.QualName)
                and (
                    (modname := self.classname.get_module_name())
                    in s_schema.STD_MODULES
                )
            ):
                raise errors.SchemaDefinitionError(
                    f'cannot {self._delta_action} {self.get_verbosename()}: '
                    f'module {modname} is read-only',
                    context=self.source_context)

    def get_verbosename(self, parent: Optional[str] = None) -> str:
        mcls = self.get_schema_metaclass()
        return mcls.get_verbosename_static(self.classname, parent=parent)

    def get_displayname(self) -> str:
        mcls = self.get_schema_metaclass()
        return mcls.get_displayname_static(self.classname)

    def get_friendly_object_name_for_description(
        self,
        *,
        parent_op: Optional[Command] = None,
        schema: Optional[s_schema.Schema] = None,
        object: Optional[so.Object_T] = None,
        object_desc: Optional[str] = None,
    ) -> str:
        if object_desc is not None:
            return object_desc
        else:
            if object is None:
                object = getattr(self, 'scls', _dummy_object)

            if object is _dummy_object or schema is None:
                if not isinstance(parent_op, ObjectCommand):
                    parent_desc = None
                else:
                    parent_desc = parent_op.get_verbosename()
                object_desc = self.get_verbosename(parent=parent_desc)
            else:
                object_desc = object.get_verbosename(schema, with_parent=True)

            return object_desc

    @overload
    def get_object(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[sn.Name] = None,
        default: Union[so.Object_T, so.NoDefaultT] = so.NoDefault,
    ) -> so.Object_T:
        ...

    @overload
    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[sn.Name] = None,
        default: None = None,
    ) -> Optional[so.Object_T]:
        ...

    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[sn.Name] = None,
        default: Union[so.Object_T, so.NoDefaultT, None] = so.NoDefault,
    ) -> Optional[so.Object_T]:
        metaclass = self.get_schema_metaclass()
        if name is None:
            name = self.classname
            rename = context.renames.get(name)
            if rename is not None:
                name = rename
        return schema.get_global(metaclass, name, default=default)

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        """Resolve, canonicalize and amend field mutations in this command.

        This is called just before the object described by this command
        is created or updated but after all prerequisite command have
        been applied, so it is safe to resolve object shells and do
        other schema inquiries here.
        """
        return schema

    def populate_ddl_identity(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        return schema

    def get_resolved_attribute_value(
        self,
        attr_name: str,
        *,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Any:
        raw_value = self.get_attribute_value(attr_name)
        if raw_value is None:
            return None

        value = context.get_cached((self, 'attribute', attr_name))
        if value is None:
            value = self.resolve_attribute_value(
                attr_name,
                raw_value,
                schema=schema,
                context=context,
            )
            context.cache_value((self, 'attribute', attr_name), value)

        return value

    def resolve_attribute_value(
        self,
        attr_name: str,
        raw_value: Any,
        *,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Any:
        metaclass = self.get_schema_metaclass()
        field = metaclass.get_field(attr_name)
        if field is None:
            raise errors.SchemaDefinitionError(
                f'got AlterObjectProperty command for '
                f'invalid field: {metaclass.__name__}.{attr_name}')

        value = self._resolve_attr_value(
            raw_value, attr_name, field, schema)

        if (isinstance(value, s_expr.Expression)
                and not value.is_compiled()):
            value = self.compile_expr_field(schema, context, field, value)

        return value

    def get_attributes(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Dict[str, Any]:
        result = {}

        for attr in self.enumerate_attributes():
            result[attr] = self.get_attribute_value(attr)

        return result

    def get_resolved_attributes(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Dict[str, Any]:
        result = {}

        for attr in self.enumerate_attributes():
            result[attr] = self.get_resolved_attribute_value(
                attr, schema=schema, context=context)

        return result

    def get_orig_attributes(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Dict[str, Any]:
        result = {}

        for attr in self.enumerate_attributes():
            result[attr] = self.get_orig_attribute_value(attr)

        return result

    def get_specified_attribute_value(
        self,
        field: str,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Optional[Any]:
        """Fetch the specified (not computed) value of a field.

        If the command is an alter, it will fall back to the value in
        the schema.

        Return None if there is no specified value or if the specified
        value is being reset.
        """
        spec = self.get_attribute_value(field)

        is_alter = (
            isinstance(self, AlterObject)
            or (
                isinstance(self, AlterObjectFragment)
                and isinstance(self.get_parent_op(context), AlterObject)
            )
        )
        if (
            is_alter
            and spec is None
            and not self.has_attribute_value(field)
            and field not in self.scls.get_computed_fields(schema)
        ):
            spec = self.scls.get_explicit_field_value(
                schema, field, default=None)

        return spec

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        field: so.Field[Any],
        value: Any,
        track_schema_ref_exprs: bool=False,
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
        return context(
            ctxcls(schema=schema, op=self, scls=scls),  # type: ignore
        )

    def get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        dummy = cast(so.Object_T, _dummy_object)

        context_class = type(self).get_context_class()
        if context_class is not None:
            with self.new_context(schema, context, dummy):
                return self._get_ast(schema, context, parent_node=parent_node)
        else:
            return self._get_ast(schema, context, parent_node=parent_node)

    def get_ddl_identity(self, aspect: str) -> Any:
        if self.ddl_identity is None:
            raise LookupError(f'{self!r} has no DDL identity information')
        value = self.ddl_identity.get(aspect)
        if value is None:
            raise LookupError(f'{self!r} has no {aspect!r} in DDL identity')
        return value

    def has_ddl_identity(self, aspect: str) -> bool:
        return (
            self.ddl_identity is not None
            and self.ddl_identity.get(aspect) is not None
        )

    def set_ddl_identity(self, aspect: str, value: Any) -> None:
        if self.ddl_identity is None:
            self.ddl_identity = {}

        self.ddl_identity[aspect] = value

    def maybe_get_object_aux_data(self, field: str) -> Any:
        if self.aux_object_data is None:
            return None
        else:
            value = self.aux_object_data.get(field)
            if value is None:
                return None
            else:
                return value

    def get_object_aux_data(self, field: str) -> Any:
        if self.aux_object_data is None:
            raise LookupError(f'{self!r} has no auxiliary object information')
        value = self.aux_object_data.get(field)
        if value is None:
            raise LookupError(
                f'{self!r} has no {field!r} in auxiliary object information')
        return value

    def has_object_aux_data(self, field: str) -> bool:
        return (
            self.aux_object_data is not None
            and self.aux_object_data.get(field) is not None
        )

    def set_object_aux_data(self, field: str, value: Any) -> None:
        if self.aux_object_data is None:
            self.aux_object_data = {}

        self.aux_object_data[field] = value

    def get_annotation(self, name: str) -> Any:
        if self.annotations is None:
            return None
        else:
            return self.annotations.get(name)

    def set_annotation(self, name: str, value: Any) -> None:
        if self.annotations is None:
            self.annotations = {}
        self.annotations[name] = value


class ObjectCommandContext(CommandContextToken[ObjectCommand[so.Object_T]]):

    def __init__(
        self,
        schema: s_schema.Schema,
        op: ObjectCommand[so.Object_T],
        scls: so.Object_T,
        *,
        modaliases: Optional[Mapping[Optional[str], str]] = None,
        localnames: AbstractSet[str] = frozenset(),
    ) -> None:
        super().__init__(
            schema, op, modaliases=modaliases, localnames=localnames)
        self.scls = scls


class QualifiedObjectCommand(ObjectCommand[so.QualifiedObject_T]):

    classname = struct.Field(sn.QualName)

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.NamedDDL,
        context: CommandContext,
    ) -> sn.QualName:
        objref = astnode.name
        module = context.modaliases.get(objref.module, objref.module)
        if module is None:
            raise errors.SchemaDefinitionError(
                f'unqualified name and no default module set',
                context=objref.context,
            )

        return sn.QualName(module=module, name=objref.name)

    @overload
    def get_object(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[sn.Name] = None,
        default: Union[so.QualifiedObject_T, so.NoDefaultT] = so.NoDefault,
    ) -> so.QualifiedObject_T:
        ...

    @overload
    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[sn.Name] = None,
        default: None = None,
    ) -> Optional[so.QualifiedObject_T]:
        ...

    def get_object(  # NoQA: F811
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        name: Optional[sn.Name] = None,
        default: Union[
            so.QualifiedObject_T, so.NoDefaultT, None] = so.NoDefault,
    ) -> Optional[so.QualifiedObject_T]:
        if name is None:
            name = self.classname
            rename = context.renames.get(name)
            if rename is not None:
                name = rename
        metaclass = self.get_schema_metaclass()
        return cast(
            Optional[so.QualifiedObject_T],
            schema.get(name, type=metaclass, default=default,
                       sourcectx=self.source_context),
        )


class GlobalObjectCommand(ObjectCommand[so.GlobalObject_T]):
    pass


class CreateObject(ObjectCommand[so.Object_T], Generic[so.Object_T]):
    _delta_action = 'create'

    # If the command is conditioned with IF NOT EXISTS
    if_not_exists = struct.Field(bool, default=False)

    def is_data_safe(self) -> bool:
        # Creations are always data-safe.
        return True

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
            dummy_op = cls(
                classname=sn.QualName('placeholder', 'placeholder'))
            ctxcls = cast(
                Type[ObjectCommandContext[so.Object_T]],
                cls.get_context_class_or_die(),
            )
            ctx = ctxcls(
                schema,
                op=dummy_op,
                scls=cast(so.Object_T, _dummy_object),
                modaliases=modaliases,
            )
            with context(ctx):
                classname = cls._classname_from_ast(schema, astnode, context)
            mcls = cls.get_schema_metaclass()
            if schema.get(classname, default=None) is not None:
                return get_object_command_class_or_die(
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

        cmd.set_attribute_value('name', cmd.classname)

        if getattr(astnode, 'abstract', False):
            cmd.set_attribute_value('abstract', True)

        return cmd

    def get_verb(self) -> str:
        return 'create'

    def validate_create(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> None:
        pass

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        self._validate_legal_command(schema, context)

        for op in self.get_prerequisites():
            schema = op.apply(schema, context)

        if context.schema_object_ids is not None:
            mcls = self.get_schema_metaclass()
            qlclass: Optional[qltypes.SchemaObjectClass]
            if issubclass(mcls, so.QualifiedObject):
                qlclass = None
            else:
                qlclass = mcls.get_ql_class_or_die()

            objname = self.classname
            if context.compat_ver_is_before(
                (1, 0, verutils.VersionStage.ALPHA, 5)
            ):
                # Pre alpha.5 used to have a different name mangling scheme.
                objname = sn.compat_name_remangle(str(objname))

            key = (objname, qlclass)
            specified_id = context.schema_object_ids.get(key)
            if specified_id is not None:
                self.set_attribute_value('id', specified_id)

        if not context.canonical:
            schema = self.populate_ddl_identity(schema, context)
            schema = self.canonicalize_attributes(schema, context)
            self.validate_create(schema, context)
            computed_status = self._get_computed_status_of_fields(
                schema, context)
            computed_fields = {n for n, v in computed_status.items() if v}
            if computed_fields:
                self.set_attribute_value(
                    'computed_fields', frozenset(computed_fields))

        props = self.get_resolved_attributes(schema, context)
        metaclass = self.get_schema_metaclass()

        # Check if functions by this name exist
        fn = props.get('name')
        if fn is not None and not sn.is_fullname(str(fn)):
            funcs = schema.get_functions(fn, tuple())
            if funcs:
                raise errors.SchemaError(
                    f'{funcs[0].get_verbosename(schema)} is already present '
                    f'in the schema {schema!r}')

        schema, self.scls = metaclass.create_in_schema(schema, **props)

        if not props.get('id'):
            # Record the generated ID.
            self.set_attribute_value('id', self.scls.id)

        return schema

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        self.set_attribute_value('builtin', context.stdmode)
        if not self.has_attribute_value('builtin'):
            self.set_attribute_value('builtin', context.stdmode)
        if not self.has_attribute_value('internal'):
            self.set_attribute_value('internal', context.internal_schema_mode)
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

    def _create_innards(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        for op in self.get_subcommands(include_prerequisites=False):
            if not isinstance(op, AlterObjectProperty):
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
        with self.new_context(schema, context, _dummy_object):  # type: ignore
            if self.if_not_exists:
                scls = self.get_object(schema, context, default=None)

                if scls is not None:
                    parent_ctx = context.parent()
                    if parent_ctx is not None and not self.canonical:
                        parent_ctx.op.discard(self)

                    self.scls = scls
                    return schema

            schema = self._create_begin(schema, context)
            ctx = context.current()
            objctx = cast(ObjectCommandContext[so.Object_T], ctx)
            objctx.scls = self.scls
            schema = self._create_innards(schema, context)
            schema = self._create_finalize(schema, context)
        return schema


class AlterObjectOrFragment(ObjectCommand[so.Object_T]):

    def canonicalize_attributes(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        schema = super().canonicalize_attributes(schema, context)
        # Hydrate the ALTER fields with original field values,
        # if not present.
        for cmd in self.get_subcommands(type=AlterObjectProperty):
            if cmd.old_value is None:
                cmd.old_value = self.scls.get_explicit_field_value(
                    schema, cmd.property, default=None)
        return schema

    def validate_alter(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> None:
        self._validate_legal_command(schema, context)

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        for op in self.get_prerequisites():
            schema = op.apply(schema, context)

        if not context.canonical:
            schema = self.populate_ddl_identity(schema, context)
            schema = self.canonicalize_attributes(schema, context)
            computed_status = self._get_computed_status_of_fields(
                schema, context)
            self._update_computed_fields(schema, context, computed_status)
            self.validate_alter(schema, context)

        props = self.get_resolved_attributes(schema, context)
        return self.scls.update(schema, props)

    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        for op in self.get_subcommands(include_prerequisites=False):
            if not isinstance(op, AlterObjectProperty):
                schema = op.apply(schema, context=context)
        return schema

    def _alter_finalize(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        return self._finalize_affected_refs(schema, context)


class AlterObjectFragment(AlterObjectOrFragment[so.Object_T]):

    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        # AlterObjectFragment must be executed in the context
        # of a parent AlterObject command.
        scls = self.get_parent_op(context).scls
        self.scls = cast(so.Object_T, scls)

        schema = self._alter_begin(schema, context)
        schema = self._alter_innards(schema, context)
        schema = self._alter_finalize(schema, context)

        return schema

    def get_parent_op(
        self,
        context: CommandContext,
    ) -> ObjectCommand[so.Object]:
        op = context.current().op
        assert isinstance(op, ObjectCommand)
        return op


class RenameObject(AlterObjectFragment[so.Object_T]):
    _delta_action = 'rename'

    astnode = qlast.Rename

    new_name = struct.Field(sn.Name)

    def is_data_safe(self) -> bool:
        # Renames are always data-safe.
        return True

    def get_verb(self) -> str:
        return 'rename'

    def get_friendly_description(
        self,
        *,
        parent_op: Optional[Command] = None,
        schema: Optional[s_schema.Schema] = None,
        object: Any = None,
        object_desc: Optional[str] = None,
    ) -> str:
        object_desc = self.get_friendly_object_name_for_description(
            parent_op=parent_op,
            schema=schema,
            object=object,
            object_desc=object_desc,
        )
        mcls = self.get_schema_metaclass()
        new_name = mcls.get_displayname_static(self.new_name)
        return f"rename {object_desc} to '{new_name}'"

    def _fix_referencing_expr(
        self,
        schema: s_schema.Schema,
        cmd: ObjectCommand[so.Object],
        fn: str,
        context: CommandContext,
        expr: s_expr.Expression,
    ) -> s_expr.Expression:
        from edb.ir import ast as irast

        # Recompile the expression with reference tracking on so that we
        # can clean up the ast.
        field = cmd.get_schema_metaclass().get_field(fn)
        compiled = cmd.compile_expr_field(
            schema, context, field, expr,
            track_schema_ref_exprs=True)
        assert isinstance(compiled.irast, irast.Statement)
        assert compiled.irast.schema_ref_exprs is not None

        # Now that the compilation is done, try to do the fixup.
        new_shortname = sn.shortname_from_fullname(self.new_name)
        old_shortname = sn.shortname_from_fullname(self.classname).name
        for ref in compiled.irast.schema_ref_exprs.get(self.scls, []):
            if isinstance(ref, qlast.Ptr):
                ref = ref.ptr

            assert isinstance(ref, (qlast.ObjectRef, qlast.FunctionCall)), (
                f"only support object refs and func calls but got {ref}")
            if isinstance(ref, qlast.FunctionCall):
                ref.func = ((new_shortname.module, new_shortname.name)
                            if isinstance(new_shortname, sn.QualName)
                            else new_shortname.name)
            elif (
                isinstance(ref, qlast.ObjectRef)
                and ref.name == old_shortname
            ):
                ref.name = new_shortname.name
                if (
                    isinstance(new_shortname, sn.QualName)
                    and new_shortname.module != "__"
                ):
                    ref.module = new_shortname.module

        # say as_fragment=True as a hack to avoid renormalizing it
        out = s_expr.Expression.from_ast(
            compiled.qlast, schema, modaliases={}, as_fragment=True)
        return out

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        scls = self.scls
        context.renames[self.classname] = self.new_name
        context.renamed_objs.add(scls)

        vn = scls.get_verbosename(schema)
        schema = self._propagate_if_expr_refs(
            schema,
            context,
            action=f'rename {vn}',
            fixer=self._fix_referencing_expr,
        )

        if not context.canonical:
            self.set_attribute_value(
                'name',
                value=self.new_name,
                orig_value=self.classname,
            )

        return super()._alter_begin(schema, context)

    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        if not context.canonical:
            self._canonicalize(schema, context, self.scls)
        return super()._alter_innards(schema, context)

    def init_rename_branch(
        self,
        ref: so.Object,
        new_ref_name: sn.Name,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> Command:

        ref_root, ref_alter, _ = ref.init_delta_branch(
            schema, context, AlterObject)

        ref_alter.add(
            ref.init_delta_command(
                schema,
                RenameObject,
                new_name=new_ref_name,
            ),
        )

        return ref_root

    def _canonicalize(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        scls: so.Object,
    ) -> None:
        mcls = self.get_schema_metaclass()

        for refdict in mcls.get_refdicts():
            all_refs = set(
                scls.get_field_value(schema, refdict.attr).objects(schema)
            )

            ref: so.Object
            for ref in all_refs:
                ref_name = ref.get_name(schema)
                quals = list(sn.quals_from_fullname(ref_name))
                assert isinstance(self.new_name, sn.QualName)
                quals[0] = str(self.new_name)
                shortname = sn.shortname_from_fullname(ref_name)
                new_ref_name = sn.QualName(
                    name=sn.get_specialized_name(shortname, *quals),
                    module=self.new_name.module,
                )
                self.add(self.init_rename_branch(
                    ref,
                    new_ref_name,
                    schema=schema,
                    context=context,
                ))

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        astnode = self._get_ast_node(schema, context)
        ref = self._deparse_name(schema, context, self.new_name)
        ref.itemclass = None
        orig_ref = self._deparse_name(schema, context, self.classname)
        if (orig_ref.module, orig_ref.name) != (ref.module, ref.name):
            return astnode(new_name=ref)
        else:
            return None

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> RenameObject[so.Object_T]:
        parent_ctx = context.current()
        parent_op = parent_ctx.op
        assert isinstance(parent_op, ObjectCommand)
        parent_class = parent_op.get_schema_metaclass()
        rename_class = get_object_command_class_or_die(
            RenameObject, parent_class)
        return rename_class._rename_cmd_from_ast(schema, astnode, context)

    @classmethod
    def _rename_cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> RenameObject[so.Object_T]:
        assert isinstance(astnode, qlast.Rename)

        parent_ctx = context.current()
        parent_op = parent_ctx.op
        assert isinstance(parent_op, ObjectCommand)
        parent_class = parent_op.get_schema_metaclass()
        rename_class = get_object_command_class_or_die(
            RenameObject, parent_class)

        new_name = cls._classname_from_ast(schema, astnode, context)

        # Populate the early_renames map of the context as we go, since
        # in-flight renames will affect the generated names of later
        # operations.
        context.early_renames[parent_op.classname] = new_name

        return rename_class(
            metaclass=parent_class,
            classname=parent_op.classname,
            new_name=new_name,
        )


class RenameQualifiedObject(AlterObjectFragment[so.Object_T]):

    new_name = struct.Field(sn.QualName)

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        astnode = self._get_ast_node(schema, context)
        new_name = self.new_name
        ref = qlast.ObjectRef(name=new_name.name, module=new_name.module)
        return astnode(new_name=ref)


class AlterObject(AlterObjectOrFragment[so.Object_T], Generic[so.Object_T]):
    _delta_action = 'alter'

    #: If True, apply the command only if the object exists.
    if_exists = struct.Field(bool, default=False)

    #: If True, only apply changes to properties, not "real" schema changes
    metadata_only = struct.Field(bool, default=False)

    def get_verb(self) -> str:
        return 'alter'

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, AlterObject)

        if getattr(astnode, 'abstract', False):
            cmd.set_attribute_value('abstract', True)

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

    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:

        if not context.canonical and self.if_exists:
            scls = self.get_object(schema, context, default=None)
            if scls is None:
                context.current().op.discard(self)
                return schema
        else:
            scls = self.get_object(schema, context)

        self.scls = scls

        with self.new_context(schema, context, scls):
            schema = self._alter_begin(schema, context)
            schema = self._alter_innards(schema, context)
            schema = self._alter_finalize(schema, context)

        return schema


class DeleteObject(ObjectCommand[so.Object_T], Generic[so.Object_T]):
    _delta_action = 'delete'

    #: If True, apply the command only if the object exists.
    if_exists = struct.Field(bool, default=False)

    #: If True, apply the command only if the object has no referrers
    #: in the schema.
    if_unused = struct.Field(bool, default=False)

    #: Potential references to this object that we know are being
    #: deleted, which we use to resolve if_unused.
    expiring_refs = struct.Field(AbstractSet[so.Object],
                                 default=frozenset())

    def get_verb(self) -> str:
        return 'drop'

    def is_data_safe(self) -> bool:
        # Deletions are only safe if the entire object class
        # has been declared as data-safe.
        return self.get_schema_metaclass()._data_safe

    def _delete_begin(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        from . import ordering

        self._validate_legal_command(schema, context)

        if not context.canonical:
            schema = self.populate_ddl_identity(schema, context)
            schema = self.canonicalize_attributes(schema, context)

            if not context.get_value(('delcanon', self)):
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
                op = ref.init_delta_command(schema, DeleteObject)
                assert isinstance(op, DeleteObject)
                subcmds = op._canonicalize(schema, context, ref)
                op.update(subcmds)
                commands.append(op)

        # Record the fact that DeleteObject._canonicalize
        # was called on this object to guard against possible
        # duplicate calls.
        context.store_value(('delcanon', self), True)

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

        if not context.canonical and not context.disable_dep_verification:
            refs = schema.get_referrers(self.scls)
            ctx = context.current()
            assert ctx is not None
            orig_schema = ctx.original_schema
            if refs:
                for ref in refs:
                    if (not context.is_deleting(ref)
                            and ref not in self.expiring_refs
                            and ref.is_blocking_ref(orig_schema, self.scls)):
                        ref_strs.append(
                            ref.get_verbosename(orig_schema, with_parent=True))

            if ref_strs:
                vn = self.scls.get_verbosename(orig_schema, with_parent=True)
                dn = self.scls.get_displayname(orig_schema)
                detail = '; '.join(f'{ref_str} depends on {dn}'
                                   for ref_str in ref_strs)
                raise errors.SchemaError(
                    f'cannot drop {vn} because '
                    f'other objects in the schema depend on it',
                    details=detail,
                )

        schema = schema.delete(self.scls)
        return schema

    def _has_outside_references(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> bool:
        # Check if the subject of this command has any outside references
        # minus any current expiring refs and minus structural child refs
        # (e.g. source backref in pointers of an object type).
        refs = [
            ref
            for ref in schema.get_referrers(self.scls) - self.expiring_refs
            if not ref.is_parent_ref(schema, self.scls)
        ]

        return bool(refs)

    def apply(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
    ) -> s_schema.Schema:
        if self.if_exists:
            scls = self.get_object(schema, context, default=None)
            if scls is None:
                context.current().op.discard(self)
                return schema
        else:
            scls = self.get_object(schema, context)

        self.scls = scls

        with self.new_context(schema, context, scls):
            if (
                not self.canonical
                and self.if_unused
                and self._has_outside_references(schema, context)
            ):
                parent_ctx = context.parent()
                if parent_ctx is not None:
                    parent_ctx.op.discard(self)

                return schema

            schema = self._delete_begin(schema, context)
            schema = self._delete_innards(schema, context)
            schema = self._delete_finalize(schema, context)

        return schema


special_field_alter_handlers: Dict[
    str,
    Dict[Type[so.Object], Type[AlterSpecialObjectField[so.Object]]],
] = {}


class AlterSpecialObjectField(AlterObjectFragment[so.Object_T]):
    """Base class for AlterObjectFragment implementations for special fields.

    When the generic `AlterObjectProperty` handling of field value transitions
    is insufficient, declare a subclass of this to implement custom handling.
    """

    _field: ClassVar[str]

    def __init_subclass__(
        cls,
        *,
        field: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(**kwargs)

        if field is None:
            if any(
                issubclass(b, AlterSpecialObjectField)
                for b in cls.__mro__[1:]
            ):
                return
            else:
                raise TypeError(
                    "AlterSpecialObjectField.__init_subclass__() missing "
                    "1 required keyword-only argument: 'field'"
                )

        handlers = special_field_alter_handlers.get(field)
        if handlers is None:
            handlers = special_field_alter_handlers[field] = {}

        schema_metaclass = cls.get_schema_metaclass()
        handlers[schema_metaclass] = cls  # type: ignore
        cls._field = field

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> ObjectCommand[so.Object_T]:
        this_op = context.current().op
        assert isinstance(this_op, ObjectCommand)
        return cls(classname=this_op.classname)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> Command:
        assert isinstance(astnode, qlast.SetField)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cmd.add(AlterObjectProperty.regular_cmd_from_ast(
            schema, astnode, context))
        return cmd

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        attrs = self._enumerate_attribute_cmds()
        assert len(attrs) == 1, "expected one attribute command"
        return attrs[0]._get_ast(schema, context, parent_node=parent_node)

    def get_verb(self) -> str:
        return f'alter the {self._field} of'


def get_special_field_alter_handler(
    field: str,
    schema_cls: Type[so.Object],
) -> Optional[Type[AlterSpecialObjectField[so.Object]]]:
    """Return a custom handler for the field value transition, if any.

    Returns a subclass of AlterSpecialObjectField, when in the context
    of an AlterObject operation, and a special handler has been declared.
    """
    field_handlers = special_field_alter_handlers.get(field)
    if field_handlers is None:
        return None
    return field_handlers.get(schema_cls)


def get_special_field_alter_handler_for_context(
    field: str,
    context: CommandContext,
) -> Optional[Type[AlterSpecialObjectField[so.Object]]]:
    """Return a custom handler for the field value transition, if any.

    Returns a subclass of AlterSpecialObjectField, when in the context
    of an AlterObject operation, and a special handler has been declared.
    """
    this_op = context.current().op
    if (
        isinstance(this_op, AlterObjectOrFragment)
        and not isinstance(this_op, AlterSpecialObjectField)
    ):
        mcls = this_op.get_schema_metaclass()
        return get_special_field_alter_handler(field, mcls)
    else:
        return None


class AlterObjectProperty(Command):
    astnode = qlast.SetField

    property = struct.Field(str)
    old_value = struct.Field[Any](object, None)
    new_value = struct.Field[Any](object, None)
    old_inherited = struct.Field(bool, False)
    new_inherited = struct.Field(bool, False)
    new_computed = struct.Field(bool, False)
    old_computed = struct.Field(bool, False)
    from_default = struct.Field(bool, False)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: CommandContext,
    ) -> Command:
        assert isinstance(astnode, qlast.SetField)
        handler = get_special_field_alter_handler_for_context(
            astnode.name, context)
        if handler is not None:
            return handler._cmd_tree_from_ast(schema, astnode, context)
        else:
            return cls.regular_cmd_from_ast(schema, astnode, context)

    @classmethod
    def regular_cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.SetField,
        context: CommandContext,
    ) -> Command:
        propname = astnode.name
        parent_ctx = context.current()
        parent_op = parent_ctx.op
        assert isinstance(parent_op, ObjectCommand)
        parent_cls = parent_op.get_schema_metaclass()

        if (
            propname.startswith('orig_')
            and context.compat_ver_is_before(
                (1, 0, verutils.VersionStage.ALPHA, 8)
            )
            and not parent_cls.has_field(propname)
        ):
            return Nop()
        else:
            try:
                field = parent_cls.get_field(propname)
            except LookupError:
                raise errors.SchemaDefinitionError(
                    f'{propname!r} is not a valid field',
                    context=astnode.context)

        if not (
            astnode.special_syntax
            or field.allow_ddl_set
            or context.stdmode
            or context.testmode
        ):
            raise errors.SchemaDefinitionError(
                f'{propname!r} is not a valid field',
                context=astnode.context)

        if field.name == 'id' and not isinstance(parent_op, CreateObject):
            raise errors.SchemaDefinitionError(
                f'cannot alter object id',
                context=astnode.context)

        new_value: Any

        if field.type is s_expr.Expression:
            if astnode.value is None:
                new_value = None
            else:
                orig_text = cls.get_orig_expr_text(
                    schema, parent_op.qlast, field.name)

                if (
                    orig_text is not None
                    and context.compat_ver_is_before(
                        (1, 0, verutils.VersionStage.ALPHA, 6)
                    )
                ):
                    # Versions prior to a6 used a different expression
                    # normalization strategy, so we must renormalize the
                    # expression.
                    expr_ql = qlcompiler.renormalize_compat(
                        astnode.value,
                        orig_text,
                        schema=schema,
                        localnames=context.localnames,
                    )
                else:
                    expr_ql = astnode.value

                new_value = s_expr.Expression.from_ast(
                    expr_ql,
                    schema,
                    context.modaliases,
                    context.localnames,
                )
        else:
            if isinstance(astnode.value, qlast.Tuple):
                new_value = tuple(
                    qlcompiler.evaluate_ast_to_python_val(
                        el, schema=schema)
                    for el in astnode.value.elements
                )

            elif isinstance(astnode.value, qlast.ObjectRef):

                new_value = utils.ast_to_object_shell(
                    astnode.value,
                    modaliases=context.modaliases,
                    schema=schema,
                )

            elif (
                isinstance(astnode.value, qlast.Set)
                and not astnode.value.elements
            ):
                # empty set
                new_value = None

            elif isinstance(astnode.value, qlast.TypeExpr):
                if not isinstance(parent_op, QualifiedObjectCommand):
                    raise AssertionError(
                        'cannot determine module for derived compound type: '
                        'parent operation is not a QualifiedObjectCommand'
                    )

                new_value = utils.ast_to_type_shell(
                    astnode.value,
                    module=parent_op.classname.module,
                    modaliases=context.modaliases,
                    schema=schema,
                )

            else:
                new_value = qlcompiler.evaluate_ast_to_python_val(
                    astnode.value, schema=schema) if astnode.value else None
                if new_value is not None:
                    new_value = field.coerce_value(schema, new_value)

        return cls(
            property=propname,
            new_value=new_value,
            source_context=astnode.context,
        )

    def is_data_safe(self) -> bool:
        # Field alterations on existing schema objects
        # generally represent semantic changes and are
        # reversible.  Non-safe field alters are normally
        # represented by a dedicated subcommand, such as
        # SetLinkType.
        return True

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        value = self.new_value

        new_value_empty = \
            (value is None or
                (isinstance(value, collections.abc.Container) and not value))
        old_value_empty = \
            (self.old_value is None or
                (isinstance(self.old_value, collections.abc.Container)
                 and not self.old_value))

        parent_ctx = context.current()
        parent_op = parent_ctx.op
        assert isinstance(parent_op, ObjectCommand)
        assert parent_node is not None
        parent_cls = parent_op.get_schema_metaclass()
        field = parent_cls.get_field(self.property)
        if field is None:
            raise errors.SchemaDefinitionError(
                f'{self.property!r} is not a valid field',
                context=self.source_context)

        if self.property == 'id':
            return None

        parent_node_attr = parent_op.get_ast_attr_for_field(
            field.name, type(parent_node))

        if (
            not field.allow_ddl_set
            and not (
                field.special_ddl_syntax
                and isinstance(parent_node, qlast.AlterObject)
            )
            and self.property != 'expr'
            and parent_node_attr is None
        ):
            # Don't produce any AST if:
            #
            # * a field does not have the "allow_ddl_set" option, unless
            #   it's an 'expr' field.
            #
            #   'expr' fields come from the "USING" clause and are specially
            #   treated in parser and codegen.
            return None

        if (
            (
                self.new_inherited
                and not self.old_inherited
                and not old_value_empty
            ) or (
                self.new_computed
                and not self.old_computed
                and not self.old_inherited
                and not old_value_empty
            )
        ):
            # The field became inherited or computed, in which case we should
            # generate a RESET.
            return qlast.SetField(
                name=self.property,
                value=None,
                special_syntax=field.special_ddl_syntax,
            )

        if self.new_inherited or self.new_computed:
            # We don't want to show inherited or computed properties unless
            # we are in "descriptive_mode" ...
            if not context.descriptive_mode:
                return None

            if not (
                field.describe_visibility
                & so.DescribeVisibilityFlags.SHOW_IF_DERIVED
            ):
                # ... or if the field shouldn't be shown when inherited
                # or computed.
                return None

            if (
                not (
                    field.describe_visibility
                    & so.DescribeVisibilityFlags.SHOW_IF_DEFAULT
                ) and field.default == value
            ):
                # ... or if the field should not be shown when the value
                # mathdes the default.
                return None

            parentop_sn = sn.shortname_from_fullname(parent_op.classname).name
            if self.property == 'default' and parentop_sn == 'id':
                # ... or if it's 'default' for the 'id' property
                # (special case).
                return None

        if self.from_default:
            if not context.descriptive_mode:
                return None

            if not (
                field.describe_visibility
                & so.DescribeVisibilityFlags.SHOW_IF_DEFAULT
            ):
                # ... or if the field should not be shown when the value
                # mathdes the default.
                return None

        if new_value_empty:
            if old_value_empty:
                return None
            else:
                value = None
        elif issubclass(field.type, s_expr.Expression):
            return self._get_expr_field_ast(
                schema,
                context,
                parent_op=parent_op,
                field=field,
                parent_node=parent_node,
                parent_node_attr=parent_node_attr,
            )
        elif parent_node_attr is not None:
            setattr(parent_node, parent_node_attr, value)
            return None
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
        elif isinstance(value, so.ObjectShell):
            value = utils.shell_to_ast(schema, value)
        else:
            value = utils.const_ast_from_python(value)

        return qlast.SetField(
            name=self.property,
            value=value,
            special_syntax=field.special_ddl_syntax,
        )

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

        assert isinstance(
            self.new_value,
            (s_expr.Expression, s_expr.ExpressionShell),
        )

        expr_ql = edgeql.parse_fragment(self.new_value.text)

        if parent_node is not None and parent_node_attr is not None:
            setattr(parent_node, parent_node_attr, expr_ql)
            return None
        else:
            return qlast.SetField(
                name=self.property,
                value=expr_ql,
                special_syntax=(self.property == 'expr'),
            )

    def __repr__(self) -> str:
        return '<%s.%s "%s":"%s"->"%s">' % (
            self.__class__.__module__, self.__class__.__name__,
            self.property, self.old_value, self.new_value)

    def get_friendly_description(
        self,
        *,
        parent_op: Optional[Command] = None,
        schema: Optional[s_schema.Schema] = None,
        object: Any = None,
        object_desc: Optional[str] = None,
    ) -> str:
        if parent_op is not None:
            assert isinstance(parent_op, ObjectCommand)
            object_desc = parent_op.get_friendly_object_name_for_description(
                schema=schema,
                object=object,
                object_desc=object_desc,
            )
            return f'alter the {self.property} of {object_desc}'
        else:
            return f'alter the {self.property} of schema object'


def compile_ddl(
    schema: s_schema.Schema,
    astnode: qlast.DDLOperation,
    *,
    context: Optional[CommandContext]=None,
) -> Command:

    if context is None:
        context = CommandContext()

    astnode_type = type(astnode)
    primary_cmdcls = CommandMeta._astnode_map.get(astnode_type)
    if primary_cmdcls is None:
        for astnode_type_base in astnode_type.__mro__[1:]:
            primary_cmdcls = CommandMeta._astnode_map.get(astnode_type_base)
            if primary_cmdcls is not None:
                break
        else:
            raise AssertionError(
                f'no delta command class for AST node {astnode!r}')

    cmdcls = primary_cmdcls.command_for_ast_node(astnode, schema, context)

    context_class = cmdcls.get_context_class()
    if context_class is not None:
        modaliases = cmdcls._modaliases_from_ast(schema, astnode, context)
        localnames = cmdcls.localnames_from_ast(schema, astnode, context)
        ctxcls = cast(
            Type[ObjectCommandContext[so.Object]],
            context_class,
        )
        ctx = ctxcls(
            schema,
            op=cast(ObjectCommand[so.Object], _dummy_command),
            scls=_dummy_object,
            modaliases=modaliases,
            localnames=localnames,
        )
        with context(ctx):
            cmd = cmdcls._cmd_tree_from_ast(schema, astnode, context)
    else:
        cmd = cmdcls._cmd_tree_from_ast(schema, astnode, context)

    return cmd


def get_object_delta_command(
    *,
    objtype: Type[so.Object_T],
    cmdtype: Type[ObjectCommand_T],
    schema: s_schema.Schema,
    name: sn.Name,
    ddl_identity: Optional[Mapping[str, Any]] = None,
    **kwargs: Any,
) -> ObjectCommand_T:

    cmdcls = cast(
        Type[ObjectCommand_T],
        get_object_command_class_or_die(cmdtype, objtype),
    )

    return cmdcls(
        classname=name,
        ddl_identity=ddl_identity,
        **kwargs,
    )


def get_object_command_id(delta: ObjectCommand[Any]) -> str:
    quoted_name: str

    if isinstance(delta.classname, sn.QualName):
        quoted_module = qlquote.quote_ident(delta.classname.module)
        quoted_nqname = qlquote.quote_ident(delta.classname.name)
        quoted_name = f'{quoted_module}::{quoted_nqname}'
    else:
        quoted_name = qlquote.quote_ident(str(delta.classname))

    if delta.orig_cmd_type is not None:
        cmdtype = delta.orig_cmd_type
    else:
        cmdtype = type(delta)

    qlcls = delta.get_schema_metaclass().get_ql_class_or_die()
    return f'{cmdtype.__name__} {qlcls} {quoted_name}'


def apply(
    delta: Command,
    *,
    schema: s_schema.Schema,
    context: Optional[CommandContext] = None,
) -> s_schema.Schema:
    if context is None:
        context = CommandContext()

    if not isinstance(delta, DeltaRoot):
        root = DeltaRoot()
        root.add(delta)
    else:
        root = delta

    return root.apply(schema, context)
