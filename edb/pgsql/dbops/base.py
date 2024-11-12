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
from typing import (
    Any,
    Final,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)
from collections.abc import MutableSequence

import collections
import enum
import numbers
import textwrap

from edb.common import markup
from edb.common import struct
from edb.common import typeutils

from ..common import quote_ident as qi
from ..common import quote_literal as ql
from ..common import quote_type as qt
from ..common import qname as qn


class NotSpecifiedT(enum.Enum):
    NotSpecified = 0


NotSpecified: Final = NotSpecifiedT.NotSpecified


def encode_value(val: Any) -> str:
    """Encode value into an appropriate SQL expression."""
    if hasattr(val, 'to_sql_expr'):
        val = val.to_sql_expr()
    elif isinstance(val, tuple):
        val_list = [encode_value(el) for el in val]
        val = f'ROW({", ".join(val_list)})'
    elif isinstance(val, struct.Struct):
        val_list = [encode_value(el) for el in val.as_tuple()]
        val = f'ROW({", ".join(val_list)})'
    elif typeutils.is_container(val):
        val_list = [encode_value(el) for el in val]
        val = f'ARRAY[{", ".join(val_list)}]'
    elif val is None:
        val = 'NULL'
    elif not isinstance(val, numbers.Number):
        val = ql(str(val))
    elif isinstance(val, int):
        val = str(int(val))
    else:
        val = str(val)

    return val


class PLExpression(str):
    pass


class SQLBlock:
    commands: list[str | PLBlock]

    def __init__(self) -> None:
        self.commands = []
        self._transactional = True

    def add_block(self) -> PLBlock:
        block = PLTopBlock()
        self.add_command(block)
        return block

    def to_string(self) -> str:
        if not self._transactional:
            raise ValueError(
                'block is non-transactional, please use .get_statements()'
            )
        stmts = self.get_statements()
        body = '\n\n'.join(stmt + ';' if stmt[-1] != ';' else stmt
                           for stmt in stmts if stmt).rstrip()
        if body and body[-1] != ';':
            body += ';'

        return body

    def get_statements(self) -> List[str]:
        return [(cmd if isinstance(cmd, str) else cmd.to_string()).rstrip()
                for cmd in self.commands]

    def add_command(self, stmt: str | PLBlock) -> None:
        self.commands.append(stmt)

    def has_declarations(self) -> bool:
        return False

    def set_non_transactional(self) -> None:
        self._transactional = False

    def is_transactional(self) -> bool:
        return self._transactional


class PLBlock(SQLBlock):

    varcounter: dict[str, int]
    shared_vars: set[str]
    declarations: list[tuple[str, str | tuple[str, str]]]
    conditions: Iterable[str | Condition]
    neg_conditions: Iterable[str | Condition]

    def __init__(self, top_block: Optional[PLTopBlock], level: int) -> None:
        super().__init__()
        self.top_block = top_block
        self.varcounter = collections.defaultdict(int)
        self.shared_vars = set()
        self.declarations = []
        self.level = level
        self.conditions = set()
        self.neg_conditions = set()

    def has_declarations(self) -> bool:
        return bool(self.declarations)

    def has_statements(self) -> bool:
        return bool(self.commands)

    def get_top_block(self) -> PLTopBlock:
        return typeutils.not_none(self.top_block)

    def add_block(self) -> PLBlock:
        block = PLBlock(top_block=self.top_block, level=self.level + 1)
        self.add_command(block)
        return block

    def to_string(self) -> str:
        if self.declarations:
            vv = (f'    {qi(n)} {qt(t)};' for n, t in self.declarations)
            decls = 'DECLARE\n' + '\n'.join(vv) + '\n'
        else:
            decls = ''

        body = super().to_string()

        if self.conditions or self.neg_conditions:
            exprs = []
            if self.conditions:
                for cond in self.conditions:
                    if not isinstance(cond, str):
                        cond_expr = f'EXISTS ({cond.code()})'
                    else:
                        cond_expr = cond
                    exprs.append(cond_expr)

            if self.neg_conditions:
                for cond in self.neg_conditions:
                    if not isinstance(cond, str):
                        cond_expr = f'EXISTS ({cond.code()})'
                    else:
                        cond_expr = cond
                    exprs.append(f'NOT {cond_expr}')

            if_clause = '\n    AND'.join(
                f'({textwrap.indent(expr, "    ").lstrip()})'
                for expr in exprs
            )

            body = textwrap.indent(body, '    ').rstrip()
            semicolon = ';' if body[-1] != ';' else ''
            body = f'IF {if_clause}\nTHEN\n{body}{semicolon}\nEND IF;'

        if decls or not isinstance(self.top_block, PLBlock):
            return textwrap.indent(
                f'{decls}BEGIN\n{body}\nEND;',
                ' ' * self.level * 4,
            )
        else:
            return body

    def add_command(
        self,
        cmd: str | PLBlock,
        *,
        conditions: Optional[Iterable[str | Condition]] = None,
        neg_conditions: Optional[Iterable[str | Condition]] = None
    ) -> None:
        stmt: str | PLBlock
        if conditions or neg_conditions:
            exprs = []
            if conditions:
                for cond in conditions:
                    if not isinstance(cond, str):
                        cond_expr = f'EXISTS ({cond.code()})'
                    else:
                        cond_expr = cond
                    exprs.append(cond_expr)

            if neg_conditions:
                for cond in neg_conditions:
                    if not isinstance(cond, str):
                        cond_expr = f'EXISTS ({cond.code()})'
                    else:
                        cond_expr = cond
                    exprs.append(f'NOT {cond_expr}')

            if_clause = '\n    AND'.join(
                f'({textwrap.indent(expr, "    ").lstrip()})'
                for expr in exprs
            )

            if isinstance(cmd, PLBlock):
                cmd = cmd.to_string()

            cmd = textwrap.indent(cmd, '    ').rstrip()
            semicolon = ';' if cmd[-1] != ';' else ''
            stmt = f'IF {if_clause}\nTHEN\n{cmd}{semicolon}\nEND IF;'
        else:
            stmt = cmd

        super().add_command(stmt)

    def get_var_name(self, hint: Optional[str] = None) -> str:
        if hint is None:
            hint = 'v'
        self.varcounter[hint] += 1
        return f'{hint}_{self.varcounter[hint]}'

    def declare_var(
        self,
        type_name: Union[str, Tuple[str, str]],
        *,
        var_name: str='',
        var_name_prefix: str='v',
        shared: bool=False,
    ) -> str:
        if shared:
            if not var_name:
                var_name = var_name_prefix
            if var_name not in self.shared_vars:
                self.declarations.append((var_name, type_name))
                self.shared_vars.add(var_name)
        else:
            if not var_name:
                var_name = self.get_var_name(var_name_prefix)
            self.declarations.append((var_name, type_name))

        return var_name


class PLTopBlock(PLBlock):
    def __init__(self) -> None:
        super().__init__(top_block=None, level=0)
        self.declare_var('text', var_name='_dummy_text', shared=True)

    def add_block(self) -> PLBlock:
        block = PLBlock(top_block=self, level=self.level + 1)
        self.add_command(block)
        return block

    def to_string(self) -> str:
        body = super().to_string()
        return f'DO LANGUAGE plpgsql $__$\n{body}\n$__$;'

    def get_top_block(self) -> PLTopBlock:
        return self


class BaseCommand(markup.MarkupCapableMixin):
    def generate(self, block: SQLBlock) -> None:
        raise NotImplementedError

    @classmethod
    def as_markup(cls, self, *, ctx) -> markup.elements.lang.TreeNode:
        return markup.elements.lang.TreeNode(name=repr(self))

    def dump(self) -> str:
        return str(self)


class Command(BaseCommand):

    conditions: Set[str | Condition]
    neg_conditions: Set[str | Condition]

    def __init__(
        self,
        *,
        conditions: Optional[Iterable[str | Condition]] = None,
        neg_conditions: Optional[Iterable[str | Condition]] = None,
    ) -> None:
        self.opid = id(self)
        self.conditions = set(conditions) if conditions else set()
        self.neg_conditions = set(neg_conditions) if neg_conditions else set()

    def generate(self, block: SQLBlock) -> None:
        self_block = self.generate_self_block(block)
        if self_block is None:
            return

        self.generate_extra(self_block)
        self_block.conditions = self.conditions
        self_block.neg_conditions = self.neg_conditions

    def generate_self_block(self, block: SQLBlock) -> Optional[PLBlock]:
        # Default implementation simply calls self.code_with_block()
        self_block = block.add_block()
        self_block.add_command(self.code_with_block(self_block))
        return self_block

    def generate_extra(self, block: PLBlock) -> None:
        pass

    def code(self) -> str:
        raise NotImplementedError

    def code_with_block(self, block: PLBlock) -> str:
        return self.code()


class CommandGroup(Command):
    commands: MutableSequence[Command]

    def __init__(
        self,
        *,
        conditions: Optional[Iterable[str | Condition]] = None,
        neg_conditions: Optional[Iterable[str | Condition]] = None,
    ) -> None:
        super().__init__(conditions=conditions, neg_conditions=neg_conditions)
        self.commands = []

    def add_command(self, cmd: Command) -> None:
        self.commands.append(cmd)

    def add_commands(self, cmds: Sequence[Command]) -> None:
        self.commands.extend(cmds)

    def generate_self_block(self, block: SQLBlock) -> Optional[PLBlock]:
        if not self.commands:
            return None

        self_block = block.add_block()

        for cmd in self.commands:
            cmd.generate(self_block)

        return self_block

    @classmethod
    def as_markup(cls, self, *, ctx) -> markup.elements.lang.TreeNode:
        node = markup.elements.lang.TreeNode(name=repr(self))

        for op in self.commands:
            node.add_child(node=markup.serialize(op, ctx=ctx))

        return node

    def __iter__(self) -> Iterator[Command]:
        return iter(self.commands)

    def __len__(self) -> int:
        return len(self.commands)


class CompositeCommand(Command):

    def generate_extra_composite(
        self, block: PLBlock, group: CompositeCommandGroup
    ) -> None:
        pass


class CompositeCommandGroup(Command):
    commands: MutableSequence[CompositeCommand]

    def __init__(
        self,
        *,
        conditions: Optional[Iterable[str | Condition]] = None,
        neg_conditions: Optional[Iterable[str | Condition]] = None,
    ) -> None:
        super().__init__(conditions=conditions, neg_conditions=neg_conditions)
        self.commands = []

    def add_command(self, cmd: CompositeCommand) -> None:
        self.commands.append(cmd)

    def add_commands(self, cmds: Sequence[CompositeCommand]) -> None:
        self.commands.extend(cmds)

    def generate_self_block(self, block: SQLBlock) -> Optional[PLBlock]:
        if not self.commands:
            return None

        self_block = block.add_block()
        prefix_code = self.prefix_code()
        actions = []
        dynamic_actions = []

        for cmd in self.commands:
            if isinstance(cmd, tuple) and (cmd[1] or cmd[2]):
                action = cmd[0].code_with_block(self_block)
                if isinstance(action, PLExpression):
                    subcommand = \
                        f"EXECUTE {ql(prefix_code)} || ' ' || {action}"
                else:
                    subcommand = prefix_code + ' ' + action
                self_block.add_command(
                    subcommand, conditions=cmd[1], neg_conditions=cmd[2])
            else:
                action = cmd.code_with_block(self_block)
                if isinstance(action, PLExpression):
                    subcommand = \
                        f"EXECUTE {ql(prefix_code)} || ' ' || {action}"
                    dynamic_actions.append(subcommand)
                else:
                    actions.append(action)

        if actions:
            command = prefix_code + ' ' + ', '.join(actions)
            self_block.add_command(command)

        if dynamic_actions:
            for action in dynamic_actions:
                self_block.add_command(action)

        extra_block = self_block.add_block()

        for cmd in self.commands:
            if isinstance(cmd, tuple) and (cmd[1] or cmd[2]):
                cmd[0].generate_extra_composite(extra_block, self)
            else:
                cmd.generate_extra_composite(extra_block, self)

        return self_block

    def prefix_code(self) -> str:
        raise NotImplementedError

    def __iter__(self) -> Iterator[CompositeCommand]:
        return iter(self.commands)

    def __len__(self) -> int:
        return len(self.commands)


class Condition(BaseCommand):

    def code(self) -> str:
        raise NotImplementedError()


class Query(Command):
    def __init__(
        self,
        text: str,
        *,
        type: Optional[str | Tuple[str, str]] = None,
        trampoline_fixup: bool = True,
    ) -> None:
        from ..import trampoline

        super().__init__()
        if trampoline_fixup:
            text = trampoline.fixup_query(text)
        self.text = text
        self.type = type

    def to_sql_expr(self) -> str:
        if self.type:
            return f'({self.text})::{qn(*self.type)}'
        else:
            return self.text

    def code(self) -> str:
        return self.text

    def __repr__(self) -> str:
        return f'<Query {self.text!r}>'


class PLQuery(Query):
    pass


class DefaultMeta(type):
    def __bool__(cls):
        return False

    def __repr__(self):
        return '<DEFAULT>'

    __str__ = __repr__


class Default(metaclass=DefaultMeta):
    pass


class DBObject:
    def __init__(
        self,
        *,
        metadata: Optional[Mapping[str, Any]] = None
    ) -> None:
        self.metadata = dict(metadata) if metadata else None

    def add_metadata(self, key: str, value: Any) -> None:
        if self.metadata is None:
            self.metadata = {}

        self.metadata[key] = value

    def get_metadata(self, key: str) -> Any:
        if self.metadata is None:
            return None
        else:
            return self.metadata.get(key)

    def is_shared(self) -> bool:
        return False

    def get_type(self) -> str:
        raise NotImplementedError()

    def get_id(self) -> str:
        raise NotImplementedError()


class InheritableDBObject(DBObject):
    def __init__(
        self,
        *,
        inherit: bool = False,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(metadata=metadata)
        if inherit:
            self.add_metadata('ddl:inherit', inherit)

    @property
    def inherit(self) -> bool:
        return self.get_metadata('ddl:inherit') or False


class NoOpCommand(Command):
    def generate_self_block(self, block: SQLBlock) -> Optional[PLBlock]:
        return None
