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

import collections
import numbers
import textwrap
from typing import *  # NoQA

from edb.common import markup
from edb.common import struct
from edb.common import typeutils

from ..common import quote_ident as qi
from ..common import quote_literal as ql
from ..common import quote_type as qt
from ..common import qname as qn


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
    def __init__(self):
        self.commands = []
        self.disable_ddl_triggers = True

    def add_block(self):
        block = PLTopBlock()
        self.add_command(block)
        return block

    def to_string(self) -> str:
        stmts = ((cmd if isinstance(cmd, str) else cmd.to_string()).rstrip()
                 for cmd in self.commands)
        body = '\n\n'.join(stmt + ';' if stmt[-1] != ';' else stmt
                           for stmt in stmts).rstrip()
        if body and body[-1] != ';':
            body += ';'

        return body

    def add_command(self, stmt) -> None:
        if isinstance(stmt, PLBlock) and not stmt.has_declarations():
            self.commands.extend(stmt.commands)
        else:
            self.commands.append(stmt)

    def has_declarations(self) -> bool:
        return False


class PLBlock(SQLBlock):
    def __init__(self, top_block, level):
        super().__init__()
        self.top_block = top_block
        self.varcounter = collections.defaultdict(int)
        self.shared_vars = set()
        self.declarations = []
        self.level = level
        if top_block is not None:
            self.disable_ddl_triggers = self.top_block.disable_ddl_triggers
        else:
            self.disable_ddl_triggers = True

    def has_declarations(self) -> bool:
        return bool(self.declarations)

    def has_statements(self) -> bool:
        return bool(self.commands)

    def get_top_block(self) -> PLTopBlock:
        return self.top_block

    def add_block(self, attach: bool=True):
        block = PLBlock(top_block=self.top_block, level=self.level + 1)
        if attach:
            self.add_command(block)
        return block

    def to_string(self):
        if self.declarations:
            vv = (f'    {qi(n)} {qt(t)};' for n, t in self.declarations)
            decls = 'DECLARE\n' + '\n'.join(vv) + '\n'
        else:
            decls = ''

        body = super().to_string()

        return textwrap.indent(f'{decls}BEGIN\n{body}\nEND;',
                               ' ' * self.level * 4)

    def add_command(self, cmd, *, conditions=None, neg_conditions=None):
        if conditions or neg_conditions:
            exprs = []
            if conditions:
                for cond in conditions:
                    if not isinstance(cond, str):
                        cond_expr = f'EXISTS ({cond.code(self)})'
                    else:
                        cond_expr = cond
                    exprs.append(cond_expr)

            if neg_conditions:
                for cond in neg_conditions:
                    if not isinstance(cond, str):
                        cond_expr = f'EXISTS ({cond.code(self)})'
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

    def get_var_name(self, hint=None):
        if hint is None:
            hint = 'v'
        self.varcounter[hint] += 1
        return f'{hint}_{self.varcounter[hint]}'

    def declare_var(
        self,
        type_name: Union[str, Tuple[str, str]],
        var_name_prefix: str='v',
        shared: bool=False,
    ) -> str:
        if shared:
            var_name = var_name_prefix
            if var_name not in self.shared_vars:
                self.declarations.append((var_name, type_name))
                self.shared_vars.add(var_name)
        else:
            var_name = self.get_var_name(var_name_prefix)
            self.declarations.append((var_name, type_name))

        return var_name


class PLTopBlock(PLBlock):
    def __init__(self, *, disable_ddl_triggers: bool=True):
        super().__init__(top_block=None, level=0)
        self.disable_ddl_triggers = disable_ddl_triggers

    def add_block(self):
        block = PLBlock(top_block=self, level=self.level + 1)
        self.add_command(block)
        return block

    def to_string(self):
        body = super().to_string()
        return f'DO LANGUAGE plpgsql $__$\n{body}\n$__$;'

    def get_top_block(self) -> PLTopBlock:
        return self


class BaseCommand(metaclass=markup.MarkupCapableMeta):
    def generate(self, block):
        raise NotImplementedError

    @classmethod
    def as_markup(cls, self, *, ctx):
        return markup.elements.lang.TreeNode(name=repr(self))

    def dump(self):
        return str(self)


class Command(BaseCommand):
    def __init__(self, *, conditions=None, neg_conditions=None, priority=0):
        self.opid = id(self)
        self.conditions = conditions or set()
        self.neg_conditions = neg_conditions or set()
        self.priority = priority

    def generate(self, block) -> None:
        self_block = self.generate_self_block(block)
        if self_block is None:
            return

        self.generate_extra(self_block)

        kwargs = {}
        if self.conditions:
            kwargs['conditions'] = self.conditions
        if self.neg_conditions:
            kwargs['neg_conditions'] = self.neg_conditions

        block.add_command(self_block, **kwargs)

    def generate_self_block(self, block: PLBlock) -> Optional[PLBlock]:
        # Default implementation simply calls self.code()
        self_block = block.add_block()
        self_block.add_command(self.code(block))
        return self_block

    def generate_extra(self, block: PLBlock) -> None:
        pass

    def code(self, block: PLBlock) -> str:
        raise NotImplementedError


class CommandGroup(Command):
    def __init__(self, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)
        self.commands = []

    def add_command(self, cmd):
        self.commands.append(cmd)

    def add_commands(self, cmds):
        self.commands.extend(cmds)

    def generate_self_block(self, block: PLBlock) -> Optional[PLBlock]:
        if not self.commands:
            return None

        self_block = block.add_block()

        for cmd in self.commands:
            cmd.generate(self_block)

        return self_block

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = markup.elements.lang.TreeNode(name=repr(self))

        for op in self.commands:
            node.add_child(node=markup.serialize(op, ctx=ctx))

        return node

    def __iter__(self):
        return iter(self.commands)

    def __call__(self, typ):
        return filter(lambda i: isinstance(i, typ), self.commands)

    def __len__(self):
        return len(self.commands)


class CompositeCommandGroup(CommandGroup):
    def generate_self_block(self, block: PLBlock) -> Optional[PLBlock]:
        if not self.commands:
            return None

        self_block = block.add_block()
        extra_block = self_block.add_block(attach=False)
        prefix_code = self.prefix_code()
        actions = []
        dynamic_actions = []

        for cmd in self.commands:
            if isinstance(cmd, tuple) and (cmd[1] or cmd[2]):
                action = cmd[0].code(block)
                cmd[0].generate_extra(extra_block, self)
                if isinstance(action, PLExpression):
                    subcommand = \
                        f"EXECUTE {ql(prefix_code)} || ' ' || {action}"
                else:
                    subcommand = prefix_code + ' ' + action
                block.add_command(
                    subcommand, conditions=cmd[1], neg_conditions=cmd[2])
            else:
                action = cmd.code(block)
                cmd.generate_extra(extra_block, self)
                if isinstance(action, PLExpression):
                    subcommand = \
                        f"EXECUTE {ql(prefix_code)} || ' ' || {action}"
                    dynamic_actions.append(subcommand)
                else:
                    actions.append(action)

        if actions:
            command = prefix_code + ' ' + ', '.join(actions)
            block.add_command(command)

        if dynamic_actions:
            for action in dynamic_actions:
                block.add_command(action)

        if extra_block.has_statements():
            block.add_command(extra_block)

        return self_block

    def prefix_code(self) -> str:
        raise NotImplementedError


class Condition(BaseCommand):
    pass


class Query(Command):
    def __init__(self, text, *, type=None):
        super().__init__()
        self.text = text
        self.type = type

    def to_sql_expr(self):
        if self.type:
            return f'({self.text})::{qn(*self.type)}'
        else:
            return self.text

    def code(self, block: PLBlock) -> str:
        return self.text

    def __repr__(self):
        return f'<Query {self.text!r}>'


class DefaultMeta(type):
    def __bool__(cls):
        return False

    def __repr__(self):
        return '<DEFAULT>'

    __str__ = __repr__


class Default(metaclass=DefaultMeta):
    pass


class DBObject:
    def __init__(self, *, metadata=None):
        self.metadata = metadata

    def add_metadata(self, key, value):
        if self.metadata is None:
            self.metadata = {}

        self.metadata[key] = value

    def get_metadata(self, key):
        if self.metadata is None:
            return None
        else:
            return self.metadata.get(key)

    def is_shared(self) -> bool:
        return False


class InheritableDBObject(DBObject):
    def __init__(self, *, inherit=False, **kwargs):
        super().__init__(**kwargs)
        if inherit:
            self.add_metadata('ddl:inherit', inherit)

    @property
    def inherit(self):
        return self.get_metadata('ddl:inherit') or False
