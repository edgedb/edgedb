#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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
import typing_extensions


class Command(NamedTuple):

    trigger: str
    desc: str
    group: str
    callback: CommandCallback
    arg_name: Optional[str] = None
    arg_optional: bool = True
    flags: Optional[Dict[str, str]] = None
    devonly: bool = False


class CommandCallback(typing_extensions.Protocol):

    def __call__(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        pass


class Parser:

    commands: Tuple[Command, ...]

    _prefixes: Tuple[Command, ...]

    def __init__(self, commands: Sequence[Command]) -> None:
        self.commands = tuple(commands)

        if len([c.trigger for c in commands]) != len(commands):
            raise ValueError('invalid commands: duplicate triggers')

        self._prefixes = tuple(
            reversed(sorted(commands, key=lambda c: len(c.trigger))))

    def render(
        self,
        *,
        show_devonly: bool = False,
        group_annos: Optional[Mapping[str, str]] = None,
    ) -> str:
        by_group: Dict[str, List[Command]] = collections.defaultdict(list)
        for c in self.commands:
            if not show_devonly and c.devonly:
                continue
            by_group[c.group].append(c)

        parts: List[Tuple[str, str, str]] = []
        max_cmdname_len = 0
        for groupname, groupcmds in by_group.items():
            parts.append(('g', groupname, ''))
            if group_annos and groupname in group_annos:
                parts.append(('gdesc', f'  {group_annos[groupname]}', ''))

            for cmd in groupcmds:
                cmdname = fR'\{cmd.trigger}'
                if cmd.flags:
                    cmdname = f'{cmdname}[{"".join(cmd.flags.keys())}]'
                if cmd.arg_name:
                    if cmd.arg_optional:
                        cmdname = f'{cmdname} [{cmd.arg_name}]'
                    else:
                        cmdname = f'{cmdname} {cmd.arg_name}'

                parts.append(('cmd', cmdname, cmd.desc))

                if len(cmdname) > max_cmdname_len:
                    max_cmdname_len = len(cmdname)

        max_cmdname_len += 8

        lines: List[str] = []
        first_desc = True
        for tag, cmdname, desc in parts:
            if not desc:
                if not first_desc and tag == 'g':
                    lines.append('')
                lines.append(cmdname)
                first_desc = False
            else:
                lines.append(f'  {cmdname:{max_cmdname_len}}{desc}')

        return '\n'.join(lines)

    def parse(
        self,
        inp: str
    ) -> Tuple[Command, AbstractSet[str], Optional[str]]:

        if not inp.startswith('\\'):
            raise ValueError('invalid command')

        inp = inp[1:]  # skip the slash

        arg: Optional[str]
        inp, *args = inp.split(None, 1)
        if args:
            arg = args[0].strip()
        else:
            arg = None

        for cmd in self._prefixes:
            if inp.startswith(cmd.trigger):
                inp_flags = inp[len(cmd.trigger):]
                break
        else:
            raise LookupError(fR'cannot resolve \{inp} command')

        options = set()
        if inp_flags:
            if not cmd.flags:
                raise LookupError(
                    fR'\{cmd.trigger} command does not have flags matching '
                    fR'any of {inp_flags!r}')

            flags = set(inp_flags)
            unknown_flags = flags - cmd.flags.keys()
            if unknown_flags:
                raise LookupError(
                    fR'\{cmd.trigger} command does not '
                    fR'recognize the following flags: '
                    fR'{", ".join(repr(c) for c in unknown_flags)}'
                )

            for flag in flags:
                options.add(cmd.flags[flag])

        if arg and not cmd.arg_name:
            raise LookupError(
                fR'\{cmd.trigger} command does not have arguments')
        if not arg and cmd.arg_name and not cmd.arg_optional:
            raise LookupError(
                fR'\{cmd.trigger} command has a required argument')

        return cmd, options, arg

    def run(self, inp: str) -> None:
        cmd, flags, arg = self.parse(inp)
        cmd.callback(flags=flags, arg=arg)
