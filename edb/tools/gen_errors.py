#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

import builtins
import json
import pathlib
import re
import sys
from dataclasses import dataclass

import click

import edb

from edb.tools.edb import edbcommands
from edb.errors import base as edb_base_errors


@dataclass(frozen=True)
class ErrorCode:
    b1: int
    b2: int
    b3: int
    b4: int

    def __iter__(self):
        return iter((self.b1, self.b2, self.b3, self.b4))


@dataclass(frozen=True)
class ErrorDescription:
    name: str
    code: ErrorCode
    tags: frozenset
    base_name: str = ''


class ErrorsTree:

    python_errors = frozenset(
        name for name in dir(builtins) if re.match(r'^\w+Error$', name)
    )

    js_errors = frozenset({
        'EvalError', 'InternalError', 'RangeError', 'ReferenceError',
        'SyntaxError', 'TypeError', 'URIError',
    })

    ruby_errors = frozenset({
        'NoMemoryError', 'ScriptError', 'LoadError', 'NotImplementedError',
        'SyntaxError', 'SecurityError', 'SignalException', 'Interrupt',
        'StandardError', 'ArgumentError', 'UncaughtThrowError',
        'EncodingError', 'FiberError', 'IOError', 'EOFError', 'IndexError',
        'KeyError', 'StopIteration', 'LocalJumpError', 'NameError',
        'NoMethodError', 'RangeError', 'FloatDomainError', 'RegexpError',
        'RuntimeError', 'SystemCallError', 'ThreadError', 'TypeError',
        'ZeroDivisionError', 'SystemExit', 'SystemStackError',
    })

    # Normally in Java application code it is unlikely to throw/catch any of
    # the below (application exceptions have "Exception" suffix), but
    # let's try to avoid using these names anyways.
    java_errors = frozenset({
        'AssertionError', 'LinkageError', 'BootstrapMethodError',
        'ClassCircularityError', 'ClassFormatError',
        'UnsupportedClassVersionError', 'ExceptionInInitializerError',
        'IncompatibleClassChangeError', 'AbstractMethodError',
        'IllegalAccessError', 'InstantiationError', 'NoSuchFieldError',
        'NoSuchMethodError', 'NoClassDefFoundError', 'UnsatisfiedLinkError',
        'VerifyError', 'ThreadDeath', 'VirtualMachineError', 'InternalError',
        'OutOfMemoryError', 'StackOverflowError', 'UnknownError',
    })

    scala_errors = frozenset({
        'MatchError', 'NotImplementedError', 'UninitializedError',
        'UninitializedFieldError', 'AbstractMethodError',
    })

    edgedb_base_errors = frozenset(
        name for name in edb_base_errors.__all__
        if re.match(r'^\w+Error$', name)
    )

    # more on exceptions:
    # * In C# and PHP built-in exceptions have an "Exception" suffix;
    # * In C++, std::exceptions use snake_case.

    errors_names = {
        'Python': python_errors,
        'JavaScript': js_errors,
        'Ruby': ruby_errors,
        'Java': java_errors,
        'Scala': scala_errors,
        'EdgeDB Base': edgedb_base_errors,
    }

    DEFAULT_BASE_IMPORT = 'from edb.errors.base import *'
    DEFAULT_BASE_CLASS = 'EdgeDBError'
    DEFAULT_MESSAGE_BASE_CLASS = 'EdgeDBMessage'
    DEFAULT_EXTRA_ALL = 'base.__all__'

    def __init__(self):
        self._tree = {}
        self._all_names = set()

    def add(self, desc):
        if desc.name in self._all_names:
            raise ValueError(f'duplicate error name {desc.name!r}')

        # Let's try to avoid name clashes with built-in exception
        # names in some popular languages
        for lang, names in self.errors_names.items():
            if desc.name in names:
                raise ValueError(f'error name {desc.name!r} conflicts with '
                                 f'{lang} exception')

        if desc.code in self._tree:
            raise ValueError(f'duplicate error code for error {desc.name!r}')

        self._all_names.add(desc.name)
        self._tree[desc.code] = desc

    def load(self, ep):
        with open(ep, 'rt') as f:
            self._load(f)

    def _load(self, f):
        for line in f.readlines():
            if re.match(r'(?x)^ (\s*\#[^\n]*) | (\s*) $', line):
                continue

            # For consistency we require a very particular format
            # for error codes (hex numbers) and error names
            # (camel case, only words, ends with "Error").
            m = re.match(
                r'''(?x)^
                    0x_(?P<b1>[0-9A-F]{2})_
                       (?P<b2>[0-9A-F]{2})_
                       (?P<b3>[0-9A-F]{2})_
                       (?P<b4>[0-9A-F]{2})

                    \s+
                    (?P<name>[A-Z][a-zA-Z]+(?:Error|Message))
                    (?P<tags>(?:\s+\#[A-Z_]+)*)
                    \s*
                $''',
                line
            )

            if not m:
                die(f'Unable to parse {line!r} line')

            code = ErrorCode(
                int(m.group('b1'), 16),
                int(m.group('b2'), 16),
                int(m.group('b3'), 16),
                int(m.group('b4'), 16),
            )
            name = m.group('name')
            tags = m.group('tags').split()
            tags = frozenset(t.strip().lstrip('#') for t in tags)
            desc = ErrorDescription(name=name, code=code, tags=tags)

            self.add(desc)

    def get_parent(self, code):
        b1, b2, b3, b4 = code

        if b4 == 0 and b3 == 0 and b2 == 0:
            return None

        if b4 == 0 and b3 == 0:
            parent_code = ErrorCode(b1, 0, 0, 0)
        elif b4 == 0:
            parent_code = ErrorCode(b1, b2, 0, 0)
        else:
            parent_code = ErrorCode(b1, b2, b3, 0)

        try:
            return self._tree[parent_code]
        except KeyError:
            raise ValueError(f'No base class for code '
                             f'0x_{b1:0>2X}_{b2:0>2X}_{b3:0>2X}_{b4:0>2X}')

    def generate_classes(self, *, message_base_class, base_class, client):
        classes = []

        for desc in self._tree.values():
            if desc.code.b1 == 0xFF and not client:
                continue

            parent = self.get_parent(desc.code)

            if parent:
                base_name = parent.name
            elif desc.name.endswith('Error'):
                base_name = base_class
            else:
                base_name = message_base_class

            tags = desc.tags
            while parent:
                tags |= parent.tags
                parent = self.get_parent(parent.code)

            # make tag list order stable
            tags = sorted(tags)
            b1, b2, b3, b4 = desc.code

            classes.append((desc.name, base_name, b1, b2, b3, b4, tags))

        return classes

    def generate_pycode(
        self, *, message_base_class, base_class, base_import, extra_all, client
    ):
        classes = self.generate_classes(
            message_base_class=message_base_class,
            base_class=base_class,
            client=client)

        lines = []
        all_lines = []
        for name, base, i1, i2, i3, i4, tags in classes:
            all_lines.append(name)
            klass = (
                f'class {name}({base}):\n'
                f'    _code = 0x_{i1:0>2X}_{i2:0>2X}_{i3:0>2X}_{i4:0>2X}'
            )
            if client and tags:
                tag_list = ", ".join(sorted(tags))
                klass += f'\n    tags = frozenset({{{tag_list}}})'
            lines.append(klass)

        lines = '\n\n\n'.join(lines)

        all_lines = '    ' + ',\n    '.join(repr(ln) for ln in all_lines) + ','
        all_lines = (
            f'__all__ = {extra_all} + (  # type: ignore\n'
            f'{all_lines}\n)'
        )

        code = (
            f'{base_import}'
            f'\n\n\n'
            f'{all_lines}'
            f'\n\n\n'
            f'{lines}'
        )

        return code


def die(msg):
    print(f'FATAL: {msg}', file=sys.stderr)
    sys.exit(1)


def main(
    *,
    base_class,
    message_base_class,
    base_import,
    stdout,
    extra_all,
    client,
    language,
):

    for p in edb.__path__:
        ep = pathlib.Path(p) / 'api' / 'errors.txt'
        if ep.exists():
            out_fn = pathlib.Path(p) / 'errors' / '__init__.py'
            break
    else:
        die('Unable to find the "edb/api/errors.txt" file')

    tree = ErrorsTree()
    tree.load(ep)

    code = tree.generate_pycode(base_class=base_class,
                                message_base_class=message_base_class,
                                base_import=base_import,
                                extra_all=extra_all,
                                client=client)

    cmd_line = '#    $ edb gen-errors'
    if base_class != ErrorsTree.DEFAULT_BASE_CLASS:
        cmd_line += f' \\\n#        --base-class "{base_class}"'
    if message_base_class != ErrorsTree.DEFAULT_MESSAGE_BASE_CLASS:
        cmd_line += \
            f' \\\n#        --message-base-class "{message_base_class}"'
    if base_import != ErrorsTree.DEFAULT_BASE_IMPORT:
        cmd_line += f' \\\n#        --import {repr(base_import)}'
    if extra_all != ErrorsTree.DEFAULT_EXTRA_ALL:
        cmd_line += f' \\\n#        --extra-all "{extra_all}"'
    if stdout:
        cmd_line += f' \\\n#        --stdout'
    if client:
        cmd_line += f' \\\n#        --client'

    code = (
        f'# AUTOGENERATED FROM "edb/api/errors.txt" WITH\n'
        f'{cmd_line}'
        f'\n\n\n'
        f'# flake8: noqa'
        f'\n\n\n'
        f'{code}'
        f'\n'
    )

    if stdout:
        print(code)
    else:
        with open(out_fn, 'wt') as f:
            f.write(code)


@edbcommands.command('gen-errors')
@click.option(
    '--base-class', type=str, default=ErrorsTree.DEFAULT_BASE_CLASS)
@click.option(
    '--message-base-class', type=str,
    default=ErrorsTree.DEFAULT_MESSAGE_BASE_CLASS)
@click.option(
    '--import', 'base_import', type=str,
    default=ErrorsTree.DEFAULT_BASE_IMPORT)
@click.option(
    '--extra-all', type=str, default=ErrorsTree.DEFAULT_EXTRA_ALL)
@click.option(
    '--stdout', type=bool, default=False, is_flag=True)
@click.option(
    '--client', type=bool, default=False, is_flag=True)
def gen_errors(*, base_class, message_base_class, base_import,
               stdout, extra_all, client):
    """Generate edb/errors.py from edb/api/errors.txt"""
    try:
        main(base_class=base_class,
             message_base_class=message_base_class,
             base_import=base_import,
             stdout=stdout,
             extra_all=extra_all,
             client=client,
             language='python')
    except Exception as ex:
        die(str(ex))


@edbcommands.command('gen-errors-json')
@click.option(
    '--client', type=bool, default=False, is_flag=True)
def gen_errors_json(*, client):
    """Generate JSON from edb/api/errors.txt"""
    for p in edb.__path__:
        ep = pathlib.Path(p) / 'api' / 'errors.txt'
        if ep.exists():
            break
    else:
        die('Unable to find the "edb/api/errors.txt" file')

    try:
        tree = ErrorsTree()
        tree.load(ep)

        clss = tree.generate_classes(
            message_base_class=None, base_class=None, client=client)
        print(json.dumps(clss))
    except Exception as ex:
        die(str(ex))
