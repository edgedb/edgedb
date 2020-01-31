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

import click

import edb

from edb.tools.edb import edbcommands
from edb.errors import base as edb_base_errors


class SparseArray:

    def __init__(self):
        self._list = {}

    def __getitem__(self, idx: int):
        if not isinstance(idx, int):
            raise TypeError
        if idx not in self._list:
            raise IndexError
        return self._list[idx]

    def __setitem__(self, idx, value):
        if not isinstance(idx, int):
            raise TypeError
        self._list[idx] = value

    def __contains__(self, idx):
        if not isinstance(idx, int):
            raise TypeError
        return idx in self._list

    def __iter__(self):
        for i in self.indices():
            yield self._list[i]

    def indices(self):
        return iter(sorted(self._list.keys()))

    def setdefault(self, idx, default_factory):
        if not isinstance(idx, int):
            raise TypeError
        if idx in self._list:
            return self._list[idx]
        self._list[idx] = default_factory()
        return self._list[idx]

    def __repr__(self):
        vals = (f'{i}: {self._list[i]}' for i in self.indices())
        vals = ', '.join(vals)
        return f'[{vals}]'


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
        self._tree = SparseArray()

        self._all_codes = set()
        self._all_names = set()

    def add(self, b1, b2, b3, b4, name):
        if name in self._all_names:
            raise ValueError(f'duplicate error name {name!r}')

        # Let's try to avoid name clashes with built-in exception
        # names in some popular languages
        for lang, names in self.errors_names.items():
            if name in names:
                raise ValueError(
                    f'error name {name!r} conflicts with {lang} exception')

        if (b1, b2, b3, b4) in self._all_codes:
            raise ValueError(f'duplicate error code for error {name!r}')

        self._all_names.add(name)
        self._all_codes.add((b1, b2, b3, b4))

        self._tree \
            .setdefault(b1, SparseArray) \
            .setdefault(b2, SparseArray) \
            .setdefault(b3, SparseArray) \
            .setdefault(b4, lambda: name)

    def load(self, ep):
        with open(ep, 'rt') as f:
            self._load(f)

    def _load(self, f):
        lines = []
        for line in f.readlines():
            if re.match(r'(?x)^ (\s*\#[^\n]*) | (\s*) $', line):
                continue
            else:
                lines.append(line)

        for line in lines:
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
                    \s*
                $''',
                line
            )

            if not m:
                die(f'Unable to parse {line!r} line')

            b1 = int(m.group('b1'), 16)
            b2 = int(m.group('b2'), 16)
            b3 = int(m.group('b3'), 16)
            b4 = int(m.group('b4'), 16)
            name = m.group('name')

            self.add(b1, b2, b3, b4, name)

    def generate_classes(self, *, message_base_class, base_class, client):
        classes = []

        for i1 in self._tree.indices():
            try:
                b1 = self._tree[i1][0][0][0]
            except (IndexError, TypeError):
                raise ValueError(f'No base class for code 0x_{i1:0>2X}_*_*_*')

            if i1 == 0xFF and not client:
                continue

            for i2 in self._tree[i1].indices():
                try:
                    b2 = self._tree[i1][i2][0][0]
                except (IndexError, TypeError):
                    raise ValueError(
                        f'No base class for code 0x_{i1:0>2X}_{i2:0>2X}_*_*')

                for i3 in self._tree[i1][i2].indices():
                    try:
                        b3 = self._tree[i1][i2][i3][0]
                    except (IndexError, TypeError):
                        raise ValueError(
                            f'No base class for code '
                            f'0x_{i1:0>2X}_{i2:0>2X}_{i3:0>2X}_*')

                    for i4 in self._tree[i1][i2][i3].indices():
                        b4 = self._tree[i1][i2][i3][i4]
                        base = None
                        if b4 != b3:
                            base = b3
                        else:
                            if b4 != b2:
                                base = b2
                            else:
                                base = b1
                        if base == b4:
                            if b4.endswith('Error'):
                                base = base_class
                            else:
                                base = message_base_class

                        classes.append((b4, base, i1, i2, i3, i4))

        return classes

    def generate_pycode(self, *, message_base_class, base_class,
                        base_import, extra_all, client):
        classes = self.generate_classes(
            message_base_class=message_base_class,
            base_class=base_class,
            client=client)

        lines = []
        all_lines = []
        for name, base, i1, i2, i3, i4 in classes:
            all_lines.append(name)
            lines.append(
                f'class {name}({base}):\n'
                f'    _code = 0x_{i1:0>2X}_{i2:0>2X}_{i3:0>2X}_{i4:0>2X}'
            )

        lines = '\n\n\n'.join(lines)

        all_lines = '    ' + ',\n    '.join(repr(ln) for ln in all_lines) + ','
        all_lines = f'__all__ = {extra_all} + (\n{all_lines}\n)'

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


def main(*, base_class, message_base_class,
         base_import, stdout, extra_all, client,
         language):

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
        cmd_line += f' \\\n#        --import "{base_import}"'
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
