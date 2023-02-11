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

import itertools
import textwrap

from . import base
from .visitor import NodeVisitor


class SourceGenerator(NodeVisitor):
    """Generate source code from an AST tree."""

    result: List[str]

    def __init__(
        self,
        indent_with: str = ' ' * 4,
        add_line_information: bool = False,
        pretty: bool = True
    ) -> None:
        self.result = []
        self.indent_with = indent_with
        self.add_line_information = add_line_information
        self.indentation = 0
        self.char_indentation = 0
        self.new_lines = 0
        self.current_line = 1
        self.pretty = pretty

    def node_visit(self, node: base.AST) -> None:
        method = 'visit_' + node.__class__.__name__
        visitor = getattr(self, method, self.generic_visit)
        return visitor(node)

    def visit_indented(
        self, node: base.AST, indent: bool = True, nest: bool = False
    ) -> None:
        if nest:
            self.write("(")
        if indent:
            self.new_lines = 1
            self.char_indentation += 1
        res = super().visit(node)
        if indent:
            self.char_indentation -= 1
        if nest:
            self.write(")")
            self.new_lines = 1
        return res

    def write(
        self,
        *x: str,
        delimiter: Optional[str] = None
    ) -> None:
        if not x:
            return
        if self.new_lines:
            if self.result and self.pretty:
                self.current_line += self.new_lines
                self.result.append('\n' * self.new_lines)
            if self.pretty:
                self.result.append(self.indent_with * self.indentation)
                self.result.append(' ' * self.char_indentation)
            else:
                self.result.append(' ')
            self.new_lines = 0
        if delimiter:
            self.result.append(x[0])
            chain = itertools.chain.from_iterable
            chunks: Iterable[str] = chain((delimiter, v) for v in x[1:])
        else:
            chunks = x

        for chunk in chunks:
            if not isinstance(chunk, str):
                raise ValueError(
                    'invalid text chunk in codegen: {!r}'.format(chunk))
            self.result.append(chunk)

    def visit_list(
        self,
        items: Sequence[base.AST],
        *,
        separator: str = ',',
        terminator: Optional[str] = None,
        newlines: bool = True,
        **kwargs: Any
    ) -> None:
        # terminator overrides separator setting
        #
        separator = terminator if terminator is not None else separator
        size = len(items)
        for i, item in enumerate(items):
            self.visit(item, **kwargs)  # type: ignore
            if i < size - 1 or terminator is not None:
                self.write(separator)
                if newlines:
                    self.new_lines = 1
                else:
                    self.write(' ')

    def newline(self, node=None, extra=0):
        self.new_lines = max(self.new_lines, 1 + extra)
        if node is not None and self.add_line_information:
            self.write('# line: %s' % node.lineno)
            self.new_lines = 1

    @classmethod
    def to_source(
        cls,
        node: Union[base.AST, Sequence[base.AST]],
        indent_with: str = ' ' * 4,
        add_line_information: bool = False,
        pretty: bool = True,
        **kwargs: Any
    ) -> str:
        generator = cls(indent_with, add_line_information,  # type: ignore
                        pretty=pretty, **kwargs)
        generator.visit(node)
        return ''.join(generator.result)

    def indent_text(self, text: str) -> str:
        return textwrap.indent(text, self.indent_with * self.indentation)
