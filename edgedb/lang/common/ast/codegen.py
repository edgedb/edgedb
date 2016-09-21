##
# Portions Copyright (c) 2008-2010, 2016 MagicStack Inc.
# Portions Copyright (c) 2008 Armin Ronacher.
# All rights reserved.
#
# This code is licensed under the PSFL license.
##

import itertools

from .visitor import NodeVisitor


class SourceGenerator(NodeVisitor):
    """Generate source code from an AST tree."""

    def __init__(
            self, indent_with=' ' * 4, add_line_information=False,
            pretty=True):
        self.result = []
        self.indent_with = indent_with
        self.add_line_information = add_line_information
        self.indentation = 0
        self.new_lines = 0
        self.current_line = 1
        self.pretty = pretty

    def write(self, *x, delimiter=None):
        if self.new_lines:
            if self.result and self.pretty:
                self.current_line += self.new_lines
                self.result.append('\n' * self.new_lines)
            if self.pretty:
                self.result.append(self.indent_with * self.indentation)
            else:
                self.result.append(' ')
            self.new_lines = 0
        if delimiter:
            self.result.append(x[0])
            chain = itertools.chain.from_iterable
            chunks = chain((delimiter, v) for v in x[1:])
        else:
            chunks = x

        for chunk in chunks:
            if not isinstance(chunk, str):
                raise ValueError(
                    'invalid text chunk in codegen: %r'.format(chunk))
            self.result.append(chunk)

    def visit_list(
            self, items, *,
            separator=',', terminator=None, newlines=True, **kwargs):
        # terminator overrides separator setting
        #
        separator = terminator if terminator is not None else separator
        size = len(items)
        for i, item in enumerate(items):
            self.visit(item, **kwargs)
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
            cls, node, indent_with=' ' * 4, add_line_information=False,
            pretty=True):
        generator = cls(indent_with, add_line_information, pretty=pretty)
        generator.visit(node)
        return ''.join(generator.result)
