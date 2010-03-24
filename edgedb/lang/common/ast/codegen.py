##
# Portions Copyright (c) 2008-2010 Sprymix Inc.
# Portions Copyright (c) 2008 Armin Ronacher.
# All rights reserved.
#
# This code is licensed under the PSFL license.
##


from .visitor import NodeVisitor

class SourceGenerator(NodeVisitor):
    """This visitor is able to transform a well formed syntax tree into python
    sourcecode.  For more details have a look at the docstring of the
    `node_to_source` function.
    """

    def __init__(self, indent_with, add_line_information):
        self.result = []
        self.indent_with = indent_with
        self.add_line_information = add_line_information
        self.indentation = 0
        self.new_lines = 0

    def write(self, x):
        if self.new_lines:
            if self.result:
                self.result.append('\n' * self.new_lines)
            self.result.append(self.indent_with * self.indentation)
            self.new_lines = 0
        self.result.append(x)

    def newline(self, node=None, extra=0):
        self.new_lines = max(self.new_lines, 1 + extra)
        if node is not None and self.add_line_information:
            self.write('# line: %s' % node.lineno)
            self.new_lines = 1

    @classmethod
    def to_source(cls, node, indent_with=' '*4, add_line_information=False):
        generator = cls(indent_with, add_line_information)
        generator.visit(node)
        return ''.join(generator.result)
