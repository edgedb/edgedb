##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import textwrap

from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common.ast import codegen
from edgedb.lang.edgeql import (generate_source as edgeql_source,
                                ast as eqlast)


class EdgeSchemaSourceGeneratorError(EdgeDBError):
    pass


class EdgeSchemaSourceGenerator(codegen.SourceGenerator):
    def generic_visit(self, node):
        if isinstance(node, eqlast.Base):
            pad = self.indent_with * self.indentation
            ind = self.indentation
            self.indentation = 0
            self.write(textwrap.indent(edgeql_source(node), pad))
            self.indentation = ind
        else:
            raise EdgeSchemaSourceGeneratorError(
                'No method to generate code for %s' % node.__class__.__name__)

    def _visit_extends(self, names):
        self.write(' extends ')
        for qname in names[:-1]:
            self.visit(qname)
            self.write(', ')
        self.visit(names[-1])

    def _visit_specs(self, node):
        if (hasattr(node, 'attributes') and node.attributes
                or hasattr(node, 'constraints') and node.constraints
                or hasattr(node, 'links') and node.links
                or hasattr(node, 'properties') and node.properties):
            self.write(':')
            self.new_lines = 1
            self.indentation += 1
            if hasattr(node, 'links'):
                self._visit_list(node.links)
            if hasattr(node, 'properties'):
                self._visit_list(node.properties)
            if hasattr(node, 'attributes'):
                self._visit_list(node.attributes)
            if hasattr(node, 'constraints'):
                self._visit_list(node.constraints)
            if hasattr(node, 'policies'):
                self._visit_list(node.policies)
            if hasattr(node, 'indexes'):
                self._visit_list(node.indexes)
            self.indentation -= 1
        self.new_lines = 2

    def _visit_list(self, items, separator=None):
        for item in items:
            self.visit(item)
            if separator and item is not items[-1]:
                self.write(separator)

    def _visit_qualifier(self, node):
        if node.abstract:
            self.write('abstract ')
        elif node.final:
            self.write('final ')

    def visit_Schema(self, node):
        for decl in node.declarations:
            self.visit(decl)

    def visit_Import(self, node):
        self.write('import ')
        self._visit_list(node.modules, separator=', ')
        self.new_lines = 1

    def visit_ImportModule(self, node):
        self.write(node.module)
        if node.alias:
            self.write(' as ')
            self.write(node.alias)

    def _visit_Declaration(self, node):
        self._visit_qualifier(node)
        self.write(node.__class__.__name__.lower().replace('declaration', ' '))
        self.write(node.name)
        if node.extends:
            self._visit_extends(node.extends)
        self._visit_specs(node)

    def _visit_Specialization(self, node):
        if node.required:
            self.write('required ')
        self.write(node.__class__.__name__.lower() + ' ')
        self.visit(node.name)

        if isinstance(node.target, eqlast.Base):
            self._visit_turnstile(node.target)
        elif node.target:
            self.write(' -> ')
            if isinstance(node.target, list):
                for qname in node.target[:-1]:
                    self.visit(qname)
                    self.write(', ')
                self.visit(node.target[-1])
            else:
                self.visit(node.target)

            self._visit_specs(node)
        else:
            self._visit_specs(node)

    def _visit_turnstile(self, node):
        self.write(':=')
        self.new_lines = 1
        self.indentation += 1
        self.visit(node)
        self.indentation -= 1
        self.new_lines = 2

    def visit_ActionDeclaration(self, node):
        self._visit_Declaration(node)

    def visit_AtomDeclaration(self, node):
        self._visit_Declaration(node)

    def visit_ConceptDeclaration(self, node):
        self._visit_Declaration(node)

    def visit_ConstraintDeclaration(self, node):
        self._visit_Declaration(node)

    def visit_EventDeclaration(self, node):
        self._visit_Declaration(node)

    def visit_LinkDeclaration(self, node):
        self._visit_Declaration(node)

    def visit_LinkPropertyDeclaration(self, node):
        self._visit_Declaration(node)

    def visit_ObjectName(self, node):
        if node.module:
            self.write(node.module)
            self.write('::')
        self.write(node.name)

    def visit_NamespaceExpression(self, node):
        self.visit(node.left)
        self.write('::')
        self.visit(node.right)

    def visit_Link(self, node):
        self._visit_Specialization(node)

    def visit_LinkProperty(self, node):
        self._visit_Specialization(node)

    def visit_Policy(self, node):
        self.write('on ')
        self.visit(node.event)
        self.write(' ')
        self.visit(node.action)
        self.new_lines = 1

    def visit_Index(self, node):
        self.write('index ')
        self.visit(node.name)
        self._visit_turnstile(node.expression)

    def visit_Constraint(self, node):
        self.write('constraint ')
        if node.value is not None:
            self.visit_Attribute(node)
        else:
            self.visit(node.name)
            self.new_lines = 2

    def visit_Attribute(self, node):
        self.visit(node.name)
        if isinstance(node.value, eqlast.Base):
            self._visit_turnstile(node.value)
        else:
            self.write(': ')
            self.visit(node.value)
            self.new_lines = 1

    def visit_StringLiteral(self, node):
        self.write('{!r}'.format(node.value))

    def visit_MappingLiteral(self, node):
        self.write(node.value)

    def visit_IntegerLiteral(self, node):
        self.write(str(node.value))

    def visit_FloatLiteral(self, node):
        self.write('{:g}'.format(node.value))

    def visit_BooleanLiteral(self, node):
        if node.value:
            self.write('true')
        else:
            self.write('false')


generate_source = EdgeSchemaSourceGenerator.to_source
