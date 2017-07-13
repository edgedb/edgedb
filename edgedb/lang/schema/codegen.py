##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import textwrap

from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common.ast import codegen
from edgedb.lang.edgeql import (generate_source as edgeql_source,
                                ast as eqlast)
from . import ast as esast
from . import quote as eschema_quote


def ident_to_str(ident):
    return eschema_quote.disambiguate_identifier(ident)


def module_to_str(module):
    return '.'.join([ident_to_str(part) for part in module.split('.')])


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
        if (hasattr(node, 'attributes') and node.attributes or
                hasattr(node, 'constraints') and node.constraints or
                hasattr(node, 'links') and node.links or
                hasattr(node, 'properties') and node.properties):
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
        self.write(module_to_str(node.module))
        if node.alias:
            self.write(' as ')
            self.write(ident_to_str(node.alias))

    def _visit_Declaration(self, node):
        self._visit_qualifier(node)
        self.write(node.__class__.__name__.lower().replace('declaration', ' '))
        self.write(ident_to_str(node.name))
        if node.args:
            self.write('(')
            self.visit_list(node.args, newlines=False)
            self.write(')')

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
            self.write(' to ')
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
        self.write(' := ')
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

    def visit_ViewDeclaration(self, node):
        self._visit_Declaration(node)

    def visit_FunctionDeclaration(self, node):
        if node.aggregate:
            self.write('aggregate ')
        else:
            self.write('function ')

        self.write(node.name)
        self.write('(')
        self.visit_list(node.args, newlines=False)
        self.write(') -> ')
        if node.set_returning:
            self.write('set of ')
        self.visit(node.returning)
        self.write(':')
        self.new_lines = 1
        self.indentation += 1
        if node.initial_value:
            self.write('initial value: ')
            self.visit(node.initial_value)
            self.new_lines = 1
        self._visit_list(node.attributes)
        self.visit(node.code)
        self.indentation -= 1
        self.new_lines = 2

    def visit_FuncArg(self, node):
        if node.variadic:
            self.write('*')
        if node.name is not None:
            self.write(ident_to_str(node.name), ': ')
        self.visit(node.type)

        if node.default:
            self.write(' = ')
            self.visit(node.default)

    def visit_FunctionCode(self, node):
        self.write(f'from {node.language}')
        if node.code:
            self.write(':>')
            self.new_lines = 1
            self.indentation += 1
            self.write(node.code)
            self.indentation -= 1
            self.new_lines = 1
        else:
            self.write(f' function: {node.from_name}')

    def visit_ObjectName(self, node):
        if node.module:
            self.write(module_to_str(node.module))
            self.write('::')
        self.write(ident_to_str(node.name))
        if node.subtypes:
            self.write('<')
            self._visit_list(node.subtypes, separator=', ')
            self.write('>')

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
        if node.abstract:
            self.write('abstract ')
        self.write('constraint ')
        self.visit(node.name)
        if node.args:
            self.write('(')
            self.visit_list(node.args, newlines=False)
            self.write(')')

        if node.attributes:
            self.write(':')
            self.new_lines = 1
        if node.attributes:
            self.new_lines = 1
            self.indentation += 1
            self._visit_list(node.attributes)
            self.indentation -= 1

        self.new_lines = 2

    def visit_Attribute(self, node):
        self.visit(node.name)
        if isinstance(node.value, eqlast.Base):
            self._visit_turnstile(node.value)
        else:
            if isinstance(node.value, esast.RawLiteral):
                self.write(':>')
            else:
                self.write(': ')
            self.visit(node.value)
            self.new_lines = 1

    def visit_StringLiteral(self, node):
        self.write(self._literal_to_str(node.value))

    def visit_MappingLiteral(self, node):
        self.write(node.value)

    def visit_IntegerLiteral(self, node):
        self.write(self._literal_to_str(node.value))

    def visit_FloatLiteral(self, node):
        self.write(self._literal_to_str(node.value))

    def _literal_to_str(self, value):
        if isinstance(value, str):
            return eschema_quote.quote_literal(value)
        elif isinstance(value, int):
            return str(value)
        elif isinstance(value, float):
            return '{:g}'.format(value)
        elif isinstance(value, bool):
            return 'true' if value else 'false'

    def visit_BooleanLiteral(self, node):
        self.write(self._literal_to_str(node.value))

    def visit_ArrayLiteral(self, node):
        self.write('[')
        val = [self._literal_to_str(el) for el in node.value]
        self.write(', '.join(val))
        self.write(']')

    def visit_RawLiteral(self, node):
        self.new_lines = 1
        self.indentation += 1
        self.write(node.value)
        self.indentation -= 1
        self.new_lines = 1


generate_source = EdgeSchemaSourceGenerator.to_source
