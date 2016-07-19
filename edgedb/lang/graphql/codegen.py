##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common.ast import codegen


class GraphQLSourceGeneratorError(EdgeDBError):
    pass


class GraphQLSourceGenerator(codegen.SourceGenerator):
    def generic_visit(self, node):
        raise GraphQLSourceGeneratorError(
            'No method to generate code for %s' % node.__class__.__name__)

    def _visit_list(self, items, separator=None):
        for item in items:
            self.visit(item)
            if item is not items[-1] and separator:
                self.write(separator)

    def _visit_arguments(self, node):
        if node.arguments:
            self.write('(')
            self._visit_list(node.arguments, separator=', ')
            self.write(')')

    def _visit_directives(self, node):
        if node.directives:
            self.write(' ')
            self._visit_list(node.directives, separator=', ')

    def _visit_type_condition(self, node):
        if node.on:
            self.write(' on ')
            self.write(node.on)

    def visit_Document(self, node):
        self._visit_list(node.definitions)

    def visit_OperationDefinition(self, node):
        if node.type:
            self.write(node.type)
            if node.name:
                self.write(' ')
                self.write(node.name)
            if node.variables:
                self.write('(')
                self._visit_list(node.variables, separator=', ')
                self.write(')')
            self._visit_directives(node)

        self.visit(node.selection_set)

    def visit_FragmentDefinition(self, node):
        self.write('fragment ')
        self.write(node.name)
        self._visit_type_condition(node)
        self._visit_directives(node)
        self.visit(node.selection_set)

    def visit_SelectionSet(self, node):
        self.write('{')
        self.new_lines = 1
        self.indentation += 1
        self._visit_list(node.selections)
        self.indentation -= 1
        self.write('}')
        self.new_lines = 2

    def visit_Field(self, node):
        if node.alias:
            self.write(node.alias)
            self.write(': ')
        self.write(node.name)
        self._visit_arguments(node)
        self._visit_directives(node)
        if node.selection_set:
            self.visit(node.selection_set)
        else:
            self.new_lines = 1

    def visit_FragmentSpread(self, node):
        self.write('...')
        self.write(node.name)
        self._visit_directives(node)
        self.new_lines = 1

    def visit_InlineFragment(self, node):
        self.write('...')
        self._visit_type_condition(node)
        self._visit_directives(node)
        self.visit(node.selection_set)

    def visit_Argument(self, node):
        self.write(node.name)
        self.write(': ')
        self.visit(node.value)

    def visit_ObjectField(self, node):
        self.visit_Argument(node)
        self.new_lines = 1

    def visit_VariableDefinition(self, node):
        self.write(node.name)
        self.write(': ')
        self.visit(node.type)
        if node.value:
            self.write(' = ')
            self.visit(node.value)

    def visit_VariableType(self, node):
        if node.list:
            self.write('[')
            self.visit(node.name)
            self.write(']')
        else:
            self.write(node.name)

        if not node.nullable:
            self.write('!')

    def visit_Directive(self, node):
        self.write('@')
        self.write(node.name)
        self._visit_arguments(node)

    def visit_StringLiteral(self, node):
        self.write(node.tosource())

    def visit_IntegerLiteral(self, node):
        self.write(str(node.value))

    def visit_FloatLiteral(self, node):
        self.write('{:g}'.format(node.value))

    def visit_BooleanLiteral(self, node):
        if node.value:
            self.write('true')
        else:
            self.write('false')

    def visit_ListLiteral(self, node):
        self.write('[')
        self._visit_list(node.value, separator=', ')
        self.write(']')

    def visit_ObjectLiteral(self, node):
        if node.value:
            self.write('{')
            self.new_lines = 1
            self.indentation += 1
            self._visit_list(node.value)
            self.indentation -= 1
            self.write('}')
        else:
            self.write('{}')

    def visit_EnumLiteral(self, node):
        self.write(node.value)

    def visit_Variable(self, node):
        self.write(node.value)


generate_source = GraphQLSourceGenerator.to_source
