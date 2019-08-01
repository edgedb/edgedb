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


import json
from edb.common.ast import codegen


class GraphQLSourceGenerator(codegen.SourceGenerator):
    def generic_visit(self, node):
        raise RuntimeError(
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
        if node.type_condition:
            self.write(' on ')
            self.visit(node.type_condition)

    def visit_Name(self, node):
        self.write(node.value)

    def visit_Document(self, node):
        self._visit_list(node.definitions)

    def visit_OperationDefinition(self, node):
        if node.operation:
            self.write(node.operation)
            if node.name:
                self.write(' ')
                self.visit(node.name)
            if node.variable_definitions:
                self.write('(')
                self._visit_list(node.variable_definitions, separator=', ')
                self.write(')')
            self._visit_directives(node)

        self.visit(node.selection_set)

    def visit_FragmentDefinition(self, node):
        self.write('fragment ')
        self.visit(node.name)
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
            self.visit(node.alias)
            self.write(': ')
        self.visit(node.name)
        self._visit_arguments(node)
        self._visit_directives(node)
        if node.selection_set:
            self.visit(node.selection_set)
        else:
            self.new_lines = 1

    def visit_FragmentSpread(self, node):
        self.write('...')
        self.visit(node.name)
        self._visit_directives(node)
        self.new_lines = 1

    def visit_InlineFragment(self, node):
        self.write('...')
        self._visit_type_condition(node)
        self._visit_directives(node)
        self.visit(node.selection_set)

    def visit_Argument(self, node):
        self.visit(node.name)
        self.write(': ')
        self.visit(node.value)

    def visit_ObjectField(self, node):
        self.visit_Argument(node)
        self.new_lines = 1

    def visit_VariableDefinition(self, node):
        self.visit(node.variable)
        self.write(': ')
        self.visit(node.type)
        if node.default_value:
            self.write(' = ')
            self.visit(node.default_value)

    def visit_Directive(self, node):
        self.write('@')
        self.visit(node.name)
        self._visit_arguments(node)

    def visit_StringValue(self, node):
        # the GQL string works same as JSON string
        self.write(json.dumps(node.value))

    def visit_IntValue(self, node):
        self.write(node.value)

    def visit_FloatValue(self, node):
        self.write(node.value)

    def visit_BooleanValue(self, node):
        if node.value:
            self.write('true')
        else:
            self.write('false')

    def visit_ListValue(self, node):
        self.write('[')
        self._visit_list(node.values, separator=', ')
        self.write(']')

    def visit_ObjectValue(self, node):
        if node.fields:
            self.write('{')
            self.new_lines = 1
            self.indentation += 1
            self._visit_list(node.fields)
            self.indentation -= 1
            self.write('}')
        else:
            self.write('{}')

    def visit_EnumValue(self, node):
        self.write(node.value)

    def visit_NullValue(self, node):
        self.write('null')

    def visit_Variable(self, node):
        self.write('$')
        self.visit(node.name)

    def visit_NamedType(self, node):
        self.visit(node.name)

    def visit_ListType(self, node):
        self.write('[')
        self.visit(node.type)
        self.write(']')

    def visit_NonNullType(self, node):
        self.visit(node.type)
        self.write('!')


generate_source = GraphQLSourceGenerator.to_source
