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


from edb.common.exceptions import EdgeDBError
from edb.common.ast import codegen
from edb.edgeql import ast as qlast
from edb.edgeql import generate_source as edgeql_source
from edb.edgeql import qltypes
from edb.edgeql import codegen as edgeql_codegen
from edb.edgeql import quote as edgeql_quote


ident_to_str = edgeql_codegen.ident_to_str


def module_to_str(module):
    return '.'.join([ident_to_str(part) for part in module.split('.')])


class EdgeSchemaSourceGeneratorError(EdgeDBError):
    pass


class EdgeSchemaSourceGenerator(codegen.SourceGenerator):
    def generic_visit(self, node):
        if isinstance(node, qlast.Base):
            self._visit_edgeql(node)
        else:
            raise EdgeSchemaSourceGeneratorError(
                'No method to generate code for %s' % node.__class__.__name__)

    def _block_ws(self, change, newlines=True):
        if newlines:
            self.indentation += change
            self.new_lines = 1
        else:
            self.write(' ')

    def _visit_extends(self, names):
        self.write(' extending ')
        self.visit_list(names, newlines=False)

    def _visit_specs(self, node):
        if (getattr(node, 'attributes', None) or
                getattr(node, 'fields', None) or
                getattr(node, 'constraints', None) or
                getattr(node, 'links', None) or
                getattr(node, 'on_target_delete', None) or
                getattr(node, 'properties', None)):

            self.write(' {')
            self._block_ws(1)
            if getattr(node, 'links', None):
                self.visit_list(node.links, terminator=';')
            if getattr(node, 'properties', None):
                self.visit_list(node.properties, terminator=';')
            if getattr(node, 'attributes', None):
                self.visit_list(node.attributes, terminator=';')
            if getattr(node, 'constraints', None):
                self.visit_list(node.constraints, terminator=';')
            if getattr(node, 'indexes', None):
                self.visit_list(node.indexes, terminator=';')
            if getattr(node, 'fields', None):
                self.visit_list(node.fields, terminator=';')
            if getattr(node, 'on_target_delete', None):
                self.visit(node.on_target_delete)
                self.write(';')

            self._block_ws(-1)
            self.write('}')

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
        self.visit_list(node.declarations, terminator=';')

    def visit_Import(self, node):
        self.write('import ')
        self.visit_list(node.modules, separator=', ')

    def visit_ImportModule(self, node):
        self.write(module_to_str(node.module))
        if node.alias:
            self.write(' as ')
            self.write(ident_to_str(node.alias))

    def _visit_Declaration(self, node, after_name=None):
        decl = node.__class__.__name__.lower() \
            .replace('declaration', ' ') \
            .replace('objecttype', 'type') \
            .replace('scalartype', 'scalar type')
        self.write(decl)
        self.write(ident_to_str(node.name))
        if after_name:
            after_name(node)
        if node.extends:
            self._visit_extends(node.extends)
        self._visit_specs(node)

    def _visit_Pointer(self, node):
        quals = []
        if node.inherited:
            quals.append('inherited')
        if node.required:
            quals.append('required')
        if node.cardinality is qltypes.Cardinality.ONE:
            quals.append('single')
        elif node.cardinality is qltypes.Cardinality.MANY:
            quals.append('multi')
        if quals:
            self.write(*quals, delimiter=' ')
            self.write(' ')

        decl = node.__class__.__name__.lower()
        self.write(decl, ' ')
        self.write(ident_to_str(node.name))
        if node.extends:
            self._visit_extends(node.extends)

        if node.expr:
            self._visit_assignment(node.expr)
        elif node.target:
            self.write(' -> ')
            self.visit_list(node.target, separator=' | ', newlines=False)
            self._visit_specs(node)
        else:
            self._visit_specs(node)

    def _visit_edgeql(self, node, *, ident=True):
        self.write(edgeql_source(node))

    def _visit_assignment(self, node):
        self.write(' := ')

        if (isinstance(node, qlast.BaseConstant) and
                (not isinstance(node.value, str) or
                 '\n' not in node.value)):
            self._visit_edgeql(node, ident=False)
            self.new_lines = 1
        else:
            self.new_lines = 1
            self.indentation += 1
            self.visit(node)
            self.indentation -= 1
            self.new_lines = 2

    def visit_ScalarTypeDeclaration(self, node):
        self._visit_qualifier(node)
        self._visit_Declaration(node)

    def visit_AttributeDeclaration(self, node):
        if node.abstract:
            self.write('abstract ')
        if node.inheritable:
            self.write('inheritable ')
        self._visit_Declaration(node)

    def visit_ObjectTypeDeclaration(self, node):
        self._visit_qualifier(node)
        self._visit_Declaration(node)

    def visit_ConstraintDeclaration(self, node):
        def after_name(node):
            if node.params:
                self.write('(')
                self.visit_list(node.params, newlines=False)
                self.write(')')
            if node.subject:
                self.write(' on (')
                self.visit(node.subject)
                self.write(')')

        if node.abstract:
            self.write('abstract ')

        self._visit_Declaration(node, after_name=after_name)

    def visit_LinkDeclaration(self, node):
        if node.abstract:
            self.write('abstract ')
        self._visit_Declaration(node)

    def visit_PropertyDeclaration(self, node):
        if node.abstract:
            self.write('abstract ')
        self._visit_Declaration(node)

    def visit_ViewDeclaration(self, node):
        if node.attributes:
            self._visit_Declaration(node)
        else:
            # there's only one field expected - expr
            self.write('view ')
            self.write(node.name)
            self.write(' := ')
            self.visit(node.fields[0].value)

    def visit_FunctionDeclaration(self, node):
        self.write('function ')

        self.write(node.name)
        self.write('(')
        self.visit_list(node.params, newlines=False)
        self.write(') -> ')
        self.write(node.returning_typemod.to_edgeql(), ' ')
        self.visit(node.returning)
        self.write(' {')
        self._block_ws(1)
        self.visit_list(node.attributes, terminator=';')
        self.visit_list(node.fields, terminator=';')

        if node.function_code.from_function:
            self.write(f'from {node.function_code.language} function ')
            self.write(f'{node.function_code.from_function!r}')
        else:
            self.write(f'from {node.function_code.language} ')
            self.visit(node.function_code.code)

        self.write(';')
        self._block_ws(-1)
        self.write('}')

    def visit_FunctionCode(self, node):
        self.write(f'from {node.language.lower()}')
        if node.code:
            self.write(' :=')
            self.new_lines = 1
            self.indentation += 1
            self.visit(node.code)
            self.indentation -= 1
            self.new_lines = 1
        else:
            self.write(f' function: {node.from_function}')

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
        self._visit_Pointer(node)

    def visit_Property(self, node):
        self._visit_Pointer(node)

    def visit_Index(self, node):
        self.write('index ')
        self.visit(node.name)
        if node.expression:
            self.write(' on (')
            self._visit_edgeql(node.expression)
            self.write(')')

    def visit_Constraint(self, node):
        if node.delegated:
            self.write('delegated ')
        self.write('constraint ')
        self.visit(node.name)
        if node.args:
            self.write('(')
            self.visit_list(node.args, newlines=False)
            self.write(')')

        if node.subject:
            self.write(' on ')
            self.visit(node.subject)

        if node.attributes or node.fields:
            self.write(' {')
            self._block_ws(1)
            self.visit_list(node.attributes, terminator=';')
            self.visit_list(node.fields, terminator=';')
            self._block_ws(-1)
            self.write('}')

    def visit_Field(self, node):
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.value)

    def visit_Attribute(self, node):
        self.write('attribute ')
        self.visit(node.name)
        self.write(' := ')
        self.visit(node.value)

    def visit_OnTargetDelete(self, node):
        self.write('on target delete ', node.cascade.lower())

    def _literal_to_str(self, value):
        if isinstance(value, str):
            return edgeql_quote.quote_literal(value)
        elif isinstance(value, int):
            return str(value)
        elif isinstance(value, float):
            return '{:g}'.format(value)
        elif isinstance(value, bool):
            return 'true' if value else 'false'


generate_source = EdgeSchemaSourceGenerator.to_source
