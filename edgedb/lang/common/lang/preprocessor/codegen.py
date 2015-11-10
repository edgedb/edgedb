##
# Copyright (c) 2008-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.ast import SourceGenerator
from metamagic.utils.lang.preprocessor import ast as ppast


class PP_SourceGenerator(SourceGenerator):
    def visit_list_helper(self, list, separator=', '):
        "goes through a list and visits each member printing ','"

        for i, var in enumerate(list):
            if var:
                self.visit(var)
            if i != (len(list) - 1):
                self.write(separator)

    def visit_PP_Error(self, node):
        self.write('#error')
        if node.message:
            self.write(' %r' % node.message)
        self.newline()

    def visit_PP_Warning(self, node):
        self.write('#warning')
        if node.message:
            self.write(' %r' % node.message)
        self.newline()

    def visit_PP_Include(self, node):
        self.write('#include ')
        self.write('%r' % node.package)
        self.newline()

    def visit_PP_DefineName(self, node):
        self.write('#define %s ' % node.name)
        self.visit_list_helper(node.chunks, separator=' ')
        self.newline()

    def visit_PP_DefineCallable(self, node):
        self.write('#define %s (' % node.name)
        self.visit_list_helper(node.param)
        self.write(') ')
        self.visit_list_helper(node.chunks, separator=' ')
        self.newline()

    def visit_PP_If(self, node):
        self.write('#if ')
        self.visit(node.condition)
        self.newline()
        self._visit_if_guts(node)

    def visit_PP_Ifdef(self, node):
        self.write('#ifdef %s' % node.name)
        self.newline()
        self._visit_if_guts(node)

    def visit_PP_Ifndef(self, node):
        self.write('#ifndef %s' % node.name)
        self.newline()
        self._visit_if_guts(node)

    def _visit_if_guts(self, node):
        self.indentation += 1
        self.visit(node.firstblock)
        self.indentation -= 1
        for elifblock in node.elifblocks:
            self.visit(elifblock)
        if node.elseblock:
            self.visit(node.elseblock)
        self.write('#endif')
        self.newline()


    def visit_PP_Elif(self, node):
        self.write('#elif ')
        self.visit(node.condition)
        self.newline()
        self.indentation += 1
        self.visit(node.block)
        self.indentation -= 1

    def visit_PP_Else(self, node):
        self.write('#else')
        self.newline()
        self.indentation += 1
        self.visit(node.block)
        self.indentation -= 1

    def visit_PP_Call(self, node):
        self.write('(')
        self.visit_list_helper(node.arguments, separator=' ')
        self.write(')')

    def visit_PP_CodeChunk(self, node):
        self.write(node.string)

    def visit_PP_Quote(self, node):
        self.write('#')

    def visit_PP_Concat(self, node):
        self.write('##')

    def visit_PP_Param(self, node):
        self.write(node.name)

