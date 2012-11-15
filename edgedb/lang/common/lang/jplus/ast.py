##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang.javascript import ast


class ModuleNode(ast.Base): __fields = [('body', list)]

class ImportAliasNode(ast.Base): __fields = ['name', 'asname']
class ImportNode(ast.Base): __fields = [('names', list)]
class ImportFromNode(ast.Base):
    __fields = [('level', int, 0), ('names', list), ('module', str, None)]

class ClassNode(ast.Base): __fields = ['name', ('bases', list), 'body', 'metaclass']
class DecoratedNode(ast.Base): __fields = ['node', ('decorators', list)]

class StaticDeclarationNode(ast.Base): __fields = ['decl']

class SuperNode(ast.Expression): __fields = ['cls', 'instance', 'method']
class SuperCallNode(ast.Expression): __fields = ['cls', 'instance', 'method', ('arguments', list)]

class ForeachNode(ast.Base): __fields = ['init', 'container', 'statement']

class NonlocalNode(ast.Base): __fields = [('ids', list)]

class FunctionParameter(ast.Base): __fields = ['name', 'default', ('rest', bool, False)]

class TryNode(ast.Base):
    __fields = ['body', ('handlers', list), 'jscatch', 'orelse', 'finalbody']

class ExceptNode(ast.Base):
    __fields = ['type', 'name', 'body']

class WithNode(ast.Base): __fields = [('withitems', list), 'body']
class WithItemNode(ast.Base): __fields = ['expr', 'asname']
