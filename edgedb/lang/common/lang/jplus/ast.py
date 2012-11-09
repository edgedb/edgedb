##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang.javascript import ast


class ImportAliasNode(ast.Base): __fields = ['name', 'asname']
class ImportNode(ast.Base): __fields = [('names', list)]
class ImportFromNode(ast.Base): __fields = [('level', int, 0), ('names', list), ('module', str, None)]

class ClassNode(ast.Base): __fields = ['name', ('bases', list), 'body']
class DecoratedNode(ast.Base): __fields = ['node', ('decorators', list)]

class StaticDeclarationNode(ast.Base): __fields = ['decl']

class SuperCallNode(ast.Expression): __fields = ['cls', 'instance', 'method', ('arguments', list)]

class ForOfNode(ast.Base): __fields = ['init', 'container', 'statement']
