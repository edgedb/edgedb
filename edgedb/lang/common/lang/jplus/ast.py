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

class ClassDefNode(ast.Base): __fields = ['name', ('bases', list), 'body']

class StaticDeclarationNode(ast.Base): __fields = ['decl']
