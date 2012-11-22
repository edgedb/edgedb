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
class BaseClassMemberNode(ast.Base): __fields = [('is_static', bool, False)]
class ClassMemberNode(BaseClassMemberNode): __fields = ['name', 'value']
class ClassMethodNode(ast.FunctionNode, BaseClassMemberNode): pass

class DecoratedNode(ast.Base): __fields = ['node', ('decorators', list)]

class SuperNode(ast.Expression): __fields = ['cls', 'instance', 'method']
class SuperCallNode(ast.Expression): __fields = ['cls', 'instance', 'method', ('arguments', list)]

class NonlocalNode(ast.Base): __fields = [('ids', list)]

class TryNode(ast.Base):
    __fields = ['body', ('handlers', list), 'jscatch', 'orelse', 'finalbody']

class ExceptNode(ast.Base):
    __fields = ['type', 'name', 'body']

class WithNode(ast.Base): __fields = [('withitems', list), 'body']
class WithItemNode(ast.Base): __fields = ['expr', 'asname']

class AssertNode(ast.Base): __fields = ['test', 'failexpr']


class FunctionParameter(ast.FunctionParameter): __fields = ['name', 'default', ('type', int)]


class _ParameterKind(int):
    def __new__(self, *args, name):
        obj = int.__new__(self, *args)
        obj._name = name
        return obj

    def __str__(self):
        return self._name

    def __repr__(self):
        return '<_ParameterKind: {!r}>'.format(self._name)


POSITIONAL_ONLY = _ParameterKind(1, name='POSITIONAL_ONLY')
POSITIONAL_ONLY_DEFAULT = _ParameterKind(2, name='POSITIONAL_ONLY_DEFAULT')
VAR_POSITIONAL = _ParameterKind(3, name='VAR_POSITIONAL')
KEYWORD_ONLY = _ParameterKind(4, name='KEYWORD_ONLY')
VAR_KEYWORD = _ParameterKind(5, name='VAR_KEYWORD')


class CallNode(ast.CallNode): pass
class CallArgument(ast.Base): __fields = [('type', int), 'name', 'value']
