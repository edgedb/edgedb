##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang.javascript import ast as js_ast
from metamagic.exceptions import SemantixError

from semantix.utils.ast.transformer import NodeTransformer
from . import ast


class ScopeHead:
    def __init__(self, head):
        self.head = head


class Variable:
    def __init__(self, name, *, needs_decl=False):
        self.name = name
        self.needs_decl = needs_decl

    def __repr__(self):
        return '<Variable {}>'.format(self.name)


class Scope:
    def __init__(self, transpiler, *, node=None):
        self.children = set()
        self.scope_head = transpiler.scope_head
        self.vars = {}
        self.parent = self.scope_head.head
        self.node = node
        if self.parent and self.parent is not self:
            self.parent.children.add(self)

    def vars(self):
        return self.vars.keys()

    def add(self, var):
        assert var.name not in self.vars
        self.vars[var.name] = var

    def get(self, name):
        return self.vars.get(name)

    def is_local(self, name):
        return name in self.vars

    def __getitem__(self, name):
        try:
            return self.vars[name]
        except KeyError:
            if self.parent:
                return self.parent[name]
            raise

    def __contains__(self, name):
        try:
            self[name]
        except KeyError:
            return False
        else:
            return True

    def __enter__(self):
        self.prev_head = self.scope_head.head
        self.scope_head.head = self
        return self

    def __exit__(self, *args):
        if self.prev_head is not None:
            self.scope_head.head = self.prev_head

    def __repr__(self):
        return '<{} {!r} 0x{:x}>'.format(type(self).__name__, self.vars, id(self))


class ModuleScope(Scope):
    def list_load_global_vars(self):
        result = set()

        def _list(node):
            for var in node.vars.values():
                if var.load_global:
                    result.add(var.name)
            for child in node.children:
                _list(child)

        _list(self)
        return result


class FunctionScope(Scope):
    pass


class ClassScope(Scope):
    pass


class BuiltinsScope(Scope):
    def __init__(self, *args):
        super().__init__(*args)
        self.vars['print'] = Variable('print')


class TranspilerError(SemantixError):
    pass


class NameError(TranspilerError):
    pass


class Transpiler(NodeTransformer):
    def __init__(self):
        super().__init__()
        self.scope_head = ScopeHead(head=None)

    @property
    def scope(self):
        return self.scope_head.head

    def transpile(self, node):
        scope = BuiltinsScope(self)
        with scope:
            return self.visit(node)

    def visit(self, node):
        name = node.__class__.__name__

        module = node.__class__.__module__
        if '.javascript.' in module:
            prefix = 'js'
        else:
            prefix = 'jp'

        method_name = 'visit_{}_{}'.format(prefix, name)

        try:
            method = getattr(self, method_name)
        except AttributeError:
            if prefix == 'js':
                return self.generic_visit(node)

            raise TranspilerError('unsupported ast node: {}'.format(node)) from None

        return method(node)

    def _gen_var(self, scope):
        if scope.vars:
            vars = [js_ast.VarInitNode(
                        name=js_ast.IDNode(
                                name=v.name)) for v in scope.vars.values() if v.needs_decl]
            if vars:
                return js_ast.StatementNode(
                           statement=js_ast.VarDeclarationNode(
                               vars=vars))

    def visit_jp_ModuleNode(self, node):
        processed = [js_ast.StatementNode(
                        statement=js_ast.StringLiteralNode(
                                        value='use strict'))]

        scope = ModuleScope(self, node=node)

        with scope:
            for child in node.body:
                processed.append(self.visit(child))

        var = self._gen_var(scope)
        if var:
            processed.insert(1, var) # respect 'use strict';

        return js_ast.SourceElementsNode(
                    code=[js_ast.StatementNode(
                        statement=js_ast.CallNode(
                            call=js_ast.FunctionNode(
                                body=js_ast.StatementBlockNode(
                                    statements=processed))))])

    def visit_js_AssignmentExpressionNode(self, node):
        assert isinstance(node.left, js_ast.IDNode)
        if not self.scope.is_local(node.left.name):
            self.scope.add(Variable(node.left.name, needs_decl=True))
        node.right = self.visit(node.right)
        return node

    def visit_js_FunctionNode(self, node):
        self.scope.add(Variable(node.name))

        scope = FunctionScope(self)

        for param in node.param:
            scope.add(Variable(param.name))

        with scope:
            self.visit(node.body)

        var = self._gen_var(scope)
        if var:
            assert isinstance(node.body, js_ast.StatementBlockNode)
            node.body.statements.insert(0, var)

        return node

    def visit_jp_NonlocalNode(self, node):
        body = []
        for var in node.ids:
            name = var.name
            if name not in self.scope:
                raise NameError(name)
            self.scope.add(Variable(name))

    def visit_js_IDNode(self, node):
        if node.name not in self.scope:
            raise NameError(node.name)
        return node


