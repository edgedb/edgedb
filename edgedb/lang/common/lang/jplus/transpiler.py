##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang.javascript import ast as js_ast
from metamagic.exceptions import SemantixError

from metamagic.utils.ast.transformer import NodeTransformer
from . import ast

from semantix.utils.lang.jplus.support import base as base_js
from semantix.utils.lang.javascript import sx as sx_js

import semantix.utils.lang.javascript
__import__('semantix.utils.lang.javascript.class')
class_js = getattr(semantix.utils.lang.javascript, 'class')


class ScopeHead:
    def __init__(self, head):
        self.head = head


class Variable:
    def __init__(self, name, *, needs_decl=False, sys=False, value=None):
        self.name = name
        self.needs_decl = needs_decl
        self.sys = sys
        self.value = value

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

    def use(self, what):
        parent = self
        while not isinstance(parent, ModuleScope):
            parent = parent.parent
        return parent.use(what)


class ModuleScope(Scope):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sys_deps = {
            'class': ('__SXJSP_define_class', class_js, 'sx.define'),
            'super': ('__SXJSP_super', class_js, 'sx.parent')
        }
        self.deps = set()

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

    def use(self, what):
        self.deps.add(what)
        return self.sys_deps[what][0]


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

    def transpile(self, node, *, module='__main__'):
        scope = BuiltinsScope(self)
        scope.add(Variable('~module_name', sys=True, value=module))
        scope.add(Variable('~deps', sys=True, value=set()))
        with scope:
            js = self.visit(node)
        return js, (scope['~deps'].value | {base_js})

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
                                name=v.name)) for v in scope.vars.values()
                                                        if v.needs_decl and not v.sys]
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

        if scope.deps:
            mod_deps = set()
            for dep in scope.deps:
                dep_desc = scope.sys_deps[dep]
                processed.insert(1, js_ast.StatementNode(
                                        statement=js_ast.VarDeclarationNode(
                                            vars=[js_ast.VarInitNode(
                                                name=js_ast.IDNode(name=dep_desc[0]),
                                                value=js_ast.IDNode(name=dep_desc[2]))])))
                mod_deps.add(dep_desc[1])
            scope['~deps'].value = mod_deps


        name = self.scope['~module_name'].value

        mod_properties = []
        if scope.vars:
            names = [v.name for v in scope.vars.values() if not v.sys]
            mod_properties = [js_ast.SimplePropertyNode(
                                name=js_ast.StringLiteralNode(value=n),
                                value=js_ast.IDNode(name=n)) for n in names]

        processed.append(js_ast.ReturnNode(
                            expression=js_ast.ObjectLiteralNode(
                                properties=mod_properties)))

        return js_ast.SourceElementsNode(
                   code=[js_ast.StatementNode(
                       statement=js_ast.CallNode(
                        call=js_ast.DotExpressionNode(
                            left=js_ast.IDNode(name='$SXJSP'),
                            right=js_ast.IDNode(name='module')),
                        arguments=[
                            js_ast.StringLiteralNode(value=name),
                            js_ast.CallNode(
                                call=js_ast.FunctionNode(
                                    body=js_ast.StatementBlockNode(
                                        statements=processed)))]))])

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

    def visit_jp_ClassNode(self, node):
        modulename = self.scope['~module_name'].value
        name = node.name
        qualname = '{}.{}'.format(modulename, name)

        js_classdef = self.scope.use('class')

        self.scope.add(Variable(name, needs_decl=True))

        scope = ClassScope(self)
        scope.add(Variable('~class_name', value=name, sys=True))

        dct_items = []
        with scope:
            for child in node.body:
                if isinstance(child, js_ast.FunctionNode):
                    child = self.visit(child)
                    dct_items.append(js_ast.SimplePropertyNode(
                                        name=js_ast.StringLiteralNode(value=child.name),
                                        value=child))
                elif isinstance(child, js_ast.AssignmentExpressionNode):
                    child = self.visit(child)
                    dct_items.append(js_ast.SimplePropertyNode(
                                        name=js_ast.StringLiteralNode(value=child.left.name),
                                        value=child.right))
                else:
                    raise TranspilerError('unsupported AST node in class body: {}'.
                                          format(child))

        return js_ast.StatementNode(
                   statement=js_ast.AssignmentExpressionNode(
                       left=js_ast.IDNode(name=name),
                       op='=',
                       right=js_ast.CallNode(
                           call=js_ast.IDNode(name=js_classdef),
                           arguments=[
                               js_ast.StringLiteralNode(value=qualname),
                               js_ast.ArrayLiteralNode(array=node.bases),
                               js_ast.ObjectLiteralNode(
                                   properties=dct_items)
                           ])))


    def visit_jp_SuperCallNode(self, node):
        super_name  = self.scope.use('super')

        clsname = self.scope['~class_name'].value

        args = [
            js_ast.IDNode(name=clsname),
            js_ast.ThisNode(),
            js_ast.StringLiteralNode(value=node.method),
        ]

        if node.arguments:
            args.append(js_ast.ArrayLiteralNode(array=node.arguments))

        return js_ast.CallNode(
                   call=js_ast.IDNode(name=super_name),
                   arguments=args)

