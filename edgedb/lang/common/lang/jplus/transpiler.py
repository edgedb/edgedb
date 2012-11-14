##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from metamagic.utils.lang.javascript import ast as js_ast
from metamagic.exceptions import SemantixError

from metamagic.utils.ast.transformer import NodeTransformer
from . import ast

from metamagic.utils.lang.jplus.support import base as base_js
from metamagic.utils.lang.javascript import sx as sx_js

import metamagic.utils.lang.javascript
__import__('metamagic.utils.lang.javascript.class')
class_js = getattr(metamagic.utils.lang.javascript, 'class')


class ScopeHead:
    def __init__(self, head):
        self.head = head


class StateHead:
    def __init__(self, head):
        self.head = head


class State:
    def __init__(self, transpiler, **kwargs):
        self._state_head = transpiler.state_head
        self._children = set()
        self._parent = self._state_head.head
        if self._parent and self._parent is not self:
            self._parent._children.add(self)
        self._vars = kwargs

    def __enter__(self):
        self._prev_head = self._state_head.head
        self._state_head.head = self
        return self

    def __exit__(self, *args):
        if self._prev_head is not None:
            self._state_head.head = self._prev_head

    def __getattr__(self, attr):
        return self._vars[attr]

    def __call__(self, parent_state_class):
        assert issubclass(parent_state_class, State)
        cur = self
        while cur is not None:
            if isinstance(cur, parent_state_class):
                return cur
            cur = cur._parent


class ModuleState(State):
    pass


class ClassState(State):
    pass


class ForeachState(State):
    pass


class SwitchState(State):
    pass


class ForInState(State):
    pass


class ForState(State):
    pass


class WhileState(State):
    pass


class Variable:
    def __init__(self, name, *, needs_decl=False, aux=False, value=None):
        self.name = name
        self.needs_decl = needs_decl
        self.value = value
        self.aux = aux

    def __repr__(self): # pragma: no cover
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
        self.aux_var_cnt = 0

    def add(self, var):
        assert var.name not in self.vars
        self.vars[var.name] = var

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

    def __repr__(self): # pragma: no cover
        return '<{} {!r} 0x{:x}>'.format(type(self).__name__, self.vars.values(),
                                         id(self))

    def use(self, what):
        parent = self
        while not isinstance(parent, ModuleScope):
            parent = parent.parent
        return parent.use(what)

    def aux_var(self, *, name='', needs_decl=True):
        self.aux_var_cnt += 1
        name = '__SXJSP_aux_{}_{}'.format(name, self.aux_var_cnt)
        self.add(Variable(name, needs_decl=needs_decl, aux=True))
        return name


class ModuleScope(Scope):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sys_deps = {
            'class': ('__SXJSP_define_class', class_js, 'sx.define'),
            'super': ('__SXJSP_super', class_js, 'sx.parent'),
            'each': ('__SXJSP_each', base_js, '$SXJSP.each'),
            'isinstance': ('__SXJSP_isinstance', class_js, 'sx.isinstance'),
            'validate_with': ('__SXJSP_validate_with', base_js, '$SXJSP.validate_with'),
            'slice1': ('__SXJSP_slice1', base_js, '$SXJSP.slice1')
        }
        self.deps = set()

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
        self.state_head = StateHead(head=None)

    @property
    def scope(self):
        return self.scope_head.head

    @property
    def state(self):
        return self.state_head.head

    def transpile(self, node, *, module='__main__'):
        scope = BuiltinsScope(self)
        state = ModuleState(self,
                            module_name=module,
                            deps=set())

        with scope, state:
            js = self.visit(node)

        return js, (state.deps | {base_js})

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

            raise TranspilerError('unsupported ast node: {}'.format(node)) from None # pragma: no cover

        return method(node)

    def _gen_var(self, scope):
        if scope.vars:
            vars = [js_ast.VarInitNode(
                        name=js_ast.IDNode(
                            name=v.name)) for v in scope.vars.values()
                                                        if v.needs_decl]
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
            self.state(ModuleState).deps |= mod_deps


        name = self.state(ModuleState).module_name

        mod_properties = []
        if scope.vars:
            names = [v.name for v in scope.vars.values() if not v.aux]
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
        if (isinstance(node.left, js_ast.IDNode) and not self.scope.is_local(node.left.name)):
            if node.op != '=':
                raise NameError(node.left.name)
            self.scope.add(Variable(node.left.name, needs_decl=True))
        node.right = self.visit(node.right)
        return node

    def visit_js_FunctionNode(self, node):
        name = node.name
        if not self.scope.is_local(name):
            self.scope.add(Variable(name))

        scope = FunctionScope(self)

        rest = None
        defaults = collections.OrderedDict()
        new_params = []
        for param in node.param:
            scope.add(Variable(param.name))
            new_params.append(js_ast.IDNode(name=param.name))
            if param.default is not None:
                param.default = self.visit(param.default)
                defaults[param.name] = param.default
            if param.rest:
                rest = param

        node.param = new_params

        with scope:
            self.visit(node.body)

        var = self._gen_var(scope)
        if var:
            assert isinstance(node.body, js_ast.StatementBlockNode)
            node.body.statements.insert(0, var)

        if not defaults and not rest:
            return node

        # We have function parameters with default values.
        #
        # Transformation example:
        #
        # function spam(a, b=10) {
        #     return a + b;
        # }
        #
        # would be transformed to:
        #
        # spam = (function() {
        #     var __sxjsp_def_b = 10;
        #     return function spam(a, b) {
        #         var __sx_jsp_arglen = arguments.length;
        #         (__sx_jsp_arglen < 2) && (b = __sxjsp_def_b);
        #         return a + b;
        #     }
        # }
        # })();

        if not isinstance(self.scope, ClassScope):
            self.scope[name].needs_decl = True

        arglen_var = self.scope.aux_var(name='argslen', needs_decl=False)
        defaults_init = [
            js_ast.StatementNode(
                statement=js_ast.VarDeclarationNode(
                    vars=[js_ast.VarInitNode(
                        name=js_ast.IDNode(
                            name=arglen_var),
                        value=js_ast.IDNode(
                            name='arguments.length'))]))]

        to_wrap = False
        if defaults:
            to_wrap = True

            wrapper_body = []

            wrapper_body.append(js_ast.StatementNode(
                            statement=js_ast.VarDeclarationNode(
                                vars=[js_ast.VarInitNode(
                                    name=js_ast.IDNode(
                                        name='__SXJSP_aux_default_{}'.format(name)),
                                    value=value) for name, value in defaults.items()])))

            wrapper_body.append(js_ast.StatementNode(
                            statement=js_ast.ReturnNode(
                                expression=node)))

            non_def_len = len(node.param) - len(defaults) - (1 if rest else 0)
            for idx, def_name in enumerate(defaults.keys()):
                defaults_init.append(js_ast.StatementNode(
                                        statement=js_ast.BinExpressionNode(
                                            left=js_ast.BinExpressionNode(
                                                left=js_ast.IDNode(
                                                    name=arglen_var),
                                                op = '<',
                                                right=js_ast.NumericLiteralNode(
                                                    value=non_def_len + idx + 1)),
                                            op='&&',
                                            right=js_ast.AssignmentExpressionNode(
                                                left=js_ast.IDNode(
                                                    name=def_name),
                                                op='=',
                                                right=js_ast.IDNode(
                                                    name='__SXJSP_aux_default_{}'.format(def_name))))))

        if rest:
            slice1_name = self.scope.use('slice1')
            defaults_init.append(js_ast.StatementNode(
                                    statement=js_ast.AssignmentExpressionNode(
                                        left=js_ast.IDNode(
                                            name=rest.name),
                                            op='=',
                                            right=js_ast.CallNode(
                                                call=js_ast.IDNode(name=slice1_name),
                                                arguments=[
                                                    js_ast.IDNode(name='arguments'),
                                                    js_ast.NumericLiteralNode(
                                                        value=len(node.param)-1)]))))


        func_body = node.body.statements
        func_body.insert(0, js_ast.SourceElementsNode(code=defaults_init))

        if to_wrap:
            node = js_ast.AssignmentExpressionNode(
                       left=js_ast.IDNode(name=name),
                       op='=',
                       right=js_ast.StatementNode(
                           statement=js_ast.CallNode(
                               call=js_ast.FunctionNode(
                                   isdeclaration=False,
                                   body=js_ast.StatementBlockNode(
                                       statements=wrapper_body)))))

        return node

    def visit_jp_NonlocalNode(self, node):
        body = []
        for var in node.ids:
            name = var.name
            if name not in self.scope:
                raise NameError(name)
            self.scope.add(Variable(name))

    def visit_jp_ClassNode(self, node):
        modulename = self.state(ModuleState).module_name
        name = node.name
        qualname = '{}.{}'.format(modulename, name)

        js_classdef = self.scope.use('class')

        self.scope.add(Variable(name, needs_decl=True))

        scope = ClassScope(self)
        state = ClassState(self, class_name=name)

        dct_items = []
        dct_static_items = []

        with scope, state:
            for child in node.body:
                collection = dct_items

                if isinstance(child, js_ast.StatementNode):
                    child = self.visit(child.statement)

                if isinstance(child, ast.StaticDeclarationNode):
                    child = child.decl
                    collection = dct_static_items

                child = self.visit(child)

                if isinstance(child, js_ast.FunctionNode):
                    collection.append(js_ast.SimplePropertyNode(
                                        name=js_ast.StringLiteralNode(value=child.name),
                                        value=child))
                elif isinstance(child, js_ast.AssignmentExpressionNode):
                    collection.append(js_ast.SimplePropertyNode(
                                        name=js_ast.StringLiteralNode(value=child.left.name),
                                        value=child.right))
                else:
                    raise TranspilerError('unsupported AST node in class body: {}'.
                                          format(child))

        if dct_static_items:
            dct_items.append(js_ast.SimplePropertyNode(
                name=js_ast.StringLiteralNode(value='statics'),
                value=js_ast.ObjectLiteralNode(
                    properties=dct_static_items)))

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

        clsname = self.state(ClassState).class_name

        assert node.cls is None
        assert node.instance is None

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

    def visit_jp_DecoratedNode(self, node):
        is_static = False

        wrapped = node.node
        if isinstance(wrapped, ast.StaticDeclarationNode):
            is_static = True
            wrapped = wrapped.decl

        if not isinstance(wrapped, (js_ast.FunctionNode, ast.ClassNode)):
            raise TranspilerError('unsupported decorated node (only funcs & classes expected): {}'.
                                  format(wrapped))

        is_class = isinstance(wrapped, ast.ClassNode)
        name = wrapped.name

        wrapped = self.visit(wrapped)

        if is_class:
            assert not is_static
            class_shell = wrapped
            wrapped = wrapped.statement.right

        for dec in reversed(node.decorators):
            wrapped = js_ast.CallNode(
                          call=dec,
                          arguments=[wrapped])

        if not isinstance(self.scope, ClassScope):
            self.scope[name].needs_decl = True

        if is_class:
            class_shell.statement.right = wrapped
            return class_shell

        func = js_ast.AssignmentExpressionNode(
                  left=js_ast.IDNode(name=name),
                  op='=',
                  right=wrapped)

        if is_static:
            func = ast.StaticDeclarationNode(decl=func)

        return func

    def visit_js_ContinueNode(self, node):
        if isinstance(self.state, ForeachState):
            return js_ast.ReturnNode()
        return node

    def visit_js_BreakNode(self, node):
        if isinstance(self.state, ForeachState):
            return js_ast.ReturnNode(expression=js_ast.BooleanLiteralNode(value=True))
        return node

    def visit_js_ForNode(self, node):
        with ForState(self):
            return self.generic_visit(node)

    def visit_js_ForInNode(self, node):
        with ForInState(self):
            assert isinstance(node.init, js_ast.IDNode)
            init_name = node.init.name
            if not self.scope.is_local(init_name):
                self.scope.add(Variable(init_name, needs_decl=True))
            return self.generic_visit(node)

    def visit_jp_ForeachNode(self, node):
        each_name  = self.scope.use('each')

        on = self.visit(node.container)

        state = ForeachState(self)
        with state:
            statement = self.visit(node.statement)

        return js_ast.StatementNode(
                   statement=js_ast.CallNode(
                       call=js_ast.IDNode(
                           name=each_name),
                       arguments=[
                           js_ast.NumericLiteralNode(
                                value=len(node.init)),
                           on,
                           js_ast.FunctionNode(
                               param=node.init,
                               body=statement),
                           js_ast.ThisNode()
                       ]))

    def visit_jp_TryNode(self, node):
        try_body = self.generic_visit(node.body).statements

        if node.jscatch:
            # We have the old 'try..catch' case here, and as we don't
            # support 'except' and 'else' keywords for such 'try' blocks
            # we just return plain TryNode.
            jscatch = self.generic_visit(node.jscatch)

            finally_body = []
            if node.finalbody:
                finally_body = self.generic_visit(node.finalbody).statements

            return js_ast.TryNode(
                        tryblock=js_ast.StatementBlockNode(
                            statements=try_body),
                        finallyblock=(js_ast.StatementBlockNode(
                            statements=finally_body) if finally_body else None),
                        catch=jscatch)

        catch_all_name = self.scope.aux_var(name='all_ex', needs_decl=False)
        catch_node = None
        if node.handlers:
            isinst_name = self.scope.use('isinstance')

            catch_body = []
            first_catch = None
            prev_catch = None
            for handle in node.handlers:
                handle = self.generic_visit(handle)
                handle_body = handle.body.statements

                if handle.type:
                    if handle.name:
                        handle_varname = handle.name.name
                        if not self.scope.is_local(handle_varname):
                            self.scope.add(Variable(handle_varname, needs_decl=True))

                        handle_body.insert(0, js_ast.StatementNode(
                                                statement=js_ast.AssignmentExpressionNode(
                                                    left=js_ast.IDNode(
                                                        name=handle_varname),
                                                    op='=',
                                                    right=js_ast.IDNode(
                                                        name=catch_all_name))))

                    check_node = js_ast.CallNode(
                                    call=js_ast.IDNode(name=isinst_name),
                                    arguments=[
                                        js_ast.IDNode(name=catch_all_name),

                                        (js_ast.ArrayLiteralNode(array=handle.type)
                                            if len(handle.type) > 1 else
                                        handle.type[0])
                                    ])
                else:
                    check_node = js_ast.BooleanLiteralNode(value=True)

                catch_node = js_ast.IfNode(
                                    ifclause=check_node,
                                    thenclause=js_ast.StatementBlockNode(
                                        statements=handle_body))

                if first_catch is None:
                    prev_catch = first_catch = catch_node
                else:
                    prev_catch.elseclause = js_ast.StatementBlockNode(
                                                statements=[catch_node])
                    prev_catch = catch_node

            prev_catch.elseclause = js_ast.StatementBlockNode(
                                        statements=[js_ast.ThrowNode(
                                            expression=js_ast.IDNode(
                                                name=catch_all_name))])


            catch_node = js_ast.CatchNode(
                            catchid=catch_all_name,
                            catchblock=js_ast.StatementBlockNode(
                                statements=[first_catch]))

        if node.orelse:
            orelse_name = self.scope.aux_var(name='orelse', needs_decl=True)
            try_body.append(js_ast.StatementNode(
                                statement=js_ast.AssignmentExpressionNode(
                                    left=js_ast.IDNode(
                                        name=orelse_name),
                                    op='=',
                                    right=js_ast.BooleanLiteralNode(value=True))))

            orelse_body = self.generic_visit(node.orelse).statements

            if node.finalbody:
                finally_body = self.generic_visit(node.finalbody).statements
                finally_body = [js_ast.TryNode(
                                    tryblock=js_ast.StatementBlockNode(
                                        statements=[
                                            js_ast.IfNode(
                                                ifclause=js_ast.IDNode(
                                                    name=orelse_name),
                                                thenclause=js_ast.StatementBlockNode(
                                                    statements=orelse_body))]),
                                    finallyblock=js_ast.StatementBlockNode(
                                        statements=finally_body))]
            else:
                finally_body = orelse_body
        else:
            finally_body = []
            if node.finalbody:
                finally_body = self.generic_visit(node.finalbody).statements

        try_node = js_ast.TryNode(
                        tryblock=js_ast.StatementBlockNode(
                            statements=try_body),
                        finallyblock=(js_ast.StatementBlockNode(
                            statements=finally_body) if finally_body else None),
                        catch=catch_node)

        return try_node

    def visit_js_SwitchNode(self, node):
        state = SwitchState(self)
        with state:
            return self.generic_visit(node)

    def visit_js_DoNode(self, node):
        with WhileState(self):
            return self.generic_visit(node)

    def visit_js_WhileNode(self, node):
        with WhileState(self):
            return self.generic_visit(node)

    def visit_jp_WithNode(self, node):
        validate_with_name = self.scope.use('validate_with')

        wrap_next = self.generic_visit(node.body)
        for withitem in reversed(node.withitems):
            body = []
            expr = self.generic_visit(withitem.expr)

            with_name = self.scope.aux_var(name='with', needs_decl=True)
            with_we_name = self.scope.aux_var(name='with_was_exc', needs_decl=True)
            body.append(js_ast.StatementNode(
                            statement=js_ast.AssignmentExpressionNode(
                                left=js_ast.IDNode(name=with_name),
                                op='=',
                                right=expr)))
            body.append(js_ast.StatementNode(
                            statement=js_ast.CallNode(
                                call=js_ast.IDNode(name=validate_with_name),
                                arguments=[js_ast.IDNode(name=with_name)])))

            with_call = js_ast.CallNode(
                            call=js_ast.DotExpressionNode(
                                left=js_ast.IDNode(name=with_name),
                                right=js_ast.IDNode(name='enter')))

            if withitem.asname:
                asname = withitem.asname
                if not self.scope.is_local(asname):
                    self.scope.add(Variable(asname, needs_decl=True))

                body.append(js_ast.StatementNode(
                                statement=js_ast.AssignmentExpressionNode(
                                    left=js_ast.IDNode(name=asname),
                                    op='=',
                                    right=with_call)))
            else:
                body.append(js_ast.StatementNode(
                                statement=with_call))

            body.append(js_ast.StatementNode(
                            statement=js_ast.AssignmentExpressionNode(
                                left=js_ast.IDNode(name=with_we_name),
                                op='=',
                                right=js_ast.BooleanLiteralNode(value=False))))

            catch_all_var = self.scope.aux_var(name='with_exc', needs_decl=False)
            catch_node = js_ast.CatchNode(
                            catchid=catch_all_var,
                            catchblock=js_ast.StatementBlockNode(
                                statements=[
                                    js_ast.StatementNode(
                                        statement=js_ast.AssignmentExpressionNode(
                                            left=js_ast.IDNode(name=with_we_name),
                                            op='=',
                                            right=js_ast.BooleanLiteralNode(value=True))),

                                    js_ast.IfNode(
                                        ifclause=js_ast.BinExpressionNode(
                                            left=js_ast.CallNode(
                                                call=js_ast.DotExpressionNode(
                                                    left=js_ast.IDNode(
                                                        name=with_name),
                                                    right=js_ast.IDNode(
                                                        name='exit')),
                                                arguments=[js_ast.IDNode(name=catch_all_var)]),
                                            op='!==',
                                            right=js_ast.BooleanLiteralNode(value=True)),
                                        thenclause=js_ast.StatementBlockNode(
                                            statements=[js_ast.ThrowNode(
                                                expression=js_ast.IDNode(
                                                    name=catch_all_var))]))]))

            finally_node = js_ast.StatementBlockNode(
                                statements=[js_ast.IfNode(
                                    ifclause=js_ast.PrefixExpressionNode(
                                        expression=js_ast.IDNode(
                                            name=with_we_name),
                                        op='!'),
                                    thenclause=js_ast.StatementBlockNode(
                                        statements=[js_ast.StatementNode(
                                            statement=js_ast.CallNode(
                                                call=js_ast.DotExpressionNode(
                                                    left=js_ast.IDNode(
                                                        name=with_name),
                                                    right=js_ast.IDNode(
                                                        name='exit'))))]))])

            try_node = js_ast.TryNode(
                            tryblock=wrap_next,
                            catch=catch_node,
                            finallyblock=finally_node)

            body.append(try_node)
            wrap_next = js_ast.StatementBlockNode(statements=body)

        return js_ast.SourceElementsNode(code=wrap_next.statements)
