##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from metamagic.utils.lang.javascript.parser.jsparser import JSParser
from metamagic.utils.lang.javascript import ast as js_ast

from metamagic.exceptions import MetamagicError

from metamagic.utils.ast import iter_fields as iter_ast_fields
from metamagic.utils.ast.visitor import NodeVisitor
from metamagic.utils.ast.transformer import NodeTransformer
from . import ast

from metamagic.utils.lang.jplus.support import builtins as builtins_js


JSBuiltins = ['NaN', 'Object', 'Function', 'undefined', 'Math', 'JSON', 'String',
              'Array', 'Infinity', 'window', 'Array', 'Boolean', 'Date',
              'RegExp',
              'decodeURI', 'decodeURIComponent', 'encodeURI', 'encodeURIComponent',
              'eval', 'isFinite', 'isNaN', 'parseInt', 'parseFloat']


_exports_cache = {}

def get_export_list(js_mod):
    global _exports_cache

    fn = js_mod.__file__

    try:
        return _exports_cache[fn]
    except KeyError:
        pass

    with open(fn, 'rt') as f:
        js = f.read()

    parser = JSParser()
    tree = parser.parse(js, filename=fn)

    class Extractor(NodeVisitor):
        def __init__(self):
            self.exports = set()

        def collect(self, ol):
            for p in ol.properties:
                assert isinstance(p, js_ast.SimplePropertyNode)
                assert isinstance(p.name, js_ast.IDNode)
                self.exports.add(p.name.name)

        def visit_CallNode(self, node):
            if isinstance(node.call, js_ast.IDNode) and node.call.name == 'EXPORTS':
                assert len(node.arguments) == 1
                ol = node.arguments[0]
                assert isinstance(ol, js_ast.ObjectLiteralNode)
                self.collect(ol)
            else:
                return self.generic_visit(node)

    ext = Extractor()
    ext.generic_visit(tree)

    exports = _exports_cache[fn] = ext.exports
    return exports


get_export_list(builtins_js)


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


class SwitchState(State):
    pass


class ForState(State):
    pass


class WhileState(State):
    pass


class Variable:
    def __init__(self, name, *, needs_decl=False,
                 aux=False, value=None, builtin=False):

        assert name
        self.name = name
        self.needs_decl = needs_decl
        self.value = value
        self.aux = aux
        self.builtin = builtin

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

    def aux_var(self, *, name='', needs_decl=True, value=None):
        assert value is None or isinstance(value, js_ast.Base)
        self.aux_var_cnt += 1
        name = '__SXJSP_aux_{}_{}'.format(name, self.aux_var_cnt)
        self.add(Variable(name, needs_decl=needs_decl, aux=True, value=value))
        return name


class ModuleScope(Scope):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.builtins_list = get_export_list(builtins_js)
        self.deps = set()

    def __getitem__(self, name):
        try:
            return super().__getitem__(name)
        except KeyError:
            pass

        if name in JSBuiltins:
            return Variable(name)

        if name in self.builtins_list:
            var = self.vars[name] = Variable(name, builtin=True,
                                       needs_decl=True,
                                       value=js_ast.DotExpressionNode(
                                            left=js_ast.IDNode(name='$SXJSP'),
                                            right=js_ast.IDNode(name=name)))
            return var

        raise KeyError(name)

    def use(self, what):
        if what not in self.builtins_list:
            raise NameError('unknown JPlus builtin requested: {}'.format(what))

        self.deps.add(what)

        if what[0] == '_':
            return '__SXJSP_builtin{}'.format(what)
        else:
            return what


class FunctionScope(Scope):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vars['arguments'] = Variable('arguments', needs_decl=False, aux=True)


class ClassScope(Scope):
    pass


class TranspilerError(MetamagicError):
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

    def check_scope_load(self, varname):
        if varname not in self.scope:
            raise NameError('unknown variable: {!r}'.format(varname))

    def check_scope_local(self, varname):
        if not self.scope.is_local(varname):
            raise NameError('variable {!r} must be local to the scope'.format(varname))

    def transpile(self, node, *, module='__main__'):
        state = ModuleState(self,
                            module_name=module,
                            deps=set())

        with state:
            js = self.visit(node)

        return js, {builtins_js}

    def visit(self, node):
        assert isinstance(node, js_ast.Base)

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
                if isinstance(node, js_ast.Expression):
                    return self.generic_visit_expression(node)
                return self.generic_visit(node)

            raise TranspilerError('unsupported ast node: {}'.format(node)) from None # pragma: no cover

        return method(node)

    def _gen_var(self, scope):
        if scope.vars:
            vars = [js_ast.VarInitNode(
                        name=js_ast.IDNode(
                            name=v.name),
                        value=v.value) for v in scope.vars.values()
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
        name = self.state(ModuleState).module_name
        scope.add(Variable(name='__SXJSP_module_name', needs_decl=True,
                           value=js_ast.StringLiteralNode(value=name), aux=True))

        with scope:
            for child in node.body:
                processed.append(self.visit(child))

        var = self._gen_var(scope)
        if var:
            processed.insert(1, var) # respect 'use strict';

        if scope.deps:
            mod_deps = set()
            for dep in scope.deps:
                if dep[0] == '_':
                    dep_jsname = '__SXJSP_builtin{}'.format(dep)
                else:
                    dep_jsname = dep

                processed.insert(1, js_ast.StatementNode(
                                        statement=js_ast.VarDeclarationNode(
                                            vars=[js_ast.VarInitNode(
                                                name=js_ast.IDNode(name=dep_jsname),
                                                value=js_ast.DotExpressionNode(
                                                    left=js_ast.IDNode(name='$SXJSP'),
                                                    right=js_ast.IDNode(name=dep)))])))

            self.state(ModuleState).deps |= mod_deps


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
                            right=js_ast.IDNode(name='_module')),
                        arguments=[
                            js_ast.StringLiteralNode(value=name),
                            js_ast.CallNode(
                                call=js_ast.FunctionNode(
                                    body=js_ast.StatementBlockNode(
                                        statements=processed)))]))])

    def visit_js_AssignmentExpressionNode(self, node):
        if isinstance(node.left, js_ast.IDNode):
            if node.op != '=':
                self.check_scope_local(node.left.name)
            else:
                if not self.scope.is_local(node.left.name):
                    self.scope.add(Variable(node.left.name, needs_decl=True))
        node.right = self.visit(node.right)
        return node

    def visit_js_FunctionNode(self, node):
        name = node.name
        if name and not self.scope.is_local(name):
            self.scope.add(Variable(name))

        scope = FunctionScope(self)

        rest = None
        defaults = collections.OrderedDict()
        new_params = []
        for param in node.param:
            scope.add(Variable(param.name, needs_decl=False))
            new_params.append(js_ast.IDNode(name=param.name))
            if param.default is not None:
                if isinstance(param.default, js_ast.IDNode):
                    self.check_scope_load(param.default.name)
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

        if name and not isinstance(self.scope, ClassScope):
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

        if defaults:
            non_def_len = len(node.param) - len(defaults) - (1 if rest else 0)
            for idx, (def_name, def_value) in enumerate(defaults.items()):
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
                                                right=def_value))))

        if rest:
            slice1_name = self.scope.use('_slice1')
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
        return node

    def visit_jp_NonlocalNode(self, node):
        body = []
        for var in node.ids:
            name = var.name
            if name not in self.scope:
                raise NameError(name)
            self.scope.add(Variable(name))

    def visit_jp_ClassNode(self, node):
        name = node.name

        js_classdef = self.scope.use('_newclass')

        self.scope.add(Variable(name, needs_decl=True))

        scope = ClassScope(self)
        state = ClassState(self, class_name=name)

        dct_items = []
        dct_static_items = []

        metaclass = js_ast.NullNode()
        if node.metaclass:
            self.check_scope_load(node.metaclass.name)
            metaclass = node.metaclass

        bases = []
        if node.bases:
            for base in node.bases:
                if isinstance(base, js_ast.IDNode):
                    self.check_scope_load(base.name)

                bases.append(self.generic_visit(base))

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
                               js_ast.BinExpressionNode(
                                    left=js_ast.IDNode(
                                        name='__SXJSP_module_name'),
                                    op='+',
                                    right=js_ast.StringLiteralNode(
                                        value='.' + name)),
                               js_ast.ArrayLiteralNode(array=bases),
                               js_ast.ObjectLiteralNode(
                                   properties=dct_items),
                               metaclass
                           ])))

    def visit_jp_SuperNode(self, node):
        super_name = self.scope.use('_super_method')

        clsname = self.state(ClassState).class_name

        assert node.cls is None
        assert node.instance is None

        return js_ast.CallNode(
            call=js_ast.IDNode(name=super_name),
            arguments=[
                js_ast.IDNode(name=clsname),
                js_ast.ThisNode(),
                js_ast.StringLiteralNode(value=node.method)
            ])

    def visit_jp_SuperCallNode(self, node):
        super_name  = self.scope.use('_super_method')

        clsname = self.state(ClassState).class_name

        assert node.cls is None
        assert node.instance is None

        super_args = [
            js_ast.IDNode(name=clsname),
            js_ast.ThisNode(),
            js_ast.StringLiteralNode(value=node.method),
        ]

        call_args = node.arguments
        call_args.insert(0, js_ast.ThisNode())

        return js_ast.DotExpressionNode(
                    left=js_ast.CallNode(
                        call=js_ast.IDNode(name=super_name),
                        arguments=super_args),
                    right=js_ast.CallNode(
                        call=js_ast.IDNode(name='call'),
                        arguments=call_args))

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

    def visit_js_ForNode(self, node):
        with ForState(self):
            return self.generic_visit(node)

    def visit_js_ForInNode(self, node):
        with ForState(self):
            assert isinstance(node.init, js_ast.IDNode)
            init_name = node.init.name
            if not self.scope.is_local(init_name):
                self.scope.add(Variable(init_name, needs_decl=True))
            return self.generic_visit(node)

    def visit_js_ForOfNode(self, node):
        on_name = self.scope.aux_var(name='forof_of', needs_decl=True)
        onlen_name = self.scope.aux_var(name='forof_of_len', needs_decl=True)
        i_name = self.scope.aux_var(name='forof_i', needs_decl=True)

        assert isinstance(node.init, js_ast.IDNode)
        init_name = node.init.name
        if not self.scope.is_local(init_name):
            self.scope.add(Variable(init_name, needs_decl=True))

        with ForState(self):
            for_container = self.visit(node.container)
            for_body = self.visit(node.statement)

        return js_ast.ForNode(
                    part1=js_ast.ExpressionListNode(
                        expressions=[
                            js_ast.AssignmentExpressionNode(
                                left=js_ast.IDNode(name=on_name),
                                op='=',
                                right=self.generic_visit(for_container)),
                            js_ast.AssignmentExpressionNode(
                                left=js_ast.IDNode(name=onlen_name),
                                op='=',
                                right=js_ast.DotExpressionNode(
                                    left=js_ast.IDNode(name=on_name),
                                    right=js_ast.IDNode(name='length'))),
                            js_ast.AssignmentExpressionNode(
                                left=js_ast.IDNode(name=i_name),
                                op='=',
                                right=js_ast.NumericLiteralNode(value=0))]),
                    part2=js_ast.ExpressionListNode(
                        expressions=[
                            js_ast.BinExpressionNode(
                                left=js_ast.IDNode(name=i_name),
                                op='<',
                                right=js_ast.IDNode(name=onlen_name)),
                            js_ast.AssignmentExpressionNode(
                                left=js_ast.IDNode(name=init_name),
                                op='=',
                                right=js_ast.SBracketExpressionNode(
                                    list=js_ast.IDNode(name=on_name),
                                    element=js_ast.IDNode(name=i_name)))]),
                    part3=js_ast.PostfixExpressionNode(
                        expression=js_ast.IDNode(name=i_name),
                        op='++'),
                    statement=for_body)

    def visit_jp_TryNode(self, node):
        try_body = self.generic_visit(node.body).statements

        if node.jscatch:
            # We have the old 'try..catch' case here, and as we don't
            # support 'except' and 'else' keywords for such 'try' blocks
            # we just return plain TryNode.

            catchid = node.jscatch.catchid
            if not self.scope.is_local(catchid):
                self.scope.add(Variable(catchid, needs_decl=False, aux=True))

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
            isinst_name = self.scope.use('_isinstance')

            catch_body = []
            first_catch = None
            prev_catch = None
            for handle in node.handlers:
                if handle.name:
                    handle_varname = handle.name.name
                    if not self.scope.is_local(handle_varname):
                        self.scope.add(Variable(handle_varname, needs_decl=True))

                handle = self.generic_visit(handle)
                handle_body = handle.body.statements

                if handle.type:
                    for handle_typename in handle.type:
                        if isinstance(handle_typename, js_ast.IDNode):
                            self.check_scope_load(handle_typename.name)

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
        validate_with_name = self.scope.use('_validate_with')

        inner_try = None
        wrap_next = None
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
                            catch=catch_node,
                            finallyblock=finally_node)

            if wrap_next is None:
                inner_try = try_node
            else:
                try_node.tryblock = wrap_next

            body.append(try_node)
            wrap_next = js_ast.StatementBlockNode(statements=body)

        # We do it here as only at this point our scope is filled
        # with 'as VAR' variables.
        inner_try.tryblock = self.generic_visit(node.body)

        return js_ast.SourceElementsNode(code=wrap_next.statements)

    def generic_visit_expression(self, node):
        for name, field in iter_ast_fields(node):
            if isinstance(field, js_ast.IDNode):
                self.check_scope_load(field.name)
            if isinstance(field, (list, tuple)):
                for sub_field in field:
                    if isinstance(sub_field, js_ast.IDNode):
                        self.check_scope_load(sub_field.name)

        return self.generic_visit(node)

    visit_js_ArrayLiteralNode = generic_visit_expression
    visit_js_StatementNode = generic_visit_expression
    visit_js_ExpressionListNode = generic_visit_expression

    def visit_js_ObjectLiteralNode(self, node):
        for p in node.properties:
            if (isinstance(p, js_ast.SimplePropertyNode)
                    and isinstance(p.value, js_ast.IDNode)):
                self.check_scope_load(p.value.name)
        return self.generic_visit(node)

    def visit_js_BinExpressionNode(self, node):
        if isinstance(node.left, js_ast.IDNode):
            self.check_scope_load(node.left.name)
        if isinstance(node.right, js_ast.IDNode):
            self.check_scope_load(node.right.name)

        if node.op in ('is', 'isnt'):
            func = self.scope.use('_' + node.op)
            left = self.visit(node.left)
            right = self.visit(node.right)
            return js_ast.CallNode(
                        call=js_ast.IDNode(name=func),
                        arguments=[left, right])
        else:
            return self.generic_visit(node)

    def visit_js_DotExpressionNode(self, node):
        if isinstance(node.left, js_ast.IDNode):
            self.check_scope_load(node.left.name)
        return self.generic_visit(node)

    def visit_js_InstanceOfNode(self, node):
        isinst = self.scope.use('_isinstance')
        if isinstance(node.type, js_ast.IDNode):
            self.check_scope_load(node.type.name)
        if isinstance(node.expression, js_ast.IDNode):
            self.check_scope_load(node.expression.name)
        return js_ast.CallNode(
                    call=js_ast.IDNode(name=isinst),
                    arguments=[self.visit(node.expression),
                              self.visit(node.type)])
