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

from metamagic.utils import config
from metamagic.utils.ast import iter_fields as iter_ast_fields
from metamagic.utils.ast.visitor import NodeVisitor
from metamagic.utils.ast.transformer import NodeTransformer
from . import ast

from metamagic.utils.lang.jplus.support import builtins as builtins_js

from metamagic.utils.lang.import_ import utils as import_utils


JSBuiltins = ['NaN', 'Object', 'Function', 'undefined', 'Math', 'JSON', 'String',
              'Array', 'Infinity', 'window', 'Array', 'Boolean', 'Date', 'Number',
              'RegExp', 'Error',
              'decodeURI', 'decodeURIComponent', 'encodeURI', 'encodeURIComponent',
              'eval', 'isFinite', 'isNaN', 'parseInt', 'parseFloat', 'TypeError']


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

    def within(self, state_class):
        cur = self
        while cur is not None:
            if isinstance(cur, state_class):
                return True
            cur = cur._parent
        return False

    def distance(self, parent_state_class):
        assert issubclass(parent_state_class, State)
        idx = 0
        cur = self
        while cur is not None:
            if isinstance(cur, parent_state_class):
                return idx
            cur = cur._parent
            idx += 1
        return 100000000


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


class JSWithState(State):
    pass


class ArrowFuncState(State):
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
        name = '__jp_{}_{}'.format(name, self.aux_var_cnt)
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
            return '__jp_bltn{}'.format(what)
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


class Transpiler(NodeTransformer, metaclass=config.ConfigurableMeta):
    debug = config.cvalue(True, type=bool)

    def __init__(self, *, debug=None):
        super().__init__()
        self.scope_head = ScopeHead(head=None)
        self.state_head = StateHead(head=None)

        # In debug mode code will:
        # - Ensure arguments correctness (more args than needed, no undefineds etc)
        if debug is None:
            self.debug = self.__class__.debug
        else:
            self.debug = debug

        self._kwargs_marker = '__jpkw' # NOTE! Same name in builtins.js.

        self.modlist = collections.OrderedDict()

    @property
    def scope(self):
        return self.scope_head.head

    @property
    def state(self):
        return self.state_head.head

    def check_scope_load(self, varname):
        if self.state.within(JSWithState):
            return
        if varname not in self.scope:
            raise NameError('unknown variable: {!r}'.format(varname))

    def check_scope_local(self, varname):
        if not self.scope.is_local(varname):
            raise NameError('variable {!r} must be local to the scope'.format(varname))

    def transpile(self, node, *, module='__main__', package=None):
        state = ModuleState(self,
                            module_name=module,
                            package=package,
                            deps=set())

        with state:
            js = self.visit(node)

        return js, tuple(self.modlist.items())

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
        processed = []

        scope = ModuleScope(self, node=node)
        name = self.state(ModuleState).module_name
        scope.add(Variable(name='__SXJSP_module_name', needs_decl=True,
                           value=js_ast.StringLiteralNode(value=name), aux=True))

        use_strict = True
        with scope:
            for child in node.body:
                if isinstance(child, js_ast.StatementNode) \
                        and isinstance(child.statement, js_ast.StringLiteralNode) \
                        and child.statement.value == 'non-strict':
                    use_strict = False
                    continue

                processed.append(self.visit(child))

        if use_strict:
            processed.insert(0,
                js_ast.StatementNode(
                    statement=js_ast.StringLiteralNode(
                        value='use strict'))
            )

        var = self._gen_var(scope)
        if var:
            processed.insert(1, var) # respect 'use strict';

        if scope.deps:
            mod_deps = set()
            for dep in scope.deps:
                if dep[0] == '_':
                    dep_jsname = '__jp_bltn{}'.format(dep)
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

    def is_assignment_list(self, node):
        assert isinstance(node, js_ast.ArrayLiteralNode)

        for el in node.array:
            if not isinstance(el, js_ast.IDNode):
                return False

        return True

    def unfold_array_assignment(self, els, expr):
        """
        XXX It's a caller responsibility to visit `expr`
        """

        exprs = []
        tmp_name = self.scope.aux_var(name='as_expr', needs_decl=True)

        exprs.append(js_ast.AssignmentExpressionNode(
                        left=js_ast.IDNode(name=tmp_name),
                        op='=',
                        right=expr))

        for idx, el in enumerate(els):
            right = js_ast.SBracketExpressionNode(
                        list=js_ast.IDNode(name=tmp_name),
                        element=js_ast.NumericLiteralNode(
                            value=idx))
            if isinstance(el, js_ast.IDNode):
                exprs.append(js_ast.AssignmentExpressionNode(
                                left=js_ast.IDNode(name=el.name),
                                op='=',
                                right=right))
            else:
                unfolded = self.unfold_assignment(el, right)
                exprs.append(unfolded)

        exprs.append(js_ast.IDNode(name=tmp_name))
        return js_ast.ExpressionListNode(expressions=exprs)

    def unfold_object_assignment(self, els, expr):
        """
        XXX It's a caller responsibility to visit `expr`
        """

        exprs = []
        tmp_name = self.scope.aux_var(name='as_expr', needs_decl=True)

        exprs.append(js_ast.AssignmentExpressionNode(
                        left=js_ast.IDNode(name=tmp_name),
                        op='=',
                        right=expr))

        for el in els:
            right = js_ast.DotExpressionNode(
                        left=js_ast.IDNode(name=tmp_name),
                        right=js_ast.IDNode(
                            name=el.name))

            if isinstance(el, js_ast.IDNode):
                exprs.append(js_ast.AssignmentExpressionNode(
                                left=js_ast.IDNode(name=el.name),
                                op='=',
                                right=right))
            else:
                unfolded, _ = self.unfold_assignment(el, right)
                exprs.append(unfolded)

        exprs.append(js_ast.IDNode(name=tmp_name))
        return js_ast.ExpressionListNode(expressions=exprs)

    def unfold_assignment(self, left, right):
        if isinstance(left, js_ast.ArrayLiteralNode):
            return self.unfold_array_assignment(left.array, right)

        if isinstance(left, js_ast.AssignmentElementList):
            return self.unfold_array_assignment(left.elements, right)

        if isinstance(left, js_ast.AssignmentPropertyList):
            return self.unfold_object_assignment(left.properties, right)

        raise TranspilerError('invalid left-hand assignment expression: {}'.format(left))

    def unfold_expression_list(self, lst):
        assert isinstance(lst, js_ast.ExpressionListNode)

        exprs = []
        for expr in lst.expressions:
            if isinstance(expr, js_ast.IDNode):
                continue

            if isinstance(expr, js_ast.ExpressionListNode):
                exprs.extend(self.unfold_expression_list(expr))
                continue

            exprs.append(expr)

        return exprs

    def visit_js_AssignmentExpressionNode(self, node):
        if isinstance(node.right, js_ast.IDNode):
            self.check_scope_load(node.right.name)

        if node.op == '=':
            self.localize_variables(node.left)

            if isinstance(node.left, (js_ast.ArrayLiteralNode, js_ast.AssignmentPropertyList)):
                node.left = self.visit(node.left)
                node.right = self.visit(node.right)
                return self.unfold_assignment(node.left, node.right)
        else:
            if isinstance(node.left, js_ast.IDNode):
                self.check_scope_local(node.left.name)

        node.left = self.visit(node.left)
        node.right = self.visit(node.right)
        return node

    def visit_js_FunctionNode(self, node):
        name = node.name
        if name and not self.scope.is_local(name):
            self.scope.add(Variable(name))

        scope = FunctionScope(self)

        has_parameters = bool(node.param)
        new_params = []
        pytype = False
        has_var_pos = has_var_kw = has_ko = False
        has_pos_only = has_pos_only_def = 0
        maxtype = ast.POSITIONAL_ONLY
        params = node.param

        if params:
            pytype = isinstance(node.param[0], ast.FunctionParameter)

            for param in params:
                if pytype:
                    if param.type == ast.POSITIONAL_ONLY:
                        has_pos_only += 1
                    elif param.type == ast.POSITIONAL_ONLY_DEFAULT:
                        has_pos_only_def += 1
                    elif param.type == ast.VAR_POSITIONAL:
                        has_var_pos = True
                    elif param.type == ast.KEYWORD_ONLY:
                        has_ko = True
                    elif param.type == ast.VAR_KEYWORD:
                        has_var_kw = True
                else:
                    param.type = ast.POSITIONAL_ONLY

                scope.add(Variable(param.name, needs_decl=False))
                if param.default is not None:
                    if isinstance(param.default, js_ast.IDNode):
                        self.check_scope_load(param.default.name)
                    param.default = self.visit(param.default)
                    if not pytype:
                        maxtype = param.type = ast.POSITIONAL_ONLY_DEFAULT

                new_params.append(js_ast.IDNode(name=param.name))

        if pytype:
            maxtype = node.param[-1].type

        # In JS we have only comma-separated list of IDs as function
        # parameters.
        node.param = new_params

        with scope:
            self.visit(node.body)

        # Generate 'var' statement for all variables that are used
        # inside the function
        func_var_stmt = self._gen_var(scope)
        if func_var_stmt:
            assert isinstance(node.body, js_ast.StatementBlockNode)
            node.body.statements.insert(0, func_var_stmt)

        if not has_parameters or (maxtype == ast.POSITIONAL_ONLY and not self.debug):
            return node # <- plain JS function with ordinary arguments

        #
        # Processing of keyword-only, defaults and *args with **kwargs
        #

        kwargs_obj = has_var_kw or has_ko

        # XXXXXX ??
        #if name and not isinstance(self.scope, ClassScope):
        #    self.scope[name].needs_decl = True

        arglen_var = self.scope.aux_var(name='argslen', needs_decl=False)
        args_init = [
            js_ast.StatementNode(
                statement=js_ast.VarDeclarationNode(
                    vars=[js_ast.VarInitNode(
                        name=js_ast.IDNode(
                            name=arglen_var),
                        value=js_ast.DotExpressionNode(
                            left=js_ast.IDNode(name='arguments'),
                            right=js_ast.IDNode(name='length')))]))]

        if kwargs_obj or self.debug:
            hop_name = self.scope.use('_hop')

            kwtmp_var = self.scope.aux_var(name='kwtmp', needs_decl=False)
            args_init[0].statement.vars.append(
                js_ast.VarInitNode(
                    name=js_ast.IDNode(name=kwtmp_var)))

            # __jpkw = (
            #    __jp_argslen_1 && (
            #        __jp_kwtmp_2 = arguments[(__jp_argslen_1 - 1)],
            #        __jp_bltn_hop(__jp_kwtmp_2, "__jpkw")
            #              ?
            #                  (--__jp_argslen_1, __jp_kwtmp_2)
            #              :
            #                  0
            #        )
            #   ) || 0

            args_init[0].statement.vars.append(
                js_ast.VarInitNode(
                    name=js_ast.IDNode(name=self._kwargs_marker),
                    value=js_ast.BinExpressionNode(
                        left=js_ast.BinExpressionNode(
                            left=js_ast.IDNode(
                                name=arglen_var),
                            op='&&',
                            right=js_ast.ExpressionListNode(
                                expressions=[
                                    js_ast.AssignmentExpressionNode(
                                        left=js_ast.IDNode(
                                            name=kwtmp_var),
                                        op='=',
                                        right=js_ast.SBracketExpressionNode(
                                            list=js_ast.IDNode(name='arguments'),
                                            element=js_ast.BinExpressionNode(
                                                left=js_ast.IDNode(
                                                    name=arglen_var),
                                                op='-',
                                                right=js_ast.NumericLiteralNode(
                                                    value=1)))),

                                    js_ast.ConditionalExpressionNode(
                                        condition=js_ast.BinExpressionNode(
                                            left=js_ast.IDNode(
                                                name=kwtmp_var),
                                            op='&&',
                                            right=js_ast.CallNode(
                                                call=js_ast.IDNode(name=hop_name),
                                                arguments=[
                                                    js_ast.IDNode(name=kwtmp_var),
                                                    js_ast.StringLiteralNode(
                                                        value=self._kwargs_marker)
                                                ])),
                                        true=js_ast.ExpressionListNode(
                                            expressions=[
                                                js_ast.PrefixExpressionNode(
                                                    op='--',
                                                    expression=js_ast.IDNode(
                                                        name=arglen_var)),
                                                js_ast.IDNode(
                                                        name=kwtmp_var)
                                            ]),
                                        false=js_ast.NumericLiteralNode(
                                                    value=0))
                                ])),
                        op='||',
                        right=js_ast.NumericLiteralNode(value=0))))

        if self.debug and (has_pos_only or has_pos_only_def):
            check_pos_only = self.scope.use('_inv_pos_only_args');

            # In debug mode we also check that all positional-only args were
            # passed, as we don't want them to be undefined

            args_init.append(js_ast.StatementNode(
                                statement=js_ast.BinExpressionNode(
                                    left=js_ast.BinExpressionNode(
                                        left=js_ast.IDNode(name=arglen_var),
                                        op='<',
                                        right=js_ast.NumericLiteralNode(
                                            value=has_pos_only)),
                                    op='&&',
                                    right=js_ast.CallNode(
                                        call=js_ast.IDNode(name=check_pos_only),
                                        arguments=[
                                            js_ast.StringLiteralNode(
                                                value=(name or 'anonymous')),
                                            js_ast.IDNode(name=arglen_var),
                                            js_ast.NumericLiteralNode(
                                                value=has_pos_only)
                                        ]))))


            if not has_var_pos:
                args_init.append(js_ast.StatementNode(
                                    statement=js_ast.BinExpressionNode(
                                        left=js_ast.BinExpressionNode(
                                            left=js_ast.IDNode(name=arglen_var),
                                            op='>',
                                            right=js_ast.NumericLiteralNode(
                                                value=has_pos_only + has_pos_only_def)),
                                        op='&&',
                                        right=js_ast.CallNode(
                                            call=js_ast.IDNode(name=check_pos_only),
                                            arguments=[
                                                js_ast.StringLiteralNode(
                                                    value=(name or 'anonymous')),
                                                js_ast.IDNode(name=arglen_var),
                                                js_ast.NumericLiteralNode(
                                                    value=has_pos_only + has_pos_only_def)
                                            ]))))



        for param_pos, param in enumerate(params):
            if param.type == ast.POSITIONAL_ONLY:
                # These should be resolved automatically
                # (as they have the same behaviour as ordinary js args)
                ## XXX workaround (a, b, *, c=1) <- (1, kwargs) ---- should set b to undefined
                continue

            if param.type == ast.POSITIONAL_ONLY_DEFAULT:
                args_init.append(js_ast.StatementNode(
                                        statement=js_ast.BinExpressionNode(
                                            left=js_ast.BinExpressionNode(
                                                left=js_ast.IDNode(
                                                    name=arglen_var),
                                                op = '<',
                                                right=js_ast.NumericLiteralNode(
                                                    value=param_pos + 1)),
                                            op='&&',
                                            right=js_ast.AssignmentExpressionNode(
                                                left=js_ast.IDNode(
                                                    name=param.name),
                                                op='=',
                                                right=param.default))))

            elif param.type == ast.VAR_POSITIONAL:
                slice2_name = self.scope.use('_slice2')
                args_init.append(js_ast.StatementNode(
                                    statement=js_ast.AssignmentExpressionNode(
                                        left=js_ast.IDNode(
                                            name=param.name),
                                        op='=',
                                        right=js_ast.CallNode(
                                            call=js_ast.IDNode(name=slice2_name),
                                            arguments=[
                                                js_ast.IDNode(name='arguments'),
                                                js_ast.NumericLiteralNode(
                                                    value=param_pos),
                                                js_ast.IDNode(
                                                    name=arglen_var)]))))

            elif param.type == ast.KEYWORD_ONLY:
                no_kwarg_mrkr = self.scope.use('_no_kwarg')

                for_default = param.default
                if not for_default:
                    missing = self.scope.use('_required_kwonly_arg_missing')
                    for_default = js_ast.CallNode(
                                        call=js_ast.IDNode(name=missing),
                                        arguments=[
                                            js_ast.StringLiteralNode(
                                                value=(name or 'anonymous')),
                                            js_ast.StringLiteralNode(
                                                value=param.name)
                                        ])

                if has_var_kw or self.debug:
                    assign = js_ast.ExpressionListNode(
                                    expressions=[
                                        js_ast.AssignmentExpressionNode(
                                            left=js_ast.IDNode(
                                                name=kwtmp_var),
                                            op='=',
                                            right=js_ast.DotExpressionNode(
                                                left=js_ast.IDNode(
                                                    name=self._kwargs_marker),
                                                right=js_ast.IDNode(
                                                    name=param.name))),
                                        js_ast.AssignmentExpressionNode(
                                            left=js_ast.DotExpressionNode(
                                                left=js_ast.IDNode(
                                                    name=self._kwargs_marker),
                                                right=js_ast.IDNode(
                                                    name=param.name)),
                                            op='=',
                                            right=js_ast.IDNode(
                                                name=no_kwarg_mrkr)),
                                        js_ast.IDNode(
                                            name=kwtmp_var)
                                    ])
                else:
                    assign = js_ast.DotExpressionNode(
                                    left=js_ast.IDNode(
                                        name=self._kwargs_marker),
                                    right=js_ast.IDNode(
                                        name=param.name))

                args_init.append(js_ast.StatementNode(
                                    statement=js_ast.AssignmentExpressionNode(
                                        left=js_ast.IDNode(
                                            name=param.name),
                                        op='=',
                                        right=js_ast.ConditionalExpressionNode(
                                            condition=js_ast.CallNode(
                                                call=js_ast.IDNode(
                                                    name=hop_name),
                                                arguments=[
                                                    js_ast.IDNode(
                                                        name=self._kwargs_marker),
                                                    js_ast.StringLiteralNode(
                                                        value=param.name)
                                                ]),
                                            true=assign,
                                            false=for_default))))

            elif param.type == ast.VAR_KEYWORD:
                filter_kwarg = self.scope.use('_filter_kwargs')

                args_init.append(js_ast.StatementNode(
                                    statement=js_ast.AssignmentExpressionNode(
                                        left=js_ast.IDNode(
                                            name=param.name),
                                        op='=',
                                        right=js_ast.CallNode(
                                            call=js_ast.IDNode(
                                                name=filter_kwarg),
                                            arguments=[
                                                js_ast.IDNode(
                                                    name=self._kwargs_marker)
                                            ]))))

        if self.debug and not has_var_kw:
            filter_kwarg = self.scope.use('_filter_kwargs')
            assert_empty_kwargs = self.scope.use('_assert_empty_kwargs')
            args_init.append(js_ast.StatementNode(
                                    statement=js_ast.CallNode(
                                        call=js_ast.IDNode(
                                            name=assert_empty_kwargs),
                                        arguments=[
                                            js_ast.StringLiteralNode(
                                                value=(name or 'anonymous')),
                                            js_ast.CallNode(
                                                call=js_ast.IDNode(
                                                    name=filter_kwarg),
                                                arguments=[
                                                    js_ast.IDNode(
                                                        name=self._kwargs_marker)
                                                ])])))

        func_body = node.body.statements
        func_body.insert(0, js_ast.SourceElementsNode(code=args_init))

        return node

    def visit_jp_ClassMethodNode(self, node):
        func = js_ast.FunctionNode(
                    name=node.name,
                    param=node.param,
                    body=node.body,
                    isdeclaration=True)

        return self.visit_js_FunctionNode(func)

    def visit_jp_CallArgument(self, node):
        if isinstance(node.value, js_ast.IDNode):
            self.check_scope_load(node.value.name)
        return self.generic_visit(node)

    def visit_jp_CallNode(self, node):
        if isinstance(node.call, js_ast.IDNode):
            self.check_scope_load(node.call.name)

        node = self.generic_visit(node)

        has_args = has_kwargs = has_ko = False

        positional_values = []
        keywords = []

        for arg in node.arguments:
            if arg.type == ast.VAR_POSITIONAL:
                has_args = True
            elif arg.type == ast.KEYWORD_ONLY:
                has_ko = True
                keywords.append(arg)
            elif arg.type == ast.VAR_KEYWORD:
                has_kwargs = True
            elif arg.type == ast.POSITIONAL_ONLY:
                positional_values.append(arg.value)
            else:
                assert 0

        assert not has_args and not has_kwargs, 'unsupported'

        if not has_ko:
            node.arguments = positional_values
            return node

        kw_properties = [js_ast.SimplePropertyNode(
                                    name=js_ast.IDNode(name=k.name),
                                    value=k.value) for k in keywords]
        kw_properties.append(js_ast.SimplePropertyNode(
                                    name=js_ast.IDNode(name=self._kwargs_marker),
                                    value=js_ast.NumericLiteralNode(
                                        value=1)))
        positional_values.append(js_ast.ObjectLiteralNode(properties=kw_properties))

        node.arguments = positional_values
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

                bases.append(self.visit(base))

        with scope, state:
            for child in node.body:
                is_static = False
                is_member = False
                child_name = None

                if isinstance(child, ast.ClassMemberNode):
                    is_static = child.is_static
                    is_member = True
                    child_name = child.name
                elif isinstance(child, ast.ClassMethodNode):
                    is_static = child.is_static
                    child_name = child.name
                elif isinstance(child, ast.DecoratedNode):
                    if not isinstance(child.node, ast.ClassMethodNode):
                        raise TranspilerError('unsupported AST node in class body: {}'.
                                              format(child.node))
                    is_static = child.node.is_static
                    child_name = child.node.name
                else:
                    raise TranspilerError('unsupported AST node in class body: {}'.
                                          format(child))

                if is_member:
                    class_node = self.visit(child.value)
                else:
                    class_node = self.visit(child)

                collection = dct_static_items if is_static else dct_items
                collection.append(js_ast.SimplePropertyNode(
                                        name=js_ast.IDNode(name=child_name),
                                        value=class_node))

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
                               js_ast.IDNode(name='__SXJSP_module_name'),
                               js_ast.StringLiteralNode(value=name),
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
        wrapped = node.node
        is_method = isinstance(wrapped, ast.ClassMethodNode)

        if not isinstance(wrapped, (js_ast.FunctionNode, ast.ClassNode)):
            raise TranspilerError('unsupported decorated node (only funcs & classes expected): {}'.
                                  format(wrapped))

        is_class = isinstance(wrapped, ast.ClassNode)
        name = wrapped.name

        wrapped = self.visit(wrapped)

        if is_class:
            assert not is_method
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

        if is_method:
            return wrapped

        return js_ast.AssignmentExpressionNode(
                  left=js_ast.IDNode(name=name),
                  op='=',
                  right=wrapped)

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

    def localize_variables(self, expr):
        if isinstance(expr, js_ast.IDNode):
            var = expr.name
            if not self.scope.is_local(var):
                self.scope.add(Variable(var, needs_decl=True))
        else:
            nodes = None
            if isinstance(expr, js_ast.AssignmentElementList):
                nodes = expr.elements
            elif isinstance(expr, js_ast.ArrayLiteralNode):
                nodes = expr.array
            elif isinstance(expr, js_ast.AssignmentPropertyList):
                nodes = expr.properties

            if nodes is not None:
                for node in nodes:
                    self.localize_variables(node)

    def visit_js_ForOfNode(self, node):
        var_on = self.scope.aux_var(name='forof_of', needs_decl=True)
        var_onlen = self.scope.aux_var(name='forof_of_len', needs_decl=True)
        var_idx = self.scope.aux_var(name='forof_i', needs_decl=True)

        init = self.visit(node.init)
        self.localize_variables(init)

        with ForState(self):
            container = self.visit(node.container)
            statement = self.visit(node.statement)

        # Initial code:
        #
        # for (INIT of CONTAINER) { STATAMENT }
        #
        # we transform to:
        #
        # var_on = CONTAINER             (var_on_set_to_container)
        # var_onlen = var_on.length      (var_onlen_to_container_len)
        # var_idx = 0                    (init_var_idx)
        # for (;
        #       var_idx < var_onlen;     (var_idx_lt_onlen)
        #       var_idx++) {             (var_idx_inc)
        #
        #    INIT = var_on[var_idx]      (init_iter_set)
        #
        #    [STATEMENT]
        # }

        var_on_set_to_container = js_ast.AssignmentExpressionNode(
                                    left=js_ast.IDNode(name=var_on),
                                    op='=',
                                    right=container)

        var_onlen_to_container_len = js_ast.AssignmentExpressionNode(
                                        left=js_ast.IDNode(name=var_onlen),
                                        op='=',
                                        right=js_ast.DotExpressionNode(
                                            left=js_ast.IDNode(name=var_on),
                                            right=js_ast.IDNode(name='length')))

        init_var_idx = js_ast.AssignmentExpressionNode(
                            left=js_ast.IDNode(name=var_idx),
                            op='=',
                            right=js_ast.NumericLiteralNode(value=0))

        var_idx_lt_onlen = js_ast.BinExpressionNode(
                                left=js_ast.IDNode(name=var_idx),
                                op='<',
                                right=js_ast.IDNode(name=var_onlen))

        var_idx_inc = js_ast.PostfixExpressionNode(
                            expression=js_ast.IDNode(name=var_idx),
                            op='++')

        if isinstance(init, js_ast.IDNode):
            init_name = init.name
            init_iter_set = js_ast.AssignmentExpressionNode(
                                left=js_ast.IDNode(name=init_name),
                                op='=',
                                right=js_ast.SBracketExpressionNode(
                                    list=js_ast.IDNode(name=var_on),
                                    element=js_ast.IDNode(name=var_idx)))

            return js_ast.ForNode(
                        part1=js_ast.ExpressionListNode(
                            expressions=[
                                var_on_set_to_container,
                                var_onlen_to_container_len,
                                init_var_idx]),
                        part2=js_ast.ExpressionListNode(
                            expressions=[
                                var_idx_lt_onlen,
                                init_iter_set]),
                        part3=var_idx_inc,
                        statement=statement)

        else:
            assert isinstance(statement, js_ast.StatementBlockNode)

            unfolded = self.unfold_assignment(init,
                                              js_ast.SBracketExpressionNode(
                                                    list=js_ast.IDNode(name=var_on),
                                                    element=js_ast.IDNode(name=var_idx)))

            statement.statements[0:0] = [js_ast.StatementNode(statement=e)
                                                for e in self.unfold_expression_list(unfolded)]

            return js_ast.ForNode(
                        part1=js_ast.ExpressionListNode(
                            expressions=[
                                var_on_set_to_container,
                                var_onlen_to_container_len,
                                init_var_idx]),
                        part2=var_idx_lt_onlen,
                        part3=var_idx_inc,
                        statement=statement)

    def visit_jp_TryNode(self, node):
        try_body = self.visit(node.body).statements

        if node.jscatch:
            # We have the old 'try..catch' case here, and as we don't
            # support 'except' and 'else' keywords for such 'try' blocks
            # we just return plain TryNode.

            catchid = node.jscatch.catchid
            if not self.scope.is_local(catchid):
                self.scope.add(Variable(catchid, needs_decl=False, aux=True))

            jscatch = self.visit(node.jscatch)

            finally_body = []
            if node.finalbody:
                finally_body = self.visit(node.finalbody).statements

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

                #: We don't have handler for ExceptNode, hence generic_visit
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

            try_body.insert(0, js_ast.StatementNode(
                                statement=js_ast.AssignmentExpressionNode(
                                    left=js_ast.IDNode(
                                        name=orelse_name),
                                    op='=',
                                    right=js_ast.BooleanLiteralNode(value=False))))

            try_body.append(js_ast.StatementNode(
                                statement=js_ast.AssignmentExpressionNode(
                                    left=js_ast.IDNode(
                                        name=orelse_name),
                                    op='=',
                                    right=js_ast.BooleanLiteralNode(value=True))))

            orelse_body = self.visit(node.orelse).statements

            else_block = [js_ast.IfNode(
                            ifclause=js_ast.IDNode(
                                name=orelse_name),
                            thenclause=js_ast.StatementBlockNode(
                                statements=orelse_body))]

            if node.finalbody:
                finally_body = self.visit(node.finalbody).statements
                finally_body = [js_ast.TryNode(
                                    tryblock=js_ast.StatementBlockNode(
                                        statements=else_block),
                                    finallyblock=js_ast.StatementBlockNode(
                                        statements=finally_body))]
            else:
                finally_body = else_block
        else:
            finally_body = []
            if node.finalbody:
                finally_body = self.visit(node.finalbody).statements

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

    def visit_js_WithNode(self, node):
        with JSWithState(self):
            return self.generic_visit(node)

    def visit_jp_WithNode(self, node):
        validate_with_name = self.scope.use('_validate_with')

        inner_try = None
        wrap_next = None
        for withitem in reversed(node.withitems):
            body = []
            expr = self.visit(withitem.expr)

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
                                                        name='exit')),
                                                arguments=[js_ast.NullNode()]))]))])

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
        inner_try.tryblock = self.visit(node.body)

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
    visit_js_ExpressionListNode = generic_visit_expression

    def visit_js_StatementNode(self, node):
        node = self.generic_visit_expression(node)
        if node is None or getattr(node, 'statement', None) is None:
            return
        return node

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

    def visit_jp_AssertNode(self, node):
        # TODO:
        # 1. Raise true AssertionError
        # 2. There should be an option to somehow turn all
        # asserts off.

        if not self.debug:
            return

        test = self.visit(node.test)
        if isinstance(test, js_ast.IDNode):
            self.check_scope_load(test.name)

        failexpr = None
        if node.failexpr is not None:
            failexpr = self.visit(node.failexpr)
            if isinstance(failexpr, js_ast.IDNode):
                self.check_scope_load(failexpr.name)

        assert_fail = self.scope.use('_throw_assert_error')
        return js_ast.StatementNode(
                statement=js_ast.IfNode(
                    ifclause=js_ast.PrefixExpressionNode(
                        expression=test,
                        op='!'),
                    thenclause=js_ast.CallNode(
                        call=js_ast.IDNode(name=assert_fail),
                        arguments=[failexpr] if failexpr else [])))

    def visit_js_ArrayComprehensionNode(self, node):
        assert isinstance(node.generator, js_ast.GeneratorExprNode)

        # NOTE: We don't "visit" any node's child here directly.
        # It'll be a task for visit_js_FunctionNode later.

        body = []

        #: This is a name of container that will hold the comprehension
        #: results
        result_name = '__SXJSP_aux_comp_res'

        # Let's init it with an empty array
        body.append(js_ast.StatementNode(
                        statement=js_ast.AssignmentExpressionNode(
                            left=js_ast.IDNode(name=result_name),
                            op='=',
                            right=js_ast.ArrayLiteralNode())))

        # That's the most inner part of comprehension -- actual push
        # of a result value to the results list
        to_wrap = js_ast.StatementNode(
                        statement=js_ast.CallNode(
                            call=js_ast.DotExpressionNode(
                                left=js_ast.IDNode(name=result_name),
                                right=js_ast.IDNode(name='push')),
                            arguments=[node.generator.expr]))

        for comp in reversed(node.generator.comprehensions):
            comp.is_expr = False

            if isinstance(comp, js_ast.IfNode):
                comp.thenclause = js_ast.StatementBlockNode(
                                    statements=[to_wrap])
            else:
                assert isinstance(comp, (js_ast.ForNode, js_ast.ForOfNode))
                comp.statement = js_ast.StatementBlockNode(
                                    statements=[to_wrap])

            to_wrap = comp

        body.append(to_wrap)

        # Return from function
        body.append(js_ast.ReturnNode(expression=js_ast.IDNode(name=result_name)))

        # That's the generator transformed into a function
        func = js_ast.FunctionNode(
                    body=js_ast.StatementBlockNode(
                        statements=body))

        # Now, let's process its guts (to register variables etc)
        func = self.visit(func)

        return js_ast.CallNode(call=func)

    def visit_js_FatArrowFunctionNode(self, node):
        body = node.body
        if not isinstance(body, js_ast.StatementBlockNode):
            body = js_ast.StatementBlockNode(
                        statements=[js_ast.ReturnNode(
                                expression=body)])

        func = js_ast.FunctionNode(
                    param=node.param,
                    body=body)

        with ArrowFuncState(self):
            func = self.visit(func)

        bind_name = self.scope.use('_bind')

        return js_ast.CallNode(
                    call=js_ast.DotExpressionNode(
                        left=js_ast.IDNode(name=bind_name),
                        right=js_ast.IDNode(name='call')),
                    arguments=[
                        func,
                        js_ast.ThisNode()
                    ])

    def add_module(self, module, implist=None):
        if module not in self.modlist:
            self.modlist[module] = set()
        self.modlist[module].update(implist)

    def visit_jp_ImportFromNode(self, node):
        mod = node.module

        if mod == '__javascript__':
            for name in node.names:
                self.scope.add(Variable(name.name, needs_decl=False))
            return

        implist = [name.name for name in node.names]
        self.add_module(node.level * '.' + mod, implist)

        if node.level > 1:
            fullmod = '.'.join(self.state.package.split('.')[:-(node.level-1)]) + '.' + mod
        elif node.level == 1:
            fullmod = self.state.package + '.' + mod
        else:
            fullmod = mod

        assigns = []
        for imp in node.names:
            cur = js_ast.IDNode(name=imp.name)
            for name in reversed(fullmod.split('.')):
                cur = js_ast.DotExpressionNode(
                    left=js_ast.IDNode(name=name),
                    right=cur)

            impname = imp.asname or imp.name

            self.scope.add(Variable(impname, needs_decl=True))

            assigns.append(
                js_ast.StatementNode(
                    statement=js_ast.AssignmentExpressionNode(
                        left=js_ast.IDNode(name=impname),
                        op='=',
                        right=js_ast.DotExpressionNode(
                            left=js_ast.IDNode(name='$SXJSP'),
                            right=js_ast.DotExpressionNode(
                                left=js_ast.IDNode(name='_modules'),
                                right=cur)))))

        return js_ast.SourceElementsNode(code=assigns)

    def visit_jp_ImportNode(self, node):
        assigns = []

        for imp in node.names:
            self.add_module(imp.name, ())

            if imp.asname:
                cur = None
                for name in reversed(imp.name.split('.')):
                    if cur is None:
                        cur = js_ast.IDNode(name=name)
                    else:
                        cur = js_ast.DotExpressionNode(
                            left=js_ast.IDNode(name=name),
                            right=cur)

                self.scope.add(Variable(imp.asname, needs_decl=True))

                assigns.append(
                    js_ast.StatementNode(
                        statement=js_ast.AssignmentExpressionNode(
                            left=js_ast.IDNode(name=imp.asname),
                            op='=',
                            right=js_ast.DotExpressionNode(
                                left=js_ast.IDNode(name='$SXJSP'),
                                right=js_ast.DotExpressionNode(
                                    left=js_ast.IDNode(name='_modules'),
                                    right=cur)))))

            else:
                impname = imp.name.split('.')[0]

                self.scope.add(Variable(impname, needs_decl=True))

                assigns.append(
                    js_ast.StatementNode(
                        statement=js_ast.AssignmentExpressionNode(
                            left=js_ast.IDNode(name=impname),
                            op='=',
                            right=js_ast.DotExpressionNode(
                                left=js_ast.IDNode(name='$SXJSP'),
                                right=js_ast.DotExpressionNode(
                                    left=js_ast.IDNode(name='_modules'),
                                    right=js_ast.IDNode(name=impname))))))

        return js_ast.SourceElementsNode(code=assigns)

    def visit_js_VarDeclarationNode(self, node):
        for init in node.vars:
            self.scope.add(Variable(init.name.name, needs_decl=False))
        return self.generic_visit(node)
