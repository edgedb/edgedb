##
# Copyright (c) 2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.caosql import ast as qlast


class ContextLevel:
    def __init__(self, prevlevel=None, mode=None):
        if prevlevel is not None:
            if mode == Context.SUBQUERY:
                self.aliascnt = prevlevel.aliascnt.copy()
                self.namespaces = prevlevel.namespaces.copy()
            else:
                self.aliascnt = prevlevel.aliascnt
                self.namespaces = prevlevel.namespaces
            self.deoptimize = prevlevel.deoptimize
        else:
            self.aliascnt = {}
            self.namespaces = {}
            self.deoptimize = False

    def genalias(self, hint=None):
        if hint is None:
            hint = 'a'

        if hint not in self.aliascnt:
            self.aliascnt[hint] = 1
        else:
            self.aliascnt[hint] += 1

        alias = hint
        number = self.aliascnt[hint]
        if number > 1:
            alias += str(number)

        return alias


class Context:
    CURRENT, NEW, SUBQUERY = range(0, 3)

    def __init__(self):
        self.stack = []
        self.push()

    def push(self, mode=None):
        level = ContextLevel(self.current, mode)
        self.stack.append(level)

        return level

    def pop(self):
        self.stack.pop()

    def __call__(self, mode=None):
        if not mode:
            mode = Context.NEW
        return ContextWrapper(self, mode)

    def _current(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    current = property(_current)


class ContextWrapper:
    def __init__(self, context, mode):
        self.context = context
        self.mode = mode

    def __enter__(self):
        if self.mode == Context.CURRENT:
            return self.context
        else:
            self.context.push()
            return self.context

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()


class CaosQLOptimizer:
    def transform(self, caosql_tree, deoptimize=False):
        context = Context()
        context.current.deoptimize = deoptimize
        self._process_expr(context, caosql_tree)

        nses = []
        for alias, fq_name in context.current.namespaces.items():
            decl = qlast.NamespaceAliasDeclNode(namespace=fq_name,
                                                alias=alias)
            nses.append(decl)

        if caosql_tree.namespaces is not None:
            caosql_tree.namespaces[:] = nses
        else:
            caosql_tree.namespaces = nses

        return caosql_tree

    def _process_expr(self, context, expr):
        if isinstance(expr, qlast.SelectQueryNode):
            pathvars = {}

            if expr.namespaces:
                for ns in expr.namespaces:
                    if isinstance(ns.namespace, str):
                        context.current.namespaces[ns.alias] = ns.namespace
                        context.current.aliascnt[ns.alias] = 1
                    else:
                        self._process_expr(context, ns.namespace)
                        pathvars[ns.alias] = ns.namespace

            if expr.where:
                self._process_expr(context, expr.where)

            if expr.groupby:
                for gb in expr.groupby:
                    self._process_expr(context, gb)

            for tgt in expr.targets:
                self._process_expr(context, tgt.expr)

            if expr.orderby:
                for sort in expr.orderby:
                    self._process_expr(context, sort.path)

            if expr.offset:
                self._process_expr(context, expr.offset)

            if expr.limit:
                self._process_expr(context, expr.limit)

        elif isinstance(expr, qlast.UpdateQueryNode):
            if expr.namespaces:
                context.current.namespaces.update(
                    (ns.alias, ns.namespace) for ns in expr.namespaces)

            self._process_expr(context, expr.subject)

            if expr.where:
                self._process_expr(context, expr.where)

            for v in expr.values:
                self._process_expr(context, v.expr)
                self._process_expr(context, v.value)

            for tgt in expr.targets:
                self._process_expr(context, tgt.expr)

        elif isinstance(expr, qlast.DeleteQueryNode):
            if expr.namespaces:
                context.current.namespaces.update(
                    (ns.alias, ns.namespace) for ns in expr.namespaces)

            self._process_expr(context, expr.subject)

            if expr.where:
                self._process_expr(context, expr.where)

            for tgt in expr.targets:
                self._process_expr(context, tgt.expr)

        elif isinstance(expr, qlast.SubqueryNode):
            with context(Context.SUBQUERY):
                self._process_expr(context, expr.expr)

        elif isinstance(expr, qlast.PredicateNode):
            self._process_expr(context, expr.expr)

        elif isinstance(expr, qlast.BinOpNode):
            self._process_expr(context, expr.left)
            self._process_expr(context, expr.right)

        elif isinstance(expr, qlast.FunctionCallNode):
            for arg in expr.args:
                self._process_expr(context, arg)

            if expr.agg_sort:
                for sort in expr.agg_sort:
                    self._process_expr(context, sort.path)

            if expr.window:
                self._process_expr(context, expr.window)

        elif isinstance(expr, qlast.WindowSpecNode):
            if expr.orderby:
                for orderby in expr.orderby:
                    self._process_expr(context, orderby.path)

            if expr.partition:
                for partition in expr.partition:
                    self._process_expr(context, partition)

        elif isinstance(expr, qlast.UnaryOpNode):
            self._process_expr(context, expr.operand)

        elif isinstance(expr, qlast.PostfixOpNode):
            self._process_expr(context, expr.operand)

        elif isinstance(expr, qlast.SequenceNode):
            for el in expr.elements:
                self._process_expr(context, el)

        elif isinstance(expr, qlast.TypeCastNode):
            self._process_expr(context, expr.expr)
            self._process_expr(context, expr.type)

        elif isinstance(expr, qlast.TypeNameNode):
            if '.' in expr.maintype:
                module, _, name = expr.maintype.rpartition('.')
                module = self._process_module_ref(context, module)
                if module:
                    expr.maintype = module + '.' + name
                else:
                    expr.maintype = name
            if expr.subtype:
                self._process_expr(context, expr.subtype)

        elif isinstance(expr, qlast.TypeRefNode):
            self._process_expr(context, expr.expr)

        elif isinstance(expr, qlast.NoneTestNode):
            self._process_expr(context, expr.expr)

        elif isinstance(expr, qlast.PrototypeRefNode):
            if expr.module:
                expr.module = self._process_module_ref(
                                context, expr.module)

        elif isinstance(expr, qlast.PathNode):
            if expr.pathspec:
                self._process_pathspec(context, expr.pathspec)

            for step in expr.steps:
                self._process_expr(context, step)

        elif isinstance(expr, qlast.PathStepNode):
            if expr.namespace:
                expr.namespace = self._process_module_ref(
                                    context, expr.namespace)

        elif isinstance(expr, (qlast.LinkExprNode, qlast.LinkPropExprNode)):
            self._process_expr(context, expr.expr)

        elif isinstance(expr, qlast.LinkNode):
            if expr.namespace:
                expr.namespace = self._process_module_ref(
                                    context, expr.namespace,
                                    strip_builtins=(expr.direction != '<'))

            if expr.target:
                self._process_expr(context, expr.target)

        elif isinstance(expr, qlast.CreateModuleNode):
            pass

        elif isinstance(expr, qlast.ObjectDDLNode):
            if expr.namespaces:
                context.current.namespaces.update(
                    (ns.alias, ns.namespace) for ns in expr.namespaces)

            expr.name.module = self._process_module_ref(
                                    context, expr.name.module,
                                    strip_builtins=False)

            if expr.commands:
                for cmd in expr.commands:
                    self._process_expr(context, cmd)

            bases = getattr(expr, 'bases', None)

            if bases:
                for base in bases:
                    base.module = self._process_module_ref(
                                    context, base.module,
                                    strip_builtins=False)

            if isinstance(expr, qlast.CreateConcreteLinkNode):
                for t in expr.targets:
                    t.module = self._process_module_ref(
                                    context, t.module,
                                    strip_builtins=False)

            elif isinstance(expr, qlast.CreateConcreteLinkPropertyNode):
                expr.target.module = self._process_module_ref(
                                        context, expr.target.module,
                                        strip_builtins=False)

        elif isinstance(expr, (qlast.CreateLocalPolicyNode,
                               qlast.AlterLocalPolicyNode)):
            expr.event.module = self._process_module_ref(
                                        context, expr.event.module,
                                        strip_builtins=False)
            for action in expr.actions:
                action.module = self._process_module_ref(
                                        context, action.module,
                                        strip_builtins=False)

        elif isinstance(expr, qlast.AlterTargetNode):
            for target in expr.targets:
                target.module = self._process_module_ref(
                                        context, target.module,
                                        strip_builtins=False)

        elif isinstance(expr, qlast.RenameNode):
            expr.new_name.module = self._process_module_ref(
                                        context, expr.new_name.module,
                                        strip_builtins=False)

        elif isinstance(expr, (qlast.AlterAddInheritNode,
                               qlast.AlterDropInheritNode)):
            for base in expr.bases:
                base.module = self._process_module_ref(
                                        context, base.module,
                                        strip_builtins=False)

    def _process_pathspec(self, context, pathspec):
        for spec in pathspec:
            if isinstance(spec, qlast.SelectPathSpecNode):
                if spec.where:
                    self._process_expr(context, spec.where)

                if spec.orderby:
                    for orderby in spec.orderby:
                        self._process_expr(context, orderby.path)

                self._process_expr(context, spec.expr)

                if spec.pathspec:
                    self._process_pathspec(context, spec.pathspec)

    def _process_module_ref(self, context, module, strip_builtins=True):
        if context.current.deoptimize:
            return context.current.namespaces.get(module, module)
        else:
            if module == 'metamagic.caos.builtins' and strip_builtins:
                return None

            if '.' in module:
                modmap = {v: k for k, v in context.current.namespaces.items()}
                try:
                    alias = modmap[module]
                except KeyError:
                    mhead, _, mtail = module.rpartition('.')
                    if mtail == 'objects' and mhead:
                        # schemas are commonly in the form <module>.objects
                        mhead, _, mtail = mhead.rpartition('.')
                    alias = context.current.genalias(hint=mtail)
                    context.current.namespaces[alias] = module

                return alias
            else:
                return module


def optimize(caosql_tree):
    """Perform optimizations on CaosQL AST tree"""

    optimizer = CaosQLOptimizer()
    return optimizer.transform(caosql_tree)


def deoptimize(caosql_tree):
    """Reverse optimizations on CaosQL AST tree"""

    optimizer = CaosQLOptimizer()
    return optimizer.transform(caosql_tree, deoptimize=True)
