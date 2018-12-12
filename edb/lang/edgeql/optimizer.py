#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2015-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from edb.lang.edgeql import ast as qlast


class ContextLevel:
    def __init__(self, prevlevel=None, mode=None):
        if prevlevel is not None:
            if mode == Context.SUBQUERY:
                self.aliascnt = prevlevel.aliascnt.copy()
                self.modaliases = prevlevel.modaliases.copy()
            else:
                self.aliascnt = prevlevel.aliascnt
                self.modaliases = prevlevel.modaliases
            self.deoptimize = prevlevel.deoptimize
            self.strip_builtins = prevlevel.strip_builtins
        else:
            self.aliascnt = {}
            self.modaliases = {}
            self.deoptimize = False
            self.strip_builtins = True

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


class EdgeQLOptimizer:
    def transform(self, edgeql_tree, deoptimize=False, strip_builtins=True):
        context = Context()
        context.current.deoptimize = deoptimize
        context.current.strip_builtins = strip_builtins
        self._process_expr(context, edgeql_tree)

        nses = []
        for alias, fq_name in context.current.modaliases.items():
            decl = qlast.ModuleAliasDecl(module=fq_name, alias=alias)
            nses.append(decl)

        if isinstance(edgeql_tree, qlast.Statement):
            if deoptimize:
                edgeql_tree.aliases[:] = [
                    a for a in edgeql_tree.aliases
                    if not isinstance(a, qlast.ModuleAliasDecl)
                ]
            else:
                if edgeql_tree.aliases is not None:
                    edgeql_tree.aliases[:] = nses
                else:
                    edgeql_tree.aliases = nses

        return edgeql_tree

    def _process_aliases(self, context, expr):
        if expr.aliases:
            for ns in expr.aliases:
                if isinstance(ns, qlast.ModuleAliasDecl):
                    context.current.modaliases[ns.alias] = ns.module
                    context.current.aliascnt[ns.alias] = 1

    def _process_expr(self, context, expr):
        if isinstance(expr, qlast.SelectQuery):
            self._process_aliases(context, expr)

            if expr.where:
                self._process_expr(context, expr.where)

            self._process_expr(context, expr.result)

            if expr.orderby:
                for sort in expr.orderby:
                    self._process_expr(context, sort.path)

            if expr.offset:
                self._process_expr(context, expr.offset)

            if expr.limit:
                self._process_expr(context, expr.limit)

        elif isinstance(expr, qlast.InsertQuery):
            self._process_aliases(context, expr)

            self._process_expr(context, expr.subject)

            if expr.shape:
                self._process_shape(context, expr.shape)

            if expr.result is not None:
                self._process_expr(context, expr.result)

        elif isinstance(expr, qlast.UpdateQuery):
            self._process_aliases(context, expr)

            self._process_expr(context, expr.subject)

            if expr.where:
                self._process_expr(context, expr.where)

            if expr.shape:
                self._process_shape(context, expr.shape)

            if expr.result is not None:
                self._process_expr(context, expr.result)

        elif isinstance(expr, qlast.DeleteQuery):
            self._process_aliases(context, expr)

            self._process_expr(context, expr.subject)

            if expr.where:
                self._process_expr(context, expr.where)

            if expr.result is not None:
                self._process_expr(context, expr.result)

        elif isinstance(expr, qlast.BinOp):
            self._process_expr(context, expr.left)
            self._process_expr(context, expr.right)

        elif isinstance(expr, qlast.FunctionCall):
            for arg in expr.args:
                self._process_expr(context, arg)

        elif isinstance(expr, qlast.WindowSpec):
            if expr.orderby:
                for orderby in expr.orderby:
                    self._process_expr(context, orderby.path)

            if expr.partition:
                for partition in expr.partition:
                    self._process_expr(context, partition)

        elif isinstance(expr, qlast.UnaryOp):
            self._process_expr(context, expr.operand)

        elif isinstance(expr, qlast.Tuple):
            for el in expr.elements:
                self._process_expr(context, el)

        elif isinstance(expr, qlast.TypeCast):
            self._process_expr(context, expr.expr)
            self._process_expr(context, expr.to_type)

        elif (isinstance(expr, qlast.TypeName)
                and not isinstance(expr.maintype, qlast.PseudoObjectRef)):
            expr.maintype.module = self._process_module_ref(
                context, expr.maintype.module)

            if expr.subtypes:
                for subtype in expr.subtypes:
                    self._process_expr(context, subtype)

        elif isinstance(expr, qlast.ObjectRef):
            expr.module = self._process_module_ref(context, expr.module)

        elif isinstance(expr, qlast.Shape):
            self._process_expr(context, expr.expr)
            self._process_shape(context, expr)

        elif isinstance(expr, qlast.Path):
            for step in expr.steps:
                self._process_expr(context, step)

        elif isinstance(expr, qlast.Ptr):
            self._process_expr(context, expr.ptr)
            if expr.target:
                self._process_expr(context, expr.target)

        elif isinstance(expr, qlast.CreateModule):
            pass

        elif isinstance(expr, qlast.CastCommand):
            self._process_expr(context, expr.from_type)
            self._process_expr(context, expr.to_type)

        elif isinstance(expr, qlast.ObjectDDL):
            self._process_aliases(context, expr)

            expr.name.module = self._process_module_ref(
                context, expr.name.module)

            if expr.commands:
                for cmd in expr.commands:
                    self._process_expr(context, cmd)

            bases = getattr(expr, 'bases', None)

            if bases:
                for base in bases:
                    if not isinstance(base.maintype, qlast.AnyType):
                        base.maintype.module = self._process_module_ref(
                            context, base.maintype.module)

            if isinstance(expr, qlast.CreateConcreteLink):
                self._process_expr(context, expr.target)

            elif isinstance(expr, qlast.CreateConcreteProperty):
                self._process_expr(context, expr.target)

            elif isinstance(expr, qlast.OperatorCommand):
                for arg in expr.params:
                    self._process_expr(context, arg)

                if isinstance(expr, qlast.CreateOperator):
                    self._process_expr(context, expr.returning)

        elif isinstance(expr, qlast.AlterTarget):
            expr.target.maintype.module = self._process_module_ref(
                context, expr.target.maintype.module)

        elif isinstance(expr, qlast.Rename):
            expr.new_name.module = self._process_module_ref(
                context, expr.new_name.module)

        elif isinstance(expr, (qlast.AlterAddInherit,
                               qlast.AlterDropInherit)):
            for base in expr.bases:
                self._process_expr(context, base)

    def _process_shape(self, context, shape):
        for spec in shape.elements:
            if isinstance(spec, qlast.ShapeElement):
                if spec.where:
                    self._process_expr(context, spec.where)

                if spec.orderby:
                    for orderby in spec.orderby:
                        self._process_expr(context, orderby.path)

                self._process_expr(context, spec.expr)

                if spec.compexpr:
                    self._process_expr(context, spec.compexpr)

                if spec.elements:
                    self._process_shape(context, spec)

    def _process_module_ref(self, context, module):
        # We cannot unabiguosly qualify naked names, as these
        # may refer either to `std::` or to the default module.
        if not module:
            return module

        if context.current.deoptimize:
            return context.current.modaliases.get(module, module)
        else:
            if module == 'std' and context.current.strip_builtins:
                return None

            if '.' in module:
                modmap = {v: k for k, v in context.current.modaliases.items()}
                try:
                    alias = modmap[module]
                except KeyError:
                    mhead, _, mtail = module.rpartition('.')
                    if mtail == 'objects' and mhead:
                        # schemas are commonly in the form <module>.objects
                        mhead, _, mtail = mhead.rpartition('.')
                    alias = context.current.genalias(hint=mtail)
                    context.current.modaliases[alias] = module

                return alias
            else:
                return module


def optimize(edgeql_tree, *, strip_builtins=True):
    """Perform optimizations on EdgeQL AST tree"""

    optimizer = EdgeQLOptimizer()
    return optimizer.transform(edgeql_tree, strip_builtins=strip_builtins)


def deoptimize(edgeql_tree, *, strip_builtins=True):
    """Reverse optimizations on EdgeQL AST tree"""

    optimizer = EdgeQLOptimizer()
    return optimizer.transform(
        edgeql_tree, deoptimize=True, strip_builtins=strip_builtins)
