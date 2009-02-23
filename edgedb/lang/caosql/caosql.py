from semantix.caos.caosql import ast
from semantix.caos.caosql.parser import nodes as qlast
from semantix.caos import backends
from semantix.caos.caosql import CaosQLError

class ParseContextLevel(object):
    def __init__(self, prevlevel=None):
        if prevlevel is not None:
            self.vars = copy.deepcopy(prevlevel.vars)
            self.paths = copy.deepcopy(prevlevel.paths)
            self.aliascnt = copy.deepcopy(prevlevel.aliascnt)
            self.location = None
        else:
            self.vars = {}
            self.aliascnt = {}
            self.paths = []
            self.location = None

    def genalias(self, alias=None, hint=None):
        if alias is None:
            if hint is None:
                hint = 'a'

            if hint not in self.aliascnt:
                self.aliascnt[hint] = 1
            else:
                self.aliascnt[hint] += 1

            return '_' + hint + str(self.aliascnt[hint])
        elif alias in self.vars:
            raise CaosQLError('Path var redefinition: % is already used' %  alias)
        else:
            return alias


class ParseContext(object):
    stack = []

    def __init__(self):
        self.push()

    def push(self):
        level = ParseContextLevel()
        self.stack.append(level)

        return level

    def pop(self):
        self.stack.pop()

    def _current(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    current = property(_current)


class CaosqlTreeTransformer(object):
    def _dump(self, tree):
        if tree is not None:
            print(tree.dump(pretty=True, colorize=True, width=180, field_mask='^_'))
        else:
            print('None')

    def transform(self, qtree):
        context = ParseContext()
        stree = self._transform_select(context, qtree)

        return stree

    def _transform_select(self, context, tree):
        context.current.graph = ast.GraphExpr()

        context.current.graph.generator = self._process_select_where(context, tree.where)
        context.current.graph.selector = self._process_select_targets(context, tree.targets)

        return context.current.graph

    def _process_select_where(self, context, where):
        if where:
            return self._process_expr(context, where.expr)
        else:
            return None

    def _process_expr(self, context, expr):
        node = None

        if isinstance(expr, qlast.BinOpNode):
            left = self._process_expr(context, expr.left)

            is_world_ref = (expr.op == 'in') \
                            and isinstance(expr.right, qlast.PathNode) \
                            and self._is_world_ref(expr.right)

            if not is_world_ref:
                right = self._process_expr(context, expr.right)
                node = ast.BinOp(op=expr.op, left=left, right=right)
                node = self._process_binop(context, node)
            else:
                if isinstance(left, ast.AtomicRef):
                    node = ast.AtomicExistPred(expr=left)
                else:
                    node = ast.ExistPred(expr=left)

        elif isinstance(expr, qlast.PathNode):
            node = self._process_path(context, expr)
        elif isinstance(expr, qlast.ConstantNode):
            node = ast.Constant(value=expr.value)
        elif isinstance(expr, qlast.SequenceNode):
            elements=[self._process_expr(context, e) for e in expr.elements]
            node = ast.Sequence(elements=elements)

        return node

    def _is_world_ref(self, expr):
        return isinstance(expr, qlast.PathNode) \
                and len(expr.steps) == 1 \
                and isinstance(expr.steps[0], qlast.PathStepNode) \
                and expr.steps[0].expr == '%'

    def _process_binop(self, context, expr):

        left = expr.left
        right = expr.right
        left_t = type(expr.left)
        right_t = type(expr.right)
        op = expr.op

        if left is None:
            return right

        if right is None:
            return left

        if left_t == right_t:
            if left_t == ast.AtomicRef:
                left.source.filters.append(expr)
                right.source.filters.append(expr)

                return None
            elif left_t == ast.Constant:
                return self._eval_const_expr(context, expr)

        if left_t == ast.Constant:
            if op in ('and', 'or'):
                return self._eval_const_bool_expr(op, left, right)

        if right_t == ast.Constant:
            if op in ('and', 'or'):
                return self._eval_const_bool_expr(op, right, left)

        if left_t == ast.AtomicRef:
            if right_t == ast.Constant:
                left.source.filters.append(expr)

                return None
            elif right_t == ast.Sequence and op == 'in':
                left.source.filters.append(expr)

                return None
            else:
                raise CaosQLError('invalid binary operator: %s %s %s'
                                        % (type(left), op, type(right)))

        if right_t == ast.AtomicRef:
            if left_t == ast.Constant:
                right.source.filters.append(expr)

                return None
            else:
                raise CaosQLError('invalid binary operator: %s %s %s'
                                        % (type(left), op, type(right)))



        return expr

    def _eval_const_bool_expr(self, op, const, other):
        if op == 'and':
            if not const.value:
                return ast.Constant(value=False)
            else:
                return other
        elif op == 'or':
            if const.value:
                return ast.Constant(value=True)
            else:
                return other

    def _eval_const_expr(self, context, expr):
        expr_t = type(expr)

        if expr_t == ast.BinOp:
            if expr.op == '=':
                op = '=='
            else:
                op = expr.op

            return ast.Constant(value=eval('%r %s %r' % (expr.left.value, op, expr.right.value)))


    def _process_path(self, context, path):
        paths = context.current.graph.paths
        path_recorded = False

        vars = context.current.vars
        pathlen = len(path.steps)

        result = None
        curstep = None

        for i, node in enumerate(path.steps):

            step = ast.EntitySet()

            if isinstance(node, qlast.PathNode):
                if len(node.steps) > 1:
                    raise CaosQLError('unsupported subpath expression')

                var = node.var
                node = self._get_path_tip(node)
                step.concept = self._normalize_concept(node.expr)
                step.name = context.current.genalias(alias=var.name, hint=step.concept)

                vars[step.name] = step

            if isinstance(node, qlast.PathStepNode):

                if node.expr in vars and i == 0:
                    refnode = vars[node.expr]
                    #curstep = ast.EntitySetRef(ptr=refnode, name=refnode.name, concept=refnode.concept)
                    curstep = refnode
                    continue
                else:
                    step.concept = self._normalize_concept(node.expr)
                    step.name = context.current.genalias(hint=step.concept)

                    if node.link_expr:
                        step.link = self._parse_link_expr(node.link_expr.expr)


            if curstep is not None:
                if i == pathlen - 1:
                    aref = self._is_attr_ref(context, curstep, step)
                    if aref:
                        result = aref
                        break

                link = ast.EntityLink(source=curstep, target=step, filter=step.link)
                curstep.links.append(link)
                step.rlinks.append(link)

            if result is None:
                result = curstep

            curstep = step

            if not path_recorded and curstep not in paths:
                paths.append(curstep)
                path_recorded = True

        return result

    def _normalize_concept(self, concept):
        if concept == '%':
            return None
        else:
            return concept

    def _parse_link_expr(self, expr):
        expr_t = type(expr)

        if expr_t == qlast.LinkNode:
            label = expr.name

            if label == '%':
                labels = None
            else:
                labels = [label]

            return ast.EntityLinkSpec(labels=labels, direction=expr.direction)
        elif expr_t == qlast.BinOpNode:
            left = self._parse_link_expr(expr.left)
            right = self._parse_link_expr(expr.right)
            return ast.BinOp(op=expr.op, left=left, right=right)

    def _process_select_targets(self, context, targets):
        selector = list()

        context.current.location = 'selector'
        for target in targets:
            selector.append(self._process_expr(context, target.expr))

        context.current.location = None
        return selector

    def _is_attr_ref(self, context, source, step):
        result = None

        if isinstance(source, ast.EntitySetRef):
            concept = source.ptr.concept
            source = source.ptr
        else:
            concept = source.concept

        if concept is None:
            atoms = ['id']
        else:
            (bases, dct) = backends.meta_backend.load('semantics', concept)
            atoms = dct['atoms']

        if step.concept in atoms:
            result = ast.AtomicRef(source=source, name=step.concept)

            if context.current.location == 'selector':
                source.selrefs.append(result)

        return result

    def _get_path_tip(self, path):
        if len(path.steps) == 0:
            return None

        last = path.steps[-1]

        if isinstance(last, qlast.PathStepNode):
            return last
        else:
            return self._get_path_tip(last)
