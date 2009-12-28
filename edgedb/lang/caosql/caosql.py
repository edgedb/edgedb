import copy
from semantix.caos.name import Name as CaosName
from semantix.caos.caosql import ast
from semantix.caos.caosql.parser import nodes as qlast
from semantix.caos.caosql import CaosQLError


class ParseContextLevel(object):
    def __init__(self, prevlevel=None):
        if prevlevel is not None:
            self.vars = copy.deepcopy(prevlevel.vars)
            self.paths = copy.deepcopy(prevlevel.paths)
            self.namespaces = copy.deepcopy(prevlevel.namespaces)
            self.prefixes = copy.deepcopy(prevlevel.prefixes)
            self.aliascnt = copy.deepcopy(prevlevel.aliascnt)
            self.location = None
        else:
            self.vars = {}
            self.prefixes = {}
            self.aliascnt = {}
            self.paths = []
            self.namespaces = {}
            self.location = None

    def genalias(self, alias=None, hint=None):
        if alias is None:
            if hint is None:
                hint = 'a'

            if hint not in self.aliascnt:
                self.aliascnt[hint] = 1
            else:
                self.aliascnt[hint] += 1

            return '_' + str(hint) + str(self.aliascnt[hint])
        elif alias in self.vars:
            raise CaosQLError('Path var redefinition: %s is already used' %  alias)
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
    def __init__(self, realm):
        self.realm = realm
        self.cls = realm.getfactory()

    def _dump(self, tree):
        if tree is not None:
            print(tree.dump(pretty=True, colorize=True, width=180, field_mask='^(_.*)|(refs)$'))
        else:
            print('None')

    def transform(self, qtree):
        context = ParseContext()
        stree = self._transform_select(context, qtree)

        return stree

    def _transform_select(self, context, tree):
        context.current.graph = ast.GraphExpr()

        if tree.namespaces:
            for ns in tree.namespaces:
                context.current.namespaces[ns.alias] = ns.namespace

        context.current.graph.generator = self._process_select_where(context, tree.where)
        context.current.graph.selector = self._process_select_targets(context, tree.targets)
        context.current.graph.sorter = self._process_sorter(context, tree.orderby)

        return context.current.graph

    def _process_select_where(self, context, where):
        context.current.location = 'generator'

        if where:
            expr = self._process_expr(context, where.expr)
            if isinstance(expr, ast.AtomicRef):
                expr.ref().filters.append(expr.expr)
                expr = None
            return expr
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
                elif left:
                    node = None #ast.ExistPred(expr=left)

        elif isinstance(expr, qlast.PathNode):
            node = self._process_path(context, expr)
        elif isinstance(expr, qlast.ConstantNode):
            node = ast.Constant(value=expr.value)
        elif isinstance(expr, qlast.SequenceNode):
            elements=[self._process_expr(context, e) for e in expr.elements]
            node = ast.Sequence(elements=elements)
        elif isinstance(expr, qlast.CallFunctionNode):
            args = [self._process_expr(context, a) for a in expr.args]
            node = ast.FunctionCall(name=expr.func, args=args)

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
                if left.refs == right.refs:
                    return ast.AtomicRef(expr=expr, refs=left.refs)
                else:
                    return expr
            elif left_t == ast.Constant:
                return self._eval_const_expr(context, expr)
            elif left_t == ast.EntitySet and op in ('=', '!='):
                """
                Reference to entity is equivalent to it's id reference
                """
                expr.left = ast.AtomicRef(refs={expr.left}, name='id')
                expr.right = ast.AtomicRef(refs={expr.right}, name='id')

        if left_t == ast.Constant:
            if op in ('and', 'or'):
                return self._eval_const_bool_expr(op, left, right)

        if right_t == ast.Constant:
            if op in ('and', 'or'):
                return self._eval_const_bool_expr(op, right, left)

        if left_t == ast.AtomicRef:
            if right_t == ast.Constant:
                if context.current.location == 'generator':
                    left.ref().filters.append(expr)
                    return None
            elif right_t == ast.Sequence and op == 'in':
                if context.current.location == 'generator':
                    left.ref().filters.append(expr)
                    return None
            else:
                raise CaosQLError('invalid binary operator: %s %s %s'
                                        % (type(left), op, type(right)))

        if right_t == ast.AtomicRef:
            if left_t == ast.Constant:
                if context.current.location == 'generator':
                    right.ref().filters.append(expr)
                    return None
            elif left_t == ast.BinOp:
                pass
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
        prefixes = context.current.prefixes
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

                if i == pathlen - 1 and self._is_attr_ref(context, curstep, node.expr):
                    step.atom = node.expr
                    hint = str(curstep.concept) + '.' + node.expr
                else:
                    step.concept = self._normalize_concept(context, node.expr, node.namespace)
                    if step.concept:
                        hint = step.concept.name
                    else:
                        hint = None

                step.id = step.name = context.current.genalias(alias=var.name, hint=hint)

                vars[step.name] = step


            if isinstance(node, qlast.PathStepNode):

                if node.expr in vars and (i == 0 or node.epxr.beginswith('#')):
                    refnode = vars[node.expr]
                    curstep = refnode
                    continue
                else:
                    if i == pathlen - 1 and self._is_attr_ref(context, curstep, node.expr):
                        step.atom = node.expr
                        hint = str(curstep.concept) + '.' + node.expr
                    else:
                        step.concept = self._normalize_concept(context, node.expr, node.namespace)
                        if step.concept:
                            hint = step.concept.name
                        else:
                            hint = None

                    if not step.id:
                        step.id = str(step.concept.name) if step.concept is not None else '_entity'

                    step.name = context.current.genalias(hint=hint)

                    if node.link_expr:
                        step.link = self._parse_link_expr(node.link_expr.expr)

            if curstep is not None:
                step.id = curstep.id + ':' + step.id

                if step.atom:
                    result = self._get_attr_ref(context, curstep, step)
                    break

            if step.id in prefixes:
                curstep = prefixes[step.id]
                continue

            prefixes[step.id] = step

            if curstep is not None:
                link = ast.EntityLink(source=curstep, target=step, filter=step.link)
                curstep.links.append(link)
                step.rlinks.append(link)



            if result is None:
                result = curstep

            curstep = step

            if not path_recorded and curstep not in paths:
                paths.append(curstep)
                path_recorded = True

        if result is None:
            result = curstep

        return result

    def _normalize_concept(self, context, concept, namespace):
        if concept == '%':
            return None
        else:
            concept = self.realm.meta.get(name=concept,
                                          module_aliases=context.current.namespaces)
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
            expr = self._process_expr(context, target.expr)
            t = ast.SelectorExpr(expr=expr, name=target.alias)
            selector.append(t)

        context.current.location = None
        return selector

    def _is_attr_ref(self, context, source, expr):
        atoms = ['id']
        if source.concept:
            atoms += [n for n, v in source.concept.links.items() if v.atomic()]

        return expr in atoms

    def _get_attr_ref(self, context, source, step):
        result = ast.AtomicRef(refs={source}, name=step.atom)

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

    def _process_sorter(self, context, sorters):
        context.current.location = 'sorter'

        result = []

        for sorter in sorters:
            s = ast.SortExpr(expr=self._process_expr(context, sorter.path), direction=sorter.direction)
            result.append(s)

        return result
