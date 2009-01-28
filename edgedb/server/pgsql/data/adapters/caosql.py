from copy import deepcopy
from semantix.parsers import caosql
from semantix.parsers.caosql import nodes as caosast
from semantix.caos import ConceptClass
from semantix.caos.query import CaosQLCursor, CaosQLError
from semantix.caos.backends.pgsql import ast as sqlast

class Query(object):
    def __init__(self, text, vars=None, context=None):
        self.text = text
        self.vars = vars
        self.context = context

class ParseContextLevel(object):
    def __init__(self, prevlevel=None):
        if prevlevel is not None:
            self.vars = copy.deepcopy(prevlevel.vars)
            self.ctes = copy.deepcopy(prevlevel.ctes)
            self.aliascnt = prevlevel.aliascnt
        else:
            self.vars = {}
            self.ctes = {}
            self.aliascnt = 0

    def genalias(self, alias=None):
        if alias is None:
            self.aliascnt += 1
            return '__a' + str(self.aliascnt)
        elif alias in self.ctes:
            raise CaosQLError('Path var redefinition: % is already used' %  alias)
        else:
            return alias

class ParseContext(object):
    stack = []

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


class CaosQLQueryAdapter(object):
    def __init__(self):
        self.parser = caosql.CaosQLParser()

    def adapt(self, query, vars=None):
        print(query)

        xtree = self.parser.parse(query)
        ntree = CaosQLCursor.normalize_query(xtree)
        qtree = self._transform_tree(xtree)
        qtext = self._render_query(qtree)

        return Query(qtext, vars)

    def _dump(self, tree):
        print(tree.dump(pretty=True, colorize=True, width=150, field_mask='^_'))

    def _transform_tree(self, xtree):

        qtree = deepcopy(xtree)
        nodetype = type(qtree)

        if nodetype != caosast.SelectQueryNode:
            selnode = caosast.SelectQueryNode()
            selnode.targets = [qtree]
            qtree = selnode

        context = ParseContext()

        context.push()
        stree = self._transform_select(context, qtree)

        self._dump(qtree)
        self._dump(stree)

        return stree

    def _transform_select(self, context, tree):
        context.current.select = sqlast.SelectQueryNode()

        self._process_select_where(context, tree.where)
        self._process_select_targets(context, tree.targets)

        return context.current.select

    def _process_select_where(self, context, where):
        self._process_expr(context, where.expr)

    def _process_expr(self, context, expr):
        op = None

        if isinstance(expr, caosast.BinOpNode):
            is_world_ref = (expr.op == 'in') \
                            and isinstance(expr.right, caosast.PathNode) \
                            and self._is_world_ref(expr.right)

            if is_world_ref:
                op = sqlast.ConstantNode(value=True)
            else:
                left = self._process_expr(context, expr.left)
                right = self._process_expr(context, expr.right)
                op = sqlast.BinOpNode(op=expr.op, left=left, right=right)

        elif isinstance(expr, caosast.PathNode):
            op = self._process_path(context, expr)

        elif isinstance(expr, caosast.ConstantNode):
            op = sqlast.ConstantNode(value=expr.value)

        return op

    def _is_world_ref(self, expr):
        return isinstance(expr, caosast.PathNode) \
                and len(expr.steps) == 1 \
                and isinstance(expr.steps[0], caosast.PathStepNode) \
                and expr.steps[0].expr == '#'


    def _process_path(self, context, path):
        query = context.current.select
        vars = context.current.vars
        pathlen = len(path.steps)

        curnode = None

        for i, node in enumerate(path.steps):
            if isinstance(node, caosast.PathStepNode):
                name = node.expr

                if i == 0 and name in vars:
                    if pathlen == 1:
                        """
                        Reference to an entity id
                        """
                        


                self._addpath(query, curnode, node)
                curnode = node

            elif isinstance(node, caosast.PathNode):
                self._process_path(context, node)
                curnode = self._get_path_tip(node)

        if path.var is not None:
            cte_name = context.current.genalias(path.var.name)
        else:
            cte_name = context.current.genalias()

        path_cte.name = cte_name

        if len(path_cte.targets) == 0:
            if isinstance(path_cte.fromlist[0], sqlast.JoinNode):
                table = path_cte.fromlist[0].right
            else:
                table = path_cte.fromlist[0]

            if table.name == 'entity_map':
                right = sqlast.TableNode(name=node.expr)
                right._bonds['entity'] = ('entity_id', 'entity_id')

                if len(path_cte.fromlist) > 0:
                    path_cte.fromlist[0] = self._join(path_cte.fromlist[0], 'entity', 'left', right)
                else:
                    path_cte.fromlist = [right]

                table = right

            target = sqlast.FieldRefNode(table=table, field='*')
            path_cte.targets.append(target)

        context.current().ctes[path_cte.name] = path_cte
        context.current().select.ctes.append(path_cte)

        return path_cte

    def _process_select_targets(self, context, targets):
        pass

    def _get_path_tip(self, path):
        if len(path.steps) == 0:
            return None

        last = path.steps[-1]

        if isinstance(last, caosast.PathStepNode):
            return last
        else:
            return self._get_path_tip(last)

    def _addpath(self, path_cte, tip, new):
        if len(path_cte.fromlist) > 0:
            left = path_cte.fromlist[0]
            srcc = ConceptClass(tip.expr)

            if (None, new.expr) in srcc.links:
                table = sqlast.TableNode(name='entity_map')
                table._bonds['entity'] = ('source_id', 'target_id')
                join = self._join(left, 'entity', 'left', table)
                path_cte.fromlist[0] = join
            elif new.expr in srcc.attributes:
                path_cte.targets.append(sqlast.FieldRefNode(table=tip.expr, field=new.expr))
            else:
                raise CaosQLError('there is no path between %s and %s' % (tip.expr, new.expr))
        else:
            table = sqlast.TableNode(name=new.expr)
            table._bonds['entity'] = ('entity_id', 'entity_id')
            path_cte.fromlist = [table]

    def _join(self, join, bond, type, table):
        condition = sqlast.BinOpNode(op='=', left=sqlast.FieldRefNode(table='#left',
                                                                      field=join._bonds[bond][1]),
                                             right=sqlast.FieldRefNode(table='#right',
                                                                       field=table._bonds[bond][0]))
        join = sqlast.JoinNode(type=type, left=join, right=table, condition=condition)
        join._bonds[bond] = (table._bonds[bond][1], table._bonds[bond][1])
        return join


    def _transform_select_targets(self, context, targets):
        pass

    def _render_query(self, qtree):
        return 'dummy'
