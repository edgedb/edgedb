from copy import deepcopy
from semantix.parsers import caosql
from semantix.caos.query import CaosQLCursor, CaosQLError
from semantix.caos.caosql import ast as caosast
from semantix.caos.backends.pgsql import ast as sqlast
from semantix.caos.backends.pgsql import codegen as sqlgen
from semantix.caos.caosql.caosql import CaosqlTreeTransformer
from semantix.ast.visitor import NodeVisitor

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
            self.aliascnt = copy.deepcopy(prevlevel.aliascnt)
            self.ctemap = copy.deepcopy(prevlevel.ctemap)
            self.location = 'query'
        else:
            self.vars = {}
            self.ctes = {}
            self.aliascnt = {}
            self.ctemap = {}
            self.location = 'query'

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


class CaosQLQueryAdapter(NodeVisitor):
    def __init__(self):
        super().__init__()
        self.parser = caosql.CaosQLParser()

    def adapt(self, query, vars=None):

        print('-' * 70)
        print('CaosQL query:')
        print('-' * 70)
        print(query)

        transformer = CaosqlTreeTransformer()

        # Parse and normalize
        xtree = self.parser.parse(query)
        ntree = CaosQLCursor.normalize_query(xtree)

        print('-' * 70)
        print('CaosQL tree:')
        print('-' * 70)
        self._dump(ntree)

        # Transform to caos tree
        qtree = transformer.transform(ntree)

        print('-' * 70)
        print('Caos tree:')
        print('-' * 70)
        self._dump(qtree)

        # Transform to sql tree
        qtree = self._transform_tree(qtree)

        print('-' * 70)
        print('SQL tree:')
        print('-' * 70)
        self._dump(qtree)

        # Generate query text
        qtext = sqlgen.SQLSourceGenerator.to_source(qtree)
        print('-' * 70)
        print('SQL query:')
        print('-' * 70)
        print(qtext)

        return Query(qtext, vars)

    def _dump(self, tree):
        print(tree.dump(pretty=True, colorize=True, width=180, field_mask='^_'))

    def _transform_tree(self, tree):

        context = ParseContext()
        context.current.query = sqlast.SelectQueryNode()

        self._process_paths(context, tree.paths)
        self._process_generator(context, tree.generator)
        self._process_selector(context, tree.selector)

        return context.current.query

    def _process_generator(self, context, generator):
        query = context.current.query
        query.where = self._process_expr(context, generator)

    def _process_selector(self, context, selector):
        query = context.current.query

        context.current.location = 'selector'
        for expr in selector:
            target = sqlast.SelectExprNode(expr=self._process_expr(context, expr))
            query.targets.append(target)

    def _process_paths(self, context, paths):
        query = context.current.query

        for path in paths:
            expr = self._process_expr(context, path)
            if expr:
                query.fromlist.append(expr)

    def _process_expr(self, context, expr):
        result = None

        expr_t = type(expr)

        if expr_t == caosast.ExistPred:
            result = self._process_expr(context, expr.expr)

        elif expr_t in (caosast.EntitySet, caosast.EntitySetRef):
            self._process_graph(context, context.current.query, expr)

        elif expr_t == caosast.BinOp:
            left = self._process_expr(context, expr.left)
            right = self._process_expr(context, expr.right)
            result = sqlast.BinOpNode(op=expr.op, left=left, right=right)

        elif expr_t == caosast.Constant:
            result = sqlast.ConstantNode(value = expr.value)

        elif expr_t == caosast.AtomicRef:
            if isinstance(expr.source, caosast.EntitySet):
                table = context.current.ctemap[expr.source]
            else:
                table = expr.source

            result = sqlast.FieldRefNode(table=table, field=expr.name)

        return result

    def _attr_in_table(self, context, table, attr):
        if isinstance(table, sqlast.TableNode):
            """
            It's either "entity_map" or one of the "*_data" tables.
            Entity data table is guaranteed to have the attr here, but entity map
            obviously can only yield ids.
            """
            return table.name == 'entity_map'
        elif isinstance(table, sqlast.SelectQueryNode):
            """
            For Select we check the aliases
            """
            for target in table.targets:
                if target.alias == attr:
                    return True

        return False

    def _process_graph(self, context, cte, startnode):
        fromnode = sqlast.FromExprNode()
        cte.fromlist.append(fromnode)

        first = None

        if isinstance(startnode, caosast.EntitySetRef):
            first = startnode.ptr
        else:
            first = startnode

        fromnode.expr = self._get_step_cte(context, cte, first, None, None)

        self._process_path(context, cte, fromnode, fromnode.expr, startnode)

    def _simple_join(self, context, left, right, bond='entity'):
        condition = sqlast.BinOpNode(op='=', left=left._bonds[bond][1],
                                             right=right._bonds[bond][0])

        join = sqlast.JoinNode(type='inner', left=left, right=right, condition=condition)
        join._bonds[bond] = (right._bonds[bond][1], right._bonds[bond][1])

        return join

    def _get_step_cte(self, context, cte, step, joinpoint, link):
        if step in context.current.ctemap:
            return context.current.ctemap[step]

        step_cte = sqlast.SelectQueryNode(name=step.name, concept=step.concept,
                                          alias=context.current.genalias(alias=step.name))
        context.current.ctemap[step] = step_cte

        fromnode = sqlast.FromExprNode()

        source = None
        concept_table = None

        if joinpoint is None or len(step.filters) > 0 or len(step.selrefs) > 0:
            concept_table = sqlast.TableNode(name=step.concept + '_data',
                                             concept=step.concept,
                                             alias=context.current.genalias(hint=step.concept))
            bond = sqlast.FieldRefNode(table=concept_table, field='entity_id')
            concept_table._bonds['entity'] = (bond, bond)

        if joinpoint is not None:
            map = sqlast.TableNode(name='entity_map', concept=step.concept,
                                   alias=context.current.genalias(hint='map'))
            map._bonds['entity'] = (sqlast.FieldRefNode(table=map, field='source_id'),
                                    sqlast.FieldRefNode(table=map, field='target_id'))

            join = self._simple_join(context, joinpoint, map)

            if concept_table is not None:
                join = self._simple_join(context, join, concept_table)

            fromnode.expr = join
        else:
            fromnode.expr = concept_table

        fieldref = fromnode.expr._bonds['entity'][1]
        selectnode = sqlast.SelectExprNode(expr=fieldref, alias='entity_id')
        step_cte.targets.append(selectnode)

        for selref in step.selrefs:
            fieldref = sqlast.FieldRefNode(table=concept_table, field=selref.name)
            selectnode = sqlast.SelectExprNode(expr=fieldref, alias=selref.name)
            step_cte.targets.append(selectnode)

        if step.filters:
            def filter_atomic_refs(node):
                return isinstance(node, caosast.AtomicRef) and node.source == step

            for filter in step.filters:
                # Fixup atomic refs sources
                atrefs = self.find_children(filter, filter_atomic_refs)
                if atrefs:
                    for atref in atrefs:
                        atref.source = concept_table

                step_cte.where = sqlast.PredicateNode(expr=self._process_expr(context, filter))

        step_cte.fromlist.append(fromnode)
        step_cte._source_graph = step

        bond = sqlast.FieldRefNode(table=step_cte, field='entity_id')
        step_cte._bonds['entity'] = (bond, bond)

        return step_cte

    def _process_path(self, context, cte, fromnode, joinpoint, pathtip):
        for link in pathtip.links:
            join = self._get_step_cte(context, cte, link.target, joinpoint, link)
            fromnode.expr = join
            self._process_path(context, cte, fromnode, join, link.target)
