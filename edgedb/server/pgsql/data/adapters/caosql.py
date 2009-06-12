from copy import copy, deepcopy
from semantix.caos.query import CaosQLError
from semantix.caos.caosql import ast as caosast
from semantix.caos.backends.pgsql import ast as sqlast
from semantix.caos.backends.pgsql import codegen as sqlgen
from semantix.ast.visitor import NodeVisitor
from semantix.utils.debug import debug, highlight

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
            self.concept_node_map = copy.deepcopy(prevlevel.concept_node_map)
            self.location = 'query'
        else:
            self.vars = {}
            self.ctes = {}
            self.aliascnt = {}
            self.ctemap = {}
            self.concept_node_map = {}
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
    @debug
    def adapt(self, query, vars=None):
        # Transform to sql tree
        qtree = self._transform_tree(query)

        """LOG [caos.query] SQL Tree
        self._dump(qtree)
        """

        # Generate query text
        qtext = sqlgen.SQLSourceGenerator.to_source(qtree)

        """LOG [caos.query] SQL Query
        print(highlight(qtext, 'sql'))
        """

        return Query(qtext, vars)

    def _dump(self, tree):
        print(tree.dump(pretty=True, colorize=True, width=180, field_mask='^(_.*)$'))

    def _transform_tree(self, tree):

        context = ParseContext()
        context.current.query = sqlast.SelectQueryNode()

        self._process_paths(context, tree.paths)
        self._process_generator(context, tree.generator)
        self._process_selector(context, tree.selector)
        self._process_sorter(context, tree.sorter)

        return context.current.query

    def _process_generator(self, context, generator):
        query = context.current.query
        query.where = self._process_expr(context, generator)

    def _process_selector(self, context, selector):
        query = context.current.query

        context.current.location = 'selector'
        for expr in selector:
            target = sqlast.SelectExprNode(expr=self._process_expr(context, expr.expr), alias=expr.name)
            query.targets.append(target)

    def _process_sorter(self, context, sorter):
        query = context.current.query
        context.current.location = 'sorter'

        for expr in sorter:
            sortexpr = sqlast.SortExprNode(expr=self._process_expr(context, expr.expr),
                                           direction=expr.direction)
            query.orderby.append(sortexpr)

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

        elif expr_t == caosast.EntitySet:
            self._process_graph(context, context.current.query, expr)

        elif expr_t == caosast.BinOp:
            left = self._process_expr(context, expr.left)
            right = self._process_expr(context, expr.right)
            result = sqlast.BinOpNode(op=expr.op, left=left, right=right)

        elif expr_t == caosast.Constant:
            result = sqlast.ConstantNode(value=expr.value)

        elif expr_t == caosast.Sequence:
            elements = [self._process_expr(context, e) for e in expr.elements]
            result = sqlast.SequenceNode(elements=elements)

        elif expr_t == caosast.FunctionCall:
            args = [self._process_expr(context, a) for a in expr.args]
            result = sqlast.FunctionCallNode(name=expr.name, args=args)

        elif expr_t == caosast.AtomicRef:
            if isinstance(expr.ref(), caosast.EntitySet):
                fieldref = context.current.concept_node_map[expr.ref()]
            else:
                fieldref = expr.ref()

            datatable = None

            if isinstance(fieldref, sqlast.FieldRefNode):
                if fieldref.field == expr.name:
                    return fieldref
                else:
                    datatable = fieldref.table

            if isinstance(fieldref, sqlast.SelectExprNode):
                fieldref_expr = fieldref.expr
            else:
                fieldref_expr = fieldref

            if expr.expr is not None:
               return self._process_expr(context, expr.expr)

            if context.current.location in ('selector', 'sorter'):
                if expr.name == 'id':
                    result = fieldref_expr
                else:
                    if not datatable:
                        query = context.current.query
                        table_name = expr.ref().concept + '_data'
                        datatable = sqlast.TableNode(name=table_name,
                                                     concept=expr.ref().concept,
                                                     alias=context.current.genalias(hint=table_name))
                        query.fromlist.append(datatable)

                        left = fieldref_expr
                        right = sqlast.FieldRefNode(table=datatable, field='entity_id')
                        whereexpr = sqlast.BinOpNode(op='=', left=left, right=right)
                        if query.where is not None:
                            query.where = sqlast.BinOpNode(op='and', left=query.where, right=whereexpr)
                        else:
                            query.where = whereexpr

                    result = sqlast.FieldRefNode(table=datatable, field=expr.name)
                    context.current.concept_node_map[expr.ref()] = result
            else:
                if isinstance(expr.ref(), caosast.EntitySet):
                    result = fieldref_expr
                else:
                    result = sqlast.FieldRefNode(table=fieldref, field=expr.name)

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
        # Avoid processing the same subgraph more than once
        if startnode in context.current.ctemap:
            return

        fromnode = sqlast.FromExprNode()
        cte.fromlist.append(fromnode)

        fromnode.expr = self._get_step_cte(context, cte, startnode, None, None)

        self._process_path(context, cte, fromnode, fromnode.expr, startnode)

    def _simple_join(self, context, left, right, bond='entity'):
        condition = sqlast.BinOpNode(op='=', left=left._bonds[bond][1],
                                             right=right._bonds[bond][0])

        join = sqlast.JoinNode(type='inner', left=left, right=right, condition=condition)
        join._bonds[bond] = (right._bonds[bond][1], right._bonds[bond][1])

        return join

    def _get_step_cte(self, context, cte, step, joinpoint, link):
        """
        Generates a Common Table Expression for a given step in the path

        @param context: parse context
        @param cte: parent CTE
        @param step: CaosQL path step expression
        @param joinpoint: current position in parent CTE join chain
        """

        # Avoid processing the same step twice
        if step in context.current.ctemap:
            return context.current.ctemap[step]

        step_cte = sqlast.SelectQueryNode(name=step.name, concept=step.concept,
                                          alias=context.current.genalias(alias=step.name))
        context.current.ctemap[step] = step_cte

        fromnode = sqlast.FromExprNode()

        source = None
        concept_table = None

        if step.concept is None:
            table_name = 'entity'
            field_name = 'id'
        else:
            table_name = step.concept + '_data'
            field_name = 'entity_id'

        concept_table = sqlast.TableNode(name=table_name,
                                         concept=step.concept,
                                         alias=context.current.genalias(hint=table_name))
        bond = sqlast.FieldRefNode(table=concept_table, field=field_name)
        concept_table._bonds['entity'] = (bond, bond)

        if joinpoint is None:
            fromnode.expr = concept_table
        else:
            #
            # Append the step to the join chain taking link filter into account
            #
            map = sqlast.TableNode(name='entity_map', concept=step.concept,
                                   alias=context.current.genalias(hint='map'))

            if link.filter:
                if link.filter.direction == link.filter.BACKWARD:
                    source_fld = 'target_id'
                    target_fld = 'source_id'
                else:
                    source_fld = 'source_id'
                    target_fld = 'target_id'
            map._bonds['entity'] = (sqlast.FieldRefNode(table=map, field=source_fld),
                                    sqlast.FieldRefNode(table=map, field=target_fld))

            join = self._simple_join(context, joinpoint, map)

            if link.filter and link.filter.labels:
                expr = self._select_link_types(context, map, link.filter.labels)
                if step_cte.where is not None:
                    step_cte.where = sqlast.BinOpNode(op='and', left=where, right=expr)
                else:
                    step_cte.where = expr

            join = self._simple_join(context, join, concept_table)

            fromnode.expr = join

            #
            # Pull the references to fields inside the CTE one level up to keep
            # them visible.
            #
            for concept_node, ref in context.current.concept_node_map.items():
                if ref.alias in joinpoint.concept_node_map:
                    refexpr = sqlast.FieldRefNode(table=joinpoint, field=ref.alias)
                    fieldref = sqlast.SelectExprNode(expr=refexpr, alias=ref.alias)
                    step_cte.targets.append(fieldref)
                    step_cte.concept_node_map[ref.alias] = fieldref
                    context.current.concept_node_map[concept_node].expr.table = step_cte

        # Include target entity id in the Select expression list ...
        fieldref = fromnode.expr._bonds['entity'][1]
        selectnode = sqlast.SelectExprNode(expr=fieldref, alias=step_cte.alias + '_entity_id')
        step_cte.targets.append(selectnode)
        step_cte.concept_node_map[selectnode.alias] = selectnode

        # ... and record in in global map in case it has to be pulled up later
        refexpr = sqlast.FieldRefNode(table=step_cte, field=selectnode.alias)
        selectnode = sqlast.SelectExprNode(expr=refexpr, alias=selectnode.alias)
        context.current.concept_node_map[step] = selectnode

        if step.filters:
            def filter_atomic_refs(node):
                return isinstance(node, caosast.AtomicRef) # and node.refs[0] == step

            for f in step.filters:
                filter = deepcopy(f)

                # Fixup atomic refs sources
                atrefs = self.find_children(filter, filter_atomic_refs)

                if atrefs:
                    for atref in atrefs:
                        atref.refs.pop()
                        atref.refs.add(concept_table)

                expr = sqlast.PredicateNode(expr=self._process_expr(context, filter))
                if step_cte.where is not None:
                    step_cte.where = sqlast.BinOpNode(op='and', left=step_cte.where, right=expr)
                else:
                    step_cte.where = expr

        step_cte.fromlist.append(fromnode)
        step_cte._source_graph = step

        bond = sqlast.FieldRefNode(table=step_cte, field=step_cte.alias + '_entity_id')
        step_cte._bonds['entity'] = (bond, bond)

        return step_cte

    def _select_link_types(self, context, map, labels):
        select = sqlast.SelectQueryNode()

        entity_1 = sqlast.TableNode(name='entity', alias=context.current.genalias(hint='entity'))
        entity_2 = sqlast.TableNode(name='entity', alias=context.current.genalias(hint='entity'))
        concept_map = sqlast.TableNode(name='concept_map', alias=context.current.genalias(hint='concept_map'))

        select.fromlist = [entity_1, entity_2, concept_map]

        left = sqlast.FieldRefNode(table=map, field='source_id')
        right = sqlast.FieldRefNode(table=entity_1, field='id')
        where = sqlast.BinOpNode(op='=', left=left, right=right)

        left = sqlast.FieldRefNode(table=map, field='target_id')
        right = sqlast.FieldRefNode(table=entity_2, field='id')
        condition = sqlast.BinOpNode(op='=', left=left, right=right)
        where = sqlast.BinOpNode(op='and', left=where, right=condition)

        left = sqlast.FieldRefNode(table=entity_1, field='concept_id')
        right = sqlast.FieldRefNode(table=concept_map, field='source_id')
        condition = sqlast.BinOpNode(op='=', left=left, right=right)
        where = sqlast.BinOpNode(op='and', left=where, right=condition)

        left = sqlast.FieldRefNode(table=entity_2, field='concept_id')
        right = sqlast.FieldRefNode(table=concept_map, field='target_id')
        condition = sqlast.BinOpNode(op='=', left=left, right=right)
        where = sqlast.BinOpNode(op='and', left=where, right=condition)

        left = sqlast.FieldRefNode(table=concept_map, field='link_type')
        right = sqlast.SequenceNode()
        for label in labels:
            right.elements.append(sqlast.ConstantNode(value=label))
        condition = sqlast.BinOpNode(op='in', left=left, right=right)
        where = sqlast.BinOpNode(op='and', left=where, right=condition)

        select.where = where
        selexpr = sqlast.FieldRefNode(table=concept_map, field='id')
        select.targets = [sqlast.SelectExprNode(expr=selexpr, alias='id')]

        left = sqlast.FieldRefNode(table=map, field='link_type_id')
        right = select

        return sqlast.BinOpNode(op='in', left=left, right=right)

    def _process_path(self, context, cte, fromnode, joinpoint, pathtip):
        jp = joinpoint

        for link in pathtip.links:
            join = self._get_step_cte(context, cte, link.target, jp, link)
            fromnode.expr = join
            self._process_path(context, cte, fromnode, join, link.target)

            # This is here to preserve the original joinpoint bond field
            # to produce correct branch join
            #
            # XXX: do something about that deepcopy
            joinpoint_bond_fields = (jp._bonds['entity'][0].field, jp._bonds['entity'][1].field)
            jp = deepcopy(join)
            (jp._bonds['entity'][0].field, jp._bonds['entity'][1].field) = joinpoint_bond_fields
