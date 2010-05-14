##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from semantix.utils import ast
from semantix.caos import caosql, tree
from semantix.caos import types as caos_types
from semantix.caos.backends import pgsql
from semantix.caos.backends.pgsql import common
from semantix.utils.debug import debug
from semantix.utils.datastructures import OrderedSet


from . import delta as delta_cmds


class Alias(str):
    def __new__(cls, value=''):
        return super(Alias, cls).__new__(cls, pgsql.common.caos_name_to_pg_colname(value))

    def __add__(self, other):
        return Alias(super().__add__(other))

    def __radd__(self, other):
        return Alias(str(other) + str(self))

    __iadd__ = __add__


class TransformerContextLevel(object):
    def __init__(self, prevlevel=None):
        if prevlevel is not None:
            self.vars = prevlevel.vars.copy()
            self.ctes = prevlevel.ctes.copy()
            self.aliascnt = prevlevel.aliascnt.copy()
            self.ctemap = prevlevel.ctemap.copy()
            self.concept_node_map = prevlevel.concept_node_map.copy()
            self.argmap = prevlevel.argmap
            self.location = 'query'
            self.append_graphs = False
            self.ignore_cardinality = prevlevel.ignore_cardinality
            self.query = prevlevel.query
            self.realm = prevlevel.realm
        else:
            self.vars = {}
            self.ctes = {}
            self.aliascnt = {}
            self.ctemap = {}
            self.concept_node_map = {}
            self.argmap = OrderedSet()
            self.location = 'query'
            self.append_graphs = False
            self.ignore_cardinality = False
            self.query = pgsql.ast.SelectQueryNode()
            self.realm = None

    def genalias(self, alias=None, hint=None):
        if alias is None:
            if hint is None:
                hint = 'a'

            if hint not in self.aliascnt:
                self.aliascnt[hint] = 1
            else:
                self.aliascnt[hint] += 1

            alias = hint + str(self.aliascnt[hint])
        elif alias in self.vars:
            raise caosql.CaosQLError('Path var redefinition: % is already used' %  alias)

        return Alias(alias)


class TransformerContext(object):
    CURRENT, ALTERNATE, NEW = range(0, 3)

    def __init__(self):
        self.stack = []
        self.push()

    def push(self, mode=None):
        level = TransformerContextLevel(self.current)

        if mode == TransformerContext.ALTERNATE:
            pass

        self.stack.append(level)

        return level

    def pop(self):
        self.stack.pop()

    def __call__(self, mode=None):
        if not mode:
            mode = TransformerContext.CURRENT
        return TransformerContextWrapper(self, mode)

    def _current(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    current = property(_current)


class TransformerContextWrapper(object):
    def __init__(self, context, mode):
        self.context = context
        self.mode = mode

    def __enter__(self):
        if self.mode == TransformerContext.CURRENT:
            return self.context
        else:
            self.context.push(self.mode)
            return self.context

    def __exit__(self, exc_type, exc_value, traceback):
        if self.mode != TransformerContext.CURRENT:
            self.context.pop()


class CaosTreeTransformer(ast.visitor.NodeVisitor):
    @debug
    def transform(self, query, realm):
        # Transform to sql tree
        qtree, argmap = self._transform_tree(query, realm)

        """LOG [caos.query] SQL Tree
        self._dump(qtree)
        """

        # Generate query text
        qtext = pgsql.codegen.SQLSourceGenerator.to_source(qtree)

        """LOG [caos.query] SQL Query
        from semantix.utils.debug import highlight
        print(highlight(qtext, 'sql'))
        """

        return qtext, argmap

    def _dump(self, tree):
        print(tree.dump(pretty=True, colorize=True, width=180, field_mask='^(_.*|caosnode)$'))

    def _transform_tree(self, tree, realm):

        context = TransformerContext()

        context.current.realm = realm
        expr = self._process_generator(context, tree.generator)
        if getattr(expr, 'aggregates', False):
            context.current.query.having = expr
        else:
            context.current.query.where = expr

        self._process_selector(context, tree.selector, context.current.query)
        self._process_sorter(context, tree.sorter)

        self._process_groupby(context, tree.grouper)

        if tree.offset:
            context.current.query.offset = self._process_constant(context, tree.offset)

        if tree.limit:
            context.current.query.limit = self._process_constant(context, tree.limit)

        if tree.op in ('update', 'delete'):
            if tree.op == 'delete':
                query = pgsql.ast.DeleteQueryNode()
            else:
                query = pgsql.ast.UpdateQueryNode()

            # Standard entity set processing produces a whole CTE, while for UPDATE and DELETE
            # we need just the origin table.  Thus, use a dummy CTE here and repace the op's
            # fromexpr with a direct reference to a table
            #
            query.fromexpr = self._relation_from_concepts(context, tree.optarget)
            context.current.concept_node_map[tree.optarget] = {'data': query.fromexpr}

            idref = pgsql.ast.FieldRefNode(table=query.fromexpr, field='semantix.caos.builtins.id',
                                           origin=query.fromexpr,
                                           origin_field='semantix.caos.builtins.id')
            query.where = pgsql.ast.BinOpNode(left=idref, op='IN', right=context.current.query)
            self._process_selector(context, tree.opselector, query)

            if tree.op == 'update':
                for expr in tree.opvalues:
                    field = self._process_expr(context, expr.expr)
                    value = self._process_expr(context, expr.value)
                    query.values.append(pgsql.ast.UpdateExprNode(expr=field, value=value))
        else:
            query = context.current.query

        self._postprocess_query(query)

        return query, context.current.argmap

    def _postprocess_query(self, query):
        ctes = set(ast.find_children(query, lambda i: isinstance(i, pgsql.ast.SelectQueryNode)))
        for cte in ctes:
            if cte.where_strong:
                cte.where = self.extend_predicate(cte.where, cte.where_strong, ast.ops.AND)
            if cte.where_weak:
                cte.where = self.extend_predicate(cte.where, cte.where_weak, ast.ops.OR)

    def _process_generator(self, context, generator):
        context.current.location = 'generator'
        result = self._process_expr(context, generator)
        if isinstance(result, pgsql.ast.IgnoreNode):
            result = None
        context.current.location = None
        return result

    def _process_selector(self, context, selector, query):
        context.current.location = 'selector'
        for expr in selector:
            target = pgsql.ast.SelectExprNode(expr=self._process_expr(context, expr.expr),
                                              alias=expr.name)
            query.targets.append(target)

    def _process_sorter(self, context, sorter):
        query = context.current.query
        context.current.location = 'sorter'

        for expr in sorter:
            sortexpr = pgsql.ast.SortExprNode(expr=self._process_expr(context, expr.expr),
                                              direction=expr.direction)
            query.orderby.append(sortexpr)

    def _process_groupby(self, context, grouper):
        query = context.current.query
        context.current.location = 'grouper'

        for expr in grouper:
            sortexpr = self._process_expr(context, expr)
            query.groupby.append(sortexpr)

    def get_caos_path_root(self, expr):
        result = expr
        while result.rlink:
            result = result.rlink.source
        return result

    def is_universal_set(self, expr):
        """Determine whether the given expression represents the universal set of entities.

        Arguments:
            - expr: Expression to test

        Return:
            True if the expression represents a universal set, False otherwise.
        """

        if isinstance(expr, tree.ast.PathCombination):
            if len(expr.paths) == 1:
                expr = next(iter(expr.paths))
            else:
                expr = None

        if isinstance(expr, tree.ast.EntitySet):
            return expr.concept.name == 'semantix.caos.builtins.BaseObject'

        return False

    def is_entity_set(self, expr):
        """Determine whether the given expression represents a set of entities.

        Arguments:
            - expr: Expression to test

        Return:
            True if the expression represents a set of entities, False otherwise.
        """

        return isinstance(expr, (tree.ast.PathCombination, tree.ast.EntitySet))

    def get_cte_fieldref_for_set(self, context, caos_node, field_name, meta=False, map=None):
        """Return FieldRef node corresponding to the specified atom or meta value set.

        Arguments:
            - context: Current context
            - caos_node: A tree.ast.EntitySet node
            - field_name: The name of the atomic link of entities represented by caos_node
            - meta: If True, field_name is a reference to concept metadata instead of the
                    atom data. Default: False.
            - map: Optional AtomicRef->FieldRef mapping to look search in.  If not specified,
                   the global map from the current context will be considered.

        Return:
            A pgsql.ast.FieldRef node representing a set of atom/meta values for the specified,
            caos_node and field_name.
        """

        if map is None:
            map = context.current.concept_node_map

        cte_refs = map[caos_node]

        # First, check if the original data source is available in this context, i.e
        # the table is present in the current sub-query.
        #
        data_table = cte_refs.get('data')
        if data_table:
            ref = pgsql.ast.FieldRefNode(table=data_table, field=field_name,
                                         origin=data_table, origin_field=field_name)
        else:
            key = ('meta', field_name) if meta else field_name
            ref = cte_refs.get(key)
            assert ref, 'Reference to an inaccessible table node %s' % key

        if isinstance(ref, pgsql.ast.SelectExprNode):
            ref = ref.expr

        return ref

    def _process_constant(self, context, expr):
        if expr.type:
            if isinstance(expr.type, caos_types.ProtoAtom):
                const_type = delta_cmds.CompositePrototypeMetaCommand.pg_type_from_atom(
                                                            context.current.realm.meta, expr.type)
            else:
                const_type = common.py_type_to_pg_type(expr.type)
        else:
            const_type = None

        if expr.expr:
            result = pgsql.ast.ConstantNode(expr=self._process_expr(context, expr.expr))
        else:
            if expr.index is not None and not isinstance(expr.index, int):
                if expr.index in context.current.argmap:
                    index = context.current.argmap.index(expr.index)
                else:
                    context.current.argmap.add(expr.index)
                    index = len(context.current.argmap) - 1
            else:
                index = expr.index
            result = pgsql.ast.ConstantNode(value=expr.value, index=index, type=const_type)
        return result

    def _text_search_refs(self, context, vector):
        for link_name, link in vector.concept.get_searchable_links():
            yield tree.ast.AtomicRefSimple(ref=vector, name=link_name, caoslink=link.first)

    def _text_search_args(self, context, vector, query, tsvector=True):
        empty_str = pgsql.ast.ConstantNode(value='')
        sep_str = pgsql.ast.ConstantNode(value='; ')
        lang_const = pgsql.ast.ConstantNode(value='english')
        cols = None

        if isinstance(vector, tree.ast.EntitySet):
            refs = self._text_search_refs(context, vector)
        elif isinstance(vector, tree.ast.Sequence):
            refs = vector.elements

        if tsvector:
            for atomref in refs:
                ref = self._process_expr(context, atomref)
                ref = pgsql.ast.FunctionCallNode(name='coalesce', args=[ref, empty_str])
                ref = pgsql.ast.FunctionCallNode(name='to_tsvector', args=[lang_const, ref])
                weight_const = pgsql.ast.ConstantNode(value=atomref.caoslink.search.weight)
                ref = pgsql.ast.FunctionCallNode(name='setweight', args=[ref, weight_const])
                cols = self.extend_predicate(cols, ref, op='||')
        else:
            cols = pgsql.ast.ArrayNode(elements=[self._process_expr(context, r) for r in refs])
            cols = pgsql.ast.FunctionCallNode(name='array_to_string', args=[cols, sep_str])

        query = self._process_expr(context, query)
        query = pgsql.ast.FunctionCallNode(name='plainto_tsquery', args=[query])

        return cols, query

    def _process_expr(self, context, expr, cte=None):
        result = None

        if isinstance(expr, tree.ast.Disjunction):
            #context.current.append_graphs = True
            variants = [self._process_expr(context, path, cte) for path in expr.paths]
            #context.current.append_graphs = False

            variants = [v for v in variants if v and not isinstance(v, pgsql.ast.IgnoreNode)]
            if variants:
                if len(variants) == 1:
                    result = variants[0]
                else:
                    result = pgsql.ast.FunctionCallNode(name='coalesce', args=variants)
            else:
                result = pgsql.ast.IgnoreNode()

        elif isinstance(expr, tree.ast.Conjunction):
            variants = [self._process_expr(context, path, cte) for path in expr.paths]
            variants = [v for v in variants if v and not isinstance(v, pgsql.ast.IgnoreNode)]
            if variants:
                if len(variants) == 1:
                    result = variants[0]
                else:
                    result = pgsql.ast.FunctionCallNode(name='coalesce', args=variants)
            else:
                result = pgsql.ast.IgnoreNode()

        elif isinstance(expr, tree.ast.InlineFilter):
            self._process_expr(context, expr.ref, cte)
            result = pgsql.ast.IgnoreNode()

        elif isinstance(expr, tree.ast.EntitySet):
            root = self.get_caos_path_root(expr)
            self._process_graph(context, cte or context.current.query, root)

        elif isinstance(expr, tree.ast.BinOp):
            left_is_universal_set = self.is_universal_set(expr.left)

            if not left_is_universal_set:
                left = self._process_expr(context, expr.left, cte)
            else:
                left = pgsql.ast.IgnoreNode()

            if expr.op == ast.ops.OR:
                context.current.append_graphs = True

            if self.is_universal_set(expr.right):
                if expr.op in (ast.ops.IN, ast.ops.EQ):
                    # Membership in universal set is always True
                    right = pgsql.ast.IgnoreNode()
                elif expr.op in (ast.ops.NOT_IN, ast.ops.NE):
                    # No entity can be outside the universal set,
                    right = pgsql.ast.ConstantNode(value=False)
                else:
                    assert False, "Unsupported universal set expression"
            else:
                right = self._process_expr(context, expr.right, cte)

            if expr.op == tree.ast.SEARCH:
                vector, query = self._text_search_args(context, expr.left, expr.right)
                result = pgsql.ast.BinOpNode(left=vector, right=query, op=expr.op)
            else:
                context.current.append_graphs = False

                cte = cte or context.current.query

                # Fold constant ops into the inner query filter.
                if isinstance(expr.left, tree.ast.Constant) and isinstance(right, pgsql.ast.IgnoreNode):
                    cte.fromlist[0].expr.where = self.extend_predicate(cte.fromlist[0].expr.where,
                                                                       left, expr.op)
                    left = pgsql.ast.IgnoreNode()
                elif isinstance(expr.right, tree.ast.Constant) and isinstance(left, pgsql.ast.IgnoreNode):
                    cte.fromlist[0].expr.where = self.extend_predicate(cte.fromlist[0].expr.where,
                                                                       right, expr.op)
                    right = pgsql.ast.IgnoreNode()

                if isinstance(left, pgsql.ast.IgnoreNode) or isinstance(right, pgsql.ast.IgnoreNode):
                    if isinstance(left, pgsql.ast.IgnoreNode):
                        result, from_expr = right, expr.right
                    elif isinstance(right, pgsql.ast.IgnoreNode):
                        result, from_expr = left, expr.left

                    if getattr(from_expr, 'aggregates', False):
                        context.current.query.having = result
                        result = pgsql.ast.IgnoreNode()
                else:
                    left_aggregates = getattr(expr.left, 'aggregates', False)
                    op_aggregates = getattr(expr, 'aggregates', False)

                    if left_aggregates and not op_aggregates:
                        context.current.query.having = left
                        result = right
                    else:
                        result = pgsql.ast.BinOpNode(op=expr.op, left=left, right=right)

        elif isinstance(expr, tree.ast.Constant):
            result = self._process_constant(context, expr)

        elif isinstance(expr, tree.ast.Sequence):
            elements = [self._process_expr(context, e, cte) for e in expr.elements]
            result = pgsql.ast.SequenceNode(elements=elements)

        elif isinstance(expr, tree.ast.FunctionCall):
            result = self._process_function(context, expr, cte)

        elif isinstance(expr, tree.ast.AtomicRefExpr):
            result = self._process_expr(context, expr.expr, cte)

        elif isinstance(expr, (tree.ast.AtomicRefSimple, tree.ast.MetaRef)):
            self._process_expr(context, expr.ref, cte)

            ref = expr.ref
            if isinstance(ref, tree.ast.Disjunction):
                datarefs = ref.paths
            else:
                datarefs = [ref]

            fieldrefs = []

            for ref in datarefs:
                is_metaref = isinstance(expr, tree.ast.MetaRef)
                ref = self.get_cte_fieldref_for_set(context, ref, expr.name, is_metaref)
                fieldrefs.append(ref)

            if len(fieldrefs) > 1:
                ##
                # Values produced by a number of diverged paths need to be converged back.
                #
                result = pgsql.ast.FunctionCallNode(name='coalesce', args=fieldrefs)
            else:
                result = fieldrefs[0]

            if isinstance(result, pgsql.ast.SelectExprNode):
                ##
                # Ensure that the result is always a FieldRefNode
                #
                result = result.expr

        elif isinstance(expr, tree.ast.ExistPred):
            result = self._process_expr(context, expr.expr, cte)

        else:
            assert False, "Unexpected expression: %s" % expr

        return result

    def _process_function(self, context, expr, cte):
        if expr.name == ('search', 'rank'):
            vector, query = self._text_search_args(context, *expr.args)
            result = pgsql.ast.FunctionCallNode(name='ts_rank_cd', args=[vector, query])

        elif expr.name == ('search', 'headline'):
            vector, query = self._text_search_args(context, *expr.args, tsvector=False)
            lang = pgsql.ast.ConstantNode(value='english')
            result = pgsql.ast.FunctionCallNode(name='ts_headline', args=[lang, vector, query])

        else:
            if expr.name == ('agg', 'sum'):
                name = 'sum'
            elif expr.name == ('agg', 'list'):
                name = 'array_agg'
            elif expr.name == ('agg', 'count'):
                name = 'count'
            elif isinstance(expr.name, tuple):
                assert False, 'unsupported function %s' % (expr.name,)
            else:
                name = expr.name

            args = [self._process_expr(context, a, cte) for a in expr.args]
            result = pgsql.ast.FunctionCallNode(name=name, args=args)

        return result

    def _process_graph(self, context, cte, startnode):
        try:
            context.current.ctemap[cte][startnode]
        except KeyError:
            pass
        else:
            # Avoid processing the same subgraph more than once
            return

        sqlpath = self._process_path(context, cte, None, startnode)

        if isinstance(sqlpath, pgsql.ast.CTENode):
            cte.ctes.add(sqlpath)
            sqlpath.referrers.add(cte)
        elif isinstance(sqlpath, pgsql.ast.UnionNode):
            for q in sqlpath.queries:
                if isinstance(q, pgsql.ast.CTENode):
                    cte.ctes.add(q)
                    q.referrers.add(cte)

        if not cte.fromlist or not context.current.append_graphs:
            fromnode = pgsql.ast.FromExprNode()
            cte.fromlist.append(fromnode)
            fromnode.expr = sqlpath
        else:
            last = cte.fromlist.pop()
            sql_paths = [last.expr, sqlpath]
            union = self.unify_paths(context, sql_paths)
            cte.fromlist.append(union)

    def _simple_join(self, context, left, right, key, type='inner', condition=None):
        if condition is None:
            condition = left.bonds(key)[-1]
            if not isinstance(condition, pgsql.ast.BinOpNode):
                condition = right.bonds(key)[-1]
                if not isinstance(condition, pgsql.ast.BinOpNode):
                    condition = pgsql.ast.BinOpNode(op='=', left=left.bonds(key)[-1],
                                                            right=right.bonds(key)[-1])
        join = pgsql.ast.JoinNode(type=type, left=left, right=right, condition=condition)

        join.updatebonds(left)
        join.updatebonds(right)

        return join

    def _relation_from_concepts(self, context, node):
        concept = node.concept
        table_schema_name, table_name = common.concept_name_to_table_name(concept.name, catenate=False)
        concept_table = pgsql.ast.TableNode(name=table_name,
                                            schema=table_schema_name,
                                            concepts=frozenset({node.concept}),
                                            alias=context.current.genalias(hint=table_name),
                                            caosnode=node)
        return concept_table

    def caos_path_to_sql_path(self, context, root_cte, step_cte, caos_path_tip, sql_path_tip, link,
                                                                                        weak=False):
        """
        Generates a Common Table Expression for a given step in the path

        @param context: parse context
        @param root_cte: root CTE
        @param step_cte: parent CTE
        @param caos_path_tip: Caos path step node
        @param sql_path_tip: current position in parent CTE join chain
        @param link: Caos link node
        @param weak:
        """

        # Avoid processing the same caos_path_tip twice
        #if caos_path_tip in context.current.ctemap:
        #    return context.current.ctemap[caos_path_tip]

        is_root = not step_cte

        if not step_cte:
            if caos_path_tip.anchor:
                # If the path caos_path_tip has been assigned a named pointer, re-use it in the
                # CTE alias.
                #
                cte_alias = context.current.genalias(hint=caos_path_tip.anchor)
            else:
                cte_alias = context.current.genalias(hint=str(caos_path_tip.id))

            if caos_path_tip.filter or caos_path_tip.rlink:
                step_cte = pgsql.ast.CTENode(concepts=frozenset({caos_path_tip.concept}),
                                             alias=cte_alias, caosnode=caos_path_tip)
            else:
                step_cte = pgsql.ast.SelectQueryNode(concepts=frozenset({caos_path_tip.concept}),
                                                     alias=cte_alias, caosnode=caos_path_tip)

        ctemap = context.current.ctemap.setdefault(root_cte, {})
        ctemap[caos_path_tip] = step_cte

        fromnode = step_cte.fromlist[0] if step_cte.fromlist else pgsql.ast.FromExprNode()

        concept_table = self._relation_from_concepts(context, caos_path_tip)

        field_name = 'semantix.caos.builtins.id'
        bond = pgsql.ast.FieldRefNode(table=concept_table, field=field_name, origin=concept_table,
                                                                             origin_field=field_name)
        concept_table.addbond(caos_path_tip.concept, bond)

        if not sql_path_tip:
            fromnode.expr = concept_table
        else:
            target_id_field = pgsql.ast.FieldRefNode(table=concept_table,
                                                     field='semantix.caos.builtins.id',
                                                     origin=concept_table,
                                                     origin_field='semantix.caos.builtins.id')

            if isinstance(sql_path_tip, pgsql.ast.CTENode) and is_root:
                sql_path_tip.referrers.add(step_cte)
                step_cte.ctes.add(sql_path_tip)

            if fromnode.expr:
                join = fromnode.expr
            else:
                join = sql_path_tip

            target_bond_expr = None

            # If specific links are provided we LEFT JOIN all corresponding link tables and then
            # INNER JOIN the concept table using an aggregated condition disjunction.
            #
            labels = link.filter.labels if link.filter and link.filter.labels else [None]
            map_join_type = 'left' if len(labels) > 1 or weak else 'inner'

            maps = {}
            existing_link = step_cte.linkmap.get((link.filter, link.source))

            for label in labels:
                if existing_link:
                    # The same link map must not be joined more than once,
                    # otherwise the cardinality of the result set will be wrong.
                    #
                    map = existing_link[label]
                else:
                    if label is None:
                        table_name = 'link_link'
                        table_schema = 'caos_semantix.caos.builtins'
                    else:
                        table_name = label.name.name + '_link'
                        table_schema = 'caos_' + label.name.module

                    map = pgsql.ast.TableNode(name=table_name, schema=table_schema,
                                              concepts=frozenset({caos_path_tip.concept}),
                                              alias=context.current.genalias(hint='map'))
                    maps[label] = map

                source_ref = pgsql.ast.FieldRefNode(table=map, field='source_id', origin=map,
                                                                                  origin_field='source_id')
                target_ref = pgsql.ast.FieldRefNode(table=map, field='target_id', origin=map,
                                                                                  origin_field='target_id')
                valent_bond = join.bonds(link.source.concept)[-1]
                forward_bond = pgsql.ast.BinOpNode(left=valent_bond, right=source_ref, op='=')
                backward_bond = pgsql.ast.BinOpNode(left=valent_bond, right=target_ref, op='=')

                if link.filter.direction == caos_types.AnyDirection:
                    map_join_cond = pgsql.ast.BinOpNode(left=forward_bond, op='or', right=backward_bond)
                    left = pgsql.ast.BinOpNode(left=target_ref, op='=', right=target_id_field)
                    right = pgsql.ast.BinOpNode(left=source_ref, op='=', right=target_id_field)
                    cond_expr = pgsql.ast.BinOpNode(left=left, op='or', right=right)

                elif link.filter.direction == caos_types.InboundDirection:
                    map_join_cond = backward_bond
                    cond_expr = pgsql.ast.BinOpNode(left=source_ref, op='=', right=target_id_field)

                else:
                    map_join_cond = forward_bond
                    cond_expr = pgsql.ast.BinOpNode(left=target_ref, op='=', right=target_id_field)

                if not existing_link:
                    join = self._simple_join(context, join, map, link.source.concept,
                                             type=map_join_type,
                                             condition=map_join_cond)

                if target_bond_expr:
                    target_bond_expr = pgsql.ast.BinOpNode(left=target_bond_expr, op='or', right=cond_expr)
                else:
                    target_bond_expr = cond_expr

            if not existing_link:
                step_cte.linkmap[(link.filter, link.source)] = maps

            join.addbond(caos_path_tip.concept, target_bond_expr)
            join = self._simple_join(context, join, concept_table,
                                     caos_path_tip.concept,
                                     type='left' if weak else 'inner')

            fromnode.expr = join

            if is_root:
                # If this is a new query, pull the references to fields inside the CTE one level up to keep
                # them visible.
                #
                for caosnode, refs in sql_path_tip.concept_node_map.items():
                    for field, ref in refs.items():
                        refexpr = pgsql.ast.FieldRefNode(table=sql_path_tip, field=ref.alias, origin=ref.expr.origin,
                                                                          origin_field=ref.expr.origin_field)
                        fieldref = pgsql.ast.SelectExprNode(expr=refexpr, alias=ref.alias)
                        step_cte.targets.append(fieldref)

                        mapslot = step_cte.concept_node_map.get(caosnode)
                        if not mapslot:
                            step_cte.concept_node_map[caosnode] = {field: fieldref}
                        else:
                            mapslot[field] = fieldref

                        bondref = pgsql.ast.FieldRefNode(table=step_cte, field=ref.alias, origin=ref.expr.origin,
                                                                          origin_field=ref.expr.origin_field)
                        if field == 'semantix.caos.builtins.id':
                            step_cte.addbond(caosnode.concept, bondref)
                        context.current.concept_node_map[caosnode][field].expr.table = step_cte

        # Include target entity id and concept class id in the Select expression list ...
        atomrefs = {'semantix.caos.builtins.id', 'concept_id'} | {f.name for f in caos_path_tip.atomrefs}

        fieldref = fromnode.expr.bonds(caos_path_tip.concept)[-1]
        context.current.concept_node_map.setdefault(caos_path_tip, {})
        step_cte.concept_node_map.setdefault(caos_path_tip, {})
        aliases ={}

        for field in atomrefs:
            fieldref = pgsql.ast.FieldRefNode(table=concept_table, field=field, origin=concept_table,
                                                                                origin_field=field)
            aliases[field] = step_cte.alias + ('_' + context.current.genalias(hint=str(field)))
            selectnode = pgsql.ast.SelectExprNode(expr=fieldref, alias=aliases[field])
            step_cte.targets.append(selectnode)

            step_cte.concept_node_map[caos_path_tip][field] = selectnode

            ##
            # Record atom references in the global map in case they have to be pulled up later
            #
            refexpr = pgsql.ast.FieldRefNode(table=step_cte, field=selectnode.alias, origin=concept_table,
                                                                                     origin_field=field)
            selectnode = pgsql.ast.SelectExprNode(expr=refexpr, alias=selectnode.alias)
            context.current.concept_node_map[caos_path_tip][field] = selectnode

        if caos_path_tip.metarefs:
            datatable = pgsql.ast.TableNode(name='metaobject',
                                            schema='caos',
                                            concepts=None,
                                            alias=context.current.genalias(hint='metaobject'))

            left = pgsql.ast.FieldRefNode(table=concept_table, field='concept_id')
            right = pgsql.ast.FieldRefNode(table=datatable, field='id')
            joincond = pgsql.ast.BinOpNode(op='=', left=left, right=right)

            fromnode.expr = self._simple_join(context, fromnode.expr, datatable, key=None,
                                              type='left' if weak else 'inner',
                                              condition=joincond)

            for metaref in caos_path_tip.metarefs:
                fieldref = pgsql.ast.FieldRefNode(table=datatable, field=metaref.name, origin=datatable,
                                                                                    origin_field=metaref.name)

                alias = context.current.genalias(hint=metaref.name)
                selectnode = pgsql.ast.SelectExprNode(expr=fieldref,
                                                      alias=step_cte.alias + ('_meta_' + alias))
                step_cte.targets.append(selectnode)
                step_cte.concept_node_map[caos_path_tip][('meta', metaref.name)] = selectnode

                ##
                # Record meta references in the global map in case they have to be pulled up later
                #
                refexpr = pgsql.ast.FieldRefNode(table=step_cte, field=selectnode.alias, origin=datatable,
                                                                                    origin_field=metaref.name)
                selectnode = pgsql.ast.SelectExprNode(expr=refexpr, alias=selectnode.alias)
                context.current.concept_node_map[caos_path_tip][('meta', metaref.name)] = selectnode

        if caos_path_tip.filter:
            ##
            # Switch context to node filter and make the concept table available for
            # atoms in filter expression to reference.
            #
            context.push()
            context.current.location = 'nodefilter'
            context.current.concept_node_map[caos_path_tip] = {'data': concept_table}
            expr = self._process_expr(context, caos_path_tip.filter)
            if expr:
                expr = pgsql.ast.PredicateNode(expr=expr)
                if weak:
                    step_cte.where_weak = self.extend_predicate(step_cte.where_weak, expr, ast.ops.OR)
                else:
                    step_cte.where_strong = self.extend_predicate(step_cte.where_strong, expr, ast.ops.AND)

            context.pop()

        if is_root:
            step_cte.fromlist.append(fromnode)
        step_cte._source_graph = caos_path_tip

        bond = pgsql.ast.FieldRefNode(table=step_cte, field=aliases['semantix.caos.builtins.id'])
        step_cte.addbond(caos_path_tip.concept, bond)

        return step_cte

    def extend_predicate(self, predicate, expr, op=ast.ops.AND):
        if predicate is not None:
            return pgsql.ast.BinOpNode(op=op, left=predicate, right=expr)
        else:
            return expr

    def init_filter_cte(self, context, sql_path_tip, caos_path_tip):
        cte = pgsql.ast.SelectQueryNode()
        fromnode = pgsql.ast.FromExprNode()
        cte.fromlist.append(fromnode)

        concept_table = self._relation_from_concepts(context, caos_path_tip)

        field_name = 'semantix.caos.builtins.id'
        bond = pgsql.ast.FieldRefNode(table=concept_table, field=field_name, origin=concept_table,
                                                                             origin_field=field_name)
        concept_table.addbond(caos_path_tip.concept, bond)
        fromnode.expr = concept_table

        ref = self.get_cte_fieldref_for_set(context, caos_path_tip, field_name,
                                            map=sql_path_tip.concept_node_map)
        cte.where = pgsql.ast.BinOpNode(left=bond, op=ast.ops.EQ, right=ref)

        target = pgsql.ast.SelectExprNode(expr=pgsql.ast.ConstantNode(value=True))
        cte.targets.append(target)

        return cte

    def _process_conjunction(self, context, cte, sql_path_tip, caos_path_tip, conjunction,
                                                               parent_cte, weak=False):
        sql_path = sql_path_tip

        for link in conjunction.paths:
            if isinstance(link, tree.ast.EntityLink):
                link_proto = link.link_proto

                target_sets = {link.target} | set(link.target.joins)
                in_selector = bool(list(filter(lambda i: 'selector' in i.users, target_sets)))

                cardinality_ok = context.current.ignore_cardinality or \
                                 in_selector or \
                                 link_proto.mapping in (caos_types.OneToOne, caos_types.ManyToOne)

                if cardinality_ok:
                    sql_path = self.caos_path_to_sql_path(context, cte, parent_cte, link.target,
                                                                        sql_path, link, weak)

                    sql_path = self._process_path(context, cte, sql_path, link.target, weak)
                else:
                    cte = self.init_filter_cte(context, parent_cte or sql_path_tip, caos_path_tip)
                    pred = pgsql.ast.ExistsNode(expr=cte)
                    op = ast.ops.OR if weak else ast.ops.AND
                    sql_path_tip.where = self.extend_predicate(sql_path_tip.where, pred, op)

                    with context(TransformerContext.NEW):
                        context.current.ignore_cardinality = True
                        sql_path = self.caos_path_to_sql_path(context, cte, cte, link.target,
                                                                            sql_path, link, weak)

                        self._process_path(context, cte, sql_path, link.target, weak)
                        sql_path = sql_path_tip
            else:
                sql_path = self._process_path(context, cte, sql_path, link, weak)

        return sql_path


    def _process_path(self, context, root_cte, sql_path_tip, caos_path_tip, weak=False):
        sql_paths = []

        if not sql_path_tip and isinstance(caos_path_tip, tree.ast.EntitySet):
            # Bootstrap the SQL path
            sql_path_tip = self.caos_path_to_sql_path(context, root_cte,
                                                      step_cte=None, caos_path_tip=caos_path_tip,
                                                      sql_path_tip=None, link=None)

        if isinstance(caos_path_tip, tree.ast.Disjunction):
            disjunction = caos_path_tip
            conjunction = None
        elif isinstance(caos_path_tip, tree.ast.Conjunction):
            disjunction = None
            conjunction = caos_path_tip
        else:
            disjunction = caos_path_tip.disjunction
            conjunction = caos_path_tip.conjunction

        if conjunction and conjunction.paths:
            sql_path_tip = self._process_conjunction(context, root_cte, sql_path_tip, caos_path_tip,
                                                              conjunction, sql_path_tip, weak)
            if isinstance(caos_path_tip, tree.ast.EntitySet):
                # Path conjunction works as a strong filter and, thus, the CTE corresponding
                # to the given Caos node must only be referenced with those conjunctions
                # included.
                #
                ctemap = context.current.ctemap.setdefault(root_cte, {})
                ctemap[caos_path_tip] = sql_path_tip


        if disjunction and disjunction.paths:
            if isinstance(caos_path_tip, tree.ast.EntitySet):
                need_union = False
                for link in disjunction.paths:
                    if isinstance(link, tree.ast.EntityLink):
                        sql_path = self.caos_path_to_sql_path(context, root_cte, sql_path_tip,
                                                              link.target, sql_path_tip, link,
                                                              weak=True)
                        sql_path = self._process_path(context, root_cte, sql_path, link.target, weak=True)
                        sql_paths.append(sql_path)
                    elif isinstance(link, tree.ast.Conjunction):
                        sql_path = self._process_conjunction(context, root_cte, sql_path_tip,
                                                             caos_path_tip, link, None, weak=True)
                        sql_paths.append(sql_path)
                        need_union = True
                    else:
                        assert False, 'unexpected expression type in disjunction path: %s' % link

                if need_union:
                    result = self.unify_paths(context, sql_paths)
                else:
                    result = sql_path_tip
            else:
                for link in disjunction.paths:
                    if isinstance(link, (tree.ast.EntitySet, tree.ast.PathCombination)):
                        sql_path = self._process_path(context, root_cte, None, link)
                        sql_paths.append(sql_path)
                    else:
                        assert False, 'unexpected expression type in disjunction path: %s' % link
                result = self.unify_paths(context, sql_paths)
        else:
            result = sql_path_tip

        return result

    def unify_paths(self, context, sql_paths, intersect=False):
        if intersect:
            result = pgsql.ast.IntersectNode(alias=context.current.genalias(hint='intersect'))
        else:
            result = pgsql.ast.UnionNode(alias=context.current.genalias(hint='union'))
        union = []
        commonctes = collections.OrderedDict()
        fieldmap = collections.OrderedDict()

        ##
        # First, analyze the given sqlpaths
        #
        for sqlpath in sql_paths:
            for caosnode, refs in sqlpath.concept_node_map.items():
                ##
                # Generate a natural union of all fields produced by given sqlpaths.
                #

                if caosnode not in fieldmap:
                    fieldmap[caosnode] = collections.OrderedDict()
                for field, ref in refs.items():
                    fieldmap[caosnode][field] = ref

            ##
            # Enumerate and count references to the CTEs used in sqlpaths.  This is needed
            # to pull up common CTEs.
            #

            ctes = ast.find_children(sqlpath, lambda n: isinstance(n, pgsql.ast.CTENode))
            for cte in ctes:
                counter = commonctes.get(cte)
                if counter is not None:
                    commonctes[cte] += 1
                else:
                    commonctes[cte] = 1

            if isinstance(sqlpath, pgsql.ast.CTENode):
                ##
                # CTEs need to be linked explicitly for proper code generation.
                #
                result.ctes.add(sqlpath)
                sqlpath.referrers.add(result)

                counter = commonctes.get(sqlpath)
                if counter is not None:
                    commonctes[sqlpath] += 1
                else:
                    commonctes[sqlpath] = 1

            result.concepts = result.concepts | sqlpath.concepts

        ##
        # Second, transform each sqlpath into a sub-query with a correct order of fields that
        # can UNION properly.  Fields originated from the same Caos node are placed into the
        # same column.  If an sqlpath does not include the needed Caos node, a NULL placeholder
        # is used.  This effectively creates a natural outer join of fields produced by all
        # provided sqlpaths which represents a sparse array of matched graph paths.
        #

        concepts = set()

        for sqlpath in sql_paths:
            query = pgsql.ast.SelectQueryNode()
            query.fromlist.append(pgsql.ast.FromExprNode(expr=sqlpath))

            joinmap = sqlpath.concept_node_map
            for caosnode, fields in fieldmap.items():
                for field, ref in fields.items():
                    selexpr = None
                    if caosnode in joinmap:
                        fieldref = joinmap[caosnode].get(field)

                        if fieldref:
                            if isinstance(fieldref.expr, pgsql.ast.ConstantNode):
                                selexpr = fieldref
                            else:
                                fieldref = pgsql.ast.FieldRefNode(table=sqlpath, field=fieldref.alias,
                                                                  origin=fieldref.expr.origin,
                                                                  origin_field=fieldref.expr.origin_field)
                                selexpr = pgsql.ast.SelectExprNode(expr=fieldref, alias=fieldref.field)

                    if not selexpr:
                        placeholder = pgsql.ast.ConstantNode(value=None)
                        selexpr = pgsql.ast.SelectExprNode(expr=placeholder, alias=ref.alias)

                    query.targets.append(selexpr)
                    if caosnode not in query.concept_node_map:
                        query.concept_node_map[caosnode] = {}
                    query.concept_node_map[caosnode][field] = selexpr

                    fieldref = pgsql.ast.FieldRefNode(table=result, field=selexpr.alias)
                    selexpr = pgsql.ast.SelectExprNode(expr=fieldref, alias=selexpr.alias)

                    if caosnode not in result.concept_node_map:
                        result.concept_node_map[caosnode] = {}
                    result.concept_node_map[caosnode][field] = selexpr

                    context.current.concept_node_map[caosnode][field] = selexpr


            path_concepts = sqlpath.concepts.union(*[c.children for c in sqlpath.concepts])
            if concepts is None:
                concepts = path_concepts.copy()
                concept_filter = None
            else:
                concept_filter = path_concepts - concepts
                concepts |= path_concepts

            if concept_filter is not None and not isinstance(sqlpath, pgsql.ast.UnionNode):
                if len(concept_filter) == 0:
                    pass
                else:
                    concept_name_ref = query.concept_node_map[sqlpath.caosnode].get(('meta', 'name'))
                    if not concept_name_ref:
                        datatable = pgsql.ast.TableNode(name='metaobject',
                                                        schema='caos',
                                                        concepts=None,
                                                        alias=context.current.genalias(hint='metaobject'))
                        query.fromlist.append(pgsql.ast.FromExprNode(expr=datatable))

                        left = query.concept_node_map[sqlpath.caosnode]['concept_id']
                        right = pgsql.ast.FieldRefNode(table=datatable, field='id')
                        whereexpr = pgsql.ast.BinOpNode(op='=', left=left.expr, right=right)
                        query.where = self.extend_predicate(query.where, whereexpr)
                        concept_name_ref = pgsql.ast.FieldRefNode(table=datatable, field='name')
                    else:
                        concept_name_ref = concept_name_ref.expr

                    left = concept_name_ref
                    values = [pgsql.ast.ConstantNode(value=str(c.name)) for c in concept_filter]
                    right = pgsql.ast.SequenceNode(elements=values)
                    filterexpr = pgsql.ast.BinOpNode(left=left, op='in', right=right)
                    query.where = self.extend_predicate(query.where, filterexpr)

            union.append(query)

        ##
        # Pull up all CTEs that are used more than once in sub-queries.
        #
        """
        commonctes = datastructures.OrderedSet(c for c, counter in commonctes.items() if counter > 1)
        result.ctes = commonctes | result.ctes
        for cte in commonctes:
            for ref in cte.referrers:
                if ref is not result:
                    ref.ctes.discard(cte)
                else:
                    cte.referrers.add(result)
        """
        result.queries = union
        return result
