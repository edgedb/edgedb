##
# Copyright (c) 2008-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import itertools
import functools
import re

from metamagic.utils import ast, markup
from metamagic.caos import caosql, tree
from metamagic.caos import types as caos_types
from metamagic.caos import name as caos_name
from metamagic.caos import utils as caos_utils
from metamagic.caos.backends import pgsql
from metamagic.caos.backends.pgsql import common, session as pg_session, driver as pg_driver
from metamagic.caos.backends.pgsql import types as pg_types
from metamagic.utils.debug import debug
from metamagic.utils.datastructures import OrderedSet
from metamagic import exceptions as base_err


from . import types


class Alias(str):
    def __new__(cls, value=''):
        return super(Alias, cls).__new__(cls, pgsql.common.caos_name_to_pg_name(value))

    def __add__(self, other):
        return Alias(super().__add__(other))

    def __radd__(self, other):
        return Alias(str(other) + str(self))

    __iadd__ = __add__


class TransformerContextLevel(object):
    def __init__(self, prevlevel=None, mode=None):
        if prevlevel is not None:
            self.argmap = prevlevel.argmap
            self.location = 'query'
            self.append_graphs = False
            self.ignore_cardinality = prevlevel.ignore_cardinality
            self.in_aggregate = prevlevel.in_aggregate
            self.query = prevlevel.query
            self.session = prevlevel.session
            self.proto_schema = prevlevel.proto_schema
            self.unwind_rlinks = prevlevel.unwind_rlinks
            self.aliascnt = prevlevel.aliascnt
            self.record_info = prevlevel.record_info
            self.output_format = prevlevel.output_format
            self.in_subquery = prevlevel.in_subquery
            self.global_ctes = prevlevel.global_ctes

            if mode == TransformerContext.NEW_TRANSPARENT:
                self.location = prevlevel.location
                self.vars = prevlevel.vars
                self.ctes = prevlevel.ctes
                self.ctemap = prevlevel.ctemap
                self.explicit_cte_map = prevlevel.explicit_cte_map
                self.concept_node_map = prevlevel.concept_node_map
                self.computable_map = prevlevel.computable_map
                self.link_node_map = prevlevel.link_node_map
                self.subquery_map = prevlevel.subquery_map
                self.direct_subquery_ref = prevlevel.direct_subquery_ref
                self.node_callbacks = prevlevel.node_callbacks

            elif mode == TransformerContext.SUBQUERY:
                self.vars = {}
                self.ctes = prevlevel.ctes.copy()
                self.ctemap = prevlevel.ctemap.copy()
                self.explicit_cte_map = prevlevel.explicit_cte_map.copy()
                self.concept_node_map = prevlevel.concept_node_map.copy()
                self.computable_map = prevlevel.computable_map.copy()
                self.link_node_map = prevlevel.link_node_map.copy()

                if prevlevel.ignore_cardinality != 'recursive':
                    self.ignore_cardinality = False

                self.in_aggregate = False
                self.query = pgsql.ast.SelectQueryNode()
                self.subquery_map = {}
                self.direct_subquery_ref = False
                self.node_callbacks = {}

                self.in_subquery = True

            else:
                self.vars = prevlevel.vars.copy()
                self.ctes = prevlevel.ctes.copy()
                self.ctemap = prevlevel.ctemap.copy()
                self.explicit_cte_map = prevlevel.explicit_cte_map.copy()
                self.concept_node_map = prevlevel.concept_node_map.copy()
                self.computable_map = prevlevel.computable_map.copy()
                self.link_node_map = prevlevel.link_node_map.copy()
                self.subquery_map = prevlevel.subquery_map
                self.direct_subquery_ref = False
                self.node_callbacks = prevlevel.node_callbacks.copy()

        else:
            self.vars = {}
            self.ctes = {}
            self.global_ctes = {}
            self.aliascnt = {}
            self.ctemap = {}
            self.explicit_cte_map = {}
            self.concept_node_map = {}
            self.computable_map = {}
            self.link_node_map = {}
            self.argmap = OrderedSet()
            self.location = 'query'
            self.append_graphs = False
            self.ignore_cardinality = False
            self.in_aggregate = False
            self.query = pgsql.ast.SelectQueryNode()
            self.session = None
            self.proto_schema = None
            self.subquery_map = {}
            self.direct_subquery_ref = False
            self.node_callbacks = {}
            self.unwind_rlinks = True
            self.record_info = {}
            self.output_format = None
            self.in_subquery = False

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
    CURRENT, ALTERNATE, NEW, NEW_TRANSPARENT, SUBQUERY = range(0, 5)

    def __init__(self):
        self.stack = []
        self.push()

    def push(self, mode=None):
        level = TransformerContextLevel(self.current, mode)

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

    def __getitem__(self, idx):
        return self.stack[idx]

    def __len__(self):
        return len(self.stack)

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


class PgSQLExprTransformer(ast.visitor.NodeVisitor):
    def transform(self, tree, schema, local_to_source=None):
        context = TransformerContext()
        context.current.source = local_to_source

        if local_to_source:
            context.current.attmap = {}

            for l in local_to_source.pointers.values():
                name = l.normal_name()
                colname = common.caos_name_to_pg_name(l.normal_name())
                source = context.current.source.get_pointer_origin(name, farthest=True)
                context.current.attmap[colname] = (name, source)

        return self._process_expr(context, tree)

    def _process_expr(self, context, expr):
        if isinstance(expr, pgsql.ast.BinOpNode):
            left = self._process_expr(context, expr.left)
            right = self._process_expr(context, expr.right)
            result = tree.ast.BinOp(left=left, op=expr.op, right=right)

        elif isinstance(expr, pgsql.ast.FieldRefNode):
            if context.current.source:
                if isinstance(context.current.source, caos_types.ProtoConcept):
                    id = caos_utils.LinearPath([context.current.source])
                    pointer, source = context.current.attmap[expr.field]
                    entset = tree.ast.EntitySet(id=id, concept=source)
                    result = tree.ast.AtomicRefSimple(ref=entset, name=pointer)
                else:
                    id = caos_utils.LinearPath([None])
                    id.add(context.current.source, caos_types.OutboundDirection, None)
                    entlink = tree.ast.EntityLink(link_proto=context.current.source)
                    result = tree.ast.LinkPropRefSimple(ref=entlink,
                                                        name=context.current.attmap[expr.field][0],
                                                        id=id)
            else:
                assert False

        elif isinstance(expr, pgsql.ast.RowExprNode):
            result = tree.ast.Sequence(elements=[self._process_expr(context, e) for e in expr.args])

        else:
            assert False, "unexpected node type: %r" % expr

        return result


class CaosExprTransformer(tree.transformer.TreeTransformer):
    def _table_from_concept(self, context, concept, node, parent_cte):
        if concept.is_virtual:
            # Virtual concepts are represented as a UNION of selects from their children,
            # which is, for most purposes, equivalent to SELECTing from a parent table.
            #
            idptr = caos_name.Name('metamagic.caos.builtins.id')
            idcol = common.caos_name_to_pg_name(idptr)
            atomrefs = {idptr: tree.ast.AtomicRefSimple(ref=node, name=idptr)}
            atomrefs.update({f.name: f for f in node.atomrefs})

            cols = [(aref, common.caos_name_to_pg_name(aref)) for aref in atomrefs]

            schema = context.current.proto_schema

            union_list = []
            children = frozenset(concept.children(schema))

            inhmap = caos_utils.get_full_inheritance_map(schema, children)

            coltypes = {}

            for c, cc in inhmap.items():
                table = self._table_from_concept(context, c, node, parent_cte)
                qry = pgsql.ast.SelectQueryNode()
                qry.fromlist.append(table)

                for aname, colname in cols:
                    if aname in c.pointers:
                        aref = atomrefs[aname]
                        if isinstance(aref, tree.ast.AtomicRefSimple):
                            selexpr = pgsql.ast.FieldRefNode(table=table, field=colname,
                                                             origin=table, origin_field=colname)

                        elif isinstance(aref, tree.ast.SubgraphRef):
                            # Result of a rewrite

                            subquery = self._process_expr(context, aref.ref, parent_cte)

                            with context(TransformerContext.NEW_TRANSPARENT):
                                # Make sure subquery outerbonds are connected to the proper
                                # table, which is an element of this union.
                                #
                                for i, (outerref, innerref) in enumerate(subquery.outerbonds):
                                    if outerref == node:
                                        fref = pgsql.ast.FieldRefNode(table=table, field=idcol,
                                                                      origin=table,
                                                                      origin_field=idcol)
                                        context.current.concept_node_map[node] = {
                                            idcol: pgsql.ast.SelectExprNode(expr=fref)
                                        }

                                self._connect_subquery_outerbonds(context, subquery.outerbonds,
                                                                  subquery, inline=True)

                            selexpr = subquery

                            # Record this subquery in the computables map to signal that
                            # the value has been computed, which lets  all outer references
                            # to this subgraph to be pointed to a SelectExpr in parent_cte.
                            try:
                                computables = context.current.computable_map[node]
                            except KeyError:
                                computables = context.current.computable_map[node] = {}

                            computables[aref.name] = aref

                        else:
                            raise ValueError('unexpected node in atomrefs list: {!r}'.format(aref))
                    else:
                        try:
                            coltype = coltypes[aname]
                        except KeyError:
                            result = concept.resolve_pointer(schema, aname, look_in_children=True)
                            target_ptr = next(iter(result[1]))
                            coltype = pg_types.pg_type_from_atom(schema, target_ptr.target)
                            coltypes[aname] = coltype

                        selexpr = pgsql.ast.ConstantNode(value=None)
                        pgtype = pgsql.ast.TypeNode(name=coltype)
                        selexpr = pgsql.ast.TypeCastNode(expr=selexpr, type=pgtype)

                    qry.targets.append(pgsql.ast.SelectExprNode(expr=selexpr, alias=colname))

                selexpr = pgsql.ast.FieldRefNode(table=table, field='concept_id',
                                                 origin=table, origin_field='concept_id')

                qry.targets.append(pgsql.ast.SelectExprNode(expr=selexpr, alias='concept_id'))

                if cc:
                    # Make sure that all sets produced by each UNION member are disjoint so
                    # that there are no duplicates, and, most importantly, the shape of each row
                    # corresponds to the class.
                    get_concept_id = context.current.session.backend.get_concept_id
                    session = context.current.session
                    cc_ids = {get_concept_id(cls, session, cache='always') for cls in cc}
                    cc_ids = [pgsql.ast.ConstantNode(value=cc_id) for cc_id in cc_ids]
                    cc_ids = pgsql.ast.SequenceNode(elements=cc_ids)

                    qry.where = pgsql.ast.BinOpNode(left=selexpr, right=cc_ids, op=ast.ops.NOT_IN)

                union_list.append(qry)

            if len(union_list) > 1:
                relation = pgsql.ast.SelectQueryNode(caosnode=node, concepts=children,
                                                     op=pgsql.ast.UNION)
                self._setop_from_list(relation, union_list, pgsql.ast.UNION)
            else:
                relation = union_list[0]

            relation.alias = context.current.genalias(hint=concept.name.name)

        else:
            table_schema_name, table_name = common.concept_name_to_table_name(concept.name,
                                                                              catenate=False)
            relation = pgsql.ast.TableNode(name=table_name,
                                           schema=table_schema_name,
                                           concepts=frozenset({node.concept}),
                                           alias=context.current.genalias(hint=table_name),
                                           caosnode=node)
        return relation

    def _relation_from_concepts(self, context, node, parent_cte):
        return self._table_from_concept(context, node.concept, node, parent_cte)

    def _table_from_link_proto(self, context, link_proto):
        """Return a TableNode corresponding to a given link prototype"""
        table_schema_name, table_name = common.get_table_name(link_proto, catenate=False)
        return pgsql.ast.TableNode(name=table_name,
                                   schema=table_schema_name,
                                   alias=context.current.genalias(hint=table_name))

    def _relation_from_link_proto(self, context, link_proto, direction, proprefs):
        """"Return a Relation subclass corresponding to a given link prototype and direction.

        If `link_proto` is a generic link, then a simple TableNode is returned, otherwise
        the return value may potentially be a UNION of all tables corresponding to
        a set of specialized links computed from the given `link_proto` taking
        source inheritance into account.
        """
        linkname = link_proto.normal_name()
        endpoint = link_proto.get_near_endpoint(direction)

        if link_proto.generic():
            # Generic links would capture the necessary set via inheritance.
            #
            relation = self._table_from_link_proto(context, link_proto)

        else:
            if proprefs:
                cols = [(pref, common.caos_name_to_pg_name(pref)) for pref in proprefs]
            else:
                cols = []

            schema = context.current.proto_schema

            union_list = []

            for source in {endpoint} | set(endpoint.descendants(schema)):
                # Sift through the descendants to see who defines this link
                try:
                    src_link_proto = source.own_pointers[linkname]
                except KeyError:
                    # This source has no such link, skip it
                    continue

                table = self._table_from_link_proto(context, src_link_proto)

                qry = pgsql.ast.SelectQueryNode()
                qry.fromlist.append(table)

                # Make sure all property references are pulled up properly
                for propname, colname in cols:
                    propref = proprefs[propname]

                    selexpr = pgsql.ast.FieldRefNode(table=table, field=colname,
                                                     origin=table, origin_field=colname)
                    qry.targets.append(pgsql.ast.SelectExprNode(expr=selexpr, alias=colname))

                union_list.append(qry)

            if len(union_list) == 0:
                # We've been given a generic link that none of the potential sources
                # contain directly, so fall back to general parent table.
                #
                relation = self._table_from_link_proto(context, link_proto.bases[0])

            elif len(union_list) > 1:
                # More than one link table, generate a UNION clause.
                #
                relation = pgsql.ast.SelectQueryNode(op=pgsql.ast.UNION)
                self._setop_from_list(relation, union_list, pgsql.ast.UNION)

            else:
                # Just one link table, so returin it directly
                #
                relation = union_list[0].fromlist[0]

            relation.alias = context.current.genalias(hint=link_proto.normal_name().name)

        return relation

    def _relation_from_link(self, context, link_node):
        proprefs = {}

        link_proto = link_node.link_proto
        if link_proto is None:
            link_proto = context.current.proto_schema.get('metamagic.caos.builtins.link')

        for ptr in link_proto.get_special_pointers():
            proprefs[ptr] = tree.ast.LinkPropRefSimple(ref=link_node, name=ptr)

        if not link_proto.generic() and link_proto.atomic():
            atom_target = caos_name.Name("metamagic.caos.builtins.target@atom")
            proprefs[atom_target] = tree.ast.LinkPropRefSimple(ref=link_node, name=atom_target)

        proprefs.update({f.name: f for f in link_node.proprefs})

        relation = self._relation_from_link_proto(context, link_proto, link_node.direction,
                                                                       proprefs)
        relation.caosnode = link_node
        return relation

    def _process_record(self, context, expr, cte):
        my_elements = []
        attribute_map = []
        testref = None

        if expr.linkprop_xvalue:
            for e in expr.elements:
                element = self._process_expr(context, e, cte)
                my_elements.append(element)

            if context.current.output_format == caos_types.JsonOutputFormat:
                attribute_map = ['t', 'p']
            else:
                attribute_map = ['value', 'attrs']
            testref = my_elements[0]
        else:
            for e in expr.elements:
                element = self._process_expr(context, e, cte)
                ptr_direction = caos_types.OutboundDirection
                ptr_target = None

                if isinstance(e, tree.ast.MetaRef):
                    ptr_name = e.name
                    testref = element
                elif isinstance(e, tree.ast.BaseRef):
                    ptr_name = e.ptr_proto.normal_name()
                    ptr_target = e.ptr_proto.target
                elif isinstance(e, (tree.ast.Record, tree.ast.SubgraphRef)):
                    ptr_name = e.rlink.link_proto.normal_name()
                    ptr_direction = e.rlink.direction or caos_types.OutboundDirection
                    if ptr_direction == caos_types.OutboundDirection:
                        ptr_target = e.rlink.link_proto.target
                    else:
                        ptr_target = e.rlink.link_proto.source

                if isinstance(ptr_name, caos_name.Name):
                    attr_name = caos_types.PointerVector(name=ptr_name.name, module=ptr_name.module,
                                                         direction=ptr_direction,
                                                         target=ptr_target.name)
                else:
                    attr_name = ptr_name

                    if (isinstance(e, tree.ast.MetaRef)
                            and context.current.output_format == caos_types.JsonOutputFormat):
                        attr_name = '$sxcls{}$'.format(attr_name)

                attribute_map.append(attr_name)
                my_elements.append(element)

        proto_class = expr.concept.get_canonical_class()
        proto_class_name = '{}.{}'.format(proto_class.__module__, proto_class.__name__)
        marker = pg_session.RecordInfo(attribute_map=attribute_map,
                                       proto_class=proto_class_name,
                                       proto_name=expr.concept.name,
                                       is_xvalue=expr.linkprop_xvalue)

        context.current.record_info[marker.id] = marker
        context.current.session.backend._register_record_info(marker)

        marker = pgsql.ast.ConstantNode(value=marker.id)
        marker_type = pgsql.ast.TypeNode(name='caos.known_record_marker_t')
        marker = pgsql.ast.TypeCastNode(expr=marker, type=marker_type)

        my_elements.insert(0, marker)

        result = pgsql.ast.RowExprNode(args=my_elements)

        if testref is not None:
            when_cond = pgsql.ast.NullTestNode(expr=testref)

            when_expr = pgsql.ast.CaseWhenNode(expr=when_cond,
                                               result=pgsql.ast.ConstantNode(value=None))
            result = pgsql.ast.CaseExprNode(args=[when_expr], default=result)

        return result


class SimpleExprTransformer(CaosExprTransformer):
    def transform(self, tree, local=False):
        context = TransformerContext()
        context.current.local = local

        qtree = self._process_expr(context, tree)

        return qtree

    def _process_expr(self, context, expr):
        if isinstance(expr, tree.ast.BinOp):
            left = self._process_expr(context, expr.left)
            right = self._process_expr(context, expr.right)
            result = pgsql.ast.BinOpNode(left=left, op=expr.op, right=right)

        elif isinstance(expr, tree.ast.UnaryOp):
            operand = self._process_expr(context, expr.expr)
            result = pgsql.ast.UnaryOpNode(op=expr.op, operand=operand)

        elif isinstance(expr, tree.ast.AtomicRefExpr):
            result = self._process_expr(context, expr.expr)

        elif isinstance(expr, tree.ast.AtomicRefSimple):
            field_name = common.caos_name_to_pg_name(expr.name)

            if not context.current.local:
                table = self._relation_from_concepts(context, expr.ref, context.current.query)
                result = pgsql.ast.FieldRefNode(table=table, field=field_name, origin=table,
                                                origin_field=field_name)
            else:
                result = pgsql.ast.FieldRefNode(table=None, field=field_name, origin=None,
                                                origin_field=field_name)

        elif isinstance(expr, tree.ast.LinkPropRefExpr):
            result = self._process_expr(context, expr.expr)

        elif isinstance(expr, tree.ast.LinkPropRefSimple):
            proto_schema = context.current.proto_schema
            stor_info = types.get_pointer_storage_info(proto_schema, expr.ptr_proto,
                                                       resolve_type=False)
            field_name = stor_info.column_name

            if not context.current.local:
                table = self._relation_from_link(context, expr.ref)
                result = pgsql.ast.FieldRefNode(table=table, field=field_name, origin=table,
                                                origin_field=field_name)
            else:
                result = pgsql.ast.FieldRefNode(table=None, field=field_name, origin=None,
                                                origin_field=field_name)

        elif isinstance(expr, tree.ast.Disjunction):
            variants = [self._process_expr(context, path) for path in expr.paths]

            if len(variants) == 1:
                result = variants[0]
            else:
                result = pgsql.ast.FunctionCallNode(name='coalesce', args=variants)

        elif isinstance(expr, tree.ast.Sequence):
            elements = [self._process_expr(context, e) for e in expr.elements]
            result = pgsql.ast.SequenceNode(elements=elements)

        else:
            assert False, "unexpected node type: %r" % expr

        return result


class CaosTreeTransformer(CaosExprTransformer):
    @debug
    def transform(self, query, session, output_format=None):
        try:
            # Transform to sql tree
            context = TransformerContext()
            context.current.session = session
            context.current.proto_schema = session.proto_schema
            context.current.output_format = output_format
            qtree = self._transform_tree(context, query)
            argmap = context.current.argmap

            """LOG [caos.query] SQL Tree
            self._dump(qtree)
            """

            # Generate query text
            codegen = self._run_codegen(qtree)
            qchunks = codegen.result
            arg_index = codegen.param_index

            """LOG [caos.query]
            from metamagic.utils import markup
            qtext = ''.join(qchunks)
            markup.dump_code(qtext, lexer='sql', header='SQL Query')
            """

        except Exception as e:
            try:
                args = [e.args[0]]
            except (AttributeError, IndexError):
                args = []
            err = tree.transformer.InternalTreeTransformerError(*args)
            err_ctx = tree.transformer.TreeTransformerExceptionContext(tree=query)
            base_err._replace_context(err, err_ctx)
            raise err from e

        return qchunks, argmap, arg_index, type(qtree), tuple(context.current.record_info.values())

    def _run_codegen(self, qtree):
        codegen = pgsql.codegen.SQLSourceGenerator()
        try:
            codegen.visit(qtree)
        except pgsql.codegen.SQLSourceGeneratorError as e:
            ctx = pgsql.codegen.SQLSourceGeneratorContext(qtree, codegen.result)
            base_err._add_context(e, ctx)
            raise
        except Exception as e:
            ctx = pgsql.codegen.SQLSourceGeneratorContext(qtree, codegen.result)
            err = pgsql.codegen.SQLSourceGeneratorError('error while generating SQL source')
            base_err._add_context(err, ctx)
            raise err from e

        return codegen

    def _dump(self, tree):
        markup.dump(tree)

    def _transform_tree(self, context, graph):
        context.current.query.subquery_referrers = graph.referrers

        if graph.cges:
            for cge in graph.cges:
                with context(TransformerContext.SUBQUERY):
                    context.current.query = pgsql.ast.CTENode()
                    cte = self._transform_tree(context, cge.expr)
                    cte.alias = cge.alias
                context.current.query.ctes.add(cte)
                context.current.explicit_cte_map[cge.expr] = cte

        if graph.generator:
            expr = self._process_generator(context, graph.generator)
            if getattr(expr, 'aggregates', False):
                context.current.query.having = expr
            else:
                context.current.query.where = expr

        self._join_subqueries(context, context.current.query)

        # Gather all subqueries not appearing in filter and consolidate them into a subquery
        # for easy reference in the main query.
        #
        non_generating_subgraphs = []
        for subgraph in graph.subgraphs:
            if 'generator' not in subgraph.referrers and 'exists' not in subgraph.referrers:
                non_generating_subgraphs.append(subgraph)

        if non_generating_subgraphs:
            context.current.query = self._consolidate_subqueries(context, context.current.query,
                                                                 non_generating_subgraphs)


        self._process_selector(context, graph.selector, context.current.query)
        self._process_sorter(context, graph.sorter)

        self._process_groupby(context, graph.grouper)

        if graph.offset:
            context.current.query.offset = self._process_constant(context, graph.offset)

        if graph.limit:
            context.current.query.limit = self._process_constant(context, graph.limit)

        if graph.op in ('update', 'delete'):
            if graph.op == 'delete':
                query = pgsql.ast.DeleteQueryNode()
            else:
                query = pgsql.ast.UpdateQueryNode()

            op_is_update = graph.op == 'update'
            opvalues = graph.opvalues

            # Standard entity set processing produces a whole CTE, while for UPDATE and DELETE
            # we need just the origin table.  Thus, use a dummy CTE here and repace the op's
            # fromexpr with a direct reference to a table
            #
            if isinstance(graph.optarget, tree.ast.LinkPropRefSimple):
                prop = graph.optarget

                # Cannot call _relation_from_link here as DELETE/UPDATE only work on
                # single tables and _relation_from_link can produce any relation.
                #
                query.fromexpr = self._table_from_link_proto(context, prop.ref.link_proto)

                ref_map = {prop.ref.link_proto: query.fromexpr}
                context.current.link_node_map[prop.ref] = {'local_ref_map': ref_map}

                sprop_name = common.caos_name_to_pg_name('metamagic.caos.builtins.source')
                sref = pgsql.ast.FieldRefNode(table=query.fromexpr,
                                              field=sprop_name,
                                              origin=query.fromexpr,
                                              origin_field=sprop_name)

                idprop_name = common.caos_name_to_pg_name('metamagic.caos.builtins.linkid')
                idref = pgsql.ast.FieldRefNode(table=query.fromexpr,
                                              field=idprop_name,
                                              origin=query.fromexpr,
                                              origin_field=idprop_name)

                filter = idref

            elif isinstance(graph.optarget, tree.ast.AtomicRefSimple):
                # Singular atom delete op translates into source table update
                query = pgsql.ast.UpdateQueryNode()
                op_is_update = True
                opvalues = [tree.ast.UpdateExpr(expr=graph.optarget,
                                                value=tree.ast.Constant(value=None))]

                query.fromexpr = self._relation_from_concepts(context, graph.optarget.ref, query)

                ref_map = {aref.name: query.fromexpr for aref in graph.optarget.ref.atomrefs}
                context.current.concept_node_map[graph.optarget.ref] = {'local_ref_map': ref_map}
                context.current.ctemap[query] = {graph.optarget.ref: query}

                filter = pgsql.ast.FieldRefNode(table=query.fromexpr,
                                                field='metamagic.caos.builtins.id',
                                                origin=query.fromexpr,
                                                origin_field='metamagic.caos.builtins.id')

                sref = idref = filter

            else:
                query.fromexpr = self._relation_from_concepts(context, graph.optarget, query)

                ref_map = {aref.name: query.fromexpr for aref in graph.optarget.atomrefs}
                context.current.concept_node_map[graph.optarget] = {'local_ref_map': ref_map}
                context.current.ctemap[query] = {graph.optarget: query}

                filter = pgsql.ast.FieldRefNode(table=query.fromexpr,
                                                field='metamagic.caos.builtins.id',
                                                origin=query.fromexpr,
                                                origin_field='metamagic.caos.builtins.id')

            query.where = pgsql.ast.BinOpNode(left=filter, op='IN', right=context.current.query)

            with context(TransformerContext.NEW_TRANSPARENT):
                # Make sure there's no walking back on links -- we're processing
                # _only_ the path tip here and "glue" the path head with the above WHERE.
                #
                context.current.unwind_rlinks = False

                if isinstance(graph.optarget, (tree.ast.LinkPropRefSimple, tree.ast.AtomicRefSimple)):
                    idexpr = pgsql.ast.SelectExprNode(expr=idref, alias='linkid')
                    query.targets.append(idexpr)

                    sexpr = pgsql.ast.SelectExprNode(expr=sref, alias='source')
                    query.targets.append(sexpr)

                self._process_selector(context, graph.opselector, query)

                if op_is_update:
                    for expr in opvalues:
                        field = self._process_expr(context, expr.expr)
                        value = self._process_expr(context, expr.value)
                        query.values.append(pgsql.ast.UpdateExprNode(expr=field, value=value))
        else:
            query = context.current.query

        self._postprocess_query(context, query)

        if not context.current.in_subquery:
            query.ctes = OrderedSet(context.current.global_ctes.values()) | query.ctes

        if graph.recurse_link is not None:
            # Looping on a specified link, generate WITH RECURSIVE
            query = self._generate_recursive_query(context, query, graph.recurse_link,
                                                                   graph.recurse_depth)

        if graph.aggregate_result:
            if len(query.targets) > 1:
                raise ValueError('cannot auto-aggregate: too many columns in subquery')

            target = query.targets[0].expr

            if isinstance(graph.selector[0].expr, (tree.ast.AtomicRefSimple,
                                                   tree.ast.LinkPropRefSimple)):
                # Cast atom refs to the base type in aggregate expressions, since
                # PostgreSQL does not create array types for custom domains and will
                # fail to process a query with custom domains appearing as array elements.
                #
                pgtype = types.pg_type_from_atom(context.current.proto_schema,
                                                 graph.selector[0].expr.ptr_proto.target,
                                                 topbase=True)
                pgtype = pgsql.ast.TypeNode(name=pgtype)
                target = pgsql.ast.TypeCastNode(expr=target, type=pgtype)

            args = []

            for arg in graph.aggregate_result.args:
                if isinstance(arg, tree.ast.Constant):
                    arg = self._process_expr(context, arg, query)
                else:
                    arg = target

                args.append(arg)

            subexpr = None

            if graph.aggregate_result.name == ('agg', 'list'):
                aggfunc = 'array_agg'
            elif graph.aggregate_result.name == ('agg', 'join'):
                separator, ref = args[:2]
                try:
                    ignore_nulls = args[2] and args[2].value
                except IndexError:
                    ignore_nulls = False

                if not ignore_nulls:
                    array_agg = pgsql.ast.FunctionCallNode(name='array_agg', args=[ref],
                                                           agg_sort=query.orderby)
                    subexpr = pgsql.ast.FunctionCallNode(name='array_to_string',
                                                         args=[array_agg, separator])
                    subexpr.args.append(pgsql.ast.ConstantNode(value=''))
                else:
                    aggfunc = 'string_agg'
                    args = [ref, separator]

            elif graph.aggregate_result.name == ('agg', 'count'):
                aggfunc = 'count'
            else:
                msg = 'unexpected auto-aggregate function: {}'.format(graph.aggregate_result.name)
                raise ValueError(msg)

            if subexpr is None:
                subexpr = pgsql.ast.FunctionCallNode(name=aggfunc, args=args,
                                                     agg_sort=query.orderby)

            if graph.recurse_link is not None:
                # Wrap the array into another record, so that the driver can detect
                # and properly transform the array into a tree of objects

                attribute_map = ['data']

                recurse_link = graph.recurse_link

                child_end = recurse_link.source
                parent_end = recurse_link.target

                if recurse_link.direction == caos_types.InboundDirection:
                    parent_end, child_end = child_end, parent_end

                proto_class = child_end.concept.get_canonical_class()
                proto_class_name = '{}.{}'.format(proto_class.__module__, proto_class.__name__)

                recptr_name = recurse_link.link_proto.normal_name()
                recptr_direction = recurse_link.direction

                recursive_attr = caos_types.PointerVector(name=recptr_name.name,
                                                          module=recptr_name.module,
                                                          direction=recptr_direction,
                                                          target=child_end.concept.name)

                marker = pg_session.RecordInfo(attribute_map=attribute_map,
                                               recursive_link=recursive_attr,
                                               proto_class=proto_class_name,
                                               proto_name=child_end.concept.name)

                context.current.record_info[marker.id] = marker
                context.current.session.backend._register_record_info(marker)

                marker = pgsql.ast.ConstantNode(value=marker.id)
                marker_type = pgsql.ast.TypeNode(name='caos.known_record_marker_t')
                marker = pgsql.ast.TypeCastNode(expr=marker, type=marker_type)

                subexpr = pgsql.ast.RowExprNode(args=[marker, subexpr])

            query.orderby = []
            query.targets = [pgsql.ast.SelectExprNode(expr=subexpr,
                                                      alias=query.targets[0].alias)]

        if graph.backend_text_override:
            argmap = context.current.argmap

            text_override = graph.backend_text_override
            text_override = re.sub(r'\$(\w+)', lambda m: '$' + str(argmap.index(m.group(1)) + 1),
                                   text_override)

            query.text_override = text_override

        return query

    def _generate_recursive_query(self, context, query, recurse_link, recurse_depth):
        idptr = caos_name.Name('metamagic.caos.builtins.id')

        child_end = recurse_link.source
        parent_end = recurse_link.target

        if recurse_depth is not None:
            recurse_depth = self._process_constant(context, recurse_depth)

        if recurse_link.direction == caos_types.InboundDirection:
            parent_end, child_end = child_end, parent_end

        parent_propref = tree.ast.AtomicRefSimple(name=idptr, ref=parent_end)
        parent_ref = self._process_expr(context, parent_propref, query)

        child_propref = tree.ast.AtomicRefSimple(name=idptr, ref=child_end)
        child_ref = self._process_expr(context, child_propref, query)

        sort_exprs = {}

        for sortexpr in query.orderby:
            sel = pgsql.ast.SelectExprNode(expr=sortexpr.expr,
                                           alias=context.current.genalias(hint=sortexpr.expr.field))
            sort_exprs[sel] = sortexpr
            query.targets.append(sel)

        depth_start = pgsql.ast.ConstantNode(value=0)

        query.targets.append(pgsql.ast.SelectExprNode(expr=child_ref, alias='__target__'))
        query.targets.append(pgsql.ast.SelectExprNode(expr=depth_start, alias='__depth__'))

        query.orderby = []

        recursive_part = pgsql.ast.SelectQueryNode()
        recursive_part.targets = query.targets[:]

        recursive_part.where = query.where
        recursive_part.fromlist = query.fromlist[:]

        rec_cte = pgsql.ast.CTENode(
            op = pgsql.ast.UNION,
            larg = query,
            rarg = recursive_part,
            recursive = True
        )

        parent_depth_ref = pgsql.ast.FieldRefNode(field='__depth__', table=rec_cte)
        one = pgsql.ast.ConstantNode(value=1)
        next_depth = pgsql.ast.BinOpNode(left=parent_depth_ref, op=ast.ops.ADD, right=one)

        cond_parent_ref = pgsql.ast.FieldRefNode(field='__target__', table=rec_cte)
        cond = pgsql.ast.BinOpNode(left=parent_ref, right=cond_parent_ref, op=ast.ops.EQ)

        recursive_part.where = self.extend_binop(recursive_part.where, cond,
                                                 cls=pgsql.ast.BinOpNode)

        if recurse_depth is not None:
            depth_cond = pgsql.ast.BinOpNode(left=next_depth, op=ast.ops.LT,
                                             right=recurse_depth)
            zero = pgsql.ast.ConstantNode(value=0)
            depth_is_zero = pgsql.ast.BinOpNode(left=recurse_depth, op=ast.ops.LE,
                                                right=zero)
            depth_cond = pgsql.ast.BinOpNode(left=depth_is_zero, op=ast.ops.OR, right=depth_cond)
            recursive_part.where = self.extend_binop(recursive_part.where, depth_cond,
                                                     cls=pgsql.ast.BinOpNode)

        recursive_part.targets[-1] = pgsql.ast.SelectExprNode(expr=next_depth, alias='__depth__')

        elements = []

        for target in query.targets:
            elements.append(pgsql.ast.TableFuncElement(name=target.alias))

        rec_cte.alias = pgsql.ast.FuncAliasNode(alias=context.current.genalias(hint='recq'),
                                                elements=elements)

        recursive_part.fromlist.append(pgsql.ast.FromExprNode(expr=rec_cte))

        rec_cte_wrap = pgsql.ast.SelectQueryNode(alias=context.current.genalias(hint='recqwrap'))
        rec_cte_wrap.ctes.add(rec_cte)
        rec_cte_wrap.fromlist.append(pgsql.ast.FromExprNode(expr=rec_cte))

        for target in query.targets:
            if target not in sort_exprs:
                fref = pgsql.ast.FieldRefNode(field=target.alias)
                selexpr = pgsql.ast.SelectExprNode(expr=fref, alias=target.alias)
                rec_cte_wrap.targets.append(selexpr)

        for select_expr, sort_expr in sort_exprs.items():
            fref = pgsql.ast.FieldRefNode(field=select_expr.alias)
            sexpr = pgsql.ast.SortExprNode(expr=fref, direction=sort_expr.direction,
                                           nulls_order=sort_expr.nulls_order)
            rec_cte_wrap.orderby.append(sexpr)

        result = pgsql.ast.SelectQueryNode()
        result.fromlist.append(pgsql.ast.FromExprNode(expr=rec_cte_wrap))

        row_args = []

        attribute_map = []

        for target in query.targets:
            if target not in sort_exprs:
                fieldref = pgsql.ast.FieldRefNode(field=target.alias, table=rec_cte_wrap)
                row_args.append(fieldref)
                attribute_map.append(target.alias)

        marker = pg_session.RecordInfo(attribute_map=attribute_map)

        context.current.record_info[marker.id] = marker
        context.current.session.backend._register_record_info(marker)

        marker = pgsql.ast.ConstantNode(value=marker.id)
        marker_type = pgsql.ast.TypeNode(name='caos.known_record_marker_t')
        marker = pgsql.ast.TypeCastNode(expr=marker, type=marker_type)

        row_args.insert(0, marker)

        target = pgsql.ast.RowExprNode(args=row_args)
        result.targets.append(pgsql.ast.SelectExprNode(expr=target))

        if query.outerbonds:
            result.proxyouterbonds[query] = query.outerbonds

        return result

    def _consolidate_subqueries(self, context, query, subgraphs):
        # Turn the given ``query`` into a subquery and translated ``subgraphs``
        # into the target list so that expressions in upper query can reference
        # subquery results.
        #

        wrapper = pgsql.ast.SelectQueryNode()
        query.alias = context.current.genalias()
        from_ = pgsql.ast.FromExprNode(expr=query)
        wrapper.fromlist.append(from_)

        # Pull up the target list
        self._pull_fieldrefs(context, wrapper, query)

        outer_refs = set()

        for subgraph in subgraphs:
            # Put subqueries into the target list
            #
            subgraph_ref = tree.ast.SubgraphRef(ref=subgraph)
            subquery = self._process_expr(context, subgraph_ref)
            refname = subquery.targets[0].alias

            alias = context.current.genalias(hint=refname)
            expr = pgsql.ast.SelectExprNode(expr=subquery, alias=alias)
            query.targets.append(expr)

            refexpr = pgsql.ast.FieldRefNode(table=query, field=expr.alias,
                                             origin=subquery, origin_field=expr.alias)
            selectnode = pgsql.ast.SelectExprNode(expr=refexpr, alias=expr.alias)
            context.current.concept_node_map[subgraph] = {refname: selectnode}

            if subquery.outerbonds:
                outer_refs.update(b[0] for b in subquery.outerbonds)

            if subquery.proxyouterbonds:
                for bonds in subquery.proxyouterbonds.values():
                    outer_refs.update(b[0] for b in bonds)

        if context.current.subquery_map:
            for subgraph, subquery in context.current.subquery_map.items():
                # Put all explicit references to attributes of joined subqueries into the
                # target list.
                #
                for attrref in subgraph.attrrefs:
                    refexpr = pgsql.ast.FieldRefNode(table=subquery, field=attrref,
                                                     origin=subquery, origin_field=attrref)
                    alias = context.current.genalias(hint=attrref)
                    selexpr = pgsql.ast.SelectExprNode(expr=refexpr, alias=alias)
                    query.targets.append(selexpr)

                    refexpr = pgsql.ast.FieldRefNode(table=query, field=alias,
                                                     origin=subquery, origin_field=attrref)
                    selexpr = pgsql.ast.SelectExprNode(expr=refexpr, alias=alias)

                    try:
                        subgraph_map = context.current.concept_node_map[subgraph]
                    except KeyError:
                        subgraph_map = context.current.concept_node_map[subgraph] = {}
                    subgraph_map[attrref] = selexpr

        # Pull up CTEs
        context.current.ctemap[wrapper] = context.current.ctemap[query]
        wrapper.ctes = query.ctes
        wrapper.proxyouterbonds = {query: query.outerbonds[:]}
        query.ctes = OrderedSet()

        for outer_ref in outer_refs:
            if outer_ref not in context.current.concept_node_map:
                # Outer ref is not visible on this query level as it has
                # been pushed into a subquery of it's own due to link cardinality
                # rule,
                continue

            # Join references to CTEs
            #
            outerbonds = self._pull_outerbonds(context, outer_ref, query)
            self._connect_subquery_outerbonds(context, outerbonds, wrapper)

            callback = functools.partial(self._inject_relation_from_caosnode, context, wrapper)

            try:
                context.current.ctemap[query][outer_ref]
            except KeyError:
                try:
                    callbacks = context.current.node_callbacks[outer_ref]
                except KeyError:
                    callbacks = context.current.node_callbacks[outer_ref] = []
                callbacks.append(callback)
            else:
                callback(outer_ref)

        return wrapper

    def _pull_outerbonds(self, context, outer_ref, target_rel):
        pulled_bonds = []

        oref = context.current.concept_node_map[outer_ref]['metamagic.caos.builtins.id']
        target_rel.targets.append(oref)

        refexpr = pgsql.ast.FieldRefNode(table=target_rel, field=oref.alias,
                                         origin=target_rel, origin_field=oref.alias)
        selectnode = pgsql.ast.SelectExprNode(expr=refexpr, alias=oref.alias)

        pulled_bonds.append((outer_ref, selectnode))

        return pulled_bonds

    def _inject_relation_from_caosnode(self, context, query, caosnode):
        cte = context.current.ctemap[query][caosnode]
        return self._inject_relation(context, query, cte)

    def _inject_relation(self, context, query, rel):

        # Make sure we don't inject the same CTE twice
        for fromexpr in query.fromlist:
            if fromexpr.expr == rel:
                break
        else:
            fromexpr = pgsql.ast.FromExprNode(expr=rel)
            query.fromlist.append(fromexpr)

    def _inject_outerbond_condition(self, context_l, subquery, inner_ref, outer_ref, inline=False,
                                                                          parent_cte=None):
        field_ref = inner_ref.expr

        for fromexpr in subquery.fromlist:
            if fromexpr.expr == field_ref.table:
                break
        else:
            # May be a ref to a part of the path pushed into a subquery due to cardinality
            # constraints in _process_(dis|con)junction, if so, push the condition into that query.
            if isinstance(field_ref.table, pgsql.ast.SelectQueryNode):
                subquery = field_ref.table
                field_ref = pgsql.ast.FieldRefNode(table=field_ref.origin,
                                                   field=field_ref.origin_field,
                                                   origin=field_ref.origin,
                                                   origin_field=field_ref.origin_field)
            else:
                raise ValueError('invalid inner reference in subquery bond')

        idcol = 'metamagic.caos.builtins.id'

        if ((context_l.direct_subquery_ref or inline) and context_l.location == 'nodefilter'):
            outer_ref = context_l.concept_node_map[outer_ref]
            # The subquery is _inside_ the parent query's WHERE, it's not a joined CTE
            outer_ref = outer_ref['local_ref_map'][idcol]

            if isinstance(outer_ref, list):
                ref_table = outer_ref[0]
            else:
                ref_table = outer_ref

            outer_ref = pgsql.ast.FieldRefNode(table=ref_table, field=idcol,
                                               origin=ref_table, origin_field=idcol)

            if isinstance(field_ref.table, pgsql.ast.SelectQueryNode):
                # Push the filter into the actual relation representing the EntitySet, not just
                # its parent query.
                origin = field_ref.origin
                if isinstance(origin, list):
                    assert len(origin) == 1
                    origin = origin[0]

                inner_rel = field_ref.table
                inner_ref = pgsql.ast.FieldRefNode(table=origin,
                                                   field=field_ref.origin_field,
                                                   origin=field_ref.origin,
                                                   origin_field=field_ref.origin_field)

                comparison = pgsql.ast.BinOpNode(left=outer_ref, op=ast.ops.EQ, right=inner_ref)
                inner_rel.where = self.extend_binop(inner_rel.where, comparison,
                                                    cls=pgsql.ast.BinOpNode)
        else:
            outer_ref = context_l.concept_node_map[outer_ref]
            outer_ref = outer_ref[idcol].expr

        comparison = pgsql.ast.BinOpNode(left=outer_ref, op=ast.ops.EQ, right=field_ref)
        subquery.where = self.extend_binop(subquery.where, comparison, cls=pgsql.ast.BinOpNode)

    def _connect_subquery_outerbonds(self, context, outerbonds, subquery, inline=False,
                                                                          parent_cte=None):
        if subquery.proxyouterbonds:
            # A subquery may be wrapped by another relation, e.g. a recursive CTE, which
            # "proxies" the original outer bonds of its non-recursive part.
            for proxied_subquery, proxied_outerbonds in subquery.proxyouterbonds.items():
                self._connect_subquery_outerbonds(context, proxied_outerbonds, proxied_subquery,
                                                  inline=inline)

        for outer_ref, inner_ref in outerbonds:
            if outer_ref in context.current.concept_node_map:
                self._inject_outerbond_condition(context.current, subquery, inner_ref, outer_ref,
                                                 inline=inline, parent_cte=parent_cte)
            else:
                # The outer ref has not been processed yet, put it in a queue
                # and glue the bond when it appears.
                callback = functools.partial(self._inject_outerbond_condition,
                                             context.current, subquery, inner_ref, inline=inline,
                                             parent_cte=parent_cte)
                try:
                    callbacks = context.current.node_callbacks[outer_ref]
                except KeyError:
                    callbacks = context.current.node_callbacks[outer_ref] = []
                callbacks.append(callback)

    def _postprocess_query(self, context, query):
        ctes = set(ast.find_children(query, lambda i: isinstance(i, pgsql.ast.SelectQueryNode)))
        for cte in ctes:
            if cte.where_strong:
                cte.where = self.extend_predicate(cte.where, cte.where_strong, ast.ops.AND,
                                                  strong=getattr(cte.where_strong, 'strong', False))
            if cte.where_weak and cte.where is not cte.where_weak:
                op = ast.ops.AND if getattr(cte.where, 'strong', False) else ast.ops.OR
                cte.where = self.extend_predicate(cte.where, cte.where_weak, op)

    def _join_subqueries(self, context, query):
        if context.current.subquery_map:
            if query.fromlist:
                join_point = query.fromlist[0].expr
            else:
                join_point = None

            cte_subqueries = []

            for subquery in context.current.subquery_map.values():
                condition = None

                if subquery.outerbonds:
                    for outer_ref, inner_ref in subquery.outerbonds:
                        outer_ref = context.current.concept_node_map[outer_ref]
                        outer_ref = outer_ref['metamagic.caos.builtins.id']

                        subquery.targets.append(inner_ref)
                        if subquery.aggregates:
                            subquery.groupby.append(inner_ref.expr)

                        left = outer_ref.expr
                        right = pgsql.ast.FieldRefNode(table=subquery, field=inner_ref.alias,
                                                       origin=inner_ref.expr.origin,
                                                       origin_field=inner_ref.expr.origin_field)
                        comparison = pgsql.ast.BinOpNode(left=left, op=ast.ops.EQ, right=right)
                        condition = self.extend_binop(condition, comparison, cls=pgsql.ast.BinOpNode)

                    if join_point:
                        join_point = pgsql.ast.JoinNode(type='left', left=join_point, right=subquery,
                                                        condition=condition)
                    else:
                        join_point = subquery
                else:
                    cte_subqueries.append(subquery)

            if join_point is not None:
                if query.fromlist:
                    query.fromlist[0].expr = join_point
                else:
                    query.fromlist = [pgsql.ast.FromExprNode(expr=join_point)]

            if cte_subqueries:
                for q in cte_subqueries:
                    query.ctes.add(q)
                    q.__class__ = pgsql.ast.CTENode
                    query.fromlist.append(pgsql.ast.FromExprNode(expr=q))

    def _process_generator(self, context, generator):
        context.current.location = 'generator'
        result = self._process_expr(context, generator)
        if isinstance(result, pgsql.ast.IgnoreNode):
            result = None
        elif isinstance(result, pgsql.ast.FieldRefNode):
            result = pgsql.ast.UnaryOpNode(operand=pgsql.ast.NullTestNode(expr=result),
                                           op=ast.ops.NOT)

        context.current.location = None
        return result

    def _process_selector(self, context, selector, query):
        context.current.location = 'selector'

        selexprs = []

        for expr in selector:
            alias = common.caos_name_to_pg_name(expr.name or expr.autoname)
            pgexpr = self._process_expr(context, expr.expr, query)
            selexprs.append((pgexpr, alias))

        if (context.current.output_format == caos_types.JsonOutputFormat
                                        and not context.current.in_subquery):
            elems = []
            for pgexpr, alias in selexprs:
                alias_c = pgsql.ast.ConstantNode(value=str(alias))
                elems.append(pgsql.ast.RowExprNode(args=[alias_c, pgexpr]))

            target = pgsql.ast.SelectExprNode(expr=pgsql.ast.RowExprNode(args=elems))
            target = pgsql.ast.FunctionCallNode(name='row_to_json', args=[target])
            query.targets.append(pgsql.ast.SelectExprNode(expr=target))

        else:
            for pgexpr, alias in selexprs:
                target = pgsql.ast.SelectExprNode(expr=pgexpr, alias=alias)
                query.targets.append(target)


    def _process_sorter(self, context, sorter):
        query = context.current.query
        context.current.location = 'sorter'

        for expr in sorter:
            sortexpr = pgsql.ast.SortExprNode(expr=self._process_expr(context, expr.expr),
                                              direction=expr.direction, nulls_order=expr.nones_order)
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
            return expr.concept.name == 'metamagic.caos.builtins.BaseObject'

        return False

    def is_entity_set(self, expr):
        """Determine whether the given expression represents a set of entities.

        Arguments:
            - expr: Expression to test

        Return:
            True if the expression represents a set of entities, False otherwise.
        """

        return isinstance(expr, (tree.ast.PathCombination, tree.ast.EntitySet))

    def get_cte_fieldref_for_set(self, context, caos_node, link_name, meta=False, map=None):
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

        field_name = common.caos_name_to_pg_name(link_name)
        ref = None

        ref_key = ('meta', link_name) if meta else link_name

        try:
            # First, check if we have a local map with direct table references.
            #
            ref_table = cte_refs['local_ref_map'][ref_key]
        except KeyError:
            # Then check subqueries
            #
            ref = cte_refs.get(ref_key)
        else:
            if meta and field_name == 'id':
                field_name = 'concept_id'

            if isinstance(ref_table, list):
                if len(ref_table) == 1:
                    ref = pgsql.ast.FieldRefNode(table=ref_table[0], field=field_name,
                                                 origin=ref_table[0], origin_field=field_name)
                else:
                    refs = [pgsql.ast.FieldRefNode(table=t, field=field_name,
                                                  origin=t, origin_field=field_name)
                            for t in ref_table]
                    ref = pgsql.ast.FunctionCallNode(name='coalesce', args=refs)
            else:
                ref = pgsql.ast.FieldRefNode(table=ref_table, field=field_name,
                                             origin=ref_table, origin_field=field_name)

        if ref is None:
            msg = 'could not resolve "{}"."{}" as table field'.format(caos_node.concept.name,
                                                                      ref_key)
            raise LookupError(msg)

        if isinstance(ref, pgsql.ast.SelectExprNode):
            ref = ref.expr

        if context.current.in_aggregate and not meta:
            # Cast atom refs to the base type in aggregate expressions, since
            # PostgreSQL does not create array types for custom domains and will
            # fail to process a query with custom domains appearing as array elements.
            #
            res = caos_node.concept.resolve_pointer(context.current.proto_schema, link_name,
                                                    look_in_children=True)
            link = next(iter(res[1]))
            pgtype = types.pg_type_from_atom(context.current.proto_schema, link.target,
                                             topbase=True)
            pgtype = pgsql.ast.TypeNode(name=pgtype)
            ref = pgsql.ast.TypeCastNode(expr=ref, type=pgtype)


        return ref

    def _process_constant(self, context, expr):
        if expr.type:
            if isinstance(expr.type, caos_types.ProtoAtom):
                const_type = types.pg_type_from_atom(context.current.proto_schema, expr.type, topbase=True)
            elif isinstance(expr.type, caos_types.ProtoConcept):
                const_type = 'record'
            elif isinstance(expr.type, tuple):
                item_type = expr.type[1]
                if isinstance(item_type, caos_types.ProtoAtom):
                    item_type = types.pg_type_from_atom(context.current.proto_schema, item_type, topbase=True)
                    const_type = '%s[]' % item_type
                elif isinstance(item_type, caos_types.ProtoConcept):
                    item_type = 'record'
                    const_type = '%s[]' % item_type
                else:
                    const_type = common.py_type_to_pg_type(expr.type)
            else:
                const_type = common.py_type_to_pg_type(expr.type)
        else:
            const_type = None

        if expr.expr:
            result = pgsql.ast.ConstantNode(expr=self._process_expr(context, expr.expr))
        else:
            value = expr.value
            const_expr = None

            if expr.index is not None and not isinstance(expr.index, int):
                if expr.index in context.current.argmap:
                    index = context.current.argmap.index(expr.index)
                else:
                    context.current.argmap.add(expr.index)
                    index = len(context.current.argmap) - 1
            else:
                index = expr.index
                data_backend = context.current.session.backend

                if isinstance(value, caos_types.ProtoConcept):
                    classes = (value,)
                elif isinstance(value, tuple) and value and \
                                                    isinstance(value[0], caos_types.ProtoConcept):
                    classes = value
                else:
                    classes = None

                if classes:
                    concept_ids = {data_backend.get_concept_id(cls, context.current.session,
                                                               cache='always') for cls in classes}
                    for cls in classes:
                        for c in cls.descendants(context.current.proto_schema):
                            concept_id = data_backend.get_concept_id(c, context.current.session,
                                                                     cache='always')
                            concept_ids.add(concept_id)

                    const_type = common.py_type_to_pg_type(classes[0].__class__)
                    elements = [pgsql.ast.ConstantNode(value=cid) for cid in concept_ids]
                    const_expr = pgsql.ast.SequenceNode(elements=elements)
                    value = None

            result = pgsql.ast.ConstantNode(value=value, expr=const_expr, index=index,
                                            type=const_type)

        if expr.substitute_for:
            result.origin_field = common.caos_name_to_pg_name(expr.substitute_for)

        return result

    def _text_search_refs(self, context, vector):
        for link_name, link in vector.concept.get_searchable_links():
            yield tree.ast.AtomicRefSimple(ref=vector, name=link_name, ptr_proto=link)

    def _build_text_search_conf_map_cte(self, context):
        code_map = [
            ('en', 'english'),
            ('ru', 'russian')
        ]

        map_array = []
        for code, confname in code_map:
            item = pgsql.ast.RowExprNode(args=[
                pgsql.ast.TypeCastNode(expr=pgsql.ast.ConstantNode(value=code),
                                       type=pgsql.ast.TypeNode(name='text')),
                pgsql.ast.TypeCastNode(expr=pgsql.ast.ConstantNode(value=confname),
                                       type=pgsql.ast.TypeNode(name='regconfig'))
            ])
            map_array.append(item)

        code_map_cte = pgsql.ast.CTENode(alias='text_search_conf_name_code_map')
        code_map_cte.fromlist.append(
            pgsql.ast.FromExprNode(
                alias = pgsql.ast.FuncAliasNode(
                    alias = 'map',
                    elements = [
                        pgsql.ast.TableFuncElement(name='code',
                                                   type=pgsql.ast.TypeNode(name='text')),
                        pgsql.ast.TableFuncElement(name='confname',
                                                   type=pgsql.ast.TypeNode(name='regconfig'))

                    ]
                ),

                expr = pgsql.ast.FunctionCallNode(
                    name = 'unnest',
                    args = [pgsql.ast.ArrayNode(elements=map_array)]
                )
            )
        )
        code_map_cte.targets.extend([
            pgsql.ast.SelectExprNode(expr=pgsql.ast.FieldRefNode(field='code')),
            pgsql.ast.SelectExprNode(expr=pgsql.ast.FieldRefNode(field='confname'))
        ])

        lang_arg = self._process_constant(context, tree.ast.Constant(index='__context_lang'))

        code_conv_union = pgsql.ast.CTENode(alias='text_search_conf_map', op=pgsql.ast.UNION)

        one = pgsql.ast.ConstantNode(value=1)
        two = pgsql.ast.ConstantNode(value=2)
        iso2 = pgsql.ast.FunctionCallNode(name='substr', args=[lang_arg, one, two])
        variants = [lang_arg, iso2]

        code_conv_union_list = []
        for variant in variants:
            qry = pgsql.ast.SelectQueryNode()
            qry.targets.append(
                pgsql.ast.SelectExprNode(expr=pgsql.ast.FieldRefNode(field='confname'))
            )
            qry.fromlist.append(
                pgsql.ast.FromExprNode(expr=code_map_cte)
            )
            coderef = pgsql.ast.FieldRefNode(table=code_map_cte, field='code')
            qry.where = pgsql.ast.BinOpNode(left=coderef, op=ast.ops.EQ, right=variant)
            code_conv_union_list.append(qry)

        code_conv_union_list.append(pgsql.ast.SelectQueryNode(
            targets=[
                pgsql.ast.SelectExprNode(expr=pgsql.ast.ConstantNode(value='english'))
            ]
        ))

        self._setop_from_list(code_conv_union, code_conv_union_list, pgsql.ast.UNION)

        code_conv_union.limit = one
        code_conv_union.ctes.add(code_map_cte)
        context.current.global_ctes['text_search_conf_map'] = code_conv_union

        return code_conv_union

    def _get_text_search_conf_ref(self, context):
        if 'text_search_conf_map' not in context.current.global_ctes:
            self._build_text_search_conf_map_cte(context)

        return pgsql.ast.SelectQueryNode(
            targets = [
                pgsql.ast.SelectExprNode(expr=pgsql.ast.FieldRefNode(field='confname'))
            ],
            fromlist = [
                context.current.global_ctes['text_search_conf_map']
            ]
        )

    def _text_search_args(self, context, vector, query, tsvector=True, extended=False):
        empty_str = pgsql.ast.ConstantNode(value='')
        sep_str = pgsql.ast.ConstantNode(value='; ')

        text_search_conf_ref = self._get_text_search_conf_ref(context)

        cols = None

        if isinstance(vector, tree.ast.EntitySet):
            refs = [(r, r.ptr_proto.search.weight) for r in self._text_search_refs(context, vector)]

        elif isinstance(vector, tree.ast.Sequence):
            refs = [(r, r.ptr_proto.search.weight) for r in vector.elements]

        elif isinstance(vector, tree.ast.AtomicRef):
            link = vector.ref.concept.getptr(context.current.proto_schema, vector.name)
            ref = tree.ast.AtomicRefSimple(ref=vector.ref, name=vector.name, ptr_proto=link)
            refs = [(ref, ref.ptr_proto.search.weight)]

        elif isinstance(vector, tree.ast.LinkPropRef):
            ref = tree.ast.LinkPropRefSimple(ref=vector.ref, name=vector.name,
                                             ptr_proto=vector.ptr_proto)
            refs = [(ref, caos_types.SearchWeight_A)]

        elif isinstance(vector, tree.ast.SearchVector):
            refs = []

            for elem in vector.items:
                refs.append((elem.ref, elem.weight))
        else:
            assert False, "unexpected node type: %r" % vector

        if tsvector:
            for atomref, weight in refs:
                ref = self._process_expr(context, atomref)
                ref = pgsql.ast.FunctionCallNode(name='coalesce', args=[ref, empty_str])
                ref = pgsql.ast.FunctionCallNode(name='to_tsvector',
                                                 args=[text_search_conf_ref, ref])
                weight_const = pgsql.ast.ConstantNode(value=weight)
                ref = pgsql.ast.FunctionCallNode(name='setweight', args=[ref, weight_const])
                cols = self.extend_predicate(cols, ref, op='||')
        else:
            cols = pgsql.ast.ArrayNode(elements=[self._process_expr(context, r[0]) for r in refs])
            cols = pgsql.ast.FunctionCallNode(name='array_to_string', args=[cols, sep_str])

        query = self._process_expr(context, query)

        if extended:
            query = pgsql.ast.FunctionCallNode(name='to_tsquery', args=[text_search_conf_ref,
                                                                        query])
        else:
            query = pgsql.ast.FunctionCallNode(name='plainto_tsquery', args=[text_search_conf_ref,
                                                                             query])

        return cols, query

    def _is_subquery(self, path):
        return isinstance(path, (tree.ast.ExistPred, tree.ast.GraphExpr)) \
               or (isinstance(path, tree.ast.UnaryOp) and isinstance(path.expr, tree.ast.ExistPred))

    def _path_weight(self, path):
        if self._is_subquery(path):
            return 2
        elif isinstance(path, tree.ast.BinOp) and \
                (self._is_subquery(path.left) or self._is_subquery(path.right)):
            return 2
        else:
            return 1

    def _sort_paths(self, paths):
        sorted_paths = sorted(paths, key=self._path_weight)
        return list(sorted_paths)

    def _process_expr(self, context, expr, cte=None):
        result = None

        if isinstance(expr, tree.ast.GraphExpr):
            with context(TransformerContext.SUBQUERY):
                result = self._transform_tree(context, expr)

        elif isinstance(expr, tree.ast.SubgraphRef):
            subgraph = expr.ref

            try:
                result = context.current.concept_node_map[subgraph][expr.name].expr
            except KeyError:
                try:
                    explicit_cte = context.current.explicit_cte_map[subgraph]

                except KeyError:
                    if (expr.force_inline or context.current.direct_subquery_ref
                                          or 'generator' not in subgraph.referrers):
                        # Subqueries in selector should always go into SQL selector
                        subquery = self._process_expr(context, subgraph, cte)
                        self._connect_subquery_outerbonds(context, subquery.outerbonds, subquery,
                                                          inline=expr.force_inline, parent_cte=cte)
                        result = subquery
                    else:
                        subquery = context.current.subquery_map.get(subgraph)
                        if subquery is None:
                            subquery = self._process_expr(context, subgraph, cte)
                            subquery.alias = context.current.genalias(hint='sq')
                            context.current.subquery_map[subgraph] = subquery

                        result = pgsql.ast.FieldRefNode(table=subquery, field=expr.name,
                                                        origin=subquery, origin_field=expr.name)
                        alias = context.current.genalias(hint=expr.name)
                        selexpr = pgsql.ast.SelectExprNode(expr=result, alias=alias)

                        try:
                            subgraph_map = context.current.concept_node_map[subgraph]
                        except KeyError:
                            subgraph_map = context.current.concept_node_map[subgraph] = {}
                        subgraph_map[expr.name] = selexpr

                else:
                    result = pgsql.ast.FieldRefNode(table=explicit_cte, field=expr.name,
                                                    origin=explicit_cte, origin_field=expr.name)
                    alias = context.current.genalias(hint=expr.name)
                    selexpr = pgsql.ast.SelectExprNode(expr=result, alias=alias)

                    try:
                        subgraph_map = context.current.concept_node_map[subgraph]
                    except KeyError:
                        subgraph_map = context.current.concept_node_map[subgraph] = {}
                    subgraph_map[expr.name] = selexpr

                    self._inject_relation(context, context.current.query, explicit_cte)

        elif isinstance(expr, tree.ast.Disjunction):
            #context.current.append_graphs = True
            sorted_paths = self._sort_paths(expr.paths)
            variants = [self._process_expr(context, path, cte) for path in sorted_paths]
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
            sorted_paths = self._sort_paths(expr.paths)
            variants = [self._process_expr(context, path, cte) for path in sorted_paths]
            variants = [v for v in variants if v and not isinstance(v, pgsql.ast.IgnoreNode)]
            if variants:
                if len(variants) == 1:
                    result = variants[0]
                else:
                    result = pgsql.ast.BinOpNode(left=variants[0], op=ast.ops.AND,
                                                 right=variants[1])
                    for v in variants[2:]:
                        result = pgsql.ast.BinOpNode(left=result, op=ast.ops.AND, right=v)
            else:
                result = pgsql.ast.IgnoreNode()

        elif isinstance(expr, tree.ast.InlineFilter):
            self._process_expr(context, expr.ref, cte)
            result = pgsql.ast.IgnoreNode()

        elif isinstance(expr, tree.ast.InlinePropFilter):
            if (expr.ref.target and isinstance(expr.ref.target, tree.ast.EntitySet)
                            and not isinstance(expr.ref.target.concept, caos_types.ProtoAtom)):
                entityset = expr.ref.target
            else:
                entityset = expr.ref.source

            self._process_expr(context, entityset, cte)
            result = pgsql.ast.IgnoreNode()

        elif isinstance(expr, tree.ast.EntitySet):
            if context.current.unwind_rlinks:
                root = self.get_caos_path_root(expr)
            else:
                root = expr
            self._process_graph(context, cte or context.current.query, root)
            result = pgsql.ast.IgnoreNode()

            try:
                callbacks = context.current.node_callbacks.pop(expr)
            except KeyError:
                pass
            else:
                for callback in callbacks:
                    callback(expr)

        elif isinstance(expr, tree.ast.EntityLink):
            if (expr.target and isinstance(expr.target, tree.ast.EntitySet)
                            and not isinstance(expr.target.concept, caos_types.ProtoAtom)):
                self._process_expr(context, expr.target, cte)
            else:
                self._process_expr(context, expr.source, cte)

            result = pgsql.ast.IgnoreNode()

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
                if expr.op in (ast.ops.IN, ast.ops.NOT_IN) \
                        and isinstance(expr.right, tree.ast.Constant) \
                        and isinstance(expr.right.expr, tree.ast.Sequence):
                    with context(TransformerContext.NEW_TRANSPARENT):
                        context.current.sequence_is_array = True
                        right = self._process_expr(context, expr.right, cte)
                else:
                    right = self._process_expr(context, expr.right, cte)

            if isinstance(expr.op, tree.ast.TextSearchOperator):
                vector, query = self._text_search_args(context, expr.left, expr.right,
                                                       extended=expr.op == tree.ast.SEARCHEX)
                result = pgsql.ast.BinOpNode(left=vector, right=query, op=tree.ast.SEARCH)
            else:
                context.current.append_graphs = False

                cte = cte or context.current.query

                if expr.op in (ast.ops.IN, ast.ops.NOT_IN) and \
                                                    isinstance(expr.right, tree.ast.Constant):
                    # "expr IN $CONST" translates into "expr = any($CONST)"
                    # and "expr NOT IN $CONST" translates into "expr != all($CONST)"
                    if expr.op == ast.ops.IN:
                        op = ast.ops.EQ
                        qual_func = 'any'
                    else:
                        op = ast.ops.NE
                        qual_func = 'all'

                    if isinstance(right.expr, pgsql.ast.SequenceNode):
                        right.expr = pgsql.ast.ArrayNode(elements=right.expr.elements)
                    elif right.type == 'text[]':
                        left_type = self.get_expr_type(expr.left, context.current.proto_schema)
                        if isinstance(left_type, caos_types.ProtoNode):
                            if isinstance(left_type, caos_types.ProtoConcept):
                                left_type = left_type.pointers['metamagic.caos.builtins.id'].target
                            left_type = types.pg_type_from_atom(context.current.proto_schema,
                                                                left_type, topbase=True)
                            right.type = left_type + '[]'

                    right = pgsql.ast.FunctionCallNode(name=qual_func, args=[right])
                else:
                    op = expr.op

                # Fold constant ops into the inner query filter.
                if isinstance(expr.left, tree.ast.Constant) and isinstance(right, pgsql.ast.IgnoreNode):
                    if expr.op == ast.ops.OR:
                        cte.fromlist[0].expr.where_weak = self.extend_predicate(cte.fromlist[0].expr.where_weak,
                                                                                left, op)
                    else:
                        cte.fromlist[0].expr.where_strong = self.extend_predicate(cte.fromlist[0].expr.where_strong,
                                                                                  left, op)
                    left = pgsql.ast.IgnoreNode()

                elif isinstance(expr.right, tree.ast.Constant) and isinstance(left, pgsql.ast.IgnoreNode):
                    if expr.op == ast.ops.OR:
                        cte.fromlist[0].expr.where_weak = self.extend_predicate(cte.fromlist[0].expr.where_weak,
                                                                                right, op)
                    else:
                        cte.fromlist[0].expr.where_strong = self.extend_predicate(cte.fromlist[0].expr.where_strong,
                                                                                  right, op)
                    right = pgsql.ast.IgnoreNode()

                if isinstance(left, pgsql.ast.IgnoreNode) and isinstance(right, pgsql.ast.IgnoreNode):
                    result = pgsql.ast.IgnoreNode()
                elif isinstance(left, pgsql.ast.IgnoreNode) or isinstance(right, pgsql.ast.IgnoreNode):
                    if isinstance(left, pgsql.ast.IgnoreNode):
                        result, from_expr = right, expr.right
                    elif isinstance(right, pgsql.ast.IgnoreNode):
                        result, from_expr = left, expr.left

                    if context.current.location in ('generator', 'nodefilter', 'linkfilter') and \
                            getattr(from_expr, 'aggregates', False):
                        context.current.query.having = result
                        result = pgsql.ast.IgnoreNode()
                else:
                    left_aggregates = getattr(expr.left, 'aggregates', False)
                    op_aggregates = getattr(expr, 'aggregates', False)

                    if context.current.location in ('generator', 'nodefilter', 'linkfilter') and \
                                                            left_aggregates and not op_aggregates:
                        context.current.query.having = left
                        result = right
                    else:
                        left_type = self.get_expr_type(expr.left, context.current.proto_schema)
                        right_type = self.get_expr_type(expr.right, context.current.proto_schema)

                        if left_type and right_type:
                            if isinstance(left_type, caos_types.ProtoNode):
                                if isinstance(left_type, caos_types.ProtoConcept):
                                    left_type = left_type.pointers['metamagic.caos.builtins.id'].target
                                left_type = types.pg_type_from_atom(context.current.proto_schema,
                                                                    left_type, topbase=True)
                            elif not isinstance(left_type, caos_types.ProtoObject) and \
                                        (not isinstance(left_type, tuple) or \
                                         not isinstance(left_type[1], caos_types.ProtoObject)):
                                left_type = common.py_type_to_pg_type(left_type)

                            if isinstance(right_type, caos_types.ProtoNode):
                                if isinstance(right_type, caos_types.ProtoConcept):
                                    right_type = right_type.pointers['metamagic.caos.builtins.id'].target
                                right_type = types.pg_type_from_atom(context.current.proto_schema,
                                                                    right_type, topbase=True)
                            elif not isinstance(right_type, caos_types.ProtoObject) and \
                                        (not isinstance(right_type, tuple) or \
                                         not isinstance(right_type[1], caos_types.ProtoObject)):
                                right_type = common.py_type_to_pg_type(right_type)

                            if left_type in ('text', 'varchar') and \
                                    right_type in ('text', 'varchar') and op == ast.ops.ADD:
                                op = '||'
                            elif left_type != right_type:
                                if isinstance(right, pgsql.ast.ConstantNode) and right_type == 'text':
                                    right.type = left_type
                                elif isinstance(left, pgsql.ast.ConstantNode) and left_type == 'text':
                                    left.type = right_type

                            if (isinstance(right, pgsql.ast.ConstantNode) \
                                    and op in {ast.ops.IS, ast.ops.IS_NOT}):
                                right.type = None

                        result = pgsql.ast.BinOpNode(op=op, left=left, right=right,
                                                     aggregates=op_aggregates,
                                                     strong=expr.strong)

        elif isinstance(expr, tree.ast.UnaryOp):
            operand = self._process_expr(context, expr.expr, cte)
            result = pgsql.ast.UnaryOpNode(op=expr.op, operand=operand)

        elif isinstance(expr, tree.ast.NoneTest):
            operand = self._process_expr(context, expr.expr, cte)
            result = pgsql.ast.NullTestNode(expr=operand)

        elif isinstance(expr, tree.ast.Constant):
            result = self._process_constant(context, expr)

        elif isinstance(expr, tree.ast.TypeCast):
            if isinstance(expr.expr, tree.ast.BinOp) and \
                                        isinstance(expr.expr.op, (ast.ops.ComparisonOperator,
                                                                  ast.ops.EquivalenceOperator)):
                expr_type = bool
            elif isinstance(expr.expr, tree.ast.BaseRefExpr) and \
                        isinstance(expr.expr.expr, tree.ast.BinOp) and \
                        isinstance(expr.expr.expr.op, (ast.ops.ComparisonOperator,
                                                       ast.ops.EquivalenceOperator)):
                expr_type = bool
            elif isinstance(expr.expr, tree.ast.Constant):
                expr_type = expr.expr.type
            else:
                expr_type = None

            schema = context.current.proto_schema
            int_proto = schema.get('metamagic.caos.builtins.int')

            pg_expr = self._process_expr(context, expr.expr, cte)

            if expr_type and expr_type is bool and expr.type.issubclass(int_proto):
                when_expr = pgsql.ast.CaseWhenNode(expr=pg_expr,
                                                   result=pgsql.ast.ConstantNode(value=1))
                default = pgsql.ast.ConstantNode(value=0)
                result = pgsql.ast.CaseExprNode(args=[when_expr], default=default)
            else:
                if isinstance(expr.type, tuple):
                    typ = expr.type[1]
                else:
                    typ = expr.type
                type = types.pg_type_from_atom(schema, typ, topbase=True)

                if isinstance(expr.type, tuple):
                    type = pgsql.ast.TypeNode(name=type, array_bounds=[-1])
                else:
                    type = pgsql.ast.TypeNode(name=type)
                result = pgsql.ast.TypeCastNode(expr=pg_expr, type=type)

        elif isinstance(expr, tree.ast.Sequence):
            elements = [self._process_expr(context, e, cte) for e in expr.elements]
            if expr.is_array:
                result = pgsql.ast.ArrayNode(elements=elements)
            elif getattr(context.current, 'sequence_is_array', False):
                result = pgsql.ast.SequenceNode(elements=elements)
            else:
                if context.current.output_format == caos_types.JsonOutputFormat:
                    elements.insert(0, pgsql.ast.ConstantNode(value=pg_driver.FREEFORM_RECORD_ID))
                result = pgsql.ast.RowExprNode(args=elements)

        elif isinstance(expr, tree.ast.Record):
            result = self._process_record(context, expr, cte)

        elif isinstance(expr, tree.ast.FunctionCall):
            result = self._process_function(context, expr, cte)

        elif isinstance(expr, tree.ast.AtomicRefExpr):
            result = self._process_expr(context, expr.expr, cte)

        elif isinstance(expr, tree.ast.LinkPropRefExpr):
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

        elif isinstance(expr, tree.ast.LinkPropRefSimple):
            self._process_expr(context, expr.ref, cte)

            link = expr.ref

            cte_refs = context.current.link_node_map[link]
            local_ref_map = cte_refs.get('local_ref_map')
            if local_ref_map:
                cteref = local_ref_map[expr.ref.link_proto]

                proto_schema = context.current.proto_schema
                stor_info = types.get_pointer_storage_info(proto_schema, expr.ptr_proto,
                                                           resolve_type=False)
                colname = stor_info.column_name
                fieldref = pgsql.ast.FieldRefNode(table=cteref, field=colname,
                                                  origin=cteref, origin_field=colname)
            else:
                fieldref = cte_refs.get(expr.name)
                assert fieldref, 'Reference to an inaccessible link table node %s' % expr.name

            if isinstance(fieldref, pgsql.ast.SelectExprNode):
                fieldref = fieldref.expr

            if context.current.in_aggregate:
                # Cast prop refs to the base type in aggregate expressions, since
                # PostgreSQL does not create array types for custom domains and will
                # fail to process a query with custom domains appearing as array elements.
                #
                prop = expr.ref.link_proto.getptr(context.current.proto_schema, expr.name)
                pgtype = types.pg_type_from_atom(context.current.proto_schema, prop.target,
                                                 topbase=True)
                pgtype = pgsql.ast.TypeNode(name=pgtype)
                fieldref = pgsql.ast.TypeCastNode(expr=fieldref, type=pgtype)

            result = fieldref

            schema = context.current.proto_schema

            if expr.name == 'metamagic.caos.builtins.target':
                localizable = schema.get('metamagic.caos.extras.l10n.localizable',
                                         default=None)
                str_t = schema.get('metamagic.caos.builtins.str')

                link_proto = expr.ptr_proto.source

                if localizable is not None and link_proto.issubclass(localizable) \
                                           and link_proto.target.issubclass(str_t):
                    lang = pgsql.ast.IdentNode(name='C')
                    result = pgsql.ast.CollateClauseNode(expr=result, collation_name=lang)

        elif isinstance(expr, tree.ast.ExistPred):
            with context(TransformerContext.NEW_TRANSPARENT):
                context.current.direct_subquery_ref = True
                context.current.ignore_cardinality = 'recursive'
                expr = self._process_expr(context, expr.expr, cte)

            result = pgsql.ast.ExistsNode(expr=expr)

        elif isinstance(expr, tree.ast.SearchVector):
            for elem in expr.items:
                self._process_expr(context, elem.ref, cte)

            result = pgsql.ast.IgnoreNode()

        else:
            assert False, "Unexpected expression: %s" % expr

        return result

    def _process_function(self, context, expr, cte):
        if expr.name == ('search', 'rank'):
            vector, query = self._text_search_args(context, *expr.args,
                                                   extended=expr.kwargs.get('extended'))
            # Normalize rank to a scale from 0 to 1
            normalization = pgsql.ast.ConstantNode(value=32)
            args = [vector, query, normalization]
            result = pgsql.ast.FunctionCallNode(name='ts_rank_cd', args=args)

        elif expr.name == ('search', 'headline'):
            kwargs = expr.kwargs.copy()
            extended = kwargs.pop('extended', False)
            vector, query = self._text_search_args(context, *expr.args, tsvector=False,
                                                   extended=extended)
            lang = self._get_text_search_conf_ref(context)

            args=[lang, vector, query]

            if kwargs:
                for i, (name, value) in enumerate(kwargs.items()):
                    value = self._process_expr(context, value, cte)
                    left = pgsql.ast.ConstantNode(value=str(name))
                    right = pgsql.ast.ConstantNode(value='=')
                    left = pgsql.ast.BinOpNode(left=left, op='||', right=right)
                    right = pgsql.ast.FunctionCallNode(name='quote_ident', args=[value])
                    value = pgsql.ast.BinOpNode(left=left, op='||', right=right)

                    if i == 0:
                        options = value
                    else:
                        left = options
                        right = pgsql.ast.ConstantNode(value=',')
                        left = pgsql.ast.BinOpNode(left=left, op='||', right=right)
                        right = value
                        options = pgsql.ast.BinOpNode(left=left, op='||', right=right)

                args.append(options)

            result = pgsql.ast.FunctionCallNode(name='ts_headline', args=args)

        else:
            result = None
            agg_sort = []
            if expr.aggregates:
                with context(context.NEW_TRANSPARENT):
                    context.current.in_aggregate = True
                    context.current.query.aggregates = True
                    args = [self._process_expr(context, a, cte) for a in expr.args]

                if expr.agg_sort:
                    for sortexpr in expr.agg_sort:
                        _sortexpr = self._process_expr(context, sortexpr.expr, cte)
                        agg_sort.append(pgsql.ast.SortExprNode(expr=_sortexpr,
                                                               direction=sortexpr.direction,
                                                               nulls_order=sortexpr.nones_order))

            else:
                args = [self._process_expr(context, a, cte) for a in expr.args]

            if expr.name == 'if':
                cond = self._process_expr(context, expr.args[0], cte)
                pos = self._process_expr(context, expr.args[1], cte)
                neg = self._process_expr(context, expr.args[2], cte)
                when_expr = pgsql.ast.CaseWhenNode(expr=cond,
                                                   result=pos)
                result = pgsql.ast.CaseExprNode(args=[when_expr], default=neg)
            elif expr.name == ('agg', 'sum'):
                name = 'sum'
            elif expr.name == ('agg', 'product'):
                name = common.qname('caos', 'agg_product')
            elif expr.name == ('agg', 'avg'):
                name = 'avg'
            elif expr.name == ('agg', 'min'):
                name = 'min'
            elif expr.name == ('agg', 'max'):
                name = 'max'
            elif expr.name == ('agg', 'list'):
                name = 'array_agg'
            elif expr.name == ('agg', 'join'):
                name = 'string_agg'
                separator, ref = args[:2]
                try:
                    ignore_nulls = args[2] and args[2].value
                except IndexError:
                    ignore_nulls = False

                if not ignore_nulls:
                    array_agg = pgsql.ast.FunctionCallNode(name='array_agg', args=[ref],
                                                           agg_sort=agg_sort)
                    result = pgsql.ast.FunctionCallNode(name='array_to_string',
                                                        args=[array_agg, separator])
                    result.args.append(pgsql.ast.ConstantNode(value=''))
                else:
                    args = [ref, separator]
            elif expr.name == ('agg', 'count'):
                name = 'count'
            elif expr.name == ('agg', 'stddev_pop'):
                name = 'stddev_pop'
            elif expr.name == ('agg', 'stddev_samp'):
                name = 'stddev_samp'
            elif expr.name == ('window', 'lag') or expr.name == ('window', 'lead'):
                schema = context.current.proto_schema
                name = expr.name[1]
                if len(args) > 1:
                    args[1] = pgsql.ast.TypeCastNode(expr=args[1],
                                                     type=pgsql.ast.TypeNode(name='int'))
                if len(args) > 2:
                    arg0_type = self.get_expr_type(expr.args[0], schema)
                    arg0_type = pg_types.pg_type_from_atom(schema, arg0_type)
                    args[2] = pgsql.ast.TypeCastNode(expr=args[2],
                                                     type=pgsql.ast.TypeNode(name=arg0_type))
            elif expr.name == ('math', 'abs'):
                name = 'abs'
            elif expr.name == ('math', 'round'):
                name = 'round'
            elif expr.name == ('math', 'min'):
                name = 'least'
            elif expr.name == ('math', 'max'):
                name = 'greatest'
            elif expr.name == ('math', 'list_sum'):
                subq = pgsql.ast.SelectQueryNode()
                op = pgsql.ast.FunctionCallNode(name='sum', args=[pgsql.ast.FieldRefNode(field='i')])
                subq.targets.append(op)
                arr = self._process_expr(context, expr.args[0], cte)
                if isinstance(arr, pgsql.ast.ConstantNode):
                    if isinstance(arr.expr, pgsql.ast.SequenceNode):
                        arr = pgsql.ast.ArrayNode(elements=arr.expr.elements)

                lower = pgsql.ast.BinOpNode(left=self._process_expr(context, expr.args[1], cte),
                                            op=ast.ops.ADD,
                                            right=pgsql.ast.ConstantNode(value=1, type='int'))
                upper = self._process_expr(context, expr.args[2], cte)
                indirection = pgsql.ast.IndexIndirectionNode(lower=lower, upper=upper)
                arr = pgsql.ast.IndirectionNode(expr=arr, indirection=indirection)
                unnest = pgsql.ast.FunctionCallNode(name='unnest', args=[arr])
                subq.fromlist.append(pgsql.ast.FromExprNode(expr=unnest, alias='i'))
                zero = pgsql.ast.ConstantNode(value=0, type='int')
                result = pgsql.ast.FunctionCallNode(name='coalesce', args=[subq, zero])
            elif expr.name == ('datetime', 'to_months'):
                years = pgsql.ast.FunctionCallNode(name='date_part',
                                                   args=[pgsql.ast.ConstantNode(value='year'),
                                                         args[0]])
                years = pgsql.ast.BinOpNode(left=years, op=ast.ops.MUL,
                                            right=pgsql.ast.ConstantNode(value=12))
                months = pgsql.ast.FunctionCallNode(name='date_part',
                                                    args=[pgsql.ast.ConstantNode(value='month'),
                                                          args[0]])
                result = pgsql.ast.BinOpNode(left=years, op=ast.ops.ADD, right=months)
            elif expr.name == ('datetime', 'extract'):
                name = 'date_part'
            elif expr.name == ('datetime', 'truncate'):
                name = 'date_trunc'
            elif expr.name == ('datetime', 'current_time'):
                result = pgsql.ast.FunctionCallNode(name='current_time', noparens=True)
            elif expr.name == ('datetime', 'current_datetime'):
                result = pgsql.ast.FunctionCallNode(name='current_timestamp', noparens=True)
            elif expr.name == ('str', 'replace'):
                name = 'replace'
            elif expr.name == ('str', 'len'):
                name = 'char_length'
            elif expr.name == ('str', 'lower'):
                name = 'lower'
            elif expr.name == ('str', 'upper'):
                name = 'upper'
            elif expr.name == ('str', 'lpad'):
                name = 'lpad'
                # lpad expects the second argument to be int, so force cast it
                args[1] = pgsql.ast.TypeCastNode(expr=args[1], type=pgsql.ast.TypeNode(name='int'))
            elif expr.name == ('str', 'rpad'):
                name = 'rpad'
                # rpad expects the second argument to be int, so force cast it
                args[1] = pgsql.ast.TypeCastNode(expr=args[1], type=pgsql.ast.TypeNode(name='int'))
            elif expr.name in (('str', 'trim'), ('str', 'ltrim'), ('str', 'rtrim')):
                name = expr.name[1]
            elif expr.name == ('str', 'levenshtein'):
                name = common.qname('caos', 'levenshtein')
            elif expr.name == ('str', 'strpos'):
                r = pgsql.ast.FunctionCallNode(name='strpos', args=args)
                result = pgsql.ast.BinOpNode(left=r, right=pgsql.ast.ConstantNode(value=1),
                                             op=ast.ops.SUB)
            elif expr.name == ('str', 'substr'):
                name = 'substr'
                args[1] = pgsql.ast.TypeCastNode(expr=args[1], type=pgsql.ast.TypeNode(name='int'))
                args[1] = pgsql.ast.BinOpNode(left=args[1], right=pgsql.ast.ConstantNode(value=1),
                                              op=ast.ops.ADD)
                if args[2] is not None:
                    args[2] = pgsql.ast.TypeCastNode(expr=args[2],
                                                     type=pgsql.ast.TypeNode(name='int'))
            elif expr.name == ('str', 'urlify'):
                re_1 = pgsql.ast.ConstantNode(value=r'[^\w\- ]')
                re_2 = pgsql.ast.ConstantNode(value=r'\s+')
                flags = pgsql.ast.ConstantNode(value='g')
                replacement = pgsql.ast.ConstantNode(value='')
                replace_1 = pgsql.ast.FunctionCallNode(name='regexp_replace',
                                                       args=[args[0], re_1, replacement, flags])
                replacement = pgsql.ast.ConstantNode(value='-')
                replace_2 = pgsql.ast.FunctionCallNode(name='regexp_replace',
                                                       args=[replace_1, re_2, replacement, flags])
                result = pgsql.ast.FunctionCallNode(name='lower', args=[replace_2])
            elif expr.name == 'getitem':
                index = self._process_expr(context, expr.args[1], cte)
                upper = pgsql.ast.BinOpNode(left=index, op=ast.ops.ADD,
                                            right=pgsql.ast.ConstantNode(value=1))
                indirection = pgsql.ast.IndexIndirectionNode(upper=upper)
                _expr = self._process_expr(context, expr.args[0], cte)
                result = pgsql.ast.IndirectionNode(expr=_expr, indirection=indirection)
            elif isinstance(expr.name, tuple):
                assert False, 'unsupported function %s' % (expr.name,)
            else:
                name = expr.name
            if not result:
                result = pgsql.ast.FunctionCallNode(name=name, args=args,
                                                    aggregates=bool(expr.aggregates),
                                                    agg_sort=agg_sort)

                if expr.window:
                    result.over = pgsql.ast.WindowDefNode()

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

        elif getattr(cte, 'op', None):
            for q in self._query_list_from_set_op(sqlpath):
                if isinstance(q, pgsql.ast.CTENode):
                    cte.ctes.add(q)
                    q.referrers.add(cte)

        if isinstance(cte, pgsql.ast.SelectQueryNode):
            if not cte.fromlist or not context.current.append_graphs:
                fromnode = pgsql.ast.FromExprNode()
                cte.fromlist.append(fromnode)
                fromnode.expr = sqlpath
            else:
                last = cte.fromlist.pop()
                sql_paths = [last.expr, sqlpath]
                union = self.unify_paths(context, sql_paths)
                cte.fromlist.append(union)

    def _join_condition(self, context, left_refs, right_refs, op='='):
        if not isinstance(left_refs, tuple):
            left_refs = (left_refs,)
        if not isinstance(right_refs, tuple):
            right_refs = (right_refs,)

        condition = None
        for left_ref, right_ref in itertools.product(left_refs, right_refs):
            op = pgsql.ast.BinOpNode(op='=', left=left_ref, right=right_ref)
            condition = self.extend_binop(condition, op, cls=pgsql.ast.BinOpNode)

        return condition

    def _simple_join(self, context, left, right, key, type='inner', condition=None):
        if condition is None:
            condition = left.bonds(key)[-1]
            if not isinstance(condition, pgsql.ast.BinOpNode):
                condition = right.bonds(key)[-1]
                if not isinstance(condition, pgsql.ast.BinOpNode):
                    left_refs = left.bonds(key)[-1]
                    right_refs = right.bonds(key)[-1]
                    condition = self._join_condition(context, left_refs, right_refs)

        join = pgsql.ast.JoinNode(type=type, left=left, right=right, condition=condition)

        join.updatebonds(left)
        join.updatebonds(right)

        return join

    def _pull_fieldrefs(self, context, target_rel, source_rel):
        for caosnode, refs in source_rel.concept_node_map.items():
            for field, ref in refs.items():
                refexpr = pgsql.ast.FieldRefNode(table=source_rel, field=ref.alias,
                                                 origin=ref.expr.origin,
                                                 origin_field=ref.expr.origin_field)

                fieldref = pgsql.ast.SelectExprNode(expr=refexpr, alias=ref.alias)
                target_rel.targets.append(fieldref)

                mapslot = target_rel.concept_node_map.get(caosnode)
                if not mapslot:
                    target_rel.concept_node_map[caosnode] = {field: fieldref}
                else:
                    mapslot[field] = fieldref

                if field == 'metamagic.caos.builtins.id':
                    bondref = pgsql.ast.FieldRefNode(table=target_rel, field=ref.alias,
                                                     origin=ref.expr.origin,
                                                     origin_field=ref.expr.origin_field)
                    target_rel.addbond(caosnode.id, bondref)
                context.current.concept_node_map[caosnode][field].expr.table = target_rel

        for caoslink, refs in source_rel.link_node_map.items():
            for field, ref in refs.items():
                refexpr = pgsql.ast.FieldRefNode(table=source_rel, field=ref.alias,
                                                 origin=ref.expr.origin,
                                                 origin_field=ref.expr.origin_field)

                fieldref = pgsql.ast.SelectExprNode(expr=refexpr, alias=ref.alias)
                target_rel.targets.append(fieldref)

                target_rel.link_node_map.setdefault(caoslink, {})[field] = fieldref
                context.current.link_node_map[caoslink][field].expr.table = target_rel

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

        id_field = common.caos_name_to_pg_name('metamagic.caos.builtins.id')

        if caos_path_tip and isinstance(caos_path_tip.concept, caos_types.ProtoConcept):
            concept_table = self._relation_from_concepts(context, caos_path_tip, step_cte)

            bond = pgsql.ast.FieldRefNode(table=concept_table, field=id_field,
                                          origin=concept_table, origin_field=id_field)
            concept_table.addbond(caos_path_tip.id, bond)

            tip_concepts = frozenset((caos_path_tip.concept,))
        else:
            assert sql_path_tip
            concept_table = None
            tip_concepts = None

        if not sql_path_tip:
            fromnode.expr = concept_table
        else:
            target_id_field = pgsql.ast.FieldRefNode(table=concept_table,
                                                     field=id_field,
                                                     origin=concept_table,
                                                     origin_field=id_field)

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
            map_join_type = 'left' if weak else 'inner'

            link_ref_map = {}
            tip_anchor = caos_path_tip.anchor if caos_path_tip else None
            existing_link = step_cte.linkmap.get((link.link_proto, link.direction,
                                                  link.source, tip_anchor))

            link_proto = link.link_proto

            if existing_link:
                # The same link map must not be joined more than once,
                # otherwise the cardinality of the result set will be wrong.
                #
                map = existing_link[link_proto]
                link_ref_map[link_proto] = map
            else:
                map = self._relation_from_link(context, link)
                map.concepts = tip_concepts
                link_ref_map[link_proto] = map

            source_ref = pgsql.ast.FieldRefNode(table=map, field='metamagic.caos.builtins.source',
                                                origin=map, origin_field='metamagic.caos.builtins.source')
            target_ref = pgsql.ast.FieldRefNode(table=map, field='metamagic.caos.builtins.target',
                                                origin=map, origin_field='metamagic.caos.buitlins.target')
            valent_bond = join.bonds(link.source.id)[-1]
            forward_bond = self._join_condition(context, valent_bond, source_ref, op='=')
            backward_bond = self._join_condition(context, valent_bond, target_ref, op='=')

            if link.direction == caos_types.InboundDirection:
                map_join_cond = backward_bond
                cond_expr = pgsql.ast.BinOpNode(left=source_ref, op='=', right=target_id_field)

            else:
                map_join_cond = forward_bond
                cond_expr = pgsql.ast.BinOpNode(left=target_ref, op='=', right=target_id_field)

            if not existing_link:
                if link and link.propfilter:
                    ##
                    # Switch context to link filter and make the concept table available for
                    # atoms in filter expression to reference.
                    #
                    context.push()
                    context.current.location = 'linkfilter'
                    context.current.link_node_map[link] = {'local_ref_map': link_ref_map}
                    propfilter_expr = self._process_expr(context, link.propfilter)
                    if propfilter_expr:
                        map_join_cond = pgsql.ast.BinOpNode(left=map_join_cond, op=ast.ops.AND,
                                                            right=propfilter_expr)
                    context.pop()

                join = self._simple_join(context, join, map, link.source.id,
                                         type=map_join_type,
                                         condition=map_join_cond)

            if target_bond_expr:
                target_bond_expr = pgsql.ast.BinOpNode(left=target_bond_expr, op='or', right=cond_expr)
            else:
                target_bond_expr = cond_expr

            if not existing_link:
                step_cte.linkmap[(link.link_proto, link.direction, link.source, tip_anchor)] = link_ref_map

            if concept_table:
                prev_bonds = join.bonds(caos_path_tip.id)
                join.addbond(caos_path_tip.id, target_bond_expr)
                join = self._simple_join(context, join, concept_table,
                                         caos_path_tip.id,
                                         type='left' if weak else 'inner')
                if prev_bonds:
                    join.addbond(caos_path_tip.id, prev_bonds[-1])

            fromnode.expr = join

            if is_root:
                # If this is a new query, pull the references to fields inside the CTE one level
                # up to keep them visible.
                #
                self._pull_fieldrefs(context, step_cte, sql_path_tip)

        if caos_path_tip and isinstance(caos_path_tip.concept, caos_types.ProtoConcept):
            # Process references to atoms.
            #
            atomrefs = {'metamagic.caos.builtins.id'} | \
                       {f.name for f in caos_path_tip.atomrefs}

            context.current.concept_node_map.setdefault(caos_path_tip, {})
            step_cte.concept_node_map.setdefault(caos_path_tip, {})
            aliases = {}

            concept = caos_path_tip.concept
            proto_schema = context.current.proto_schema

            ref_map = {n: [concept_table] for n, p in concept.pointers.items() if p.atomic()}
            joined_atomref_sources = {concept: concept_table}

            computables = context.current.computable_map.get(caos_path_tip, {})

            for field in atomrefs:
                try:
                    atomref_tables = ref_map[field]
                except KeyError:
                    sources, _, _ = concept.resolve_pointer(proto_schema, field,
                                                            look_in_children=True,
                                                            strict_ancestry=True)
                    assert sources
                    if concept.is_virtual:
                        # Atom refs to columns present in direct children of a virtual concept
                        # are guaranteed to be included in the relation representing the virtual
                        # concept.
                        #
                        proto_schema = context.current.proto_schema
                        chain = itertools.chain.from_iterable
                        child_ptrs = set(chain(c.pointers for c in concept.children(proto_schema)))
                        if field in child_ptrs:
                            descendants = set(concept.descendants(proto_schema))
                            sources -= descendants
                            sources.add(concept)
                    for source in sources:
                        if source not in joined_atomref_sources:
                            atomref_table = self._table_from_concept(context, source,
                                                                     caos_path_tip, step_cte)
                            joined_atomref_sources[source] = atomref_table
                            left = pgsql.ast.FieldRefNode(table=concept_table, field=id_field)
                            right = pgsql.ast.FieldRefNode(table=atomref_table, field=id_field)
                            joincond = pgsql.ast.BinOpNode(op='=', left=left, right=right)
                            fromnode.expr = self._simple_join(context, fromnode.expr,
                                                              atomref_table,
                                                              key=None, type='left',
                                                              condition=joincond)
                    ref_map[field] = atomref_tables = [joined_atomref_sources[c] for c in sources]

                colname = common.caos_name_to_pg_name(field)

                fieldrefs = [pgsql.ast.FieldRefNode(table=atomref_table, field=colname,
                                                    origin=atomref_table, origin_field=colname)
                             for atomref_table in atomref_tables]
                aliases[field] = step_cte.alias + ('_' + context.current.genalias(hint=str(field)))

                # If the required atom column was defined in multiple descendant tables
                # and there is no common parent with this column, we'll have to coalesce
                # fieldrefs to all tables.
                #
                if len(fieldrefs) > 1:
                    refexpr = pgsql.ast.FunctionCallNode(name='coalesce', args=fieldrefs)
                else:
                    refexpr = fieldrefs[0]

                selectnode = pgsql.ast.SelectExprNode(expr=refexpr, alias=aliases[field])
                step_cte.targets.append(selectnode)

                step_cte.concept_node_map[caos_path_tip][field] = selectnode

                # Record atom references in the global map in case they have to be pulled up later
                #
                refexpr = pgsql.ast.FieldRefNode(table=step_cte, field=selectnode.alias,
                                                 origin=atomref_tables, origin_field=colname)
                selectnode = pgsql.ast.SelectExprNode(expr=refexpr, alias=selectnode.alias)
                context.current.concept_node_map[caos_path_tip][field] = selectnode

                try:
                    computable = computables[field]
                except KeyError:
                    pass
                else:
                    try:
                        subquery_map = context.current.concept_node_map[computable.ref]
                    except KeyError:
                        subquery_map = context.current.concept_node_map[computable.ref] = {}

                    subquery_map[field] = selectnode

            # Process references to class attributes.
            #
            metarefs = {'id'} | {f.name for f in caos_path_tip.metarefs}

            if len(metarefs) > 1:
                if isinstance(caos_path_tip.concept, caos_types.ProtoConcept):
                    metatable = 'concept'
                else:
                    msg ='unexpected path tip type when resolving metarefs: {}' \
                                .format(caos_path_tip.concept)
                    raise ValueError(msg)

                datatable = pgsql.ast.TableNode(name=metatable,
                                                schema='caos',
                                                concepts=None,
                                                alias=context.current.genalias(hint='metaobject'))

                left = pgsql.ast.FieldRefNode(table=concept_table, field='concept_id')
                right = pgsql.ast.FieldRefNode(table=datatable, field='id')
                joincond = pgsql.ast.BinOpNode(op='=', left=left, right=right)

                fromnode.expr = self._simple_join(context, fromnode.expr, datatable, key=None,
                                                  type='left' if weak else 'inner',
                                                  condition=joincond)

            for metaref in metarefs:
                if metaref == 'id':
                    metaref_name = 'concept_id'
                    srctable = concept_table
                else:
                    metaref_name = metaref
                    srctable = datatable

                ref_map[('meta', metaref)] = srctable

                fieldref = pgsql.ast.FieldRefNode(table=srctable, field=metaref_name,
                                                  origin=srctable, origin_field=metaref_name)

                if metaref == 'title':
                    # Title is a WordCombination object with multiple grammatical forms,
                    # which is stored as an hstore in the database.  Direct reference
                    # defaults to the "singular" form
                    hstore_key = pgsql.ast.ConstantNode(value='singular')
                    op = 'operator(caos.->)'
                    fieldref = pgsql.ast.BinOpNode(left=fieldref, right=hstore_key, op=op)

                alias = context.current.genalias(hint=metaref_name)
                selectnode = pgsql.ast.SelectExprNode(expr=fieldref,
                                                      alias=step_cte.alias + ('_meta_' + alias))
                step_cte.targets.append(selectnode)
                step_cte.concept_node_map[caos_path_tip][('meta', metaref)] = selectnode

                ##
                # Record meta references in the global map in case they have to be pulled up later
                #
                refexpr = pgsql.ast.FieldRefNode(table=step_cte, field=selectnode.alias,
                                                 origin=srctable, origin_field=metaref_name)
                selectnode = pgsql.ast.SelectExprNode(expr=refexpr, alias=selectnode.alias)
                context.current.concept_node_map[caos_path_tip][('meta', metaref)] = selectnode

        if caos_path_tip and caos_path_tip.filter:
            ##
            # Switch context to node filter and make the concept table available for
            # atoms in filter expression to reference.
            #

            weak_filter = weak

            parent_expr = caos_path_tip.filter.parent

            if isinstance(parent_expr, tree.ast.InlineFilter):
                expr = parent_expr.parent
                if isinstance(expr, tree.ast.BinOp):
                    weak_filter = expr.op in (ast.ops.OR, ast.ops.IN, ast.ops.NOT_IN)

            context.push()
            context.current.location = 'nodefilter'
            context.current.concept_node_map[caos_path_tip] = {'local_ref_map': ref_map}
            expr = self._process_expr(context, caos_path_tip.filter)
            if expr:
                if weak_filter:
                    step_cte.where_weak = self.extend_predicate(step_cte.where_weak, expr, ast.ops.OR)
                else:
                    step_cte.where_strong = self.extend_predicate(step_cte.where_strong, expr, ast.ops.AND,
                                                                  strong=True)

            context.pop()

        if link and link.proprefs:
            for propref in link.proprefs:
                link_proto = link.link_proto

                maptable = link_ref_map[link_proto]

                proto_schema = context.current.proto_schema
                prop_stor_info = types.get_pointer_storage_info(proto_schema, propref.ptr_proto,
                                                                resolve_type=False)
                colname = prop_stor_info.column_name

                fieldref = pgsql.ast.FieldRefNode(table=maptable, field=colname,
                                                  origin=maptable, origin_field=colname)

                alias = str(link_proto.name) + str(propref.name)
                alias = step_cte.alias + ('_' + context.current.genalias(hint=alias))
                selectnode = pgsql.ast.SelectExprNode(expr=fieldref, alias=alias)
                step_cte.targets.append(selectnode)

                step_cte.link_node_map.setdefault(link, {})[propref.name] = selectnode

                # Record references in the global map in case they have to be pulled up later
                #
                refexpr = pgsql.ast.FieldRefNode(table=step_cte, field=selectnode.alias,
                                                 origin=map, origin_field=colname)
                selectnode = pgsql.ast.SelectExprNode(expr=refexpr, alias=selectnode.alias)
                context.current.link_node_map.setdefault(link, {})[propref.name] = selectnode

        if caos_path_tip and caos_path_tip.reference:
            # Do not attempt to resolve the outer reference here since it may have not
            # been processed yet.
            #
            outer_ref = caos_path_tip.reference

            inner_ref = context.current.concept_node_map[caos_path_tip]
            inner_ref = inner_ref['metamagic.caos.builtins.id']

            context.current.query.outerbonds.append((outer_ref, inner_ref))

        if is_root:
            step_cte.fromlist.append(fromnode)

        if caos_path_tip and isinstance(caos_path_tip.concept, caos_types.ProtoConcept):
            step_cte._source_graph = caos_path_tip

            has_bonds = step_cte.bonds(caos_path_tip.id)
            if not has_bonds:
                bond = pgsql.ast.FieldRefNode(table=step_cte, field=aliases['metamagic.caos.builtins.id'])
                step_cte.addbond(caos_path_tip.id, bond)

        return step_cte

    def extend_predicate(self, predicate, expr, op=ast.ops.AND, strong=False):
        if predicate is not None:
            return pgsql.ast.BinOpNode(op=op, left=predicate, right=expr, strong=strong)
        else:
            if isinstance(expr, pgsql.ast.BinOpNode) and not expr.strong:
                expr.strong = strong
            return expr

    def init_filter_cte(self, context, sql_path_tip, caos_path_tip):
        cte = pgsql.ast.SelectQueryNode()
        fromnode = pgsql.ast.FromExprNode()
        cte.fromlist.append(fromnode)

        concept_table = self._relation_from_concepts(context, caos_path_tip, sql_path_tip)

        field_name = 'metamagic.caos.builtins.id'
        innerref = pgsql.ast.FieldRefNode(table=concept_table, field=field_name,
                                          origin=concept_table, origin_field=field_name)
        outerref = self.get_cte_fieldref_for_set(context, caos_path_tip, field_name,
                                                 map=sql_path_tip.concept_node_map)

        bond = (innerref, outerref)
        concept_table.addbond(caos_path_tip.id, bond)
        fromnode.expr = concept_table

        target = pgsql.ast.SelectExprNode(expr=pgsql.ast.ConstantNode(value=True))
        cte.targets.append(target)

        return cte

    def _check_join_cardinality(self, context, link):
        link_proto = link.link_proto

        flt = lambda i: set(('selector', 'sorter', 'grouper')) & i.users
        if link.target:
            target_sets = {link.target} | set(getattr(link.target, 'joins', ()))
            target_outside_generator = bool(list(filter(flt, target_sets)))
        else:
            target_outside_generator = False

        link_outside_generator = bool(flt(link))

        cardinality_ok = context.current.ignore_cardinality or \
                         target_outside_generator or link_outside_generator or \
                         (link.direction == caos_types.OutboundDirection and
                          link_proto.mapping in (caos_types.OneToOne, caos_types.ManyToOne)) or \
                         (link.direction == caos_types.InboundDirection and
                          link_proto.mapping in (caos_types.OneToOne, caos_types.OneToMany))

        return cardinality_ok

    def _process_conjunction(self, context, cte, sql_path_tip, caos_path_tip, conjunction,
                                                               parent_cte, weak=False):
        sql_path = sql_path_tip

        for link in conjunction.paths:
            if isinstance(link, tree.ast.EntityLink):

                cardinality_ok = self._check_join_cardinality(context, link)

                link_target = link.target if isinstance(link.target, tree.ast.EntitySet) else None

                if cardinality_ok:
                    sql_path = self.caos_path_to_sql_path(context, cte, parent_cte, link_target,
                                                                        sql_path, link, weak)

                    sql_path = self._process_path(context, cte, sql_path, link_target, weak)
                else:
                    cte = self.init_filter_cte(context, parent_cte or sql_path_tip, caos_path_tip)
                    pred = pgsql.ast.ExistsNode(expr=cte)
                    op = ast.ops.OR if weak else ast.ops.AND
                    sql_path_tip.where = self.extend_predicate(sql_path_tip.where, pred, op)

                    with context(TransformerContext.NEW):
                        context.current.ignore_cardinality = True
                        sql_path = self.caos_path_to_sql_path(context, cte, cte, link_target,
                                                                            sql_path, link, weak)

                        self._process_path(context, cte, sql_path, link_target, weak)
                        sql_path = sql_path_tip
            else:
                sql_path = self._process_path(context, cte, sql_path, link, weak)

        return sql_path

    def _process_disjunction(self, context, cte, sql_path_tip, caos_path_tip, disjunction):
        need_union = False
        sql_paths = []

        for link in disjunction.paths:
            if isinstance(link, tree.ast.EntityLink):
                link_target = link.target if isinstance(link.target, tree.ast.EntitySet) else None

                if self._check_join_cardinality(context, link):
                    sql_path = self.caos_path_to_sql_path(context, cte, sql_path_tip,
                                                          link_target, sql_path_tip, link,
                                                          weak=True)
                    sql_path = self._process_path(context, cte, sql_path, link_target,
                                                                          weak=True)
                    sql_paths.append(sql_path)
                else:
                    cte = self.init_filter_cte(context, sql_path_tip, caos_path_tip)
                    pred = pgsql.ast.ExistsNode(expr=cte)
                    op = ast.ops.OR
                    sql_path_tip.where_weak = self.extend_predicate(sql_path_tip.where_weak,
                                                                    pred, op)

                    with context(TransformerContext.NEW):
                        context.current.ignore_cardinality = True
                        sql_path = self.caos_path_to_sql_path(context, cte, cte, link_target,
                                                              sql_path_tip, link, weak=True)

                        self._process_path(context, cte, sql_path, link_target, weak=True)

            elif isinstance(link, tree.ast.Conjunction):
                sql_path = self._process_conjunction(context, cte, sql_path_tip,
                                                     caos_path_tip, link, None, weak=True)
                sql_paths.append(sql_path)
                need_union = True
            else:
                assert False, 'unexpected expression type in disjunction path: %s' % link

        if need_union:
            result = self.unify_paths(context, sql_paths)
        else:
            result = sql_path_tip

        return result

    def _process_path(self, context, root_cte, sql_path_tip, caos_path_tip, weak=False):
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
            if caos_path_tip:
                disjunction = caos_path_tip.disjunction
                conjunction = caos_path_tip.conjunction
            else:
                disjunction = conjunction = None

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
                result = self._process_disjunction(context, root_cte, sql_path_tip, caos_path_tip,
                                                   disjunction)
            else:
                sql_paths = []
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
            op = pgsql.ast.INTERSECT
        else:
            op = pgsql.ast.UNION
        union = []
        commonctes = collections.OrderedDict()
        fieldmap = collections.OrderedDict()
        result = pgsql.ast.SelectQueryNode(op=op, alias=context.current.genalias(hint=op.lower()))

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


            path_concepts = sqlpath.concepts.union(*[c.children(context.current.proto_schema)
                                                     for c in sqlpath.concepts])
            if concepts is None:
                concepts = path_concepts.copy()
                concept_filter = None
            else:
                concept_filter = path_concepts - concepts
                concepts |= path_concepts

            if concept_filter is not None and not getattr(sqlpath, 'op', None):
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

                        left = query.concept_node_map[sqlpath.caosnode][('meta', 'id')]
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
        self._setop_from_list(result, union, op)
        return result

    def _setop_from_list(self, parent_qry, oplist, op):
        nq = len(oplist)

        assert nq >= 2, 'set operation requires at least two arguments'

        for i in range(nq):
            parent_qry.larg = oplist[i]
            if i == nq - 2:
                parent_qry.rarg = oplist[i + 1]
                break
            else:
                parent_qry.rarg = pgsql.ast.SelectQueryNode(op=op)
                parent_qry = parent_qry.rarg

    def _query_list_from_set_op(self, set_qry):
        result = []

        while set_qry.op:
            result.append(set_qry.larg)
            set_qry = set_qry.rarg
            if not set_qry.op:
                result.append(set_qry.rarg)

        return result
