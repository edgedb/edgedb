##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.ir import ast2 as irast

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import utils as s_utils

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types

from edgedb.lang.common import ast


class IRCompilerDBObjects:
    def _schema_type_to_pg_type(self, schema_type):
        ctx = self.context.current

        if isinstance(schema_type, s_atoms.Atom):
            const_type = pg_types.pg_type_from_atom(
                ctx.schema, schema_type, topbase=True)
        elif isinstance(schema_type, (s_concepts.Concept, s_links.Link)):
            const_type = 'json'
        elif isinstance(schema_type, s_obj.MetaClass):
            const_type = 'int'
        elif isinstance(schema_type, tuple):
            item_type = schema_type[1]
            if isinstance(item_type, s_atoms.Atom):
                item_type = pg_types.pg_type_from_atom(
                    ctx.schema, item_type, topbase=True)
                const_type = '%s[]' % item_type
            elif isinstance(item_type, (s_concepts.Concept, s_links.Link)):
                item_type = 'json'
                const_type = '%s[]' % item_type
            elif isinstance(item_type, s_obj.MetaClass):
                item_type = 'int'
                const_type = '%s[]' % item_type
            else:
                raise ValueError('unexpected constant type: '
                                 '{!r}'.format(schema_type))
        else:
            raise ValueError('unexpected constant type: '
                             '{!r}'.format(schema_type))

        return const_type

    def _table_from_concept(self, concept, node, parent_cte):
        ctx = self.context.current

        if concept.is_virtual:
            # Virtual concepts are represented as a UNION of selects from their
            # children, which is, for most purposes, equivalent to SELECTing
            # from a parent table.
            #
            idptr = sn.Name('std::id')
            idcol = common.edgedb_name_to_pg_name(idptr)
            atomrefs = {idptr: irast.AtomicRefSimple(ref=node, name=idptr)}
            atomrefs.update({f.name: f for f in node.atomrefs})

            cols = [(aref, common.edgedb_name_to_pg_name(aref))
                    for aref in atomrefs]

            schema = ctx.schema

            union_list = []
            children = frozenset(concept.children(schema))

            inhmap = s_utils.get_full_inheritance_map(schema, children)

            coltypes = {}

            for c, cc in inhmap.items():
                table = self._table_from_concept(c, node, parent_cte)
                qry = pgast.SelectQueryNode()
                qry.fromlist.append(table)

                for aname, colname in cols:
                    if aname in c.pointers:
                        aref = atomrefs[aname]
                        if isinstance(aref, irast.AtomicRefSimple):
                            selexpr = pgast.FieldRefNode(
                                table=table, field=colname)

                        elif isinstance(aref, irast.SubgraphRef):
                            # Result of a rewrite

                            subquery = self.visit(aref.ref)

                            with self.context.new():
                                # Make sure subquery outerbonds are connected
                                # to the proper table, which is an element of
                                # this union.
                                for i, (outerref, innerref
                                        ) in enumerate(subquery.outerbonds):
                                    if outerref == node:
                                        fref = pgast.FieldRefNode(
                                            table=table, field=idcol)
                                        cmap = ctx.ir_set_field_map
                                        cmap[node] = {
                                            idcol: pgast.SelectExprNode(
                                                expr=fref)
                                        }

                                self._connect_subquery_outerbonds(
                                    subquery.outerbonds, subquery,
                                    inline=True)

                            selexpr = subquery

                            # Record this subquery in the computables map to
                            # signal that the value has been computed, which
                            # lets  all outer references to this subgraph to be
                            # pointed to a SelectExpr in parent_cte.
                            try:
                                computables = ctx.computable_map[node]
                            except KeyError:
                                computables = ctx.computable_map[node] = {}

                            computables[aref.name] = aref

                        else:
                            raise ValueError(
                                'unexpected node in atomrefs list: {!r}'.
                                format(aref))
                    else:
                        try:
                            coltype = coltypes[aname]
                        except KeyError:
                            target_ptr = concept.resolve_pointer(
                                schema, aname, look_in_children=True)
                            coltype = pg_types.pg_type_from_atom(
                                schema, target_ptr.target)
                            coltypes[aname] = coltype

                        selexpr = pgast.ConstantNode(value=None)
                        pgtype = pgast.TypeNode(name=coltype)
                        selexpr = pgast.TypeCastNode(
                            expr=selexpr, type=pgtype)

                    qry.targets.append(
                        pgast.SelectExprNode(expr=selexpr, alias=colname))

                selexpr = pgast.FieldRefNode(
                    table=table, field='std::__class__')

                qry.targets.append(
                    pgast.SelectExprNode(
                        expr=selexpr, alias='std::__class__'))

                if cc:
                    # Make sure that all sets produced by each UNION member are
                    # disjoint so that there are no duplicates, and, most
                    # importantly, the shape of each row corresponds to the
                    # class.
                    get_concept_id = ctx.backend.get_concept_id
                    cc_ids = {get_concept_id(cls) for cls in cc}
                    cc_ids = [
                        pgast.ConstantNode(value=cc_id) for cc_id in cc_ids
                    ]
                    cc_ids = pgast.SequenceNode(elements=cc_ids)

                    qry.where = pgast.BinOpNode(
                        left=selexpr, right=cc_ids, op=ast.ops.NOT_IN)

                union_list.append(qry)

            if len(union_list) > 1:
                relation = pgast.SelectQueryNode(
                    edgedbnode=node, concepts=children, op=pgast.UNION)
                self._setop_from_list(relation, union_list, pgast.UNION)
            else:
                relation = union_list[0]

            relation.alias = ctx.genalias(hint=concept.name.name)

        else:
            table_schema_name, table_name = common.concept_name_to_table_name(
                concept.name, catenate=False)
            if concept.name.module == 'schema':
                # Redirect all queries to schema tables to edgedbss
                table_schema_name = 'edgedbss'

            relation = pgast.TableNode(
                name=table_name, schema=table_schema_name,
                concepts=frozenset({node.scls}),
                alias=ctx.genalias(hint=table_name),
                edgedbnode=node)
        return relation

    def _relation_from_concepts(self, node, parent_cte):
        return self._table_from_concept(node.scls, node, parent_cte)

    def _table_from_ptrcls(self, ptrcls):
        """Return a TableNode corresponding to a given Link."""
        table_schema_name, table_name = common.get_table_name(
            ptrcls, catenate=False)
        if ptrcls.normal_name().module == 'schema':
            # Redirect all queries to schema tables to edgedbss
            table_schema_name = 'edgedbss'
        return pgast.TableNode(
            name=table_name, schema=table_schema_name,
            alias=self.context.current.genalias(hint=table_name))

    def _relation_from_ptrcls(self, ptrcls, direction):
        """"Return a Relation subclass corresponding to a given ptr step.

        If `ptrcls` is a generic link, then a simple TableNode is returned,
        otherwise the return value may potentially be a UNION of all tables
        corresponding to a set of specialized links computed from the given
        `ptrcls` taking source inheritance into account.
        """
        ctx = self.context.current
        linkname = ptrcls.normal_name()
        endpoint = ptrcls.source

        if ptrcls.generic():
            # Generic links would capture the necessary set via inheritance.
            #
            relation = self._table_from_ptrcls(ptrcls)

        else:
            cols = []

            schema = ctx.schema

            union_list = []

            ptrclses = set()

            for source in {endpoint} | set(endpoint.descendants(schema)):
                # Sift through the descendants to see who has this link
                try:
                    src_ptrcls = source.pointers[linkname]
                except KeyError:
                    # This source has no such link, skip it
                    continue
                else:
                    if src_ptrcls in ptrclses:
                        # Seen this link already
                        continue
                    ptrclses.add(src_ptrcls)

                table = self._table_from_ptrcls(src_ptrcls)

                qry = pgast.SelectQueryNode()
                qry.fromlist.append(table)

                # Make sure all property references are pulled up properly
                for propname, colname in cols:
                    selexpr = pgast.FieldRefNode(
                        table=table, field=colname)
                    qry.targets.append(
                        pgast.SelectExprNode(expr=selexpr, alias=colname))

                union_list.append(qry)

            if len(union_list) == 0:
                # We've been given a generic link that none of the potential
                # sources contain directly, so fall back to general parent
                # table. #
                relation = self._table_from_ptrcls(ptrcls.bases[0])

            elif len(union_list) > 1:
                # More than one link table, generate a UNION clause.
                #
                relation = pgast.SelectQueryNode(op=pgast.UNION)
                self._setop_from_list(relation, union_list, pgast.UNION)

            else:
                # Just one link table, so returin it directly
                #
                relation = union_list[0].fromlist[0]

            relation.alias = ctx.genalias(hint=ptrcls.normal_name().name)

        return relation

    def _relation_from_link(self, link_node):
        ptrcls = link_node.ptrcls
        if ptrcls is None:
            ptrcls = self.context.current.schema.get('std::link')

        relation = self._relation_from_ptrcls(
            ptrcls, link_node.direction)
        relation.edgedbnode = link_node
        return relation
