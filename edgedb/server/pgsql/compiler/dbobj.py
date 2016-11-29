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

from edgedb.server.pgsql import ast2 as pgast
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

    def _range_for_concept(self, concept, parent_cte):
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
                qry = pgast.SelectQuery()
                qry.fromlist.append(table)

                for aname, colname in cols:
                    if aname in c.pointers:
                        aref = atomrefs[aname]
                        if isinstance(aref, irast.AtomicRefSimple):
                            selexpr = pgast.FieldRef(
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
                                        fref = pgast.FieldRef(
                                            table=table, field=idcol)
                                        cmap = ctx.ir_set_field_map
                                        cmap[node] = {
                                            idcol: pgast.SelectExpr(
                                                expr=fref)
                                        }

                                self._connect_subquery_outerbonds(
                                    subquery.outerbonds, subquery,
                                    inline=True)

                            selexpr = subquery
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

                        selexpr = pgast.Constant(value=None)
                        pgtype = pgast.Type(name=coltype)
                        selexpr = pgast.TypeCast(
                            expr=selexpr, type=pgtype)

                    qry.targets.append(
                        pgast.SelectExpr(expr=selexpr, alias=colname))

                selexpr = pgast.FieldRef(
                    table=table, field='std::__class__')

                qry.targets.append(
                    pgast.SelectExpr(
                        expr=selexpr, alias='std::__class__'))

                if cc:
                    # Make sure that all sets produced by each UNION member are
                    # disjoint so that there are no duplicates, and, most
                    # importantly, the shape of each row corresponds to the
                    # class.
                    get_concept_id = ctx.backend.get_concept_id
                    cc_ids = {get_concept_id(cls) for cls in cc}
                    cc_ids = [
                        pgast.Constant(value=cc_id) for cc_id in cc_ids
                    ]
                    cc_ids = pgast.Sequence(elements=cc_ids)

                    qry.where = pgast.BinOp(
                        left=selexpr, right=cc_ids, op=ast.ops.NOT_IN)

                union_list.append(qry)

            if len(union_list) > 1:
                relation = self._setop_from_list(union_list, pgast.UNION)
            else:
                relation = union_list[0]

            relation.alias = ctx.genalias(hint=concept.name.name)

        else:
            table_schema_name, table_name = common.concept_name_to_table_name(
                concept.name, catenate=False)

            if concept.name.module == 'schema':
                # Redirect all queries to schema tables to edgedbss
                table_schema_name = 'edgedbss'

            relation = pgast.Relation(
                schemaname=table_schema_name,
                relname=table_name
            )

            rvar = pgast.RangeVar(
                relation=relation,
                alias=pgast.Alias(
                    aliasname=ctx.genalias(hint=concept.name.name)
                )
            )

        return rvar

    def _range_for_set(self, ir_set, parent_cte):
        rvar = self._range_for_concept(ir_set.scls, parent_cte)
        rvar.relation.path_id = ir_set.path_id

        return rvar

    def _table_from_ptrcls(self, ptrcls):
        """Return a Table corresponding to a given Link."""
        table_schema_name, table_name = common.get_table_name(
            ptrcls, catenate=False)

        pname = ptrcls.normal_name()

        if pname.module == 'schema':
            # Redirect all queries to schema tables to edgedbss
            table_schema_name = 'edgedbss'

        relation = pgast.Relation(
            schemaname=table_schema_name, relname=table_name)

        rvar = pgast.RangeVar(
            relation=relation,
            alias=pgast.Alias(
                aliasname=self.context.current.genalias(hint=pname.name)
            )
        )

        return rvar

    def _range_for_ptrcls(self, ptrcls, direction):
        """"Return a Range subclass corresponding to a given ptr step.

        If `ptrcls` is a generic link, then a simple RangeVar is returned,
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
            rvar = self._table_from_ptrcls(ptrcls)

        else:
            cols = [
                'std::source',
                'std::target'
            ]

            schema = ctx.schema

            set_ops = []

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

                qry = pgast.SelectStmt()
                qry.from_clause.append(table)

                # Make sure all property references are pulled up properly
                for colname in cols:
                    selexpr = pgast.ColumnRef(
                        name=[table.alias.aliasname, colname])
                    qry.target_list.append(
                        pgast.ResTarget(val=selexpr, name=colname))

                set_ops.append(('union', qry))

                overlays = ctx.rel_overlays.get(src_ptrcls)
                if overlays:
                    for op, cte in overlays:
                        qry = pgast.SelectStmt(
                            target_list=[
                                pgast.ColumnRef(
                                    name=[col]
                                )
                                for col in cols],
                            from_clause=[
                                pgast.RangeVar(
                                    relation=cte
                                )
                            ]
                        )
                        set_ops.append((op, qry))

            if len(set_ops) == 0:
                # We've been given a generic link that none of the potential
                # sources contain directly, so fall back to general parent
                # table.
                rvar = self._table_from_ptrcls(ptrcls.bases[0])

            elif len(set_ops) > 1:
                # More than one link table, generate a UNION/EXCEPT clause.
                qry = pgast.SelectStmt(
                    all=True,
                    larg=set_ops[0][1]
                )

                for op, rarg in set_ops[1:]:
                    qry.op, qry.rarg = op, rarg
                    qry = pgast.SelectStmt(
                        all=True,
                        larg=qry
                    )

                qry = qry.larg

                rvar = pgast.RangeSubselect(
                    subquery=qry,
                    alias=pgast.Alias(
                        aliasname=ctx.genalias(hint=ptrcls.normal_name().name)
                    )
                )

            else:
                # Just one link table, so returin it directly
                rvar = set_ops[0][1].from_clause[0]

        return rvar

    def _range_for_pointer(self, pointer):
        return self._range_for_ptrcls(pointer.ptrcls, pointer.direction)
