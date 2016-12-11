##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import objects as s_obj

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types


class IRCompilerDBObjects:
    def _schema_type_to_pg_type(self, schema_type):
        ctx = self.context.current

        if isinstance(schema_type, s_atoms.Atom):
            const_type = pg_types.pg_type_from_atom(
                ctx.schema, schema_type, topbase=True)
        elif isinstance(schema_type, (s_concepts.Concept, s_links.Link)):
            const_type = ('json',)
        elif isinstance(schema_type, s_obj.MetaClass):
            const_type = ('uuid',)
        else:
            raise ValueError(f'unexpected constant type: {schema_type!r}')

        return const_type

    def _range_for_concept(self, concept, parent_cte, *,
                           include_overlays=True):
        ctx = self.context.current

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

        overlays = ctx.rel_overlays.get(concept)
        if overlays and include_overlays:
            set_ops = []

            qry = pgast.SelectStmt()
            qry.from_clause.append(rvar)
            qry.scls_rvar = rvar

            set_ops.append(('union', qry))

            for op, cte in overlays:
                rvar = pgast.RangeVar(
                    relation=cte,
                    alias=pgast.Alias(
                        aliasname=ctx.genalias(hint=cte.name)
                    )
                )

                qry = pgast.SelectStmt(
                    from_clause=[rvar],
                    scls_rvar=rvar
                )

                set_ops.append((op, qry))

            rvar = self._range_from_queryset(set_ops, concept)

        return rvar

    def _range_for_set(self, ir_set, parent_cte):
        rvar = self._range_for_concept(ir_set.scls, parent_cte)
        if isinstance(rvar, pgast.RangeSubselect):
            rvar.subquery.path_id = ir_set.path_id
        else:
            rvar.relation.path_id = ir_set.path_id

        return rvar

    def _table_from_ptrcls(self, ptrcls):
        """Return a Table corresponding to a given Link."""
        table_schema_name, table_name = common.get_table_name(
            ptrcls, catenate=False)

        pname = ptrcls.shortname

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

    def _range_for_ptrcls(self, ptrcls, direction, *,
                          include_overlays=True):
        """"Return a Range subclass corresponding to a given ptr step.

        If `ptrcls` is a generic link, then a simple RangeVar is returned,
        otherwise the return value may potentially be a UNION of all tables
        corresponding to a set of specialized links computed from the given
        `ptrcls` taking source inheritance into account.
        """
        ctx = self.context.current
        linkname = ptrcls.shortname
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
                qry.rptr_rvar = table

                # Make sure all property references are pulled up properly
                for colname in cols:
                    selexpr = pgast.ColumnRef(
                        name=[table.alias.aliasname, colname])
                    qry.target_list.append(
                        pgast.ResTarget(val=selexpr, name=colname))

                set_ops.append(('union', qry))

                overlays = ctx.rel_overlays.get(src_ptrcls)
                if overlays and include_overlays:
                    for op, cte in overlays:
                        rvar = pgast.RangeVar(
                            relation=cte,
                            alias=pgast.Alias(
                                aliasname=ctx.genalias(hint=cte.name)
                            )
                        )

                        qry = pgast.SelectStmt(
                            target_list=[
                                pgast.ResTarget(
                                    val=pgast.ColumnRef(
                                        name=[col]
                                    )
                                )
                                for col in cols
                            ],
                            from_clause=[rvar],
                            rptr_rvar=rvar
                        )
                        set_ops.append((op, qry))

            if len(set_ops) == 0:
                # We've been given a generic link that none of the potential
                # sources contain directly, so fall back to general parent
                # table.
                rvar = self._table_from_ptrcls(ptrcls.bases[0])

            else:
                rvar = self._range_from_queryset(set_ops, ptrcls)

        return rvar

    def _range_for_pointer(self, pointer):
        return self._range_for_ptrcls(pointer.ptrcls, pointer.direction)

    def _range_from_queryset(self, set_ops, scls):
        ctx = self.context.current

        if len(set_ops) > 1:
            # More than one class table, generate a UNION/EXCEPT clause.
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
                    aliasname=ctx.genalias(hint=scls.shortname.name)
                )
            )

        else:
            # Just one class table, so return it directly
            rvar = set_ops[0][1].from_clause[0]

        return rvar
