##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast

from edgedb.lang.ir import ast2 as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types


class IRCompilerDMLSupport:
    def visit_InsertStmt(self, stmt):
        with self.context.subquery():
            ctx = self.context.current
            ctx.query = pgast.InsertQueryNode()

            if stmt.substmts:
                for substmt in stmt.substmts:
                    with self.context.subquery():
                        ctx.rel = ctx.query = pgast.CTENode()
                        cte = self.visit(substmt)
                        cte.alias = substmt.name
                    ctx.query.ctes.add(cte)
                    ctx.explicit_cte_map[substmt] = cte

            ctx.query.fromexpr = self._table_from_concept(
                stmt.shape.scls, stmt.shape, None)

            path_id = irutils.LinearPath([stmt.shape.scls])

            refexpr = pgast.FieldRefNode(
                table=ctx.query.fromexpr,
                field=common.edgedb_name_to_pg_name('std::id'))

            ctx.query.concept_node_map[path_id] = \
                pgast.SelectExprNode(expr=refexpr)

            if stmt.result is not None:
                # with self.context.subquery():
                #     self._process_selector(
                #         stmt.result, transform_output=True)
                #     returning = self.context.current.query
                #
                # ctx.query.targets.append(
                #     pgast.SelectExprNode(
                #         expr=returning,
                #         alias='output'
                #     )
                # )
                #
                # ctx.subquery_map[ctx.query].add(returning)
                # self._connect_subrels(ctx.query)
                pass

            ctx.entityref_as_id = True

            return self._process_insert_data(stmt)

    def visit_UpdateStmt(self, stmt):
        with self.context.subquery():
            ctx = self.context.current
            update_range = ctx.query

            self._process_selector(stmt.result, transform_output=False)

            path_id = irutils.LinearPath([stmt.shape.scls])
            update_range.targets = [update_range.concept_node_map[path_id]]

            if stmt.substmts:
                for substmt in stmt.substmts:
                    with self.context.subquery():
                        ctx.rel = ctx.query = pgast.CTENode()
                        cte = self.visit(substmt)
                        cte.alias = substmt.name
                    ctx.query.ctes.add(cte)
                    ctx.explicit_cte_map[substmt] = cte

            if stmt.where:
                with self.context.new():
                    self.context.current.location = 'where'
                    where = self.visit(stmt.where)

                if isinstance(where, pgast.FieldRefNode):
                    where = pgast.UnaryOpNode(
                        operand=pgast.NullTestNode(expr=where),
                        op=ast.ops.NOT)

                ctx.query.where = where

            self._connect_subrels(ctx.query)

            update_target = self._table_from_concept(
                stmt.shape.scls, stmt.shape, None)

            id_ref = pgast.FieldRefNode(
                table=update_target, field='std::id')

            ctx.query = pgast.UpdateQueryNode(
                fromexpr=update_target,
                where=pgast.BinOpNode(
                    left=id_ref, op='IN', right=update_range)
            )

            ctx.entityref_as_id = True

            return self._process_update_data(stmt)

    def visit_DeleteStmt(self, stmt):
        with self.context.subquery():
            ctx = self.context.current
            delete_range = ctx.query

            self._process_selector(stmt.result, transform_output=False)

            if stmt.substmts:
                for substmt in stmt.substmts:
                    with self.context.subquery():
                        ctx.rel = ctx.query = pgast.CTENode()
                        cte = self.visit(substmt)
                        cte.alias = substmt.name
                    ctx.query.ctes.add(cte)
                    ctx.explicit_cte_map[substmt] = cte

            if stmt.where:
                with self.context.new():
                    self.context.current.location = 'where'
                    where = self.visit(stmt.where)

                if isinstance(where, pgast.FieldRefNode):
                    where = pgast.UnaryOpNode(
                        operand=pgast.NullTestNode(expr=where),
                        op=ast.ops.NOT)

                ctx.query.where = where

            self._connect_subrels(ctx.query)

            path_id = irutils.LinearPath([stmt.shape.scls])
            delete_range.targets = [delete_range.concept_node_map[path_id]]

            delete_target = self._table_from_concept(
                stmt.shape.scls, stmt.shape, None)

            id_ref = pgast.FieldRefNode(
                table=delete_target, field='std::id')

            ctx.query = pgast.DeleteQueryNode(
                fromexpr=delete_target,
                where=pgast.BinOpNode(
                    left=id_ref, op='IN', right=delete_range)
            )

            if ctx.output_format == 'json':
                keyvals = [pgast.ConstantNode(value='id'), id_ref]
                target = pgast.SelectExprNode(expr=pgast.FunctionCallNode(
                    name='jsonb_build_object', args=keyvals))
            else:
                target = pgast.SelectExprNode(expr=id_ref)

            ctx.query.targets.append(target)

            return ctx.query

    def _process_insert_data(self, stmt):
        """Generate SQL INSERTs from an Insert IR."""
        ctx = self.context.current

        cols = [pgast.FieldRefNode(field='std::__class__')]
        select = pgast.SelectQueryNode()
        values = pgast.SequenceNode()
        select.values = [values]

        query = ctx.query

        query.cols = cols
        query.select = select

        # Type reference is always inserted.
        values.elements.append(
            pgast.SelectQueryNode(
                targets=[
                    pgast.SelectExprNode(
                        expr=pgast.FieldRefNode(field='id'))
                ],
                fromlist=[
                    pgast.TableNode(name='concept', schema='edgedb')
                ],
                where=pgast.BinOpNode(
                    op=ast.ops.EQ,
                    left=pgast.FieldRefNode(field='name'),
                    right=pgast.ConstantNode(value=stmt.shape.scls.name)
                )
            )
        )

        if not stmt.shape.elements:
            return query

        external_inserts = []

        for expr in stmt.shape.elements:
            ptrcls = expr.rptr.ptrcls
            insvalue = expr.stmt.result

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, schema=ctx.schema, resolve_type=True,
                link_bias=False)

            props_only = False
            ins_props = None
            operation = None

            # First, process all local link inserts.
            if ptr_info.table_type == 'concept':
                props_only = True
                field = pgast.FieldRefNode(
                    field=ptr_info.column_name, table=None)
                cols.append(field)

                with self.context.new():
                    if self._is_composite_cast(insvalue):
                        insvalue, ins_props = self._extract_update_value(
                            insvalue, ptr_info.column_type)

                    else:
                        insvalue = pgast.TypeCastNode(
                            expr=self.visit(insvalue),
                            type=pgast.TypeNode(name=ptr_info.column_type))

                    values.elements.append(insvalue)

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=True)

            if ptr_info and ptr_info.table_type == 'link':
                external_inserts.append((expr, props_only, operation))

        toplevel = None

        # Inserting externally-stored links requires repackaging everything
        # into a series of CTEs so that multiple statements can be executed
        # as a single query.
        #
        for expr, props_only, operation in external_inserts:
            if toplevel is None:
                toplevel = pgast.SelectQueryNode()
                toplevel.ctes.add(query)
                query.alias = ctx.genalias(hint='m')

                query.targets.append(
                    pgast.SelectExprNode(
                        expr=pgast.FieldRefNode(field='std::id'),
                        alias=None))

                ref = pgast.FieldRefNode(table=query, field='std::id')
                toplevel.fromlist.append(pgast.CTERefNode(cte=query))

                if ctx.output_format == 'json':
                    keyvals = [pgast.ConstantNode(value='id'), ref]
                    target = pgast.SelectExprNode(expr=pgast.FunctionCallNode(
                        name='jsonb_build_object', args=keyvals))
                else:
                    target = pgast.SelectExprNode(expr=ref)

                toplevel.targets.append(target)

            self._process_update_expr(
                expr, props_only, operation, toplevel, query)

        if toplevel is not None:
            query = toplevel

        return query

    def _process_update_values(
            self, updvalexpr, target_tab, tab_cols, col_data, sources,
            props_only, target_is_atom):
        """Unpack data from an update expression into a series of selects."""
        ctx = self.context.current

        # Recurse down to process update expressions like
        # col := col + val1 + val2
        #
        if (isinstance(updvalexpr, irast.BinOp) and
                isinstance(updvalexpr.left, irast.BinOp)):
            tranches = self._process_update_values(
                updvalexpr.left, target_tab, tab_cols, col_data,
                sources, props_only, target_is_atom)
        else:
            tranches = []

        if isinstance(updvalexpr, irast.BinOp):
            updval = updvalexpr.right
        else:
            updval = updvalexpr

        if isinstance(updval, irast.TypeCast):
            # Link property updates will have the data casted into
            # an appropriate selector shape which specifies which properties
            # are being updated.
            #
            data = updval.expr
            typ = updval.type

            if not isinstance(typ, tuple):
                raise ValueError(
                    'unexpected value type in update expr: {!r}'.format(typ))

            if not isinstance(typ[1], irast.CompositeType):
                raise ValueError(
                    'unexpected value type in update expr: {!r}'.format(typ))

            props = [p.ptr_class.normal_name() for p in typ[1].pathspec]

        else:
            # Target-only update
            #
            data = updval
            props = ['std::target']

        e = common.edgedb_name_to_pg_name

        spec_cols = {e(prop): i for i, prop in enumerate(props)}

        if (props == ['std::target'] and props_only and not target_is_atom):
            # No property upates and the target value is stored
            # in the source table, so we don't need to modify
            # any link tables.
            #
            return tranches

        with self.context.new():
            self.context.current.output_format = None
            input_data = self.visit(data)

        if (isinstance(input_data, pgast.ConstantNode) and
                input_data.type.endswith('[]')):
            data_is_json = input_data.type == 'json[]'
            input_data = pgast.FunctionCallNode(
                name='UNNEST', args=[input_data])
        else:
            data_is_json = False

        input_rel = pgast.FromExprNode(
            expr=input_data,
            alias=ctx.genalias('i')
        )

        unnested = pgast.SelectQueryNode(
            targets=[
                pgast.SelectExprNode(
                    expr=pgast.FieldRefNode(field='*', table=input_rel))
            ], fromlist=[input_rel], alias='j', coldef='(_)')

        row = pgast.SequenceNode()

        for col in tab_cols:
            if (col == 'std::target' and (props_only or target_is_atom)):
                expr = pgast.TypeCastNode(
                    expr=pgast.ConstantNode(value=None),
                    type=pgast.TypeNode(name='uuid'))
            else:
                if col == 'std::target@atom':
                    col = 'std::target'

                data_idx = spec_cols.get(col)
                if data_idx is None:
                    try:
                        expr = col_data[col]
                    except KeyError:
                        if tab_cols[col]['column_default'] is not None:
                            expr = pgast.LiteralExprNode(
                                expr=tab_cols[col]['column_default'])
                        else:
                            expr = pgast.ConstantNode(value=None)
                else:
                    expr = pgast.FieldRefNode(table=unnested, field='_')
                    if data_is_json:
                        expr = pgast.BinOpNode(
                            left=expr, op='->>',
                            right=pgast.ConstantNode(value=data_idx))

            row.elements.append(expr)

        tranch_data = pgast.SelectQueryNode(
            targets=[
                pgast.SelectExprNode(
                    expr=pgast.IndirectionNode(
                        expr=pgast.TypeCastNode(
                            expr=row,
                            type=pgast.TypeNode(
                                name=common.qname(*target_tab)
                            )
                        ),
                        indirection=pgast.StarIndirectionNode()
                    )
                )
            ],
            fromlist=[unnested],
            alias=ctx.genalias(hint='r')
        )

        tranch_data.fromlist.extend(sources)

        tranches.append((tab_cols, tranch_data))

        return tranches

    def _process_update_expr(self, updexpr, props_only, operation, query,
                             scope_cte):
        ctx = self.context.current

        edgedb_link = pgast.TableNode(
            schema='edgedb', name='link', alias=ctx.genalias(hint='l'))

        rptr = updexpr.rptr
        ptrcls = rptr.ptrcls
        target_is_atom = isinstance(rptr.target, s_atoms.Atom)

        lname_to_id = pgast.CTENode(
            fromlist=[
                edgedb_link
            ],
            targets=[
                pgast.SelectExprNode(
                    expr=pgast.FieldRefNode(table=edgedb_link, field='id'),
                    alias='id')
            ],
            where=pgast.BinOpNode(
                left=pgast.FieldRefNode(table=edgedb_link,
                                        field='name'), op=ast.ops.EQ,
                right=pgast.ConstantNode(value=ptrcls.name)
            ),
            alias=ctx.genalias(hint='lid')
        )

        query.ctes.add(lname_to_id)

        target_tab = self._table_from_ptrcls(ptrcls)

        if target_is_atom:
            target_tab_name = (target_tab.schema, target_tab.name)
        else:
            target_tab_name = common.link_name_to_table_name(
                ptrcls.normal_name(), catenate=False)

        tab_cols = \
            ctx.backend._type_mech.get_cached_table_columns(target_tab_name)

        assert tab_cols, "could not get cols for {!r}".format(target_tab_name)

        col_data = {
            'link_type_id': pgast.SelectExprNode(
                expr=pgast.FieldRefNode(table=lname_to_id, field='id')),
            'std::source': pgast.FieldRefNode(
                table=scope_cte, field='std::id')
        }

        if operation is None:
            # Drop previous entries first
            delcte = pgast.DeleteQueryNode(
                fromexpr=target_tab,
                where=pgast.BinOpNode(
                    left=col_data['std::source'],
                    op=ast.ops.EQ,
                    right=pgast.FieldRefNode(
                        table=target_tab,
                        field='std::source'
                    )
                ),
                alias=ctx.genalias(hint='d'),
                using=[scope_cte],
                targets=[
                    pgast.SelectExprNode(
                        expr=col_data['std::source'], alias='std::id')
                ])
            query.ctes.add(delcte)
            scope_cte = pgast.JoinNode(
                type='NATURAL LEFT', left=pgast.CTERefNode(cte=scope_cte),
                right=pgast.CTERefNode(cte=delcte))
        else:
            delcte = None

        tranches = self._process_update_values(
            updexpr, target_tab_name, tab_cols, col_data,
            [scope_cte, lname_to_id], props_only, target_is_atom)

        for cols, data in tranches:
            query.ctes.add(data)
            data = pgast.SelectQueryNode(
                targets=[
                    pgast.SelectExprNode(
                        expr=pgast.FieldRefNode(field='*', table=data))
                ], fromlist=[pgast.CTERefNode(cte=data)])

            if operation == ast.ops.SUB:
                # Removing links
                updcte = pgast.DeleteQueryNode(
                    alias=ctx.genalias(hint='d'),
                    targets=[
                        pgast.SelectExprNode(
                            expr=pgast.FieldRefNode(field='std::source'),
                            alias='std::id')
                    ]
                )

                updcte.fromexpr = target_tab
                data.alias = ctx.genalias(hint='q')
                updcte.where = pgast.BinOpNode(
                    left=pgast.FieldRefNode(field='std::linkid'),
                    op=ast.ops.IN,
                    right=pgast.SelectQueryNode(
                        targets=[
                            pgast.SelectExprNode(
                                expr=pgast.FieldRefNode(
                                    field='std::linkid'))
                        ],
                        fromlist=[data]
                    )
                )

            else:
                # Inserting links
                updcte = pgast.InsertQueryNode(
                    alias=ctx.genalias(hint='i'),
                    targets=[
                        pgast.SelectExprNode(
                            expr=pgast.FieldRefNode(field='std::source'),
                            alias='std::id'
                        )
                    ]
                )

                updcte.fromexpr = target_tab

                updcte.select = data
                updcte.cols = [
                    pgast.FieldRefNode(field=col) for col in cols
                ]

                update_clause = pgast.UpdateExprNode(
                    expr=pgast.SequenceNode(elements=updcte.cols),
                    value=data)

                updcte.on_conflict = pgast.OnConflictNode(
                    action='update',
                    infer=[pgast.FieldRefNode(field='std::linkid')],
                    targets=[update_clause])

            query.ctes.add(updcte)

    def _process_update_data(self, stmt):
        ctx = self.context.current
        query = ctx.query

        external_updates = []

        for expr in stmt.shape.elements:
            ptrcls = expr.rptr.ptrcls
            updvalue = expr.stmt.result

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, schema=ctx.schema, resolve_type=True, link_bias=False)

            props_only = False
            upd_props = None
            operation = None

            # First, process all internal link updates
            if ptr_info.table_type == 'concept':
                field = pgast.FieldRefNode(
                    field=ptr_info.column_name, table=None)
                props_only = True

                with self.context.new():
                    if self._is_composite_cast(updvalue):
                        updvalue, upd_props = self._extract_update_value(
                            updvalue, ptr_info.column_type)

                    else:
                        updvalue = pgast.TypeCastNode(
                            expr=self.visit(updvalue),
                            type=pgast.TypeNode(name=ptr_info.column_type))

                    query.values.append(
                        pgast.UpdateExprNode(expr=field, value=updvalue))

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=True)

            if ptr_info and ptr_info.table_type == 'link':
                external_updates.append((expr, props_only, operation))

        if not query.values:
            # No atomic updates
            query = pgast.CTENode(
                ctes=query.ctes, targets=query.targets,
                fromlist=[query.fromexpr], where=query.where)

        if not external_updates:
            ref = pgast.FieldRefNode(field='std::id')

            if ctx.output_format == 'json':
                keyvals = [pgast.ConstantNode(value='id'), ref]
                target = pgast.SelectExprNode(expr=pgast.FunctionCallNode(
                    name='jsonb_build_object', args=keyvals))
            else:
                target = pgast.SelectExprNode(expr=ref)

            query.targets.append(target)

        toplevel = None

        # Updating externally-stored linksrequires repackaging everything into
        # a series of CTEs so that multiple statements can be executed as a
        # single query.
        #
        for expr, props_only, operation in external_updates:
            if toplevel is None:
                toplevel = pgast.SelectQueryNode()
                toplevel.ctes.update(query.ctes)
                toplevel.ctes.add(query)
                query.ctes.clear()
                query.alias = ctx.genalias(hint='m')

                query.targets.append(
                    pgast.SelectExprNode(
                        expr=pgast.FieldRefNode(field='std::id'),
                        alias=None))

                ref = pgast.FieldRefNode(table=query, field='std::id')

                if ctx.output_format == 'json':
                    keyvals = [pgast.ConstantNode(value='id'), ref]
                    target = pgast.SelectExprNode(expr=pgast.FunctionCallNode(
                        name='jsonb_build_object', args=keyvals))
                else:
                    target = pgast.SelectExprNode(expr=ref)

                toplevel.fromlist.append(pgast.CTERefNode(cte=query))
                toplevel.targets.append(target)

            self._process_update_expr(
                expr, props_only, operation, toplevel, query)

        if toplevel is not None:
            query = toplevel

        return query
