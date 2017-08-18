##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import collections
import itertools

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import astexpr as irastexpr
from edgedb.lang.ir import utils as ir_utils
from edgedb.lang.edgeql import compiler as ql_compiler
from edgedb.lang.edgeql import ast as qlast

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import error as s_err
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as sn

from edgedb.lang.common import ast

from .datasources import introspection
from . import ast as pg_ast
from . import dbops
from . import deltadbops
from . import common
from . import types
from . import compiler
from . import codegen


class ConstraintMech:
    def __init__(self):
        self._constraints_cache = None

    async def init_cache(self, connection):
        self._constraints_cache = \
            await self._populate_constraint_cache(connection)

    def invalidate_schema_cache(self):
        self._constraints_cache = None

    async def _populate_constraint_cache(self, connection):
        constraints = {}
        rows = await introspection.constraints.fetch(
            connection,
            schema_pattern='edgedb%', constraint_pattern='%;schemaconstr%')
        for row in rows:
            constraints[row['constraint_name']] = row

        return constraints

    async def constraint_name_from_pg_name(self, connection, pg_name):
        if self._constraints_cache is None:
            self._constraints_cache = \
                await self._populate_constraint_cache(connection)

        try:
            cdata = self._constraints_cache[pg_name]
        except KeyError:
            return None
        else:
            name = cdata['constraint_description']
            name, _, _ = name.rpartition(';')
            return sn.Name(name)

    @classmethod
    def _get_unique_refs(cls, tree):
        # Check if the expression is
        #   std::is_dictinct(<arg>) [and std::is_distinct (<arg>)...]
        expr = tree.result

        astexpr = irastexpr.DistinctConjunctionExpr()
        refs = astexpr.match(expr)

        if refs is None:
            return refs
        else:
            all_refs = []
            for ref in refs:
                # Unnest sequences in refs
                all_refs.append(ref)

            return all_refs

    @classmethod
    def _get_ref_storage_info(cls, schema, refs):
        link_biased = {}
        concept_biased = {}

        ref_ptrs = {}
        for ref in refs:
            rptr = ref.rptr
            if rptr is not None:
                ptr = ref.rptr.ptrcls
                if isinstance(ptr, s_lprops.LinkProperty):
                    src = ref.rptr.source.rptr.ptrcls
                    if src.is_derived:
                        # This specialized pointer was derived specifically
                        # for the purposes of constraint expr compilation.
                        src = src.bases[0]
                else:
                    src = ref.rptr.source.scls
                ref_ptrs[ref] = (ptr, src)

        for ref, (ptr, src) in ref_ptrs.items():
            ptr_info = types.get_pointer_storage_info(
                ptr, source=src, resolve_type=False)

            # See if any of the refs are hosted in pointer tables and others
            # are not...
            if ptr_info.table_type == 'link':
                link_biased[ref] = ptr_info
            else:
                concept_biased[ref] = ptr_info

            if link_biased and concept_biased:
                break

        if link_biased and concept_biased:
            for ref in concept_biased.copy():
                ptr, src = ref_ptrs[ref]
                ptr_info = types.get_pointer_storage_info(
                    ptr, source=src, resolve_type=False, link_bias=True)

                if ptr_info.table_type == 'link':
                    link_biased[ref] = ptr_info
                    concept_biased.pop(ref)

        ref_tables = {}

        for ref, ptr_info in itertools.chain(
                concept_biased.items(), link_biased.items()):
            ptr, src = ref_ptrs[ref]

            try:
                ref_tables[ptr_info.table_name].append(
                    (ref, ptr, src, ptr_info))
            except KeyError:
                ref_tables[ptr_info.table_name] = [(ref, ptr, src, ptr_info)]

        return ref_tables

    @classmethod
    def _edgeql_ref_to_pg_constr(cls, subject, tree, schema, link_bias):
        sql_tree = compiler.compile_ir_to_sql_tree(
            tree, schema=schema, singleton_mode=True)

        if isinstance(sql_tree, pg_ast.SelectStmt):
            # XXX: use ast pattern matcher for this
            sql_expr = sql_tree.from_clause[0].relation\
                .query.target_list[0].val
        else:
            sql_expr = sql_tree

        if isinstance(tree, irast.SelectStmt):
            is_multicol = isinstance(tree.result.expr, irast.Tuple)
        else:
            is_multicol = isinstance(tree.expr, irast.Tuple)

        # Determine if the sequence of references are all simple refs, not
        # expressions.  This influences the type of Postgres constraint used.
        #
        is_trivial = (
            isinstance(sql_expr, pg_ast.ColumnRef) or (
                isinstance(sql_expr, pg_ast.ImplicitRowExpr) and all(
                    isinstance(el, pg_ast.ColumnRef)
                    for el in sql_expr.args)))

        # Find all field references
        #
        flt = lambda n: isinstance(n, pg_ast.ColumnRef) and len(n.name) == 1
        refs = set(ast.find_children(sql_expr, flt))

        if isinstance(subject, s_atoms.Atom):
            # Domain constraint, replace <atom_name> with VALUE

            subject_pg_name = common.edgedb_name_to_pg_name(subject.name)

            for ref in refs:
                if ref.name != [subject_pg_name]:
                    raise ValueError(
                        f'unexpected node reference in '
                        f'Atom constraint: {".".join(ref.name)}'
                    )

                ref.name = ['VALUE']

        plain_expr = codegen.SQLSourceGenerator.to_source(sql_expr)

        if is_multicol:
            chunks = []

            for elem in sql_expr.args:
                chunks.append(codegen.SQLSourceGenerator.to_source(elem))
        else:
            chunks = [plain_expr]

        if isinstance(sql_expr, pg_ast.ColumnRef):
            refs.add(sql_expr)

        for ref in refs:
            ref.name.insert(0, 'NEW')
        new_expr = codegen.SQLSourceGenerator.to_source(sql_expr)

        for ref in refs:
            ref.name[0] = 'OLD'
        old_expr = codegen.SQLSourceGenerator.to_source(sql_expr)

        exprdata = dict(
            plain=plain_expr, plain_chunks=chunks, new=new_expr, old=old_expr)

        return dict(
            exprdata=exprdata, is_multicol=is_multicol, is_trivial=is_trivial)

    @classmethod
    def schema_constraint_to_backend_constraint(
            cls, subject, constraint, schema):
        assert constraint.subject is not None

        ir = ql_compiler.compile_to_ir(
            constraint.finalexpr, schema, anchors={qlast.Subject: subject})

        terminal_refs = ir_utils.get_terminal_references(ir.result)
        ref_tables = cls._get_ref_storage_info(schema, terminal_refs)

        if len(ref_tables) > 1:
            raise ValueError(
                'backend: multi-table constraints are not currently supported')
        elif ref_tables:
            subject_db_name = next(iter(ref_tables))
        else:
            subject_db_name = common.atom_name_to_domain_name(
                subject.name, catenate=False)

        link_bias = ref_tables and next(iter(ref_tables.values()))[0][
            3].table_type == 'link'

        unique_expr_refs = cls._get_unique_refs(ir)

        pg_constr_data = {
            'subject_db_name': subject_db_name,
            'expressions': []
        }

        exprs = pg_constr_data['expressions']

        if unique_expr_refs:
            for ref in unique_expr_refs:
                exprdata = cls._edgeql_ref_to_pg_constr(
                    subject, ref, schema, link_bias)
                exprs.append(exprdata)

            pg_constr_data['scope'] = 'relation'
            pg_constr_data['type'] = 'unique'
            pg_constr_data['subject_db_name'] = subject_db_name
        else:
            exprdata = cls._edgeql_ref_to_pg_constr(
                subject, ir, schema, link_bias)
            exprs.append(exprdata)

            pg_constr_data['subject_db_name'] = subject_db_name
            pg_constr_data['scope'] = 'row'
            pg_constr_data['type'] = 'check'

        if isinstance(constraint.subject, s_atoms.Atom):
            constraint = SchemaDomainConstraint(
                subject=subject, constraint=constraint,
                pg_constr_data=pg_constr_data)
        else:
            constraint = SchemaTableConstraint(
                subject=subject, constraint=constraint,
                pg_constr_data=pg_constr_data)
        return constraint


class SchemaDomainConstraint:
    def __init__(self, subject, constraint, pg_constr_data):
        self._subject = subject
        self._constraint = constraint
        self._pg_constr_data = pg_constr_data

    @classmethod
    def _domain_constraint(cls, constr):
        domain_name = constr._pg_constr_data['subject_db_name']
        expressions = constr._pg_constr_data['expressions']

        constr = deltadbops.SchemaConstraintDomainConstraint(
            domain_name, constr._constraint, expressions)

        return constr

    def create_ops(self):
        ops = dbops.CommandGroup()

        domconstr = self._domain_constraint(self)
        add_constr = dbops.AlterDomainAddConstraint(
            name=domconstr.get_subject_name(quote=False), constraint=domconstr)

        ops.add_command(add_constr)

        return ops

    def rename_ops(self, orig_constr):
        ops = dbops.CommandGroup()

        domconstr = self._domain_constraint(self)
        orig_domconstr = self._domain_constraint(orig_constr)

        add_constr = dbops.AlterDomainRenameConstraint(
            name=domconstr.get_subject_name(quote=False),
            constraint=orig_domconstr, new_constraint=domconstr)

        ops.add_command(add_constr)

        return ops

    def alter_ops(self, orig_constr):
        ops = dbops.CommandGroup()
        return ops

    def delete_ops(self):
        ops = dbops.CommandGroup()

        domconstr = self._domain_constraint(self)
        add_constr = dbops.AlterDomainDropConstraint(
            name=domconstr.get_subject_name(quote=False), constraint=domconstr)

        ops.add_command(add_constr)

        return ops


class SchemaTableConstraint:
    def __init__(self, subject, constraint, pg_constr_data):
        self._subject = subject
        self._constraint = constraint
        self._pg_constr_data = pg_constr_data

    @classmethod
    def _table_constraint(cls, constr):
        pg_c = constr._pg_constr_data

        table_name = pg_c['subject_db_name']
        expressions = pg_c['expressions']

        constr = deltadbops.SchemaConstraintTableConstraint(
            table_name, constraint=constr._constraint, exprdata=expressions,
            scope=pg_c['scope'], type=pg_c['type'])

        return constr

    def create_ops(self):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        add_constr = deltadbops.AlterTableAddInheritableConstraint(
            name=tabconstr.get_subject_name(quote=False), constraint=tabconstr)

        ops.add_command(add_constr)

        return ops

    def rename_ops(self, orig_constr):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        orig_tabconstr = self._table_constraint(orig_constr)

        rename_constr = deltadbops.AlterTableRenameInheritableConstraint(
            name=tabconstr.get_subject_name(quote=False),
            constraint=orig_tabconstr, new_constraint=tabconstr)

        ops.add_command(rename_constr)

        return ops

    def alter_ops(self, orig_constr):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        orig_tabconstr = self._table_constraint(orig_constr)

        alter_constr = deltadbops.AlterTableAlterInheritableConstraint(
            name=tabconstr.get_subject_name(quote=False),
            constraint=orig_tabconstr, new_constraint=tabconstr)

        ops.add_command(alter_constr)

        return ops

    def delete_ops(self):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        add_constr = deltadbops.AlterTableDropInheritableConstraint(
            name=tabconstr.get_subject_name(quote=False), constraint=tabconstr)

        ops.add_command(add_constr)

        return ops


class TypeMech:
    def __init__(self):
        self._column_cache = None
        self._table_cache = None

    def invalidate_schema_cache(self):
        self._column_cache = None
        self._table_cache = None

    async def init_cache(self, connection):
        await self._load_table_columns(('edgedb_%', None), connection)

    async def _load_table_columns(self, table_name, connection):
        cols = await introspection.tables.fetch_columns(
            connection,
            table_pattern=table_name[1], schema_pattern=table_name[0])

        if self._column_cache is None:
            self._column_cache = {}

        for col in cols:
            key = (col['table_schema'], col['table_name'])

            try:
                table_cols = self._column_cache[key]
            except KeyError:
                table_cols = collections.OrderedDict()
                self._column_cache[key] = table_cols

            table_cols[col['column_name']] = col

    async def get_table_columns(self, table_name, connection=None,
                                cache='auto'):
        if cache is not None:
            cols = self.get_cached_table_columns(table_name)

        if cols is None and cache != 'always':
            cols = await self._load_table_columns(table_name, connection)

        return self._column_cache.get(table_name)

    def get_cached_table_columns(self, table_name):
        if self._column_cache is not None:
            cols = self._column_cache.get(table_name)
        else:
            cols = None

        return cols

    async def _load_type_attributes(self, type_name, connection):
        cols = await introspection.types.fetch_attributes(
            connection,
            type_pattern=type_name[1], schema_pattern=type_name[0])

        if self._column_cache is None:
            self._column_cache = {}

        for col in cols:
            key = (col['type_schema'], col['type_name'])

            try:
                type_attrs = self._column_cache[key]
            except KeyError:
                type_attrs = collections.OrderedDict()
                self._column_cache[key] = type_attrs

            type_attrs[col['attribute_name']] = col

    async def get_type_attributes(self, type_name, connection, cache='auto'):
        if cache is not None and self._column_cache is not None:
            cols = self._column_cache.get(type_name)
        else:
            cols = None

        if cols is None and cache != 'always':
            await self._load_type_attributes(type_name, connection)

        return self._column_cache.get(type_name)

    def get_table(self, scls, schema):
        if self._table_cache is None:
            self._table_cache = {}

        table = self._table_cache.get(scls)

        if table is None:
            table_name = common.get_table_name(scls, catenate=False)
            table = dbops.Table(table_name)

            cols = []

            if isinstance(scls, s_links.Link):
                cols.extend([
                    dbops.Column(name='link_type_id', type='uuid'),
                    dbops.Column(name='std::linkid', type='uuid'),
                    dbops.Column(name='std::source', type='uuid'),
                    dbops.Column(name='std::target', type='uuid')
                ])

            elif isinstance(scls, s_concepts.Concept):
                cols.extend([dbops.Column(name='std::__class__', type='uuid')])

            else:
                assert False

            if isinstance(scls, s_concepts.Concept):
                expected_table_type = 'concept'
            else:
                expected_table_type = 'link'

            for pointer_name, pointer in scls.pointers.items():
                if not pointer.singular():
                    continue

                if pointer_name == 'std::source':
                    continue

                if pointer_name == 'std::linkid':
                    continue

                ptr_stor_info = types.get_pointer_storage_info(
                    pointer, schema=schema)

                if ptr_stor_info.column_name == 'std::target':
                    continue

                if ptr_stor_info.table_type == expected_table_type:
                    cols.append(
                        dbops.Column(
                            name=ptr_stor_info.column_name,
                            type=common.qname(*ptr_stor_info.column_type)))
            table.add_columns(cols)

            self._table_cache[scls] = table

        return table


def ptr_default_to_col_default(schema, ptr, expr):
    try:
        ir = ql_compiler.compile_to_ir(expr, schema)
    except s_err.SchemaError:
        # Referene errors mean that is is a non-constant default
        # referring to a not-yet-existing objects.
        return None

    if not ir_utils.is_const(ir):
        return None

    sql_expr = compiler.compile_ir_to_sql_tree(
        ir, schema=schema, singleton_mode=True)
    sql_text = codegen.SQLSourceGenerator.to_source(sql_expr)

    return sql_text
