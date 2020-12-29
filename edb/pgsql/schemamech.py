#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

import itertools

from edb import errors

from edb.ir import ast as irast
from edb.ir import astexpr as irastexpr
from edb.ir import typeutils as irtyputils
from edb.ir import utils as ir_utils
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import ast as qlast
from edb.edgeql import parser as ql_parser
from edb.edgeql.compiler import astutils as ql_astutils

from edb.schema import scalars as s_scalars
from edb.schema import utils as s_utils
from edb.schema import types as s_types

from edb.common import ast

from . import ast as pg_ast
from . import dbops
from . import deltadbops
from . import common
from . import types
from . import compiler
from . import codegen


class ConstraintMech:

    @classmethod
    def _get_exclusive_refs(cls, tree):
        # Check if the expression is
        #   std::_is_exclusive(<arg>) [and std::_is_exclusive(<arg>)...]
        expr = tree.expr.expr.result

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
    def _edgeql_ref_to_pg_constr(cls, subject, origin_subject, tree, schema):
        sql_tree = compiler.compile_ir_to_sql_tree(
            tree, singleton_mode=True)

        if isinstance(sql_tree, pg_ast.SelectStmt):
            # XXX: use ast pattern matcher for this
            sql_expr = sql_tree.from_clause[0].relation\
                .query.target_list[0].val
        else:
            sql_expr = sql_tree

        if isinstance(tree, irast.Statement):
            tree = tree.expr

        if isinstance(tree.expr, irast.SelectStmt):
            tree = tree.expr.result

        is_multicol = irtyputils.is_tuple(tree.typeref)

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

        if isinstance(subject, s_scalars.ScalarType):
            # Domain constraint, replace <scalar_name> with VALUE

            subj_pgname = common.edgedb_name_to_pg_name(str(subject.id))
            orgsubj_pgname = common.edgedb_name_to_pg_name(
                str(origin_subject.id))

            for ref in refs:
                if ref.name != [subj_pgname] and ref.name != [orgsubj_pgname]:
                    raise ValueError(
                        f'unexpected node reference in '
                        f'ScalarType constraint: {".".join(ref.name)}'
                    )

                # work around the immutability check
                object.__setattr__(ref, 'name', ['VALUE'])

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
    def _get_constraint_origin(cls, schema, constraint):
        ancestors = list(constraint.get_ancestors(schema).objects(schema))
        origin = constraint
        for ancestor in ancestors:
            if ancestor.generic(schema) or ancestor.get_delegated(schema):
                break
            origin = ancestor

        return origin

    @classmethod
    def schema_constraint_to_backend_constraint(
            cls, subject, constraint, schema, context):
        assert constraint.get_subject(schema) is not None

        constraint_origin = cls._get_constraint_origin(schema, constraint)
        if constraint_origin != constraint:
            origin_subject = constraint_origin.get_subject(schema)
        else:
            origin_subject = subject

        path_prefix_anchor = (
            qlast.Subject().name if isinstance(subject, s_types.Type)
            else None
        )

        ir = qlcompiler.compile_ast_to_ir(
            constraint.get_finalexpr(schema).qlast,
            schema,
            options=qlcompiler.CompilerOptions(
                anchors={qlast.Subject().name: subject},
                path_prefix_anchor=path_prefix_anchor,
                apply_query_rewrites=not context.stdmode,
            ),
        )

        terminal_refs = ir_utils.get_longest_paths(ir.expr.expr.result)
        ref_tables = get_ref_storage_info(ir.schema, terminal_refs)

        if len(ref_tables) > 1:
            raise ValueError(
                'backend: multi-table constraints are not currently supported')
        elif ref_tables:
            subject_db_name, _ = next(iter(ref_tables.items()))
        else:
            subject_db_name = common.get_backend_name(
                schema, subject, catenate=False)

        exclusive_expr_refs = cls._get_exclusive_refs(ir)

        pg_constr_data = {
            'subject_db_name': subject_db_name,
            'expressions': [],
            'origin_expressions': [],
        }

        if constraint_origin != constraint:
            origin_path_prefix_anchor = (
                qlast.Subject().name
                if isinstance(origin_subject, s_types.Type) else None
            )
            origin_ir = qlcompiler.compile_ast_to_ir(
                constraint_origin.get_finalexpr(schema).qlast,
                schema,
                options=qlcompiler.CompilerOptions(
                    anchors={qlast.Subject().name: origin_subject},
                    path_prefix_anchor=origin_path_prefix_anchor,
                    apply_query_rewrites=not context.stdmode,
                ),
            )

            origin_terminal_refs = ir_utils.get_longest_paths(
                origin_ir.expr.expr.result)
            origin_ref_tables = get_ref_storage_info(
                origin_ir.schema, origin_terminal_refs)

            if origin_ref_tables:
                origin_subject_db_name, _ = (
                    next(iter(origin_ref_tables.items()))
                )
            else:
                origin_subject_db_name = common.get_backend_name(
                    schema, origin_subject, catenate=False,
                )

            origin_exclusive_expr_refs = cls._get_exclusive_refs(origin_ir)
            pg_constr_data['origin_subject_db_name'] = origin_subject_db_name
        else:
            origin_exclusive_expr_refs = None
            pg_constr_data['origin_subject_db_name'] = subject_db_name

        if exclusive_expr_refs:
            for ref in exclusive_expr_refs:
                exprdata = cls._edgeql_ref_to_pg_constr(
                    subject, origin_subject, ref, schema)
                pg_constr_data['expressions'].append(exprdata)

            if origin_exclusive_expr_refs:
                for ref in origin_exclusive_expr_refs:
                    exprdata = cls._edgeql_ref_to_pg_constr(
                        subject, origin_subject, ref, schema)
                    pg_constr_data['origin_expressions'].append(exprdata)
            else:
                pg_constr_data['origin_expressions'] = (
                    pg_constr_data['expressions'])

            pg_constr_data['scope'] = 'relation'
            pg_constr_data['type'] = 'unique'
        else:
            exprdata = cls._edgeql_ref_to_pg_constr(
                subject, origin_subject, ir, schema)
            pg_constr_data['expressions'].append(exprdata)

            pg_constr_data['scope'] = 'row'
            pg_constr_data['type'] = 'check'

        if isinstance(constraint.get_subject(schema), s_scalars.ScalarType):
            constraint = SchemaDomainConstraint(
                subject=subject, constraint=constraint,
                pg_constr_data=pg_constr_data,
                schema=schema)
        else:
            constraint = SchemaTableConstraint(
                subject=subject, constraint=constraint,
                pg_constr_data=pg_constr_data,
                schema=schema)
        return constraint


class SchemaDomainConstraint:
    def __init__(self, subject, constraint, pg_constr_data, schema):
        self._subject = subject
        self._constraint = constraint
        self._pg_constr_data = pg_constr_data
        self._schema = schema

    def _domain_constraint(self, constr):
        domain_name = constr._pg_constr_data['subject_db_name']
        expressions = constr._pg_constr_data['expressions']

        constr = deltadbops.SchemaConstraintDomainConstraint(
            domain_name, constr._constraint, expressions,
            schema=self._schema)

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
    def __init__(self, subject, constraint, pg_constr_data, schema):
        self._subject = subject
        self._constraint = constraint
        self._pg_constr_data = pg_constr_data
        self._schema = schema

    def _table_constraint(self, constr):
        pg_c = constr._pg_constr_data

        table_name = pg_c['subject_db_name']
        origin_table_name = pg_c['origin_subject_db_name']
        expressions = pg_c['expressions']
        origin_expressions = pg_c['origin_expressions']

        constr = deltadbops.SchemaConstraintTableConstraint(
            table_name,
            origin_table_name=origin_table_name,
            constraint=constr._constraint,
            exprdata=expressions,
            origin_exprdata=origin_expressions,
            scope=pg_c['scope'],
            type=pg_c['type'],
            schema=constr._schema,
        )

        return constr

    def create_ops(self):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        add_constr = deltadbops.AlterTableAddConstraint(
            name=tabconstr.get_subject_name(quote=False), constraint=tabconstr)

        ops.add_command(add_constr)

        return ops

    def rename_ops(self, orig_constr):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        orig_tabconstr = self._table_constraint(orig_constr)

        rename_constr = deltadbops.AlterTableRenameConstraint(
            name=tabconstr.get_subject_name(quote=False),
            constraint=orig_tabconstr, new_constraint=tabconstr)

        ops.add_command(rename_constr)

        return ops

    def alter_ops(self, orig_constr):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        orig_tabconstr = self._table_constraint(orig_constr)

        alter_constr = deltadbops.AlterTableAlterConstraint(
            name=tabconstr.get_subject_name(quote=False),
            constraint=orig_tabconstr, new_constraint=tabconstr)

        ops.add_command(alter_constr)

        return ops

    def delete_ops(self):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        add_constr = deltadbops.AlterTableDropConstraint(
            name=tabconstr.get_subject_name(quote=False), constraint=tabconstr)

        ops.add_command(add_constr)

        return ops


def ptr_default_to_col_default(schema, ptr, expr):
    try:
        # NOTE: This code currently will only be invoked for scalars.
        # Blindly cast the default expression into the ptr target
        # type, validation of the expression type is not the concern
        # of this function.
        eql = ql_parser.parse(expr.text)
        eql = ql_astutils.ensure_qlstmt(
            qlast.TypeCast(
                type=s_utils.typeref_to_ast(
                    schema, ptr.get_target(schema)),
                expr=eql,
            )
        )
        ir = qlcompiler.compile_ast_to_ir(eql, schema)
    except errors.SchemaError:
        # Reference errors mean that is is a non-constant default
        # referring to a not-yet-existing objects.
        return None

    if not ir_utils.is_const(ir):
        return None

    sql_expr = compiler.compile_ir_to_sql_tree(ir, singleton_mode=True)
    sql_text = codegen.SQLSourceGenerator.to_source(sql_expr)

    return sql_text


def get_ref_storage_info(schema, refs):
    link_biased = {}
    objtype_biased = {}

    ref_ptrs = {}
    refs = list(refs)
    for ref in refs:
        rptr = ref.rptr
        if rptr is None:
            source_typeref = ref.typeref
            if not irtyputils.is_object(source_typeref):
                continue
            schema, t = irtyputils.ir_typeref_to_type(schema, ref.typeref)
            ptr = t.getptr(schema, 'id')
        else:
            ptrref = ref.rptr.ptrref
            schema, ptr = irtyputils.ptrcls_from_ptrref(ptrref, schema=schema)
            source_typeref = ref.rptr.source.typeref

        if ptr.is_link_property(schema):
            srcref = ref.rptr.source.rptr.ptrref
            schema, src = irtyputils.ptrcls_from_ptrref(
                srcref, schema=schema)
            if src.get_is_derived(schema):
                # This specialized pointer was derived specifically
                # for the purposes of constraint expr compilation.
                src = src.get_bases(schema).first(schema)
        elif ptr.is_tuple_indirection():
            refs.append(ref.rptr.source)
            continue
        elif ptr.is_type_intersection():
            refs.append(ref.rptr.source)
            continue
        else:
            schema, src = irtyputils.ir_typeref_to_type(schema, source_typeref)
        ref_ptrs[ref] = (ptr, src)

    for ref, (ptr, src) in ref_ptrs.items():
        ptr_info = types.get_pointer_storage_info(
            ptr, source=src, resolve_type=False, schema=schema)

        # See if any of the refs are hosted in pointer tables and others
        # are not...
        if ptr_info.table_type == 'link':
            link_biased[ref] = ptr_info
        else:
            objtype_biased[ref] = ptr_info

        if link_biased and objtype_biased:
            break

    if link_biased and objtype_biased:
        for ref in objtype_biased.copy():
            ptr, src = ref_ptrs[ref]
            ptr_info = types.get_pointer_storage_info(
                ptr, source=src, resolve_type=False, link_bias=True,
                schema=schema)

            if ptr_info is not None and ptr_info.table_type == 'link':
                link_biased[ref] = ptr_info
                objtype_biased.pop(ref)

    ref_tables = {}

    for ref, ptr_info in itertools.chain(
            objtype_biased.items(), link_biased.items()):
        ptr, src = ref_ptrs[ref]

        try:
            ref_tables[ptr_info.table_name].append((ref, ptr, src, ptr_info))
        except KeyError:
            ref_tables[ptr_info.table_name] = [(ref, ptr, src, ptr_info)]

    return ref_tables
