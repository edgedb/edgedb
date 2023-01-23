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
from typing import *

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

from edb.schema import name as s_name
from edb.schema import pointers as s_pointers
from edb.schema import scalars as s_scalars
from edb.schema import utils as s_utils
from edb.schema import types as s_types
from edb.schema import constraints as s_constraints
from edb.schema import schema as s_schema

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
    def _edgeql_tree_to_exprdata(cls, sql_expr, is_multicol=False, refs=None):
        if refs is None:
            refs = set(ast.find_children(
                sql_expr, pg_ast.ColumnRef, lambda n: len(n.name) == 1))

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

        return dict(
            plain=plain_expr, new=new_expr, old=old_expr, plain_chunks=chunks)

    @classmethod
    def _edgeql_ref_to_pg_constr(cls, subject, origin_subject, tree, schema):
        sql_tree, _ = compiler.compile_ir_to_sql_tree(
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

        is_multicol = isinstance(sql_expr, (
            pg_ast.RowExpr, pg_ast.ImplicitRowExpr))

        # Determine if the sequence of references are all simple refs, not
        # expressions.  This influences the type of Postgres constraint used.
        #
        is_trivial = (
            isinstance(sql_expr, pg_ast.ColumnRef) or (
                is_multicol and all(
                    isinstance(el, pg_ast.ColumnRef)
                    for el in sql_expr.args)))

        # Find all field references
        #
        refs = set(ast.find_children(
            sql_expr, pg_ast.ColumnRef, lambda n: len(n.name) == 1))

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

        exprdata = cls._edgeql_tree_to_exprdata(
            sql_expr, is_multicol=is_multicol, refs=refs)

        return dict(
            exprdata=exprdata, is_multicol=is_multicol, is_trivial=is_trivial)

    @classmethod
    def schema_constraint_to_backend_constraint(
        cls,
        subject: s_types.Type,
        constraint: s_constraints.Constraint,
        schema: s_schema.Schema,
        context,
        source_context,
    ) -> SchemaDomainConstraint | SchemaTableConstraint:
        assert constraint.get_subject(schema) is not None

        constraint_origins = constraint.get_constraint_origins(schema)

        TypeOrPointer = s_types.Type | s_pointers.Pointer
        singletons: Collection[TypeOrPointer] = frozenset({subject})

        options = qlcompiler.CompilerOptions(
            anchors={qlast.Subject().name: subject},
            path_prefix_anchor=qlast.Subject().name,
            apply_query_rewrites=False,
            singletons=singletons,
            schema_object_context=type(constraint),
            # Remap the constraint origin to the subject, so that if
            # we have B <: A, and the constraint references A.foo, it
            # gets rewritten in the subtype to B.foo. It's OK to only
            # look at one constraint origin, because if there were
            # multiple different origins, they couldn't get away with
            # referring to the type explicitly.
            type_remaps={constraint_origins[0].get_subject(schema): subject},
        )

        final_expr = constraint.get_finalexpr(schema)
        assert final_expr is not None and final_expr.qlast is not None
        ir = qlcompiler.compile_ast_to_ir(
            final_expr.qlast,
            schema,
            options=options,
        )

        except_data = None
        if (except_expr := constraint.get_except_expr(schema)):
            except_ir = qlcompiler.compile_ast_to_ir(
                except_expr.qlast,
                schema,
                options=options,
            )
            except_sql, _ = compiler.compile_ir_to_sql_tree(
                except_ir, singleton_mode=True)
            except_data = cls._edgeql_tree_to_exprdata(except_sql)

        terminal_refs = ir_utils.get_longest_paths(ir.expr.expr.result)
        ref_tables = get_ref_storage_info(ir.schema, terminal_refs)

        if len(ref_tables) > 1:
            raise errors.InvalidConstraintDefinitionError(
                f'Constraint {constraint.get_displayname(schema)} on '
                f'{subject.get_displayname(schema)} is not supported '
                f'because it would depend on multiple objects',
                context=source_context,
            )
        elif ref_tables:
            subject_db_name, info = next(iter(ref_tables.items()))
            table_type = info[0][3].table_type
        else:
            subject_db_name = common.get_backend_name(
                schema, subject, catenate=False)
            table_type = 'ObjectType'

        exclusive_expr_refs = cls._get_exclusive_refs(ir)

        pg_constr_data = {
            'subject_db_name': subject_db_name,
            'expressions': [],
            'origin_expressions': [],
            'table_type': table_type,
            'except_data': except_data,
        }

        different_origins = [
            origin for origin in constraint_origins
            if origin != constraint
        ]

        per_origin_parts = []
        for constraint_origin in different_origins:
            sub = constraint_origin.get_subject(schema)
            assert isinstance(sub, (s_types.Type, s_pointers.Pointer))
            origin_subject: s_types.Type | s_pointers.Pointer = sub

            origin_path_prefix_anchor = (
                qlast.Subject().name
                if isinstance(origin_subject, s_types.Type) else None
            )
            singletons = frozenset({origin_subject})

            origin_options = qlcompiler.CompilerOptions(
                anchors={qlast.Subject().name: origin_subject},
                path_prefix_anchor=origin_path_prefix_anchor,
                apply_query_rewrites=False,
                singletons=singletons,
                schema_object_context=type(constraint),
            )

            final_expr = constraint_origin.get_finalexpr(schema)
            assert final_expr is not None and final_expr.qlast is not None
            origin_ir = qlcompiler.compile_ast_to_ir(
                final_expr.qlast,
                schema,
                options=origin_options,
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

            origin_except_data = None
            if (except_expr := constraint_origin.get_except_expr(schema)):
                except_ir = qlcompiler.compile_ast_to_ir(
                    except_expr.qlast,
                    schema,
                    options=origin_options,
                )
                except_sql, _ = compiler.compile_ir_to_sql_tree(
                    except_ir, singleton_mode=True)
                origin_except_data = cls._edgeql_tree_to_exprdata(except_sql)

            origin_exclusive_expr_refs = cls._get_exclusive_refs(origin_ir)
            per_origin_parts.append((
                origin_subject,
                origin_exclusive_expr_refs,
                origin_subject_db_name,
                origin_except_data,
            ))

        if not per_origin_parts:
            origin_subject = subject
            origin_subject_db_name = subject_db_name
            origin_except_data = except_data
            per_origin_parts.append((
                origin_subject,
                None,
                origin_subject_db_name,
                origin_except_data,
            ))

        if exclusive_expr_refs:
            exprdatas = []
            for ref in exclusive_expr_refs:
                exprdata = cls._edgeql_ref_to_pg_constr(
                    subject, None, ref, schema)
                exprdata.update(
                    origin_subject_db_name=subject_db_name,
                    origin_except_data=except_data,
                )
                exprdatas.append(exprdata)

            pg_constr_data['expressions'].extend(exprdatas)

        else:
            assert len(constraint_origins) == 1
            exprdata = cls._edgeql_ref_to_pg_constr(
                subject, origin_subject, ir, schema)
            exprdata.update(
                origin_subject_db_name=origin_subject_db_name,
                origin_except_data=origin_except_data,
            )
            pg_constr_data['expressions'].append(exprdata)

        for (
            origin_subject,
            origin_exclusive_expr_refs,
            origin_subject_db_name,
            origin_except_data
        ) in per_origin_parts:
            if not exclusive_expr_refs:
                continue

            if origin_exclusive_expr_refs:
                for ref in origin_exclusive_expr_refs:
                    exprdata = cls._edgeql_ref_to_pg_constr(
                        subject, origin_subject, ref, schema)
                    exprdata.update(
                        origin_subject_db_name=origin_subject_db_name,
                        origin_except_data=origin_except_data,
                    )
                    pg_constr_data['origin_expressions'].append(exprdata)
            else:
                pg_constr_data['origin_expressions'].extend(exprdatas)

        if exclusive_expr_refs:
            pg_constr_data['scope'] = 'relation'
            pg_constr_data['type'] = 'unique'
        else:
            pg_constr_data['scope'] = 'row'
            pg_constr_data['type'] = 'check'

        if isinstance(constraint.get_subject(schema), s_scalars.ScalarType):
            return SchemaDomainConstraint(
                subject=subject, constraint=constraint,
                pg_constr_data=pg_constr_data,
                schema=schema)
        else:
            return SchemaTableConstraint(
                subject=subject, constraint=constraint,
                pg_constr_data=pg_constr_data,
                schema=schema)


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

    def alter_ops(self, orig_constr, only_modify_enabled=False):
        ops = dbops.CommandGroup()
        return ops

    def delete_ops(self):
        ops = dbops.CommandGroup()

        domconstr = self._domain_constraint(self)
        add_constr = dbops.AlterDomainDropConstraint(
            name=domconstr.get_subject_name(quote=False), constraint=domconstr)

        ops.add_command(add_constr)

        return ops

    def enforce_ops(self):
        ops = dbops.CommandGroup()
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
        expressions = pg_c['expressions']
        origin_expressions = pg_c['origin_expressions']

        constr = deltadbops.SchemaConstraintTableConstraint(
            table_name,
            constraint=constr._constraint,
            exprdata=expressions,
            origin_exprdata=origin_expressions,
            except_data=pg_c['except_data'],
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

    def alter_ops(self, orig_constr, only_modify_enabled=False):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        orig_tabconstr = self._table_constraint(orig_constr)

        alter_constr = deltadbops.AlterTableAlterConstraint(
            name=tabconstr.get_subject_name(quote=False),
            constraint=orig_tabconstr, new_constraint=tabconstr,
            only_modify_enabled=only_modify_enabled)

        ops.add_command(alter_constr)

        return ops

    def delete_ops(self):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)
        add_constr = deltadbops.AlterTableDropConstraint(
            name=tabconstr.get_subject_name(quote=False), constraint=tabconstr)

        ops.add_command(add_constr)

        return ops

    def enforce_ops(self):
        ops = dbops.CommandGroup()

        tabconstr = self._table_constraint(self)

        constr_name = tabconstr.constraint_name()
        raw_constr_name = tabconstr.constraint_name(quote=False)

        for expr, origin_expr in zip(
            itertools.cycle(tabconstr._exprdata),
            tabconstr._origin_exprdata
        ):
            exprdata = expr['exprdata']
            origin_exprdata = origin_expr['exprdata']
            old_expr = origin_exprdata['old']
            new_expr = exprdata['new']

            schemaname, tablename = origin_expr['origin_subject_db_name']
            real_tablename = tabconstr.get_subject_name(quote=False)

            errmsg = 'duplicate key value violates unique ' \
                     'constraint {constr}'.format(constr=constr_name)
            detail = common.quote_literal(
                f"Key ({origin_exprdata['plain']}) already exists."
            )

            if (
                isinstance(self._subject, s_pointers.Pointer)
                and self._pg_constr_data['table_type'] == 'link'
            ):
                key = "source"
            else:
                key = "id"

            except_data = tabconstr._except_data
            origin_except_data = origin_expr['origin_except_data']

            if except_data:
                except_part = f'''
                    AND ({origin_except_data['old']} is not true)
                    AND ({except_data['new']} is not true)
                '''
            else:
                except_part = ''

            check = dbops.Query(
                f'''
                SELECT
                    edgedb.raise(
                        NULL::text,
                        'unique_violation',
                        msg => '{errmsg}',
                        "constraint" => '{raw_constr_name}',
                        "table" => '{tablename}',
                        "schema" => '{schemaname}',
                        detail => {detail}
                    )
                FROM {common.qname(schemaname, tablename+"_t")} AS OLD
                CROSS JOIN {common.qname(*real_tablename)} AS NEW
                WHERE {old_expr} = {new_expr} and OLD.{key} != NEW.{key}
                {except_part}
                INTO _dummy_text;
                '''
            )
            ops.add_command(check)

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

    try:
        sql_expr, _ = compiler.compile_ir_to_sql_tree(ir, singleton_mode=True)
    except errors.UnsupportedFeatureError:
        return None
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
            ptr = t.getptr(schema, s_name.UnqualName('id'))
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
