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

from typing import Optional, Tuple, Iterable, Sequence, Collection, Dict, List

from edb import errors

from edb.schema import indexes as s_indexes
from edb.schema import types as s_types
from edb.schema import expr as s_expr
from edb.schema import schema as s_schema
from edb.schema import delta as sd
from edb.schema import name as sn

from edb.ir import ast as irast

from edb.edgeql import compiler as qlcompiler

from . import common
from . import dbops
from . import compiler
from . import codegen
from . import types
from . import ast as pgast

from .common import qname as q
from .common import quote_literal as ql
from .common import quote_ident as qi
from .compiler import astutils
from .compiler import enums as pgce


def create_fts_index(
    index: s_indexes.Index,
    index_expr: irast.Set,
    predicate_src: Optional[str],
    sql_kwarg_exprs: Dict[str, str],
    options: qlcompiler.CompilerOptions,
    schema: s_schema.Schema,
    context: sd.CommandContext,
) -> dbops.Command:
    subject = index.get_subject(schema)
    assert isinstance(subject, s_indexes.IndexableSubject)

    effective, has_overridden = s_indexes.get_effective_object_index(
        schema, subject, sn.QualName("std::fts", "index")
    )

    if index != effective:
        return dbops.CommandGroup()

    # When creating an index on a child that already has an fts index
    # inherited from the parent, we don't need to create the index, but just
    # update the populating expressions.
    if has_overridden:
        return _refresh_fts_document(index, options, schema, context)
    else:
        return _create_fts_document(
            index,
            index_expr,
            predicate_src,
            sql_kwarg_exprs,
            schema,
            context,
        )


def delete_fts_index(
    index: s_indexes.Index,
    drop_index: dbops.Command,
    options: qlcompiler.CompilerOptions,
    schema: s_schema.Schema,
    orig_schema: s_schema.Schema,
    context: sd.CommandContext,
) -> dbops.Command:
    subject = index.get_subject(orig_schema)
    assert isinstance(subject, s_indexes.IndexableSubject)

    effective, _ = s_indexes.get_effective_object_index(
        schema, subject, sn.QualName("std::fts", "index")
    )

    if not effective:
        return _delete_fts_document(index, drop_index, orig_schema, context)
    else:
        # effective index remains: don't drop the fts document

        effective_subject = effective.get_subject(schema)
        is_eff_on_direct_parent = effective_subject in subject.get_bases(
            schema
        ).objects(schema)

        if is_eff_on_direct_parent:
            return _refresh_fts_document(index, options, schema, context)
        else:
            return dbops.CommandGroup()


def _compile_ir_index_exprs(
    index: s_indexes.Index, index_expr: irast.Set, schema: s_schema.Schema
):
    subject = index.get_subject(schema)
    assert isinstance(subject, s_types.Type)

    subject_id = irast.PathId.from_type(schema, subject, env=None)
    sql_res = compiler.compile_ir_to_sql_tree(
        index_expr,
        singleton_mode=True,
        external_rvars={
            (subject_id, pgce.PathAspect.SOURCE): pgast.RelRangeVar(
                alias=pgast.Alias(aliasname='NEW'),
                relation=pgast.Relation(name='NEW'),
            )
        },
    )
    return astutils.maybe_unpack_row(sql_res.ast)


def _create_fts_document(
    index: s_indexes.Index,
    index_expr: irast.Set,
    predicate_src: Optional[str],
    sql_kwarg_exprs: Dict[str, str],
    schema: s_schema.Schema,
    context: sd.CommandContext,
) -> dbops.Command:
    exprs = _compile_ir_index_exprs(index, index_expr, schema)

    from edb.common import debug

    if debug.flags.zombodb:
        return _zombo_create_fts_document(
            index, exprs, predicate_src, sql_kwarg_exprs, schema
        )
    else:
        return _pg_create_fts_document(
            index, exprs, predicate_src, sql_kwarg_exprs, schema
        )


def _delete_fts_document(
    index: s_indexes.Index,
    drop_index: dbops.Command,
    schema: s_schema.Schema,
    context: sd.CommandContext,
) -> dbops.Command:
    table_name = common.get_index_table_backend_name(index, schema)

    ops = dbops.CommandGroup()
    ops.add_command(drop_index)

    from edb.common import debug

    if debug.flags.zombodb:
        zombo_func_name = _zombo_func_name(table_name)
        ops.add_command(dbops.DropFunction(zombo_func_name, args=[table_name]))

        zombo_type_name = _zombo_type_name(table_name)
        ops.add_command(dbops.DropCompositeType(zombo_type_name))
    else:
        ops.add_command(_pg_drop_trigger(table_name))

        # When the ObjectType is being deleted, we don't drop the index, as it
        # will get dropped with parent table.
        # The same goes for the __fts_document__ column.
        source_drop = isinstance(drop_index, dbops.NoOpCommand)

        if not source_drop:
            fts_document = dbops.Column(
                name=f'__fts_document__',
                type=('pg_catalog', 'tsvector'),
            )
            alter_table = dbops.AlterTable(table_name)
            alter_table.add_operation(dbops.AlterTableDropColumn(fts_document))
            ops.add_command(alter_table)
    return ops


def update_fts_document(
    index: s_indexes.Index,
    options: qlcompiler.CompilerOptions,
    schema: s_schema.Schema,
) -> dbops.Query:
    table_name = common.get_index_table_backend_name(index, schema)

    # compile the expression
    index_sexpr: Optional[s_expr.Expression] = index.get_expr(schema)
    assert index_sexpr
    index_expr = index_sexpr.ensure_compiled(
        schema=schema,
        options=options,
        context=None,
    )
    exprs = _compile_ir_index_exprs(index, index_expr.irast.expr, schema)

    from edb.common import debug
    if debug.flags.zombodb:
        raise NotImplementedError('zombo refresh index not implemented')
    else:
        # to avoid code duplication, we call code for creating triggers and
        # extract the first UPDATE command
        create_trigger_ops = _pg_create_trigger(table_name, exprs)
        update_fts_document_op = create_trigger_ops.commands[0]
        assert isinstance(update_fts_document_op, dbops.Query)

        return update_fts_document_op


def _refresh_fts_document(
    index: s_indexes.Index,
    options: qlcompiler.CompilerOptions,
    schema: s_schema.Schema,
    context: sd.CommandContext,
) -> dbops.Command:
    table_name = common.get_index_table_backend_name(index, schema)

    # compile the expression
    index_sexpr: Optional[s_expr.Expression] = index.get_expr(schema)
    assert index_sexpr
    index_expr = index_sexpr.ensure_compiled(
        schema=schema,
        options=options,
        context=context,
    )

    exprs = _compile_ir_index_exprs(index, index_expr.irast.expr, schema)

    ops = dbops.CommandGroup()

    from edb.common import debug

    if debug.flags.zombodb:
        raise NotImplementedError('zombo refresh index not implemented')
    else:
        ops.add_command(_pg_drop_trigger(table_name))
        ops.add_command(_pg_create_trigger(table_name, exprs))

    return ops


def _raise_unsupported_language_error(
    unsupported: Collection[str],
) -> None:
    unsupported = list(unsupported)
    unsupported.sort()

    msg = 'Full text search language'
    if len(unsupported) > 1:
        msg += 's'

    msg += ' ' + ', '.join(f'`{l}`' for l in unsupported)
    msg += ' not supported'

    raise errors.UnsupportedFeatureError(msg)


# --- pg fts ---


def _pg_create_fts_document(
    index: s_indexes.Index,
    exprs: Sequence[pgast.BaseExpr],
    predicate_src: Optional[str],
    sql_kwarg_exprs: Dict[str, str],
    schema: s_schema.Schema,
) -> dbops.Command:
    ops = dbops.CommandGroup()

    # create column __fts_document__
    table_name = common.get_index_table_backend_name(index, schema)

    module_name = index.get_name(schema).module
    index_name = common.get_index_backend_name(
        index.id, module_name, catenate=False
    )

    fts_document = dbops.Column(
        name=f'__fts_document__', type='pg_catalog.tsvector'
    )
    alter_table = dbops.AlterTable(table_name)
    alter_table.add_operation(dbops.AlterTableAddColumn(fts_document))
    ops.add_command(alter_table)

    ops.add_command(_pg_create_trigger(table_name, exprs))

    pg_index = dbops.Index(
        name=index_name[1],
        table_name=table_name,  # type: ignore
        exprs=['__fts_document__'],
        unique=False,
        inherit=True,
        predicate=predicate_src,
        metadata={
            'schemaname': str(index.get_name(schema)),
            'kwargs': sql_kwarg_exprs,
            # use a reference to the new column in the index instead
            'code': 'gin (__col__)',
        },
    )
    ops.add_command(dbops.CreateIndex(pg_index))
    return ops


def _pg_create_trigger(
    table_name: Tuple[str, str],
    exprs: Sequence[pgast.BaseExpr],
) -> dbops.CommandGroup:
    ops = dbops.CommandGroup()

    # prepare the expression to update __fts_document__
    document_exprs = []
    for expr in exprs:
        assert isinstance(expr, pgast.FTSDocument)

        lang_domain: Iterable[str] = expr.language_domain
        lang_domain = map(types.to_regconfig, lang_domain)
        unsupported = set(lang_domain).difference(types.pg_langs)
        if len(unsupported) > 0:
            _raise_unsupported_language_error(unsupported)

        text_sql = codegen.generate_source(expr.text)
        language_sql = codegen.generate_source(expr.language)

        document_expr = f'''
            to_tsvector(
                edgedb.fts_to_regconfig(({language_sql})::text),
                COALESCE({text_sql}, '')
            )
        '''
        if expr.weight:
            document_expr = f'setweight({document_expr}, {ql(expr.weight)})'
        document_exprs.append(document_expr)

    document_sql = ' || '.join(document_exprs) if document_exprs else 'NULL'

    # update existing rows
    ops.add_command(
        dbops.Query(
            f"""
        UPDATE {q(*table_name)} as NEW SET __fts_document__ = ({document_sql});
        """
        )
    )

    # create update function
    func_name = _pg_update_func_name(table_name)
    function = dbops.Function(
        name=func_name,
        text=f'''
            BEGIN
                NEW.__fts_document__ := ({document_sql});
                RETURN NEW;
            END;
        ''',
        volatility='immutable',
        returns='trigger',
        language='plpgsql',
    )
    ops.add_command(dbops.CreateFunction(function))

    # create trigger to update the __fts_document__
    trigger_name = _pg_trigger_name(table_name[1])
    trigger = dbops.Trigger(
        name=trigger_name,
        table_name=table_name,
        events=('insert', 'update'),
        timing=dbops.TriggerTiming.Before,
        procedure=func_name,
    )
    ops.add_command(dbops.CreateTrigger(trigger))
    return ops


def _pg_drop_trigger(
    table_name: Tuple[str, str],
) -> dbops.Command:
    ops = dbops.CommandGroup()

    ops.add_command(
        dbops.DropTrigger(
            dbops.Trigger(
                _pg_trigger_name(table_name[1]),
                table_name=table_name,
                events=(),
                procedure='',
            )
        )
    )

    ops.add_command(
        dbops.DropFunction(
            _pg_update_func_name(table_name),
            (),
        )
    )
    return ops


def _pg_update_func_name(
    tbl_name: Tuple[str, str],
) -> Tuple[str, ...]:
    return (
        tbl_name[0],
        common.edgedb_name_to_pg_name(tbl_name[1] + '_ftsupdate'),
    )


def _pg_trigger_name(
    tbl_name: str,
) -> str:
    return common.edgedb_name_to_pg_name(tbl_name + '_ftstrigger')


# --- zombo ---


def _zombo_create_fts_document(
    index: s_indexes.Index,
    exprs: Sequence[pgast.BaseExpr],
    predicate_src: Optional[str],
    sql_kwarg_exprs: Dict[str, str],
    schema: s_schema.Schema,
) -> dbops.Command:
    ops = dbops.CommandGroup()

    table_name = common.get_index_table_backend_name(index, schema)

    module_name = index.get_name(schema).module
    index_name = common.get_index_backend_name(
        index.id, module_name, catenate=False
    )

    zombo_type_name = _zombo_type_name(table_name)
    ops.add_command(
        dbops.CreateCompositeType(
            dbops.CompositeType(
                name=zombo_type_name,
                columns=[
                    dbops.Column(
                        name=f'field{idx}',
                        type='text',
                    )
                    for idx, _ in enumerate(exprs)
                ],
            )
        )
    )

    type_mappings: List[Tuple[str, str]] = []
    document_exprs = []
    for idx, expr in enumerate(exprs):
        assert isinstance(expr, pgast.FTSDocument)

        text_sql = codegen.generate_source(expr.text)

        if len(expr.language_domain) != 1:
            raise errors.UnsupportedFeatureError(
                'zombo fts indexes support only exactly one language'
            )
        language = next(iter(expr.language_domain))

        document_exprs.append(text_sql)
        type_mappings.append((f'field{idx}', language))

    zombo_func_name = _zombo_func_name(table_name)
    ops.add_command(
        dbops.CreateFunction(
            dbops.Function(
                name=zombo_func_name,
                args=[('new', table_name)],
                returns=zombo_type_name,
                text=f'''
                SELECT
                    ROW({','.join(document_exprs)})::{q(*zombo_type_name)};
                ''',
            )
        )
    )

    for col_name, language in type_mappings:
        mapping = f'{{"type": "text", "analyzer": "{language}"}}'

        ops.add_command(
            dbops.Query(
                f"""PERFORM zdb.define_field_mapping(
                    {ql(q(*table_name))}::regclass,
                    {ql(col_name)}::text,
                    {ql(mapping)}::json
                )"""
            )
        )

    index_exprs = [f'{q(*zombo_func_name)}({qi(table_name[1])}.*)']

    pg_index = dbops.Index(
        name=index_name[1],
        table_name=table_name,  # type: ignore
        exprs=index_exprs,
        unique=False,
        inherit=True,
        with_clause={'url': ql('http://localhost:9200/')},
        predicate=predicate_src,
        metadata={
            'schemaname': str(index.get_name(schema)),
            'code': 'zombodb ((__col__))',
            'kwargs': sql_kwarg_exprs,
        },
    )
    ops.add_command(dbops.CreateIndex(pg_index))
    return ops


def _zombo_type_name(
    tbl_name: Tuple[str, str],
) -> Tuple[str, str]:
    return (
        tbl_name[0],
        common.edgedb_name_to_pg_name(tbl_name[1] + '_zombo_type'),
    )


def _zombo_func_name(
    tbl_name: Tuple[str, str],
) -> Tuple[str, ...]:
    return (
        tbl_name[0],
        common.edgedb_name_to_pg_name(tbl_name[1] + '_zombo_func'),
    )
