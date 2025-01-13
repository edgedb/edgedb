#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


"""Database structure and objects supporting Gel metadata."""

from __future__ import annotations
from typing import (
    Callable,
    Optional,
    Protocol,
    Tuple,
    Iterable,
    List,
    Set,
    Sequence,
    cast,
)

import functools
import json
import re

import edb._edgeql_parser as ql_parser

from edb.common import debug
from edb.common import exceptions
from edb.common import ordered
from edb.common import uuidgen
from edb.common import xdedent
from edb.common.typeutils import not_none

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from edb.edgeql import quote as qlquote
from edb.edgeql import compiler as qlcompiler

from edb.ir import statypes

from edb.schema import constraints as s_constr
from edb.schema import links as s_links
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import properties as s_props
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema
from edb.schema import sources as s_sources
from edb.schema import types as s_types
from edb.schema import utils as s_utils

from edb.server import defines
from edb.server import compiler as edbcompiler
from edb.server import config as edbconfig
from edb.server import pgcon  # HM.

from .resolver import sql_introspection

from . import codegen
from . import common
from . import compiler
from . import dbops
from . import inheritance
from . import params
from . import trampoline
from . import types

q = common.qname
qi = common.quote_ident
ql = common.quote_literal
qt = common.quote_type
V = common.versioned_schema


DATABASE_ID_NAMESPACE = uuidgen.UUID('0e6fed66-204b-11e9-8666-cffd58a5240b')
CONFIG_ID_NAMESPACE = uuidgen.UUID('a48b38fa-349b-11e9-a6be-4f337f82f5ad')
CONFIG_ID = {
    None: uuidgen.UUID('172097a4-39f4-11e9-b189-9321eb2f4b97'),
    qltypes.ConfigScope.INSTANCE: uuidgen.UUID(
        '172097a4-39f4-11e9-b189-9321eb2f4b98'),
    qltypes.ConfigScope.DATABASE: uuidgen.UUID(
        '172097a4-39f4-11e9-b189-9321eb2f4b99'),
}


def qtl(t: tuple[str, ...]) -> str:
    """Quote type literal"""
    return ql(f'{t[0]}.{t[1]}') if len(t) == 2 else ql(f'pg_catalog.{t[0]}')


class PGConnection(Protocol):

    async def sql_execute(
        self,
        sql: bytes,
    ) -> None:
        ...

    async def sql_fetch(
        self,
        sql: bytes,
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
    ) -> list[tuple[bytes, ...]]:
        ...

    async def sql_fetch_val(
        self,
        sql: bytes,
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
    ) -> bytes:
        ...

    async def sql_fetch_col(
        self,
        sql: bytes,
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
    ) -> list[bytes]:
        ...


class DBConfigTable(dbops.Table):
    def __init__(self) -> None:
        super().__init__(name=('edgedb', '_db_config'))

        self.add_columns([
            dbops.Column(name='name', type='text'),
            dbops.Column(name='value', type='jsonb'),
        ])

        self.add_constraint(
            dbops.UniqueConstraint(
                table_name=('edgedb', '_db_config'),
                columns=['name'],
            ),
        )


class InstDataTable(dbops.Table):
    def __init__(self) -> None:
        sname = V('edgedbinstdata')
        super().__init__(
            name=(sname, 'instdata'),
            columns=[
                dbops.Column(
                    name='key',
                    type='text',
                ),
                dbops.Column(
                    name='bin',
                    type='bytea',
                ),
                dbops.Column(
                    name='text',
                    type='text',
                ),
                dbops.Column(
                    name='json',
                    type='jsonb',
                ),
            ],
            constraints=ordered.OrderedSet([
                dbops.PrimaryKey(
                    table_name=(sname, 'instdata'),
                    columns=['key'],
                ),
            ]),
        )


class DMLDummyTable(dbops.Table):
    """A empty dummy table used when we need to emit no-op DML.

    This is used by scan_check_ctes in the pgsql compiler to
    force the evaluation of error checking.
    """
    def __init__(self) -> None:
        super().__init__(name=('edgedb', '_dml_dummy'))

        self.add_columns([
            dbops.Column(name='id', type='int8'),
            dbops.Column(name='flag', type='bool'),
        ])

        self.add_constraint(
            dbops.UniqueConstraint(
                table_name=('edgedb', '_dml_dummy'),
                columns=['id'],
            ),
        )

    SETUP_QUERY = '''
        INSERT INTO edgedb._dml_dummy VALUES (0, false)
    '''


class QueryCacheTable(dbops.Table):
    def __init__(self) -> None:
        super().__init__(name=('edgedb', '_query_cache'))

        self.add_columns([
            dbops.Column(name='key', type='uuid', required=True),
            dbops.Column(name='schema_version', type='uuid', required=True),
            dbops.Column(name='input', type='bytea', required=True),
            dbops.Column(name='output', type='bytea', required=True),
            dbops.Column(name='evict', type='text', required=True),
            dbops.Column(
                name='creation_time',
                type='timestamp with time zone',
                required=True,
                default='current_timestamp',
            ),
        ])

        self.add_constraint(
            dbops.PrimaryKey(
                table_name=('edgedb', '_query_cache'),
                columns=['key'],
            ),
        )


class EvictQueryCacheFunction(trampoline.VersionedFunction):

    text = f'''
    DECLARE
        evict_sql text;
    BEGIN
        DELETE FROM "edgedb"."_query_cache"
            WHERE "key" = cache_key
            RETURNING "evict" INTO evict_sql;
        IF evict_sql IS NOT NULL THEN
            EXECUTE evict_sql;
        END IF;
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_evict_query_cache'),
            args=[("cache_key", ("uuid",))],
            returns=("void",),
            language='plpgsql',
            volatility='volatile',
            text=self.text,
        )


class ClearQueryCacheFunction(trampoline.VersionedFunction):

    # TODO(fantix): this may consume a lot of memory in Postgres
    text = f'''
    DECLARE
        row record;
    BEGIN
        FOR row IN
            DELETE FROM "edgedb"."_query_cache"
            RETURNING "input", "evict"
        LOOP
            EXECUTE row."evict";
            RETURN NEXT row."input";
        END LOOP;
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_clear_query_cache'),
            args=[],
            returns=('bytea',),
            set_returning=True,
            language='plpgsql',
            volatility='volatile',
            text=self.text,
        )


class CreateTrampolineViewFunction(trampoline.VersionedFunction):
    text = f'''
        DECLARE
            cols text;
            tgt text;
            dummy text;
        BEGIN
            tgt := quote_ident(tgt_schema) || '.' || quote_ident(tgt_name);

            -- Check if the view already exists.
            select viewname into dummy
            from pg_catalog.pg_views
            where schemaname = tgt_schema
            and viewname = tgt_name;

            IF FOUND THEN
                -- If the view already existed, we need to generate a column
                -- list that maintains the order of anything that was present in
                -- the old view, and that doesn't remove any columns that were
                -- dropped.
                select
                  string_agg(
                    COALESCE(
                      quote_ident(tname),
                      'NULL::' || vtypname || ' AS ' || quote_ident(vname)
                    ),
                    ','
                  )
                from (
                  select
                    a1.attname as tname,
                    a2.attname as vname,
                    pg_catalog.format_type(a2.atttypid, NULL) as vtypname
                  from (
                    select * from pg_catalog.pg_attribute
                    where attrelid = src::regclass::oid
                    and attnum >= 0
                  ) a1
                  full outer join (
                    select * from pg_catalog.pg_attribute
                    where attrelid = tgt::regclass::oid
                  ) a2
                  on a1.attname = a2.attname
                  order by a2.attnum, a1.attnum
                ) t
                INTO cols;

            END IF;

            -- If it doesn't exist or has no columns, create it with SELECT *
            cols := COALESCE(cols, '*');

            EXECUTE 'CREATE OR REPLACE VIEW ' || tgt || ' AS ' ||
              'SELECT ' || cols || ' FROM ' || src;

        END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_create_trampoline_view'),
            args=[
                ('src', ('text',)),
                ('tgt_schema', ('text',)),
                ('tgt_name', ('text',)),
            ],
            returns=('void',),
            language='plpgsql',
            volatility='volatile',
            text=self.text,
        )


class BigintDomain(dbops.Domain):
    """Bigint: a variant of numeric that enforces zero digits after the dot.

    We're using an explicit scale check as opposed to simply specifying
    the numeric bounds, because using bounds severly restricts the range
    of the numeric type (1000 vs 131072 digits).
    """
    def __init__(self) -> None:
        super().__init__(
            name=('edgedbt', 'bigint_t'),
            base='numeric',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedbt', 'bigint_t'),
                    expr=("scale(VALUE) = 0 AND VALUE != 'NaN'"),
                ),
            ),
        )


class ConfigMemoryDomain(dbops.Domain):
    """Represents the cfg::memory type. Stores number of bytes.

    Defined just as edgedbt.bigint_t:

    * numeric is used to ensure we can comfortably represent huge amounts
      of data beyond petabytes;
    * enforces zero digits after the dot.
    """
    def __init__(self) -> None:
        super().__init__(
            name=('edgedbt', 'memory_t'),
            base='int8',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedbt', 'memory_t'),
                    expr=("VALUE >= 0"),
                ),
            ),
        )


class TimestampTzDomain(dbops.Domain):
    """Timestamptz clamped to years 0001-9999.

    The default timestamp range of (4713 BC - 294276 AD) has problems:
    Postgres isn't ISO compliant with years out of the 1-9999 range and
    language compatibility is questionable.
    """
    def __init__(self) -> None:
        super().__init__(
            name=('edgedbt', 'timestamptz_t'),
            base='timestamptz',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedbt', 'timestamptz_t'),
                    expr=("EXTRACT(years from VALUE) BETWEEN 1 AND 9999"),
                ),
            ),
        )


class TimestampDomain(dbops.Domain):
    """Timestamp clamped to years 0001-9999.

    The default timestamp range of (4713 BC - 294276 AD) has problems:
    Postgres isn't ISO compliant with years out of the 1-9999 range and
    language compatibility is questionable.
    """
    def __init__(self) -> None:
        super().__init__(
            name=('edgedbt', 'timestamp_t'),
            base='timestamp',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedbt', 'timestamp_t'),
                    expr=("EXTRACT(years from VALUE) BETWEEN 1 AND 9999"),
                ),
            ),
        )


class DateDomain(dbops.Domain):
    """Date clamped to years 0001-9999.

    The default timestamp range of (4713 BC - 294276 AD) has problems:
    Postgres isn't ISO compliant with years out of the 1-9999 range and
    language compatibility is questionable.
    """
    def __init__(self) -> None:
        super().__init__(
            name=('edgedbt', 'date_t'),
            base='date',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedbt', 'date_t'),
                    expr=("EXTRACT(years from VALUE) BETWEEN 1 AND 9999"),
                ),
            ),
        )


class DurationDomain(dbops.Domain):
    def __init__(self) -> None:
        super().__init__(
            name=('edgedbt', 'duration_t'),
            base='interval',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedbt', 'duration_t'),
                    expr=r'''
                        EXTRACT(months from VALUE) = 0 AND
                        EXTRACT(years from VALUE) = 0 AND
                        EXTRACT(days from VALUE) = 0
                    ''',
                ),
            ),
        )


class RelativeDurationDomain(dbops.Domain):
    def __init__(self) -> None:
        super().__init__(
            name=('edgedbt', 'relative_duration_t'),
            base='interval',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedbt', 'relative_duration_t'),
                    expr="true",
                ),
            ),
        )


class DateDurationDomain(dbops.Domain):
    def __init__(self) -> None:
        super().__init__(
            name=('edgedbt', 'date_duration_t'),
            base='interval',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedbt', 'date_duration_t'),
                    expr=r'''
                        EXTRACT(hour from VALUE) = 0 AND
                        EXTRACT(minute from VALUE) = 0 AND
                        EXTRACT(second from VALUE) = 0
                    ''',
                ),
            ),
        )


class Float32Range(dbops.Range):
    def __init__(self) -> None:
        super().__init__(
            name=types.type_to_range_name_map[('float4',)],
            subtype=('float4',),
        )


class Float64Range(dbops.Range):
    def __init__(self) -> None:
        super().__init__(
            name=types.type_to_range_name_map[('float8',)],
            subtype=('float8',),
            subtype_diff=('float8mi',)
        )


class DatetimeRange(dbops.Range):
    def __init__(self) -> None:
        super().__init__(
            name=types.type_to_range_name_map[('edgedbt', 'timestamptz_t')],
            subtype=('edgedbt', 'timestamptz_t'),
        )


class LocalDatetimeRange(dbops.Range):
    def __init__(self) -> None:
        super().__init__(
            name=types.type_to_range_name_map[('edgedbt', 'timestamp_t')],
            subtype=('edgedbt', 'timestamp_t'),
        )


class RangeToJsonFunction(trampoline.VersionedFunction):
    """Convert anyrange to a jsonb object."""
    text = r'''
        SELECT
            CASE
            WHEN val IS NULL THEN
                NULL
            WHEN isempty(val) THEN
                jsonb_build_object('empty', true)
            ELSE
                to_jsonb(o)
            END
        FROM
            (SELECT
                lower(val) as lower,
                lower_inc(val) as inc_lower,
                upper(val) as upper,
                upper_inc(val) as inc_upper
            ) AS o
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'range_to_jsonb'),
            args=[
                ('val', ('anyrange',)),
            ],
            returns=('jsonb',),
            volatility='immutable',
            language='sql',
            text=self.text,
        )


class MultiRangeToJsonFunction(trampoline.VersionedFunction):
    """Convert anymultirange to a jsonb object."""
    text = r'''
        SELECT
            CASE
            WHEN val IS NULL THEN
                NULL
            WHEN isempty(val) THEN
                jsonb_build_array()
            ELSE
                (
                    SELECT
                        jsonb_agg(edgedb_VER.range_to_jsonb(m.el))
                    FROM
                        (SELECT
                            unnest(val) AS el
                        ) AS m
                )
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'multirange_to_jsonb'),
            args=[
                ('val', ('anymultirange',)),
            ],
            returns=('jsonb',),
            volatility='immutable',
            language='sql',
            text=self.text,
        )


class RangeValidateFunction(trampoline.VersionedFunction):
    """Range constructor validation function."""
    text = r'''
        SELECT
            CASE
            WHEN
                empty
                AND (lower IS DISTINCT FROM upper
                     OR lower IS NOT NULL AND inc_upper AND inc_lower)
            THEN
                edgedb_VER.raise(
                    NULL::bool,
                    'invalid_parameter_value',
                    msg => 'conflicting arguments in range constructor:'
                           || ' "empty" is `true` while the specified'
                           || ' bounds suggest otherwise'
                )
            ELSE
                empty
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'range_validate'),
            args=[
                ('lower', ('anyelement',)),
                ('upper', ('anyelement',)),
                ('inc_lower', ('bool',)),
                ('inc_upper', ('bool',)),
                ('empty', ('bool',)),
            ],
            returns=('bool',),
            volatility='immutable',
            language='sql',
            text=self.text,
        )


class RangeUnpackLowerValidateFunction(trampoline.VersionedFunction):
    """Range unpack validation function."""
    text = r'''
        SELECT
            CASE WHEN
                NOT isempty(range)
            THEN
                edgedb_VER.raise_on_null(
                    lower(range),
                    'invalid_parameter_value',
                    msg => 'cannot unpack an unbounded range'
                )
            ELSE
                lower(range)
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'range_lower_validate'),
            args=[
                ('range', ('anyrange',)),
            ],
            returns=('anyelement',),
            volatility='immutable',
            language='sql',
            text=self.text,
        )


class RangeUnpackUpperValidateFunction(trampoline.VersionedFunction):
    """Range unpack validation function."""
    text = r'''
        SELECT
            CASE WHEN
                NOT isempty(range)
            THEN
                edgedb_VER.raise_on_null(
                    upper(range),
                    'invalid_parameter_value',
                    msg => 'cannot unpack an unbounded range'
                )
            ELSE
                upper(range)
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'range_upper_validate'),
            args=[
                ('range', ('anyrange',)),
            ],
            returns=('anyelement',),
            volatility='immutable',
            language='sql',
            text=self.text,
        )


class StrToConfigMemoryFunction(trampoline.VersionedFunction):
    """An implementation of std::str to cfg::memory cast."""
    text = r'''
        SELECT
            (CASE
                WHEN m.v[1] IS NOT NULL AND m.v[2] IS NOT NULL
                THEN (
                    CASE
                        WHEN m.v[2] = 'B'
                        THEN m.v[1]::int8

                        WHEN m.v[2] = 'KiB'
                        THEN m.v[1]::int8 * 1024

                        WHEN m.v[2] = 'MiB'
                        THEN m.v[1]::int8 * 1024 * 1024

                        WHEN m.v[2] = 'GiB'
                        THEN m.v[1]::int8 * 1024 * 1024 * 1024

                        WHEN m.v[2] = 'TiB'
                        THEN m.v[1]::int8 * 1024 * 1024 * 1024 * 1024

                        WHEN m.v[2] = 'PiB'
                        THEN m.v[1]::int8 * 1024 * 1024 * 1024 * 1024 * 1024

                        ELSE
                            -- Won't happen but we still have a guard for
                            -- completeness.
                            edgedb_VER.raise(
                                NULL::int8,
                                'invalid_parameter_value',
                                msg => (
                                    'unsupported memory size unit "' ||
                                    m.v[2] || '"'
                                )
                            )
                    END
                )
                ELSE
                    CASE
                        WHEN "val" = '0'
                        THEN 0::int8
                        ELSE
                            edgedb_VER.raise(
                                NULL::int8,
                                'invalid_parameter_value',
                                msg => (
                                    'unable to parse memory size "' ||
                                    "val" || '"'
                                )
                            )
                    END
            END)::edgedbt.memory_t
        FROM LATERAL (
            SELECT regexp_match(
                "val", '^(\d+)([[:alpha:]]+)$') AS v
        ) AS m
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'str_to_cfg_memory'),
            args=[
                ('val', ('text',)),
            ],
            returns=('edgedbt', 'memory_t'),
            strict=True,
            volatility='immutable',
            language='sql',
            text=self.text,
        )


class ConfigMemoryToStrFunction(trampoline.VersionedFunction):
    """An implementation of cfg::memory to std::str cast."""
    text = r'''
        SELECT
            CASE
                WHEN
                    "val" >= (1024::int8 * 1024 * 1024 * 1024 * 1024) AND
                    "val" % (1024::int8 * 1024 * 1024 * 1024 * 1024) = 0
                THEN
                    (
                        "val" / (1024::int8 * 1024 * 1024 * 1024 * 1024)
                    )::text || 'PiB'

                WHEN
                    "val" >= (1024::int8 * 1024 * 1024 * 1024) AND
                    "val" % (1024::int8 * 1024 * 1024 * 1024) = 0
                THEN
                    (
                        "val" / (1024::int8 * 1024 * 1024 * 1024)
                    )::text || 'TiB'

                WHEN
                    "val" >= (1024::int8 * 1024 * 1024) AND
                    "val" % (1024::int8 * 1024 * 1024) = 0
                THEN ("val" / (1024::int8 * 1024 * 1024))::text || 'GiB'

                WHEN "val" >= 1024::int8 * 1024 AND
                     "val" % (1024::int8 * 1024) = 0
                THEN ("val" / (1024::int8 * 1024))::text || 'MiB'

                WHEN "val" >= 1024 AND "val" % 1024 = 0
                THEN ("val" / 1024::int8)::text || 'KiB'

                ELSE "val"::text || 'B'
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'cfg_memory_to_str'),
            args=[
                ('val', ('edgedbt', 'memory_t')),
            ],
            returns=('text',),
            volatility='immutable',
            language='sql',
            text=self.text,
        )


class AlterCurrentDatabaseSetString(trampoline.VersionedFunction):
    """Alter a PostgreSQL configuration parameter of the current database."""
    text = '''
    BEGIN
        EXECUTE 'ALTER DATABASE ' || quote_ident(current_database())
        || ' SET ' || quote_ident(parameter) || ' = '
        || coalesce(quote_literal(value), 'DEFAULT');
        RETURN value;
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_alter_current_database_set'),
            args=[('parameter', ('text',)), ('value', ('text',))],
            returns=('text',),
            volatility='volatile',
            language='plpgsql',
            text=self.text,
        )


class AlterCurrentDatabaseSetStringArray(trampoline.VersionedFunction):
    """Alter a PostgreSQL configuration parameter of the current database."""
    text = '''
    BEGIN
        EXECUTE 'ALTER DATABASE ' || quote_ident(current_database())
        || ' SET ' || quote_ident(parameter) || ' = '
        || coalesce(
            (SELECT
                array_to_string(array_agg(quote_literal(q.v)), ',')
             FROM
                unnest(value) AS q(v)
            ),
            'DEFAULT'
        );
        RETURN value;
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_alter_current_database_set'),
            args=[
                ('parameter', ('text',)),
                ('value', ('text[]',)),
            ],
            returns=('text[]',),
            volatility='volatile',
            language='plpgsql',
            text=self.text,
        )


class AlterCurrentDatabaseSetNonArray(trampoline.VersionedFunction):
    """Alter a PostgreSQL configuration parameter of the current database."""
    text = '''
    BEGIN
        EXECUTE 'ALTER DATABASE ' || quote_ident(current_database())
        || ' SET ' || quote_ident(parameter) || ' = '
        || coalesce(value::text, 'DEFAULT');
        RETURN value;
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_alter_current_database_set'),
            args=[
                ('parameter', ('text',)),
                ('value', ('anynonarray',)),
            ],
            returns=('anynonarray',),
            volatility='volatile',
            language='plpgsql',
            text=self.text,
        )


class AlterCurrentDatabaseSetArray(trampoline.VersionedFunction):
    """Alter a PostgreSQL configuration parameter of the current database."""
    text = '''
    BEGIN
        EXECUTE 'ALTER DATABASE ' || quote_ident(current_database())
        || ' SET ' || quote_ident(parameter) || ' = '
        || coalesce(
            (SELECT
                array_to_string(array_agg(q.v::text), ',')
             FROM
                unnest(value) AS q(v)
            ),
            'DEFAULT'
        );
        RETURN value;
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_alter_current_database_set'),
            args=[
                ('parameter', ('text',)),
                ('value', ('anyarray',)),
            ],
            returns=('anyarray',),
            volatility='volatile',
            language='plpgsql',
            text=self.text,
        )


class CopyDatabaseConfigs(trampoline.VersionedFunction):
    """Copy database configs from one database to the current one"""
    text = '''
        SELECT edgedb_VER._alter_current_database_set(
            nameval.name, nameval.value)
        FROM
            pg_db_role_setting AS cfg,
            LATERAL unnest(cfg.setconfig) as cfg_set(s),
            LATERAL (
                SELECT
                    split_part(cfg_set.s, '=', 1) AS name,
                    split_part(cfg_set.s, '=', 2) AS value
            ) AS nameval
        WHERE
            setdatabase = (
                SELECT oid
                FROM pg_database
                WHERE datname = source_db
            )
            AND setrole = 0;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_copy_database_configs'),
            args=[('source_db', ('text',))],
            returns=('text',),
            volatility='volatile',
            text=self.text,
        )


class StrToBigint(trampoline.VersionedFunction):
    """Parse bigint from text."""

    # The plpgsql execption handling nonsense is actually just so that
    # we can produce an exception that mentions edgedbt.bigint_t
    # instead of numeric, and thus produce the right user-facing
    # exception. As a nice side effect it is like twice as fast
    # as the previous code too.
    text = r'''
        DECLARE
            v numeric;
        BEGIN
            BEGIN
              v := val::numeric;
            EXCEPTION
              WHEN OTHERS THEN
                 v := NULL;
            END;

            IF scale(v) = 0 THEN
                RETURN v::edgedbt.bigint_t;
            ELSE
                EXECUTE edgedb_VER.raise(
                    NULL::numeric,
                    'invalid_text_representation',
                    msg => (
                        'invalid input syntax for type edgedbt.bigint_t: '
                        || quote_literal(val)
                    )
                );
            END IF;
        END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'str_to_bigint'),
            args=[('val', ('text',))],
            returns=('edgedbt', 'bigint_t'),
            language='plpgsql',
            volatility='immutable',
            strict=True,
            text=self.text)


class StrToDecimal(trampoline.VersionedFunction):
    """Parse decimal from text."""
    text = r'''
        SELECT
            (CASE WHEN v.column1 != 'NaN' THEN
                v.column1
            ELSE
                edgedb_VER.raise(
                    NULL::numeric,
                    'invalid_text_representation',
                    msg => (
                        'invalid input syntax for type numeric: '
                        || quote_literal(val)
                    )
                )
            END)
        FROM
            (VALUES (
                val::numeric
            )) AS v
        ;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'str_to_decimal'),
            args=[('val', ('text',))],
            returns=('numeric',),
            volatility='immutable',
            strict=True,
            text=self.text,
        )


class StrToInt64NoInline(trampoline.VersionedFunction):
    """String-to-int64 cast with noinline guard.

    Adding a LIMIT clause to the function statement makes it
    uninlinable due to the Postgres inlining heuristic looking
    for simple SELECT expressions only (i.e. no clauses.)

    This might need to change in the future if the heuristic
    changes.
    """
    text = r'''
        SELECT
            "val"::bigint
        LIMIT
            1
        ;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'str_to_int64_noinline'),
            args=[('val', ('text',))],
            returns=('bigint',),
            volatility='immutable',
            text=self.text,
        )


class StrToInt32NoInline(trampoline.VersionedFunction):
    """String-to-int32 cast with noinline guard."""
    text = r'''
        SELECT
            "val"::int
        LIMIT
            1
        ;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'str_to_int32_noinline'),
            args=[('val', ('text',))],
            returns=('int',),
            volatility='immutable',
            text=self.text,
        )


class StrToInt16NoInline(trampoline.VersionedFunction):
    """String-to-int16 cast with noinline guard."""
    text = r'''
        SELECT
            "val"::smallint
        LIMIT
            1
        ;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'str_to_int16_noinline'),
            args=[('val', ('text',))],
            returns=('smallint',),
            volatility='immutable',
            text=self.text,
        )


class StrToFloat64NoInline(trampoline.VersionedFunction):
    """String-to-float64 cast with noinline guard."""
    text = r'''
        SELECT
            "val"::float8
        LIMIT
            1
        ;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'str_to_float64_noinline'),
            args=[('val', ('text',))],
            returns=('float8',),
            volatility='immutable',
            text=self.text,
        )


class StrToFloat32NoInline(trampoline.VersionedFunction):
    """String-to-float32 cast with noinline guard."""
    text = r'''
        SELECT
            "val"::float4
        LIMIT
            1
        ;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'str_to_float32_noinline'),
            args=[('val', ('text',))],
            returns=('float4',),
            volatility='immutable',
            text=self.text,
        )


class GetBackendCapabilitiesFunction(trampoline.VersionedFunction):

    text = f'''
        SELECT
            (json ->> 'capabilities')::bigint
        FROM
            edgedbinstdata_VER.instdata
        WHERE
            key = 'backend_instance_params'
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_backend_capabilities'),
            args=[],
            returns=('bigint',),
            language='sql',
            volatility='stable',
            text=self.text,
        )


class GetBackendTenantIDFunction(trampoline.VersionedFunction):

    text = f'''
        SELECT
            (json ->> 'tenant_id')::text
        FROM
            edgedbinstdata_VER.instdata
        WHERE
            key = 'backend_instance_params'
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_backend_tenant_id'),
            args=[],
            returns=('text',),
            language='sql',
            volatility='stable',
            text=self.text,
        )


class GetDatabaseBackendNameFunction(trampoline.VersionedFunction):

    text = f'''
    SELECT
        CASE
        WHEN
            (edgedb_VER.get_backend_capabilities()
             & {int(params.BackendCapabilities.CREATE_DATABASE)}) != 0
        THEN
            edgedb_VER.get_backend_tenant_id() || '_' || "db_name"
        ELSE
            current_database()::text
        END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_database_backend_name'),
            args=[('db_name', ('text',))],
            returns=('text',),
            language='sql',
            volatility='stable',
            text=self.text,
        )


class GetDatabaseFrontendNameFunction(trampoline.VersionedFunction):

    text = f'''
    SELECT
        CASE
        WHEN
            (edgedb_VER.get_backend_capabilities()
             & {int(params.BackendCapabilities.CREATE_DATABASE)}) != 0
        THEN
            substring(db_name, position('_' in db_name) + 1)
        ELSE
            'edgedb'
        END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_database_frontend_name'),
            args=[('db_name', ('text',))],
            returns=('text',),
            language='sql',
            volatility='stable',
            text=self.text,
        )


class GetRoleBackendNameFunction(trampoline.VersionedFunction):

    text = f'''
    SELECT
        CASE
        WHEN
            (edgedb_VER.get_backend_capabilities()
             & {int(params.BackendCapabilities.CREATE_ROLE)}) != 0
        THEN
            edgedb_VER.get_backend_tenant_id() || '_' || "role_name"
        ELSE
            current_user::text
        END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_role_backend_name'),
            args=[('role_name', ('text',))],
            returns=('text',),
            language='sql',
            volatility='stable',
            text=self.text,
        )


class GetUserSequenceBackendNameFunction(trampoline.VersionedFunction):

    text = f"""
        SELECT
            'edgedbpub',
            "sequence_type_id"::text || '_sequence'
    """

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_user_sequence_backend_name'),
            args=[('sequence_type_id', ('uuid',))],
            returns=('record',),
            language='sql',
            volatility='stable',
            text=self.text,
        )


class GetSequenceBackendNameFunction(trampoline.VersionedFunction):

    text = f'''
        SELECT
            (CASE
                WHEN edgedb_VER.get_name_module(st.name)
                     = any(edgedb_VER.get_std_modules())
                THEN 'edgedbstd'
                ELSE 'edgedbpub'
             END),
            "sequence_type_id"::text || '_sequence'
        FROM
            edgedb_VER."_SchemaScalarType" AS st
        WHERE
            st.id = "sequence_type_id"
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_sequence_backend_name'),
            args=[('sequence_type_id', ('uuid',))],
            returns=('record',),
            language='sql',
            volatility='stable',
            text=self.text,
        )


class GetStdModulesFunction(trampoline.VersionedFunction):

    text = f'''
        SELECT ARRAY[{",".join(ql(str(m)) for m in s_schema.STD_MODULES)}]
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_std_modules'),
            args=[],
            returns=('text[]',),
            language='sql',
            volatility='immutable',
            text=self.text,
        )


class GetObjectMetadata(trampoline.VersionedFunction):
    """Return Gel metadata associated with a backend object."""
    text = '''
        SELECT
            CASE WHEN substr(d, 1, char_length({prefix})) = {prefix}
            THEN substr(d, char_length({prefix}) + 1)::jsonb
            ELSE '{{}}'::jsonb
            END
        FROM
            obj_description("objoid", "objclass") AS d
    '''.format(
        prefix=f'E{ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)}',
    )

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'obj_metadata'),
            args=[('objoid', ('oid',)), ('objclass', ('text',))],
            returns=('jsonb',),
            volatility='stable',
            text=self.text)


class GetColumnMetadata(trampoline.VersionedFunction):
    """Return Gel metadata associated with a backend object."""
    text = '''
        SELECT
            CASE WHEN substr(d, 1, char_length({prefix})) = {prefix}
            THEN substr(d, char_length({prefix}) + 1)::jsonb
            ELSE '{{}}'::jsonb
            END
        FROM
            col_description("tableoid", "column") AS d
    '''.format(
        prefix=f'E{ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)}',
    )

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'col_metadata'),
            args=[('tableoid', ('oid',)), ('column', ('integer',))],
            returns=('jsonb',),
            volatility='stable',
            text=self.text)


class GetSharedObjectMetadata(trampoline.VersionedFunction):
    """Return Gel metadata associated with a backend object."""
    text = '''
        SELECT
            CASE WHEN substr(d, 1, char_length({prefix})) = {prefix}
            THEN substr(d, char_length({prefix}) + 1)::jsonb
            ELSE '{{}}'::jsonb
            END
        FROM
            shobj_description("objoid", "objclass") AS d
    '''.format(
        prefix=f'E{ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)}',
    )

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'shobj_metadata'),
            args=[('objoid', ('oid',)), ('objclass', ('text',))],
            returns=('jsonb',),
            volatility='stable',
            text=self.text)


class GetDatabaseMetadataFunction(trampoline.VersionedFunction):
    """Return Gel metadata associated with a given database."""
    text = f'''
        SELECT
            CASE
            WHEN
                "dbname" = {ql(defines.EDGEDB_SUPERUSER_DB)}
                OR (edgedb_VER.get_backend_capabilities()
                    & {int(params.BackendCapabilities.CREATE_DATABASE)}) != 0
            THEN
                edgedb_VER.shobj_metadata(
                    (SELECT
                        oid
                     FROM
                        pg_database
                     WHERE
                        datname = edgedb_VER.get_database_backend_name("dbname")
                    ),
                    'pg_database'
                )
            ELSE
                COALESCE(
                    (SELECT
                        json
                     FROM
                        edgedbinstdata_VER.instdata
                     WHERE
                        key = "dbname" || 'metadata'
                    ),
                    '{{}}'::jsonb
                )
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_database_metadata'),
            args=[('dbname', ('text',))],
            returns=('jsonb',),
            volatility='stable',
            text=self.text,
        )


class GetCurrentDatabaseFunction(trampoline.VersionedFunction):

    text = f'''
        SELECT
            CASE
            WHEN
                (edgedb_VER.get_backend_capabilities()
                 & {int(params.BackendCapabilities.CREATE_DATABASE)}) != 0
            THEN
                substr(
                    current_database(),
                    char_length(edgedb_VER.get_backend_tenant_id()) + 2
                )
            ELSE
                {ql(defines.EDGEDB_SUPERUSER_DB)}
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_current_database'),
            args=[],
            returns=('text',),
            language='sql',
            volatility='stable',
            text=self.text,
        )


class RaiseNoticeFunction(trampoline.VersionedFunction):
    text = '''
    BEGIN
        RAISE NOTICE USING
            MESSAGE = "msg",
            DETAIL = COALESCE("detail", ''),
            HINT = COALESCE("hint", ''),
            COLUMN = COALESCE("column", ''),
            CONSTRAINT = COALESCE("constraint", ''),
            DATATYPE = COALESCE("datatype", ''),
            TABLE = COALESCE("table", ''),
            SCHEMA = COALESCE("schema", '');
        RETURN "rtype";
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'notice'),
            args=[
                ('rtype', ('anyelement',)),
                ('msg', ('text',), "''"),
                ('detail', ('text',), "''"),
                ('hint', ('text',), "''"),
                ('column', ('text',), "''"),
                ('constraint', ('text',), "''"),
                ('datatype', ('text',), "''"),
                ('table', ('text',), "''"),
                ('schema', ('text',), "''"),
            ],
            returns=('anyelement',),
            # NOTE: The main reason why we don't want this function to be
            # immutable is that immutable functions can be
            # pre-evaluated by the query planner once if they have
            # constant arguments. This means that using this function
            # as the second argument in a COALESCE will raise a
            # notice regardless of whether the first argument is
            # NULL or not.
            volatility='stable',
            language='plpgsql',
            text=self.text,
        )


# edgedb.indirect_return() to be used to return values from
# anonymous code blocks or other contexts that have no return
# data channel.
class IndirectReturnFunction(trampoline.VersionedFunction):
    text = """
    SELECT
        edgedb_VER.notice(
            NULL::text,
            msg => 'edb:notice:indirect_return',
            detail => "value"
        )
    """

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'indirect_return'),
            args=[
                ('value', ('text',)),
            ],
            returns=('text',),
            # NOTE: The main reason why we don't want this function to be
            # immutable is that immutable functions can be
            # pre-evaluated by the query planner once if they have
            # constant arguments. This means that using this function
            # as the second argument in a COALESCE will raise a
            # notice regardless of whether the first argument is
            # NULL or not.
            volatility='stable',
            language='sql',
            text=self.text,
        )


class RaiseExceptionFunction(trampoline.VersionedFunction):
    text = '''
    BEGIN
        RAISE EXCEPTION USING
            ERRCODE = "exc",
            MESSAGE = "msg",
            DETAIL = COALESCE("detail", ''),
            HINT = COALESCE("hint", ''),
            COLUMN = COALESCE("column", ''),
            CONSTRAINT = COALESCE("constraint", ''),
            DATATYPE = COALESCE("datatype", ''),
            TABLE = COALESCE("table", ''),
            SCHEMA = COALESCE("schema", '');
        RETURN "rtype";
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'raise'),
            args=[
                ('rtype', ('anyelement',)),
                ('exc', ('text',), "'raise_exception'"),
                ('msg', ('text',), "''"),
                ('detail', ('text',), "''"),
                ('hint', ('text',), "''"),
                ('column', ('text',), "''"),
                ('constraint', ('text',), "''"),
                ('datatype', ('text',), "''"),
                ('table', ('text',), "''"),
                ('schema', ('text',), "''"),
            ],
            returns=('anyelement',),
            # NOTE: The main reason why we don't want this function to be
            # immutable is that immutable functions can be
            # pre-evaluated by the query planner once if they have
            # constant arguments. This means that using this function
            # as the second argument in a COALESCE will raise an
            # exception regardless of whether the first argument is
            # NULL or not.
            volatility='stable',
            language='plpgsql',
            text=self.text,
        )


class RaiseExceptionOnNullFunction(trampoline.VersionedFunction):
    """Return the passed value or raise an exception if it's NULL."""
    text = '''
        SELECT coalesce(
            val,
            edgedb_VER.raise(
                val,
                exc,
                msg => msg,
                detail => detail,
                hint => hint,
                "column" => "column",
                "constraint" => "constraint",
                "datatype" => "datatype",
                "table" => "table",
                "schema" => "schema"
            )
        )
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'raise_on_null'),
            args=[
                ('val', ('anyelement',)),
                ('exc', ('text',)),
                ('msg', ('text',)),
                ('detail', ('text',), "''"),
                ('hint', ('text',), "''"),
                ('column', ('text',), "''"),
                ('constraint', ('text',), "''"),
                ('datatype', ('text',), "''"),
                ('table', ('text',), "''"),
                ('schema', ('text',), "''"),
            ],
            returns=('anyelement',),
            # Same volatility as raise()
            volatility='stable',
            text=self.text,
        )


class RaiseExceptionOnNotNullFunction(trampoline.VersionedFunction):
    """Return the passed value or raise an exception if it's NOT NULL."""
    text = '''
        SELECT
            CASE
            WHEN val IS NULL THEN
                val
            ELSE
                edgedb_VER.raise(
                    val,
                    exc,
                    msg => msg,
                    detail => detail,
                    hint => hint,
                    "column" => "column",
                    "constraint" => "constraint",
                    "datatype" => "datatype",
                    "table" => "table",
                    "schema" => "schema"
                )
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'raise_on_not_null'),
            args=[
                ('val', ('anyelement',)),
                ('exc', ('text',)),
                ('msg', ('text',)),
                ('detail', ('text',), "''"),
                ('hint', ('text',), "''"),
                ('column', ('text',), "''"),
                ('constraint', ('text',), "''"),
                ('datatype', ('text',), "''"),
                ('table', ('text',), "''"),
                ('schema', ('text',), "''"),
            ],
            returns=('anyelement',),
            # Same volatility as raise()
            volatility='stable',
            text=self.text,
        )


class RaiseExceptionOnEmptyStringFunction(trampoline.VersionedFunction):
    """Return the passed string or raise an exception if it's empty."""
    text = '''
        SELECT
            CASE WHEN edgedb_VER._length(val) = 0 THEN
                edgedb_VER.raise(val, exc, msg => msg, detail => detail)
            ELSE
                val
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'raise_on_empty'),
            args=[
                ('val', ('anyelement',)),
                ('exc', ('text',)),
                ('msg', ('text',)),
                ('detail', ('text',), "''"),
            ],
            returns=('anyelement',),
            # Same volatility as raise()
            volatility='stable',
            text=self.text,
        )


class AssertJSONTypeFunction(trampoline.VersionedFunction):
    """Assert that the JSON type matches what is expected."""
    text = '''
        SELECT
            CASE WHEN array_position(typenames, jsonb_typeof(val)) IS NULL THEN
                edgedb_VER.raise(
                    NULL::jsonb,
                    'wrong_object_type',
                    msg => coalesce(
                        msg,
                        (
                            'expected JSON '
                            || array_to_string(typenames, ' or ')
                            || '; got JSON '
                            || coalesce(jsonb_typeof(val), 'UNKNOWN')
                        )
                    ),
                    detail => detail
                )
            ELSE
                val
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'jsonb_assert_type'),
            args=[
                ('val', ('jsonb',)),
                ('typenames', ('text[]',)),
                ('msg', ('text',), 'NULL'),
                ('detail', ('text',), "''"),
            ],
            returns=('jsonb',),
            # Max volatility of raise() and array_to_string() (stable)
            volatility='stable',
            text=self.text,
        )


class ExtractJSONScalarFunction(trampoline.VersionedFunction):
    """Convert a given JSON scalar value into a text value."""
    text = '''
        SELECT
            (to_jsonb(ARRAY[
                edgedb_VER.jsonb_assert_type(
                    coalesce(val, 'null'::jsonb),
                    ARRAY[json_typename, 'null'],
                    msg => msg,
                    detail => detail
                )
            ])->>0)
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'jsonb_extract_scalar'),
            args=[
                ('val', ('jsonb',)),
                ('json_typename', ('text',)),
                ('msg', ('text',), 'NULL'),
                ('detail', ('text',), "''"),
            ],
            returns=('text',),
            volatility='immutable',
            text=self.text,
        )


class GetSchemaObjectNameFunction(trampoline.VersionedFunction):
    text = '''
        SELECT coalesce(
            (SELECT name FROM edgedb_VER."_SchemaObject"
             WHERE id = type::uuid),
            edgedb_VER.raise(
                NULL::text,
                msg => 'resolve_type_name: unknown type: "' || type || '"'
            )
        )
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_get_schema_object_name'),
            args=[('type', ('uuid',))],
            returns=('text',),
            # Max volatility of raise() and a SELECT from a
            # table (stable).
            volatility='stable',
            text=self.text,
            strict=True,
        )


class IssubclassFunction(trampoline.VersionedFunction):
    text = '''
        SELECT
            clsid = any(classes) OR (
                SELECT classes && q.ancestors
                FROM
                    (SELECT
                        array_agg(o.target) AS ancestors
                        FROM edgedb_VER."_SchemaInheritingObject__ancestors" o
                        WHERE o.source = clsid
                    ) AS q
            );
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'issubclass'),
            args=[('clsid', 'uuid'), ('classes', 'uuid[]')],
            returns='bool',
            volatility='stable',
            text=self.__class__.text)


class IssubclassFunction2(trampoline.VersionedFunction):
    text = '''
        SELECT
            clsid = pclsid OR (
                SELECT
                    pclsid IN (
                        SELECT
                            o.target
                        FROM edgedb_VER."_SchemaInheritingObject__ancestors" o
                            WHERE o.source = clsid
                    )
            );
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'issubclass'),
            args=[('clsid', 'uuid'), ('pclsid', 'uuid')],
            returns='bool',
            volatility='stable',
            text=self.__class__.text)


class NormalizeNameFunction(trampoline.VersionedFunction):
    text = '''
        SELECT
            CASE WHEN strpos(name, '@') = 0 THEN
                name
            ELSE
                CASE WHEN strpos(name, '::') = 0 THEN
                    replace(split_part(name, '@', 1), '|', '::')
                ELSE
                    replace(
                        split_part(
                            -- "reverse" calls are to emulate "rsplit"
                            reverse(split_part(reverse(name), '::', 1)),
                            '@', 1),
                        '|', '::')
                END
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'shortname_from_fullname'),
            args=[('name', 'text')],
            returns='text',
            volatility='immutable',
            language='sql',
            text=self.__class__.text)


class GetNameModuleFunction(trampoline.VersionedFunction):
    text = '''
        SELECT reverse(split_part(reverse("name"), '::', 1))
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_name_module'),
            args=[('name', 'text')],
            returns='text',
            volatility='immutable',
            language='sql',
            text=self.__class__.text)


class NullIfArrayNullsFunction(trampoline.VersionedFunction):
    """Check if array contains NULLs and if so, return NULL."""
    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_nullif_array_nulls'),
            args=[('a', 'anyarray')],
            returns='anyarray',
            volatility='stable',
            language='sql',
            text='''
                SELECT CASE WHEN array_position(a, NULL) IS NULL
                THEN a ELSE NULL END
            ''')


class NormalizeArrayIndexFunction(trampoline.VersionedFunction):
    """Convert an EdgeQL index to SQL index."""

    text = '''
        SELECT
            CASE WHEN index > (2147483647-1) OR index < -2147483648 THEN
                NULL
            WHEN index < 0 THEN
                length + index::int + 1
            ELSE
                index::int + 1
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_normalize_array_index'),
            args=[('index', ('bigint',)), ('length', ('int',))],
            returns=('int',),
            volatility='immutable',
            text=self.text,
        )


class NormalizeArraySliceIndexFunction(trampoline.VersionedFunction):
    """Convert an EdgeQL index to SQL index (for slices)"""

    text = '''
        SELECT
            GREATEST(0, LEAST(2147483647,
                CASE WHEN index < 0 THEN
                    length::bigint + index + 1
                ELSE
                    index + 1
                END
            ))
        WHERE index IS NOT NULL
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_normalize_array_slice_index'),
            args=[('index', ('bigint',)), ('length', ('int',))],
            returns=('int',),
            volatility='immutable',
            text=self.text,
        )


class IntOrNullFunction(trampoline.VersionedFunction):
    """
    Convert bigint to int. If it does not fit, return NULL.
    """

    text = """
        SELECT
            CASE WHEN val <= 2147483647 AND val >= -2147483648 THEN
                val
            ELSE
                NULL
            END
    """

    def __init__(self) -> None:
        super().__init__(
            name=("edgedb", "_int_or_null"),
            args=[("val", ("bigint",))],
            returns=("int",),
            volatility="immutable",
            strict=True,
            text=self.text,
        )


class ArrayIndexWithBoundsFunction(trampoline.VersionedFunction):
    """Get an array element or raise an out-of-bounds exception."""

    text = '''
        SELECT CASE WHEN val IS NULL THEN
            NULL
        ELSE
            edgedb_VER.raise_on_null(
                val[edgedb_VER._normalize_array_index(
                    index, array_upper(val, 1))],
                'array_subscript_error',
                msg => 'array index ' || index::text || ' is out of bounds',
                detail => detail
            )
        END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('anyarray',)), ('index', ('bigint',)),
                  ('detail', ('text',))],
            returns=('anyelement',),
            # Min volatility of exception helpers and pg_typeof is 'stable',
            # but for all practical purposes, we can assume 'immutable'
            volatility='immutable',
            text=self.text,
        )


class ArraySliceFunction(trampoline.VersionedFunction):
    """Get an array slice."""

    # This function is also inlined in expr.py#_inline_array_slicing.

    # Known bug: if array has 2G elements and both bounds are overflowing,
    # this will return last element instead of an empty array.
    text = """
        SELECT val[
            edgedb_VER._normalize_array_slice_index(start, cardinality(val))
            :
            edgedb_VER._normalize_array_slice_index(stop, cardinality(val)) - 1
        ]
    """

    def __init__(self) -> None:
        super().__init__(
            name=("edgedb", "_slice"),
            args=[
                ("val", ("anyarray",)),
                ("start", ("bigint",)),
                ("stop", ("bigint",)),
            ],
            returns=("anyarray",),
            volatility="immutable",
            text=self.text,
        )


class StringIndexWithBoundsFunction(trampoline.VersionedFunction):
    """Get a string character or raise an out-of-bounds exception."""

    text = '''
        SELECT edgedb_VER.raise_on_empty(
            CASE WHEN pg_index IS NULL THEN
                ''
            ELSE
                substr("val", pg_index, 1)
            END,
            'invalid_parameter_value',
            "typename" || ' index ' || "index"::text || ' is out of bounds',
            "detail"
        )
        FROM (
            SELECT (
                edgedb_VER._normalize_array_index("index", char_length("val"))
            ) as pg_index
        ) t
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_index'),
            args=[
                ('val', ('text',)),
                ('index', ('bigint',)),
                ('detail', ('text',)),
                ('typename', ('text',), "'string'"),
            ],
            returns=('text',),
            # Min volatility of exception helpers and pg_typeof is 'stable',
            # but for all practical purposes, we can assume 'immutable'
            volatility='immutable',
            text=self.text,
        )


class BytesIndexWithBoundsFunction(trampoline.VersionedFunction):
    """Get a bytes character or raise an out-of-bounds exception."""

    text = '''
        SELECT edgedb_VER.raise_on_empty(
            CASE WHEN pg_index IS NULL THEN
                ''::bytea
            ELSE
                substr("val", pg_index, 1)
            END,
            'invalid_parameter_value',
            'byte string index ' || "index"::text || ' is out of bounds',
            "detail"
        )
        FROM (
            SELECT (
                edgedb_VER._normalize_array_index("index", length("val"))
            ) as pg_index
        ) t
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_index'),
            args=[
                ('val', ('bytea',)),
                ('index', ('bigint',)),
                ('detail', ('text',)),
            ],
            returns=('bytea',),
            # Min volatility of exception helpers and pg_typeof is 'stable',
            # but for all practical purposes, we can assume 'immutable'
            volatility='immutable',
            text=self.text,
        )


class SubstrProxyFunction(trampoline.VersionedFunction):
    """Same as substr, but interpret negative length as 0 instead."""

    text = r"""
        SELECT
            CASE
                WHEN length < 0 THEN ''
                ELSE substr(val, start::int, length)
            END
    """

    def __init__(self) -> None:
        super().__init__(
            name=("edgedb", "_substr"),
            args=[
                ("val", ("anyelement",)),
                ("start", ("int",)),
                ("length", ("int",)),
            ],
            returns=("anyelement",),
            volatility="immutable",
            strict=True,
            text=self.text,
        )


class LengthStringProxyFunction(trampoline.VersionedFunction):
    """Same as substr, but interpret negative length as 0 instead."""
    text = r'''
        SELECT char_length(val)
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_length'),
            args=[('val', ('text',))],
            returns=('int',),
            volatility='immutable',
            strict=True,
            text=self.text)


class LengthBytesProxyFunction(trampoline.VersionedFunction):
    """Same as substr, but interpret negative length as 0 instead."""
    text = r'''
        SELECT length(val)
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_length'),
            args=[('val', ('bytea',))],
            returns=('int',),
            volatility='immutable',
            strict=True,
            text=self.text)


class StringSliceImplFunction(trampoline.VersionedFunction):
    """Get a string slice."""

    text = r"""
        SELECT
            edgedb_VER._substr(
                val,
                pg_start,
                pg_end - pg_start
            )
        FROM (SELECT
            edgedb_VER._normalize_array_slice_index(
                start, edgedb_VER._length(val)
            ) as pg_start,
            edgedb_VER._normalize_array_slice_index(
                stop, edgedb_VER._length(val)
            ) as pg_end
        ) t
    """

    def __init__(self) -> None:
        super().__init__(
            name=("edgedb", "_str_slice"),
            args=[
                ("val", ("anyelement",)),
                ("start", ("bigint",)),
                ("stop", ("bigint",)),
            ],
            returns=("anyelement",),
            volatility="immutable",
            text=self.text,
        )


class StringSliceFunction(trampoline.VersionedFunction):
    """Get a string slice."""
    text = r'''
        SELECT edgedb_VER._str_slice(val, start, stop)
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_slice'),
            args=[
                ('val', ('text',)),
                ('start', ('bigint',)),
                ('stop', ('bigint',)),
            ],
            returns=('text',),
            volatility='immutable',
            text=self.text)


class BytesSliceFunction(trampoline.VersionedFunction):
    """Get a string slice."""
    text = r'''
        SELECT edgedb_VER._str_slice(val, start, stop)
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_slice'),
            args=[
                ('val', ('bytea',)),
                ('start', ('bigint',)),
                ('stop', ('bigint',)),
            ],
            returns=('bytea',),
            volatility='immutable',
            text=self.text)


class JSONIndexByTextFunction(trampoline.VersionedFunction):
    """Get a JSON element by text index or raise an exception."""
    text = r'''
        SELECT
            CASE jsonb_typeof(val)
            WHEN 'object' THEN (
                edgedb_VER.raise_on_null(
                    val -> index,
                    'invalid_parameter_value',
                    msg => (
                        'JSON index ' || quote_literal(index)
                        || ' is out of bounds'
                    ),
                    detail => detail
                )
            )
            WHEN 'array' THEN (
                edgedb_VER.raise(
                    NULL::jsonb,
                    'wrong_object_type',
                    msg => (
                        'cannot index JSON ' || jsonb_typeof(val)
                        || ' by ' || pg_typeof(index)::text
                    ),
                    detail => detail
                )
            )
            ELSE
                edgedb_VER.raise(
                    NULL::jsonb,
                    'wrong_object_type',
                    msg => (
                        'cannot index JSON '
                        || coalesce(jsonb_typeof(val), 'UNKNOWN')
                    ),
                    detail => (
                        '{"hint":"Retrieving an element by a string index '
                        || 'is only available for JSON objects."}'
                    )
                )
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_index'),
            args=[
                ('val', ('jsonb',)),
                ('index', ('text',)),
                ('detail', ('text',), "''"),
            ],
            returns=('jsonb',),
            # Min volatility of exception helpers 'stable',
            # but for all practical purposes, we can assume 'immutable'
            volatility='immutable',
            strict=True,
            text=self.text,
        )


class JSONIndexByIntFunction(trampoline.VersionedFunction):
    """Get a JSON element by int index or raise an exception."""

    text = r'''
        SELECT
            CASE jsonb_typeof(val)
            WHEN 'object' THEN (
                edgedb_VER.raise(
                    NULL::jsonb,
                    'wrong_object_type',
                    msg => (
                        'cannot index JSON ' || jsonb_typeof(val)
                        || ' by ' || pg_typeof(index)::text
                    ),
                    detail => detail
                )
            )
            WHEN 'array' THEN (
                edgedb_VER.raise_on_null(
                    val -> edgedb_VER._int_or_null(index),
                    'invalid_parameter_value',
                    msg => 'JSON index ' || index::text || ' is out of bounds',
                    detail => detail
                )
            )
            WHEN 'string' THEN (
                to_jsonb(edgedb_VER._index(
                    val#>>'{}',
                    index,
                    detail,
                    'JSON'
                ))
            )
            ELSE
                edgedb_VER.raise(
                    NULL::jsonb,
                    'wrong_object_type',
                    msg => (
                        'cannot index JSON '
                        || coalesce(jsonb_typeof(val), 'UNKNOWN')
                    ),
                    detail => (
                        '{"hint":"Retrieving an element by an integer index '
                        || 'is only available for JSON arrays and strings."}'
                    )
                )
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_index'),
            args=[
                ('val', ('jsonb',)),
                ('index', ('bigint',)),
                ('detail', ('text',), "''"),
            ],
            returns=('jsonb',),
            # Min volatility of exception helpers and pg_typeof is 'stable',
            # but for all practical purposes, we can assume 'immutable'
            volatility='immutable',
            strict=True,
            text=self.text,
        )


class JSONSliceFunction(trampoline.VersionedFunction):
    """Get a JSON array slice."""

    text = r"""
        SELECT
            CASE
            WHEN val IS NULL THEN NULL
            WHEN jsonb_typeof(val) = 'array' THEN (
                to_jsonb(edgedb_VER._slice(
                    (
                        SELECT coalesce(array_agg(value), '{}'::jsonb[])
                        FROM jsonb_array_elements(val)
                    ),
                    start, stop
                ))
            )
            WHEN jsonb_typeof(val) = 'string' THEN (
                to_jsonb(edgedb_VER._slice(val#>>'{}', start, stop))
            )
            ELSE
                edgedb_VER.raise(
                    NULL::jsonb,
                    'wrong_object_type',
                    msg => (
                        'cannot slice JSON '
                        || coalesce(jsonb_typeof(val), 'UNKNOWN')
                    ),
                    detail => (
                        '{"hint":"Slicing is only available for JSON arrays'
                        || ' and strings."}'
                    )
                )
            END
    """

    def __init__(self) -> None:
        super().__init__(
            name=("edgedb", "_slice"),
            args=[
                ("val", ("jsonb",)),
                ("start", ("bigint",)),
                ("stop", ("bigint",)),
            ],
            returns=("jsonb",),
            # Min volatility of to_jsonb is 'stable',
            # but for all practical purposes, we can assume 'immutable'
            volatility="immutable",
            text=self.text,
        )


# We need custom casting functions for various datetime scalars in
# order to enforce correctness w.r.t. local vs time-zone-aware
# datetime. Postgres does a lot of magic and guessing for time zones
# and generally will accept text with or without time zone for any
# particular flavor of timestamp. In order to guarantee that we can
# detect time-zones we restrict the inputs to ISO8601 format.
#
# See issue #740.
class DatetimeInFunction(trampoline.VersionedFunction):
    """Cast text into timestamptz using ISO8601 spec."""
    text = r'''
        SELECT
            CASE WHEN val !~ (
                    '^\s*(' ||
                        '(\d{4}-\d{2}-\d{2}|\d{8})' ||
                        '[ tT]' ||
                        '(\d{2}(:\d{2}(:\d{2}(\.\d+)?)?)?|\d{2,6}(\.\d+)?)' ||
                        '([zZ]|[-+](\d{2,4}|\d{2}:\d{2}))' ||
                    ')\s*$'
                )
            THEN
                edgedb_VER.raise(
                    NULL::edgedbt.timestamptz_t,
                    'invalid_datetime_format',
                    msg => (
                        'invalid input syntax for type timestamptz: '
                        || quote_literal(val)
                    ),
                    detail => (
                        '{"hint":"Please use ISO8601 format. Example: '
                        || '2010-12-27T23:59:59-07:00. Alternatively '
                        || '\"to_datetime\" function provides custom '
                        || 'formatting options."}'
                    )
                )
            ELSE
                val::edgedbt.timestamptz_t
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'datetime_in'),
            args=[('val', ('text',))],
            returns=('edgedbt', 'timestamptz_t'),
            # Same volatility as raise() (stable)
            volatility='stable',
            text=self.text)


class DurationInFunction(trampoline.VersionedFunction):
    """Cast text into duration, ensuring there is no days or months units"""
    text = r'''
        SELECT
            CASE WHEN
                EXTRACT(MONTH FROM v.column1) != 0 OR
                EXTRACT(YEAR FROM v.column1) != 0 OR
                EXTRACT(DAY FROM v.column1) != 0
            THEN
                edgedb_VER.raise(
                    NULL::edgedbt.duration_t,
                    'invalid_datetime_format',
                    msg => (
                        'invalid input syntax for type std::duration: '
                        || quote_literal(val)
                    ),
                    detail => (
                        '{"hint":"Day, month and year units cannot be used '
                        || 'for std::duration."}'
                    )
                )
            ELSE v.column1::edgedbt.duration_t
            END
        FROM
            (VALUES (
                val::interval
            )) AS v
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'duration_in'),
            args=[('val', ('text',))],
            returns=('edgedbt', 'duration_t'),
            volatility='immutable',
            text=self.text,
        )


class DateDurationInFunction(trampoline.VersionedFunction):
    """
    Cast text into date_duration, ensuring there is no unit smaller
    than days.
    """
    text = r'''
        SELECT
            CASE WHEN
                EXTRACT(HOUR FROM v.column1) != 0 OR
                EXTRACT(MINUTE FROM v.column1) != 0 OR
                EXTRACT(SECOND FROM v.column1) != 0
            THEN
                edgedb_VER.raise(
                    NULL::edgedbt.date_duration_t,
                    'invalid_datetime_format',
                    msg => (
                        'invalid input syntax for type '
                        || 'std::cal::date_duration: '
                        || quote_literal(val)
                    ),
                    detail => (
                        '{"hint":"Units smaller than days cannot be used '
                        || 'for std::cal::date_duration."}'
                    )
                )
            ELSE v.column1::edgedbt.date_duration_t
            END
        FROM
            (VALUES (
                val::interval
            )) AS v
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'date_duration_in'),
            args=[('val', ('text',))],
            returns=('edgedbt', 'date_duration_t'),
            volatility='immutable',
            text=self.text,
        )


class LocalDatetimeInFunction(trampoline.VersionedFunction):
    """Cast text into timestamp using ISO8601 spec."""
    text = r'''
        SELECT
            CASE WHEN
                val !~ (
                    '^\s*(' ||
                        '(\d{4}-\d{2}-\d{2}|\d{8})' ||
                        '[ tT]' ||
                        '(\d{2}(:\d{2}(:\d{2}(\.\d+)?)?)?|\d{2,6}(\.\d+)?)' ||
                    ')\s*$'
                )
            THEN
                edgedb_VER.raise(
                    NULL::edgedbt.timestamp_t,
                    'invalid_datetime_format',
                    msg => (
                        'invalid input syntax for type timestamp: '
                        || quote_literal(val)
                    ),
                    detail => (
                        '{"hint":"Please use ISO8601 format. Example '
                        || '2010-04-18T09:27:00 Alternatively '
                        || '\"to_local_datetime\" function provides custom '
                        || 'formatting options."}'
                    )
                )
            ELSE
                val::edgedbt.timestamp_t
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'local_datetime_in'),
            args=[('val', ('text',))],
            returns=('edgedbt', 'timestamp_t'),
            volatility='immutable',
            text=self.text)


class LocalDateInFunction(trampoline.VersionedFunction):
    """Cast text into date using ISO8601 spec."""
    text = r'''
        SELECT
            CASE WHEN
                val !~ (
                    '^\s*(' ||
                        '(\d{4}-\d{2}-\d{2}|\d{8})' ||
                    ')\s*$'
                )
            THEN
                edgedb_VER.raise(
                    NULL::edgedbt.date_t,
                    'invalid_datetime_format',
                    msg => (
                        'invalid input syntax for type date: '
                        || quote_literal(val)
                    ),
                    detail => (
                        '{"hint":"Please use ISO8601 format. Example '
                        || '2010-04-18 Alternatively '
                        || '\"to_local_date\" function provides custom '
                        || 'formatting options."}'
                    )
                )
            ELSE
                val::edgedbt.date_t
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'local_date_in'),
            args=[('val', ('text',))],
            returns=('edgedbt', 'date_t'),
            volatility='immutable',
            text=self.text)


class LocalTimeInFunction(trampoline.VersionedFunction):
    """Cast text into time using ISO8601 spec."""
    text = r'''
        SELECT
            CASE WHEN date_part('hour', x.t) = 24
            THEN
                edgedb_VER.raise(
                    NULL::time,
                    'invalid_datetime_format',
                    msg => (
                        'std::cal::local_time field value out of range: '
                        || quote_literal(val)
                    )
                )
            ELSE
                x.t
            END
        FROM (
            SELECT
                CASE WHEN val !~ ('^\s*(' ||
                        '(\d{2}(:\d{2}(:\d{2}(\.\d+)?)?)?|\d{2,6}(\.\d+)?)' ||
                    ')\s*$')
                THEN
                    edgedb_VER.raise(
                        NULL::time,
                        'invalid_datetime_format',
                        msg => (
                            'invalid input syntax for type time: '
                            || quote_literal(val)
                        ),
                        detail => (
                            '{"hint":"Please use ISO8601 format. Examples: '
                            || '18:43:27 or 18:43 Alternatively '
                            || '\"to_local_time\" function provides custom '
                            || 'formatting options."}'
                        )
                    )
                ELSE
                    val::time
                END as t
        ) as x;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'local_time_in'),
            args=[('val', ('text',))],
            returns=('time',),
            volatility='immutable',
            text=self.text,
        )


class ToTimestampTZCheck(trampoline.VersionedFunction):
    """Checks if the original text has time zone or not."""
    # What are we trying to mitigate?
    # We're trying to detect that when we're casting to datetime the
    # time zone is in fact present in the input. It is a problem if
    # it's not since then one gets assigned implicitly based on the
    # server settings.
    #
    # It is insufficient to rely on the presence of TZH in the format
    # string, since `to_timestamp` will happily ignore the missing
    # time-zone in the input anyway. So in order to tell whether the
    # input string contained a time zone that was in fact parsed we
    # employ the following trick:
    #
    # If the time zone is in the input then it is unambiguous and the
    # parsed value will not depend on the current server time zone.
    # However, if the time zone was omitted, then the parsed value
    # will default to the server time zone. This implies that if
    # changing the server time zone for the same input string affects
    # the parsed value, the input string itself didn't contain a time
    # zone.
    text = r'''
        DECLARE
            result timestamptz;
            chk timestamptz;
            msg text;
        BEGIN
            result := to_timestamp(val, fmt);
            PERFORM set_config('TimeZone', 'America/Toronto', true);
            chk := to_timestamp(val, fmt);
            -- We're deliberately not doing any save/restore because
            -- the server MUST be in UTC. In fact, this check relies
            -- on it.
            PERFORM set_config('TimeZone', 'UTC', true);

            IF hastz THEN
                msg := 'missing required';
            ELSE
                msg := 'unexpected';
            END IF;

            IF (result = chk) != hastz THEN
                RAISE EXCEPTION USING
                    ERRCODE = 'invalid_datetime_format',
                    MESSAGE = msg || ' time zone in input ' ||
                        quote_literal(val),
                    DETAIL = '';
            END IF;

            RETURN result::edgedbt.timestamptz_t;
        END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_to_timestamptz_check'),
            args=[('val', ('text',)), ('fmt', ('text',)),
                  ('hastz', ('bool',))],
            returns=('edgedbt', 'timestamptz_t'),
            # We're relying on changing settings, so it's volatile.
            volatility='volatile',
            language='plpgsql',
            text=self.text)


class ToDatetimeFunction(trampoline.VersionedFunction):
    """Convert text into timestamptz using a formatting spec."""
    # NOTE that if only the TZM (minutes) are mentioned it is not
    # enough for a valid time zone definition
    text = r'''
        SELECT
            CASE WHEN fmt !~ (
                    '^(' ||
                        '("([^"\\]|\\.)*")|' ||
                        '([^"]+)' ||
                    ')*(TZH).*$'
                )
            THEN
                edgedb_VER.raise(
                    NULL::edgedbt.timestamptz_t,
                    'invalid_datetime_format',
                    msg => (
                        'missing required time zone in format: '
                        || quote_literal(fmt)
                    ),
                    detail => (
                        $h${"hint":"Use one or both of the following: $h$
                        || $h$'TZH', 'TZM'"}$h$
                    )
                )
            ELSE
                edgedb_VER._to_timestamptz_check(val, fmt, true)
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'to_datetime'),
            args=[('val', ('text',)), ('fmt', ('text',))],
            returns=('edgedbt', 'timestamptz_t'),
            # Same as _to_timestamptz_check.
            volatility='volatile',
            text=self.text)


class ToLocalDatetimeFunction(trampoline.VersionedFunction):
    """Convert text into timestamp using a formatting spec."""
    # NOTE time zone should not be mentioned at all.
    text = r'''
        SELECT
            CASE WHEN fmt ~ (
                    '^(' ||
                        '("([^"\\]|\\.)*")|' ||
                        '([^"]+)' ||
                    ')*(TZH|TZM).*$'
                )
            THEN
                edgedb_VER.raise(
                    NULL::edgedbt.timestamp_t,
                    'invalid_datetime_format',
                    msg => (
                        'unexpected time zone in format: '
                        || quote_literal(fmt)
                    )
                )
            ELSE
                edgedb_VER._to_timestamptz_check(val, fmt, false)
                    ::edgedbt.timestamp_t
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'to_local_datetime'),
            args=[('val', ('text',)), ('fmt', ('text',))],
            returns=('edgedbt', 'timestamp_t'),
            # Same as _to_timestamptz_check.
            volatility='volatile',
            text=self.text)


class StrToBool(trampoline.VersionedFunction):
    """Parse bool from text."""
    # We first try to match case-insensitive "true|false" at all. On
    # null, we raise an exception. But otherwise we know that we have
    # an array of matches. The first element matching "true" and
    # second - "false". So the boolean value is then "true" if the
    # second array element is NULL and false otherwise.
    text = r'''
        SELECT (
            coalesce(
                regexp_match(val, '^\s*(?:(true)|(false))\s*$', 'i')::text[],
                edgedb_VER.raise(
                    NULL::text[],
                    'invalid_text_representation',
                    msg => 'invalid input syntax for type bool: '
                           || quote_literal(val)
                )
            )
        )[2] IS NULL;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'str_to_bool'),
            args=[('val', ('text',))],
            returns=('bool',),
            strict=True,
            # Stable because it's raising exceptions.
            volatility='stable',
            text=self.text)


class QuoteLiteralFunction(trampoline.VersionedFunction):
    """Encode string as edgeql literal quoted string"""
    text = r'''
        SELECT concat('\'',
            replace(
                replace(val, '\\', '\\\\'),
                '\'', '\\\''),
            '\'')
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'quote_literal'),
            args=[('val', ('text',))],
            returns=('str',),
            volatility='immutable',
            text=self.text)


class QuoteIdentFunction(trampoline.VersionedFunction):
    """Quote ident function."""
    # TODO do not quote valid identifiers unless they are reserved
    text = r'''
        SELECT concat('`', replace(val, '`', '``'), '`')
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'quote_ident'),
            args=[('val', ('text',))],
            returns=('text',),
            volatility='immutable',
            text=self.text,
        )


class QuoteNameFunction(trampoline.VersionedFunction):

    text = r"""
        SELECT
            string_agg(edgedb_VER.quote_ident(np), '::')
        FROM
            unnest(string_to_array("name", '::')) AS np
    """

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'quote_name'),
            args=[('name', ('text',))],
            returns=('text',),
            volatility='immutable',
            text=self.text,
        )


class DescribeRolesAsDDLFunctionForwardDecl(trampoline.VersionedFunction):
    """Forward declaration for _describe_roles_as_ddl"""

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_describe_roles_as_ddl'),
            args=[],
            returns=('text'),
            # Stable because it's raising exceptions.
            volatility='stable',
            text='SELECT NULL::text',
        )


class DescribeRolesAsDDLFunction(trampoline.VersionedFunction):
    """Describe roles as DDL"""

    def __init__(self, schema: s_schema.Schema) -> None:
        role_obj = schema.get("sys::Role", type=s_objtypes.ObjectType)
        roles = _schema_alias_view_name(schema, role_obj)
        roles = (common.maybe_versioned_schema(roles[0]), roles[1])

        member_of = role_obj.getptr(schema, s_name.UnqualName('member_of'))
        members = _schema_alias_view_name(schema, member_of)
        members = (common.maybe_versioned_schema(members[0]), members[1])

        name_col = ptr_col_name(schema, role_obj, 'name')
        pass_col = ptr_col_name(schema, role_obj, 'password')
        qi_superuser = qlquote.quote_ident(defines.EDGEDB_SUPERUSER)
        text = f"""
            WITH RECURSIVE
            dependencies AS (
                SELECT r.id AS id, m.target AS parent
                    FROM {q(*roles)} r
                        LEFT OUTER JOIN {q(*members)} m ON r.id = m.source
            ),
            roles_with_depths(id, depth) AS (
                SELECT id, 0 FROM dependencies WHERE parent IS NULL
                UNION ALL
                SELECT dependencies.id, roles_with_depths.depth + 1
                FROM dependencies
                INNER JOIN roles_with_depths
                    ON dependencies.parent = roles_with_depths.id
            ),
            ordered_roles AS (
                SELECT id, max(depth) FROM roles_with_depths
                GROUP BY id
                ORDER BY max(depth) ASC
            )
            SELECT
            coalesce(string_agg(
                CASE WHEN
                    role.{qi(name_col)} = {ql(defines.EDGEDB_SUPERUSER)} THEN
                    NULLIF(concat(
                        'ALTER ROLE {qi_superuser} {{',
                        NULLIF((SELECT
                            concat(
                                ' EXTENDING ',
                                string_agg(
                                    edgedb_VER.quote_ident(parent.{qi(name_col)}),
                                    ', '
                                ),
                                ';'
                            )
                            FROM {q(*members)} member
                                INNER JOIN {q(*roles)} parent
                                ON parent.id = member.target
                            WHERE member.source = role.id
                        ), ' EXTENDING ;'),
                        CASE WHEN role.{qi(pass_col)} IS NOT NULL THEN
                            concat(' SET password_hash := ',
                                   quote_literal(role.{qi(pass_col)}),
                                   ';')
                        ELSE '' END,
                        '}};'
                    ), 'ALTER ROLE {qi_superuser} {{}};')
                ELSE
                    concat(
                        'CREATE SUPERUSER ROLE ',
                        edgedb_VER.quote_ident(role.{qi(name_col)}),
                        NULLIF((SELECT
                            concat(' EXTENDING ',
                                string_agg(
                                    edgedb_VER.quote_ident(parent.{qi(name_col)}),
                                    ', '
                                )
                            )
                            FROM {q(*members)} member
                                INNER JOIN {q(*roles)} parent
                                ON parent.id = member.target
                            WHERE member.source = role.id
                        ), ' EXTENDING '),
                        CASE WHEN role.{qi(pass_col)} IS NOT NULL THEN
                            concat(' {{ SET password_hash := ',
                                   quote_literal(role.{qi(pass_col)}),
                                   '}};')
                        ELSE ';' END
                    )
                END,
                '\n'
            ), '') str
            FROM ordered_roles
                JOIN {q(*roles)} role
                ON role.id = ordered_roles.id
        """

        super().__init__(
            name=('edgedb', '_describe_roles_as_ddl'),
            args=[],
            returns=('text'),
            # Stable because it's raising exceptions.
            volatility='stable',
            text=text)


class DumpSequencesFunction(trampoline.VersionedFunction):

    text = r"""
        SELECT
            string_agg(
                'SELECT std::sequence_reset('
                || 'INTROSPECT ' || edgedb_VER.quote_name(seq.name)
                || (CASE WHEN seq_st.is_called
                    THEN ', ' || seq_st.last_value::text
                    ELSE '' END)
                || ');',
                E'\n'
            )
        FROM
            (SELECT
                id,
                name
             FROM
                edgedb_VER."_SchemaScalarType"
             WHERE
                id = any("seqs")
            ) AS seq,
            LATERAL (
                SELECT
                    COALESCE(last_value, start_value)::text AS last_value,
                    last_value IS NOT NULL AS is_called
                FROM
                    pg_sequences,
                    LATERAL ROWS FROM (
                        edgedb_VER.get_sequence_backend_name(seq.id)
                    ) AS seq_name(schema text, name text)
                WHERE
                    (pg_sequences.schemaname, pg_sequences.sequencename)
                    = (seq_name.schema, seq_name.name)
            ) AS seq_st
    """

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_dump_sequences'),
            args=[('seqs', ('uuid[]',))],
            returns=('text',),
            # Volatile because sequence state is volatile
            volatility='volatile',
            text=self.text,
        )


class SysConfigSourceType(dbops.Enum):
    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_sys_config_source_t'),
            values=[
                'default',
                'postgres default',
                'postgres environment variable',
                'postgres configuration file',
                'environment variable',
                'command line',
                'postgres command line',
                'postgres global',
                'postgres client',
                'system override',
                'database',
                'postgres override',
                'postgres interactive',
                'postgres test',
                'session',
            ]
        )


class SysConfigScopeType(dbops.Enum):
    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_sys_config_scope_t'),
            values=[
                'INSTANCE',
                'DATABASE',
                'SESSION',
            ]
        )


class SysConfigValueType(dbops.CompositeType):
    """Type of values returned by _read_sys_config."""
    def __init__(self) -> None:
        super().__init__(name=('edgedb', '_sys_config_val_t'))

        self.add_columns([
            dbops.Column(name='name', type='text'),
            dbops.Column(name='value', type='jsonb'),
            dbops.Column(name='source', type='edgedb._sys_config_source_t'),
            dbops.Column(name='scope', type='edgedb._sys_config_scope_t'),
        ])


class SysConfigEntryType(dbops.CompositeType):
    """Type of values returned by _read_sys_config_full."""
    def __init__(self) -> None:
        super().__init__(name=('edgedb', '_sys_config_entry_t'))

        self.add_columns([
            dbops.Column(name='max_source', type='edgedb._sys_config_source_t'),
            dbops.Column(name='value', type='edgedb._sys_config_val_t'),
        ])


class IntervalToMillisecondsFunction(trampoline.VersionedFunction):
    """Cast an interval into milliseconds."""

    text = r'''
        SELECT
            trunc(extract(hours from "val"))::numeric * 3600000 +
            trunc(extract(minutes from "val"))::numeric * 60000 +
            trunc(extract(milliseconds from "val"))::numeric
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_interval_to_ms'),
            args=[('val', ('interval',))],
            returns=('numeric',),
            volatility='immutable',
            text=self.text,
        )


class SafeIntervalCastFunction(trampoline.VersionedFunction):
    """A safer text to interval casting implementaion.

    Casting large-unit durations (like '4032000000us') results in an error.
    Huge durations like this can be returned when introspecting current
    database config. Fix that by parsing the argument and using multiplication.
    """

    text = r'''
        SELECT
            CASE

                WHEN m.v[1] IS NOT NULL AND m.v[2] IS NOT NULL
                THEN
                    m.v[1]::numeric * ('1' || m.v[2])::interval

                ELSE
                    "val"::interval
            END
        FROM LATERAL (
            SELECT regexp_match(
                "val", '^(\d+)\s*(us|ms|s|min|h)$') AS v
        ) AS m
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_interval_safe_cast'),
            args=[('val', ('text',))],
            returns=('interval',),
            volatility='immutable',
            text=self.text,
        )


class ConvertPostgresConfigUnitsFunction(trampoline.VersionedFunction):
    """Convert duration/memory values to milliseconds/kilobytes.

    See https://www.postgresql.org/docs/12/config-setting.html
    for information about the units Postgres config system has.
    """

    text = r"""
    SELECT (
        CASE
            WHEN "unit" = any(ARRAY['us', 'ms', 's', 'min', 'h'])
            THEN to_jsonb(
                edgedb_VER._interval_safe_cast(
                    ("value" * "multiplier")::text || "unit"
                )
            )

            WHEN "unit" = 'B'
            THEN to_jsonb(
                ("value" * "multiplier")::text || 'B'
            )

            WHEN "unit" = 'kB'
            THEN to_jsonb(
                ("value" * "multiplier")::text || 'KiB'
            )

            WHEN "unit" = 'MB'
            THEN to_jsonb(
                ("value" * "multiplier")::text || 'MiB'
            )

            WHEN "unit" = 'GB'
            THEN to_jsonb(
                ("value" * "multiplier")::text || 'GiB'
            )

            WHEN "unit" = 'TB'
            THEN to_jsonb(
                ("value" * "multiplier")::text || 'TiB'
            )

            WHEN "unit" = ''
            THEN ("value" * "multiplier")::text::jsonb

            ELSE edgedb_VER.raise(
                NULL::jsonb,
                msg => (
                    'unknown configuration unit "' ||
                    COALESCE("unit", '<NULL>') ||
                    '"'
                )
            )
        END
    )
    """

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_convert_postgres_config_units'),
            args=[
                ('value', ('numeric',)),
                ('multiplier', ('numeric',)),
                ('unit', ('text',))
            ],
            returns=('jsonb',),
            volatility='immutable',
            text=self.text,
        )


class TypeIDToConfigType(trampoline.VersionedFunction):
    """Get a postgres config type from a type id.

    (We typically try to read extension configs straight from the
    config tables, but for extension configs those aren't present.)
    """

    config_types = {
        'bool': ['std::bool'],
        'string': ['std::str'],
        'integer': ['std::int16', 'std::int32', 'std::int64'],
        'real': ['std::float32', 'std::float64'],
    }
    cases = [
        f'''
        WHEN "typeid" = '{s_obj.get_known_type_id(t)}' THEN '{ct}'
        '''
        for ct, types in config_types.items()
        for t in types
    ]
    scases = '\n'.join(cases)

    text = f"""
    SELECT (
        CASE
            {scases}
            ELSE edgedb_VER.raise(
                NULL::text,
                msg => (
                    'unknown configuration type "' || "typeid" || '"'
                )
            )
        END
    )
    """

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_type_id_to_config_type'),
            args=[
                ('typeid', ('uuid',)),
            ],
            returns=('text',),
            volatility='immutable',
            text=self.text,
        )


class NormalizedPgSettingsView(trampoline.VersionedView):
    """Just like `pg_settings` but with the parsed 'unit' column."""

    query = r'''
        SELECT
            s.name AS name,
            s.setting AS setting,
            s.vartype AS vartype,
            s.source AS source,
            unit.multiplier AS multiplier,
            unit.unit AS unit

        FROM pg_settings AS s,

        LATERAL (
            SELECT regexp_match(
                s.unit, '^(\d*)\s*([a-zA-Z]{1,3})$') AS v
        ) AS _unit,

        LATERAL (
            SELECT
                COALESCE(
                    CASE
                        WHEN _unit.v[1] = '' THEN 1
                        ELSE _unit.v[1]::int
                    END,
                    1
                ) AS multiplier,
                COALESCE(_unit.v[2], '') AS unit
        ) AS unit
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_normalized_pg_settings'),
            query=self.query,
        )


class InterpretConfigValueToJsonFunction(trampoline.VersionedFunction):
    """Convert a Postgres config value to jsonb.

    This function:

    * converts booleans to JSON true/false;
    * converts enums and strings to JSON strings;
    * converts real/integers to JSON numbers:
      - for durations: we always convert to milliseconds;
      - for memory size: we always convert to kilobytes;
      - already unitless numbers are left as is.

    See https://www.postgresql.org/docs/12/config-setting.html
    for information about the units Postgres config system has.
    """

    text = r"""
    SELECT (
        CASE
            WHEN "type" = 'bool'
            THEN (
                CASE
                WHEN lower("value") = any(ARRAY['on', 'true', 'yes', '1'])
                THEN 'true'
                ELSE 'false'
                END
            )::jsonb

            WHEN "type" = 'enum' OR "type" = 'string'
            THEN to_jsonb("value")

            WHEN "type" = 'integer' OR "type" = 'real'
            THEN edgedb_VER._convert_postgres_config_units(
                    "value"::numeric, "multiplier"::numeric, "unit"
                 )

            ELSE
                edgedb_VER.raise(
                    NULL::jsonb,
                    msg => (
                        'unknown configuration type "' ||
                        COALESCE("type", '<NULL>') ||
                        '"'
                    )
                )
        END
    )
    """

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_interpret_config_value_to_json'),
            args=[
                ('value', ('text',)),
                ('type', ('text',)),
                ('multiplier', ('int',)),
                ('unit', ('text',))
            ],
            returns=('jsonb',),
            volatility='immutable',
            text=self.text,
        )


class PostgresConfigValueToJsonFunction(trampoline.VersionedFunction):
    """Convert a Postgres setting to JSON value.

    Steps:

    * Lookup the `setting_name` in pg_settings to determine its
      type and unit.

    * Parse `setting_value` to see if it starts with numbers and ends
      with what looks like a unit.

    * Fetch the unit/multiplier pg_settings (well, from our view over it).

    * If `setting_value` has a unit, pass it to
      `_interpret_config_value_to_json`

    * If `setting_value` doesn't have a unit, pass it to
      `_interpret_config_value_to_json` along with the base unit/multiplier
      from pg_settings.

    * Then, the `_interpret_config_value_to_json` is capable of casting the
      value correctly based on the pg_settings type and the supplied
      unit/multiplier.
    """

    text = r"""
        SELECT
            (CASE

                WHEN parsed_value.unit != ''
                THEN
                    edgedb_VER._interpret_config_value_to_json(
                        parsed_value.val,
                        settings.vartype,
                        1,
                        parsed_value.unit
                    )

                ELSE
                    edgedb_VER._interpret_config_value_to_json(
                        "setting_value",
                        settings.vartype,
                        settings.multiplier,
                        settings.unit
                    )

            END)
        FROM
            LATERAL (
                SELECT regexp_match(
                    "setting_value", '^(\d+)\s*([a-zA-Z]{0,3})$') AS v
            ) AS _unit,

            LATERAL (
                SELECT
                    COALESCE(_unit.v[1], "setting_value") AS val,
                    COALESCE(_unit.v[2], '') AS unit
            ) AS parsed_value
        LEFT OUTER JOIN
            (
                SELECT
                    epg_settings.vartype AS vartype,
                    epg_settings.multiplier AS multiplier,
                    epg_settings.unit AS unit
                FROM
                    edgedb_VER._normalized_pg_settings AS epg_settings
                WHERE
                    epg_settings.name = "setting_name"
            ) AS settings_in ON true
        CROSS JOIN LATERAL
            (
                SELECT
                    COALESCE(settings_in.vartype,
                             edgedb_VER._type_id_to_config_type("setting_typeid"))
                    as vartype,
                    COALESCE(settings_in.multiplier, '1') as multiplier,
                    COALESCE(settings_in.unit, '') as unit
            ) as settings

    """

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_postgres_config_value_to_json'),
            args=[
                ('setting_name', ('text',)),
                ('setting_typeid', ('uuid',)),
                ('setting_value', ('text',)),
            ],
            returns=('jsonb',),
            volatility='volatile',
            text=self.text,
        )


class SysConfigFullFunction(trampoline.VersionedFunction):

    # This is a function because "_edgecon_state" is a temporary table
    # and therefore cannot be used in a view.

    text = f'''
    DECLARE
        query text;
    BEGIN

    query := $$
        WITH

        config_spec AS (
            SELECT
                s.key AS name,
                s.value->'default' AS default,
                (s.value->>'internal')::bool AS internal,
                (s.value->>'system')::bool AS system,
                (s.value->>'typeid')::uuid AS typeid,
                (s.value->>'typemod') AS typemod,
                (s.value->>'backend_setting') AS backend_setting
            FROM
                edgedbinstdata_VER.instdata as id,
            LATERAL jsonb_each(id.json) AS s
            WHERE id.key LIKE 'configspec%'
        ),

        config_defaults AS (
            SELECT
                s.name AS name,
                s.default AS value,
                'default' AS source,
                s.backend_setting IS NOT NULL AS is_backend
            FROM
                config_spec s
        ),
        config_extension_defaults AS (
            SELECT * FROM config_defaults WHERE name like '%::%'
        ),

        config_static AS (
            SELECT
                s.name AS name,
                s.value AS value,
                (CASE
                    WHEN s.type = 'A' THEN 'command line'
                    -- Due to inplace upgrade limits, without adding a new
                    -- layer, configuration file values are manually squashed
                    -- into the `environment variables` layer, see below.
                    ELSE 'environment variable'
                END) AS source,
                config_spec.backend_setting IS NOT NULL AS is_backend
            FROM
                _edgecon_state s
                INNER JOIN config_spec ON (config_spec.name = s.name)
            WHERE
                -- Give precedence to configuration file values over
                -- environment variables manually.
                s.type = 'A' OR s.type = 'F' OR (
                    s.type = 'E' AND NOT EXISTS (
                        SELECT 1 FROM _edgecon_state ss
                        WHERE ss.name = s.name AND ss.type = 'F'
                    )
                )
        ),

        config_sys AS (
            SELECT
                s.key AS name,
                s.value AS value,
                'system override' AS source,
                config_spec.backend_setting IS NOT NULL AS is_backend
            FROM
                jsonb_each(
                    edgedb_VER.get_database_metadata(
                        {ql(defines.EDGEDB_SYSTEM_DB)}
                    ) -> 'sysconfig'
                ) AS s
                INNER JOIN config_spec ON (config_spec.name = s.key)
        ),

        config_db AS (
            SELECT
                s.name AS name,
                s.value AS value,
                'database' AS source,
                config_spec.backend_setting IS NOT NULL AS is_backend
            FROM
                edgedb._db_config s
                INNER JOIN config_spec ON (config_spec.name = s.name)
        ),

        config_sess AS (
            SELECT
                s.name AS name,
                s.value AS value,
                'session' AS source,
                FALSE AS is_backend  -- only 'B' is for backend settings
            FROM
                _edgecon_state s
            WHERE
                s.type = 'C'
        ),

        pg_db_setting AS (
            SELECT
                spec.name,
                edgedb_VER._postgres_config_value_to_json(
                    spec.backend_setting, spec.typeid, nameval.value
                ) AS value,
                'database' AS source,
                TRUE AS is_backend
            FROM
                (SELECT
                    setconfig
                FROM
                    pg_db_role_setting
                WHERE
                    setdatabase = (
                        SELECT oid
                        FROM pg_database
                        WHERE datname = current_database()
                    )
                    AND setrole = 0
                ) AS cfg_array,
                LATERAL unnest(cfg_array.setconfig) AS cfg_set(s),
                LATERAL (
                    SELECT
                        split_part(cfg_set.s, '=', 1) AS name,
                        split_part(cfg_set.s, '=', 2) AS value
                ) AS nameval,
                LATERAL (
                    SELECT
                        config_spec.name,
                        config_spec.backend_setting,
                        config_spec.typeid
                    FROM
                        config_spec
                    WHERE
                        nameval.name = config_spec.backend_setting
                ) AS spec
        ),
    $$;

    IF fs_access THEN
        query := query || $$
            pg_conf_settings AS (
                SELECT
                    spec.name,
                    edgedb_VER._postgres_config_value_to_json(
                        spec.backend_setting, spec.typeid, setting
                    ) AS value,
                    'postgres configuration file' AS source,
                    TRUE AS is_backend
                FROM
                    pg_file_settings,
                    LATERAL (
                        SELECT
                            config_spec.name,
                            config_spec.backend_setting,
                            config_spec.typeid
                        FROM
                            config_spec
                        WHERE
                            pg_file_settings.name = config_spec.backend_setting
                    ) AS spec
                WHERE
                    sourcefile != ((
                        SELECT setting
                        FROM pg_settings WHERE name = 'data_directory'
                    ) || '/postgresql.auto.conf')
                    AND applied
            ),

            pg_auto_conf_settings AS (
                SELECT
                    spec.name,
                    edgedb_VER._postgres_config_value_to_json(
                        spec.backend_setting, spec.typeid, setting
                    ) AS value,
                    'system override' AS source,
                    TRUE AS is_backend
                FROM
                    pg_file_settings,
                    LATERAL (
                        SELECT
                            config_spec.name,
                            config_spec.backend_setting,
                            config_spec.typeid
                        FROM
                            config_spec
                        WHERE
                            pg_file_settings.name = config_spec.backend_setting
                    ) AS spec
                WHERE
                    sourcefile = ((
                        SELECT setting
                        FROM pg_settings WHERE name = 'data_directory'
                    ) || '/postgresql.auto.conf')
                    AND applied
            ),
        $$;
    END IF;

    query := query || $$
        pg_config AS (
            SELECT
                spec.name,
                edgedb_VER._interpret_config_value_to_json(
                    settings.setting,
                    settings.vartype,
                    settings.multiplier,
                    settings.unit
                ) AS value,
                source AS source,
                TRUE AS is_backend
            FROM
                (
                    SELECT
                        epg_settings.name AS name,
                        epg_settings.unit AS unit,
                        epg_settings.multiplier AS multiplier,
                        epg_settings.vartype AS vartype,
                        epg_settings.setting AS setting,
                        (CASE
                            WHEN epg_settings.source = 'session' THEN
                                epg_settings.source
                            ELSE
                                'postgres ' || epg_settings.source
                        END) AS source
                    FROM
                        edgedb_VER._normalized_pg_settings AS epg_settings
                    WHERE
                        epg_settings.source != 'database'
                ) AS settings,

                LATERAL (
                    SELECT
                        config_spec.name
                    FROM
                        config_spec
                    WHERE
                        settings.name = config_spec.backend_setting
                ) AS spec
            ),

        -- extension session configs don't show up in any system view, so we
        -- check _edgecon_state to see when they are present.
        pg_extension_config AS (
            SELECT
                config_spec.name,
                -- XXX: Or would it be better to just use the json directly?
                edgedb_VER._postgres_config_value_to_json(
                    config_spec.backend_setting,
                    config_spec.typeid,
                    current_setting(config_spec.backend_setting, true)
                ) AS value,
                'session' AS source,
                TRUE AS is_backend
            FROM _edgecon_state s
            INNER JOIN config_spec
            ON s.name = config_spec.name
            WHERE s.type = 'B' AND s.name LIKE '%::%'
        ),

        edge_all_settings AS MATERIALIZED (
            SELECT
                q.*
            FROM
                (
                    SELECT * FROM config_defaults UNION ALL
                    SELECT * FROM config_static UNION ALL
                    SELECT * FROM config_sys UNION ALL
                    SELECT * FROM config_db UNION ALL
                    SELECT * FROM config_sess
                ) AS q
            WHERE
                NOT q.is_backend
        ),

    $$;

    IF fs_access THEN
        query := query || $$
            pg_all_settings AS MATERIALIZED (
                SELECT
                    q.*
                FROM
                    (
                        -- extension defaults aren't in any system views
                        SELECT * FROM config_extension_defaults UNION ALL
                        SELECT * FROM pg_db_setting UNION ALL
                        SELECT * FROM pg_conf_settings UNION ALL
                        SELECT * FROM pg_auto_conf_settings UNION ALL
                        SELECT * FROM pg_config UNION ALL
                        SELECT * FROM pg_extension_config
                    ) AS q
                WHERE
                    q.is_backend
            )
        $$;
    ELSE
        query := query || $$
            pg_all_settings AS MATERIALIZED (
                SELECT
                    q.*
                FROM
                    (
                        -- extension defaults aren't in any system views
                        SELECT * FROM config_extension_defaults UNION ALL
                        -- config_sys is here, because there
                        -- is no other way to read instance-level
                        -- configuration overrides.
                        SELECT * FROM config_sys UNION ALL
                        SELECT * FROM pg_db_setting UNION ALL
                        SELECT * FROM pg_config UNION ALL
                        SELECT * FROM pg_extension_config
                    ) AS q
                WHERE
                    q.is_backend
            )
        $$;
    END IF;

    query := query || $$
        SELECT
            max_source AS max_source,
            (q.name,
            q.value,
            q.source,
            (CASE
                WHEN q.source < 'database'::edgedb._sys_config_source_t THEN
                    'INSTANCE'
                WHEN q.source = 'database'::edgedb._sys_config_source_t THEN
                    'DATABASE'
                ELSE
                    'SESSION'
            END)::edgedb._sys_config_scope_t
            )::edgedb._sys_config_val_t as value
        FROM
            unnest($2) as max_source,
            LATERAL (SELECT
                u.name,
                u.value,
                u.source::edgedb._sys_config_source_t,
                row_number() OVER (
                    PARTITION BY u.name
                    ORDER BY u.source::edgedb._sys_config_source_t DESC
                ) AS n
            FROM
                (SELECT
                    *
                FROM
                    (
                        SELECT * FROM edge_all_settings UNION ALL
                        SELECT * FROM pg_all_settings
                    ) AS q
                WHERE
                    q.value IS NOT NULL
                    AND ($1 IS NULL OR
                        q.source::edgedb._sys_config_source_t = any($1)
                    )
                    AND (max_source IS NULL OR
                        q.source::edgedb._sys_config_source_t <= max_source
                    )
                ) AS u
            ) AS q
        WHERE
            q.n = 1;
    $$;

    RETURN QUERY EXECUTE query USING source_filter, max_sources;
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_read_sys_config_full'),
            args=[
                (
                    'source_filter',
                    ('edgedb', '_sys_config_source_t[]',),
                    'NULL',
                ),
                (
                    'max_sources',
                    ('edgedb', '_sys_config_source_t[]'),
                    'NULL',
                ),
                (
                    'fs_access',
                    ('bool',),
                    'TRUE',
                )
            ],
            returns=('edgedb', '_sys_config_entry_t'),
            set_returning=True,
            language='plpgsql',
            volatility='volatile',
            text=self.text,
        )


class SysConfigUncachedFunction(trampoline.VersionedFunction):

    text = f'''
    DECLARE
        backend_caps bigint;
    BEGIN

    backend_caps := edgedb_VER.get_backend_capabilities();
    IF (backend_caps
        & {int(params.BackendCapabilities.CONFIGFILE_ACCESS)}) != 0
    THEN
        RETURN QUERY
        SELECT *
        FROM edgedb_VER._read_sys_config_full(
            source_filter, max_sources, TRUE);
    ELSE
        RETURN QUERY
        SELECT *
        FROM edgedb_VER._read_sys_config_full(
            source_filter, max_sources, FALSE);
    END IF;

    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_read_sys_config_uncached'),
            args=[
                (
                    'source_filter',
                    ('edgedb', '_sys_config_source_t[]',),
                    'NULL',
                ),
                (
                    'max_sources',
                    ('edgedb', '_sys_config_source_t[]'),
                    'NULL',
                ),
            ],
            returns=('edgedb', '_sys_config_entry_t'),
            set_returning=True,
            language='plpgsql',
            volatility='volatile',
            text=self.text,
        )


class SysConfigFunction(trampoline.VersionedFunction):

    text = f'''
    DECLARE
    BEGIN

    -- Only bother caching the source_filter IS NULL case, since that
    -- is what drives the config views. source_filter is used in
    -- DESCRIBE CONFIG
    IF source_filter IS NOT NULL OR array_position(
     ARRAY[NULL, 'database', 'system override']::edgedb._sys_config_source_t[],
      max_source) IS NULL
     THEN
        RETURN QUERY
        SELECT
          (c.value).name, (c.value).value, (c.value).source, (c.value).scope
        FROM edgedb_VER._read_sys_config_uncached(
          source_filter, ARRAY[max_source]) AS c;
        RETURN;
    END IF;

    IF count(*) = 0 FROM "_config_cache" c
       WHERE source IS NOT DISTINCT FROM max_source
    THEN
        INSERT INTO "_config_cache"
        SELECT (s.max_source), (s.value)
        FROM edgedb_VER._read_sys_config_uncached(
          source_filter, ARRAY[
            NULL, 'database', 'system override']::edgedb._sys_config_source_t[])
             AS s;
    END IF;

    RETURN QUERY
    SELECT (c.value).name, (c.value).value, (c.value).source, (c.value).scope
    FROM "_config_cache" c WHERE source IS NOT DISTINCT FROM max_source;

    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_read_sys_config'),
            args=[
                (
                    'source_filter',
                    ('edgedb', '_sys_config_source_t[]',),
                    'NULL',
                ),
                (
                    'max_source',
                    ('edgedb', '_sys_config_source_t'),
                    'NULL',
                ),
            ],
            returns=('edgedb', '_sys_config_val_t'),
            set_returning=True,
            language='plpgsql',
            volatility='volatile',
            text=self.text,
        )


class SysClearConfigCacheFunction(trampoline.VersionedFunction):

    text = f'''
    DECLARE
    BEGIN

    DELETE FROM "_config_cache" c;
    RETURN true;

    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_clear_sys_config_cache'),
            args=[],
            returns=("boolean"),
            set_returning=False,
            language='plpgsql',
            volatility='volatile',
            text=self.text,
        )


class ResetSessionConfigFunction(trampoline.VersionedFunction):

    text = f'''
        RESET ALL
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_reset_session_config'),
            args=[],
            returns=('void',),
            language='sql',
            volatility='volatile',
            text=self.text,
        )


class ApplySessionConfigFunction(trampoline.VersionedFunction):
    """Apply an Gel config setting to the backend, if possible.

    The function accepts any Gel config name/value pair. If this
    specific config setting happens to be implemented via a backend
    setting, it would be applied to the current PostgreSQL session.
    If the config setting doesn't reflect into a backend setting the
    function is a no-op.

    The function always returns the passed config name, unmodified
    (this simplifies using the function in queries.)
    """

    def __init__(self, config_spec: edbconfig.Spec) -> None:

        backend_settings = {}
        for setting_name in config_spec:
            setting = config_spec[setting_name]

            if setting.backend_setting and not setting.system:
                backend_settings[setting_name] = setting.backend_setting

        variants_list = []
        for setting_name, backend_setting_name in backend_settings.items():
            setting = config_spec[setting_name]

            valql = '"value"->>0'
            if (
                isinstance(setting.type, type)
                and issubclass(setting.type, statypes.Duration)
            ):
                valql = f"""
                    edgedb_VER._interval_to_ms(({valql})::interval)::text \
                    || 'ms'
                """

            variants_list.append(f'''
                WHEN "name" = {ql(setting_name)}
                THEN
                    pg_catalog.set_config(
                        {ql(backend_setting_name)}::text,
                        {valql},
                        false
                    )
            ''')

        ext_config = '''
            SELECT pg_catalog.set_config(
                (s.val->>'backend_setting')::text,
                "value"->>0,
                false
            )
            FROM
                edgedbinstdata_VER.instdata as id,
            LATERAL jsonb_each(id.json) AS s(key, val)
            WHERE id.key = 'configspec_ext' AND s.key = "name"
        '''

        variants = "\n".join(variants_list)
        text = f'''
        SELECT (
            CASE
                WHEN "name" = any(
                    ARRAY[{",".join(ql(str(bs)) for bs in backend_settings)}]
                )
                THEN (
                    CASE
                        WHEN
                            (CASE
                                {variants}
                            END) IS NULL
                        THEN "name"
                        ELSE "name"
                    END
                )

                WHEN "name" LIKE '%::%'
                THEN
                    CASE WHEN ({ext_config}) IS NULL
                    THEN "name"
                    ELSE "name"
                END

                ELSE "name"
            END
        )
        '''

        super().__init__(
            name=('edgedb', '_apply_session_config'),
            args=[
                ('name', ('text',)),
                ('value', ('jsonb',)),
            ],
            returns=('text',),
            language='sql',
            volatility='volatile',
            text=text,
        )


class SysGetTransactionIsolation(trampoline.VersionedFunction):
    "Get transaction isolation value as text compatible with Gel's enum."
    text = r'''
        SELECT
            CASE setting
                WHEN 'repeatable read' THEN 'RepeatableRead'
                WHEN 'serializable' THEN 'Serializable'
                ELSE (
                    SELECT edgedb_VER.raise(
                        NULL::text,
                        msg => (
                            'unknown transaction isolation level "'
                            || setting || '"'
                        )
                    )
                )
            END
        FROM pg_settings
        WHERE name = 'transaction_isolation'
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_get_transaction_isolation'),
            args=[],
            returns=('text',),
            # This function only reads from a table.
            volatility='stable',
            text=self.text)


class GetCachedReflection(trampoline.VersionedFunction):
    "Return a list of existing schema reflection helpers."
    text = '''
        SELECT
            substring(proname, '__rh_#"%#"', '#') AS eql_hash,
            proargnames AS argnames
        FROM
            pg_proc
            INNER JOIN pg_namespace ON (pronamespace = pg_namespace.oid)
        WHERE
            proname LIKE '\\_\\_rh\\_%'
            AND nspname = 'edgedb_VER'
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_get_cached_reflection'),
            args=[],
            returns=('record',),
            set_returning=True,
            # This function only reads from a table.
            volatility='stable',
            text=self.text,
        )


class GetBaseScalarTypeMap(trampoline.VersionedFunction):
    """Return a map of base Gel scalar type ids to Postgres type names."""

    text = "VALUES" + ", ".join(
        f"({ql(str(k))}::uuid, {qtl(v)})"
        for k, v in types.base_type_name_map.items()
    )

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_get_base_scalar_type_map'),
            args=[],
            returns=('record',),
            set_returning=True,
            volatility='immutable',
            text=self.text,
        )


class GetTypeToRangeNameMap(trampoline.VersionedFunction):
    """Return a map of type names to the name of the associated range type"""

    text = f"VALUES" + ", ".join(
        f"({qtl(k)}, {qtl(v)})"
        for k, v in types.type_to_range_name_map.items()
    )

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_get_type_to_range_type_map'),
            args=[],
            returns=('record',),
            set_returning=True,
            volatility='immutable',
            text=self.text,
        )


class GetTypeToMultiRangeNameMap(trampoline.VersionedFunction):
    "Return a map of type names to the name of the associated multirange type"

    text = f"VALUES" + ", ".join(
        f"({qtl(k)}, {qtl(v)})"
        for k, v in types.type_to_multirange_name_map.items()
    )

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_get_type_to_multirange_type_map'),
            args=[],
            returns=('record',),
            set_returning=True,
            volatility='immutable',
            text=self.text,
        )


class GetPgTypeForEdgeDBTypeFunction(trampoline.VersionedFunction):
    """Return Postgres OID representing a given Gel type."""

    text = f'''
        SELECT
            coalesce(
                sql_type::regtype::oid,
                (
                    SELECT
                        tn::regtype::oid
                    FROM
                        edgedb_VER._get_base_scalar_type_map()
                            AS m(tid uuid, tn text)
                    WHERE
                        m.tid = "typeid"
                ),
                (
                    SELECT
                        typ.oid
                    FROM
                        pg_catalog.pg_type typ
                    WHERE
                        typ.typname = "typeid"::text || '_domain'
                        OR typ.typname = "typeid"::text || '_t'
                ),
                (
                    SELECT
                        typ.typarray
                    FROM
                        pg_catalog.pg_type typ
                    WHERE
                        "kind" = 'schema::Array'
                         AND (
                            typ.typname = "elemid"::text || '_domain'
                            OR typ.typname = "elemid"::text || '_t'
                            OR typ.oid = (
                                SELECT
                                    tn::regtype::oid
                                FROM
                                    edgedb_VER._get_base_scalar_type_map()
                                        AS m(tid uuid, tn text)
                                WHERE
                                    tid = "elemid"
                            )
                        )
                ),
                (
                    SELECT
                        rng.rngtypid
                    FROM
                        pg_catalog.pg_range rng
                    WHERE
                        "kind" = 'schema::Range'
                        -- For ranges, we need to do the lookup based on
                        -- our internal map of elem names to range names,
                        -- because we use the builtin daterange as the range
                        -- for edgedbt.date_t.
                        AND rng.rngtypid = (
                            SELECT
                                rn::regtype::oid
                            FROM
                                edgedb_VER._get_base_scalar_type_map()
                                    AS m(tid uuid, tn text)
                            INNER JOIN
                                edgedb_VER._get_type_to_range_type_map()
                                    AS m2(tn2 text, rn text)
                                ON tn = tn2
                            WHERE
                                tid = "elemid"
                        )
                ),
                (
                    SELECT
                        rng.rngmultitypid
                    FROM
                        pg_catalog.pg_range rng
                    WHERE
                        "kind" = 'schema::MultiRange'
                        -- For multiranges, we need to do the lookup based on
                        -- our internal map of elem names to range names,
                        -- because we use the builtin daterange as the range
                        -- for edgedbt.date_t.
                        AND rng.rngmultitypid = (
                            SELECT
                                rn::regtype::oid
                            FROM
                                edgedb_VER._get_base_scalar_type_map()
                                    AS m(tid uuid, tn text)
                            INNER JOIN
                                edgedb_VER._get_type_to_multirange_type_map()
                                    AS m2(tn2 text, rn text)
                                ON tn = tn2
                            WHERE
                                tid = "elemid"
                        )
                ),
                edgedb_VER.raise(
                    NULL::bigint,
                    'invalid_parameter_value',
                    msg => (
                        format(
                            'cannot determine OID of Gel type %L',
                            "typeid"::text
                        )
                    )
                )
            )::bigint
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_pg_type_for_edgedb_type'),
            args=[
                ('typeid', ('uuid',)),
                ('kind', ('text',)),
                ('elemid', ('uuid',)),
                ('sql_type', ('text',)),
            ],
            returns=('bigint',),
            volatility='stable',
            text=self.text,
        )


class GetPgTypeForEdgeDBTypeFunction2(trampoline.VersionedFunction):
    """Return Postgres OID representing a given Gel type.

    This is an updated version that should replace the original. It takes
    advantage of the schema views to correctly identify non-trivial array
    types.
    """

    text = f'''
        SELECT
            coalesce(
                sql_type::regtype::oid,
                (
                    SELECT
                        tn::regtype::oid
                    FROM
                        edgedb_VER._get_base_scalar_type_map()
                            AS m(tid uuid, tn text)
                    WHERE
                        m.tid = "typeid"
                ),
                (
                    SELECT
                        typ.oid
                    FROM
                        pg_catalog.pg_type typ
                    WHERE
                        typ.typname = "typeid"::text || '_domain'
                        OR typ.typname = "typeid"::text || '_t'
                ),
                (
                    SELECT
                        typ.typarray
                    FROM
                        pg_catalog.pg_type typ
                    WHERE
                        "kind" = 'schema::Array'
                         AND (
                            typ.typname = "elemid"::text || '_domain'
                            OR typ.typname = "elemid"::text || '_t'
                            OR typ.oid = (
                                SELECT
                                    tn::regtype::oid
                                FROM
                                    edgedb_VER._get_base_scalar_type_map()
                                        AS m(tid uuid, tn text)
                                WHERE
                                    tid = "elemid"
                            )
                        )
                ),
                (
                    SELECT
                        typ.typarray
                    FROM
                        pg_catalog.pg_type typ
                    WHERE
                        "kind" = 'schema::Array'
                         AND (
                            typ.typname = "elemid"::text || '_domain'
                            OR typ.typname = "elemid"::text
                            OR typ.oid = (
                                SELECT
                                    st.backend_id
                                FROM
                                    edgedb_VER."_SchemaType" AS st
                                WHERE
                                    st.id = "elemid"
                            )
                        )
                ),
                (
                    SELECT
                        rng.rngtypid
                    FROM
                        pg_catalog.pg_range rng
                    WHERE
                        "kind" = 'schema::Range'
                        -- For ranges, we need to do the lookup based on
                        -- our internal map of elem names to range names,
                        -- because we use the builtin daterange as the range
                        -- for edgedbt.date_t.
                        AND rng.rngtypid = (
                            SELECT
                                rn::regtype::oid
                            FROM
                                edgedb_VER._get_base_scalar_type_map()
                                    AS m(tid uuid, tn text)
                            INNER JOIN
                                edgedb_VER._get_type_to_range_type_map()
                                    AS m2(tn2 text, rn text)
                                ON tn = tn2
                            WHERE
                                tid = "elemid"
                        )
                ),
                (
                    SELECT
                        rng.rngmultitypid
                    FROM
                        pg_catalog.pg_range rng
                    WHERE
                        "kind" = 'schema::MultiRange'
                        -- For multiranges, we need to do the lookup based on
                        -- our internal map of elem names to range names,
                        -- because we use the builtin daterange as the range
                        -- for edgedbt.date_t.
                        AND rng.rngmultitypid = (
                            SELECT
                                rn::regtype::oid
                            FROM
                                edgedb_VER._get_base_scalar_type_map()
                                    AS m(tid uuid, tn text)
                            INNER JOIN
                                edgedb_VER._get_type_to_multirange_type_map()
                                    AS m2(tn2 text, rn text)
                                ON tn = tn2
                            WHERE
                                tid = "elemid"
                        )
                ),
                edgedb_VER.raise(
                    NULL::bigint,
                    'invalid_parameter_value',
                    msg => (
                        format(
                            'cannot determine Postgres OID of Gel %s(%L)%s',
                            "kind",
                            "typeid"::text,
                            (case when "elemid" is not null
                             then ' with element type ' || "elemid"::text
                             else ''
                             end)
                        )
                    )
                )
            )::bigint
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_pg_type_for_edgedb_type'),
            args=[
                ('typeid', ('uuid',)),
                ('kind', ('text',)),
                ('elemid', ('uuid',)),
                ('sql_type', ('text',)),
            ],
            returns=('bigint',),
            volatility='stable',
            text=self.text,
        )


class FTSParseQueryFunction(trampoline.VersionedFunction):
    """Return tsquery representing the given FTS input query."""

    text = r'''
    DECLARE
        parts text[];
        exclude text;
        term text;
        rest text;
        cur_op text := NULL;
        default_op text;
        tsq tsquery;
        el tsquery;
        result tsquery := ''::tsquery;

    BEGIN
        IF q IS NULL OR q = '' THEN
            RETURN result;
        END IF;

        -- Break up the query string into the current term, optional next
        -- operator and the rest.
        parts := regexp_match(
            q, $$^(-)?((?:"[^"]*")|(?:\S+))\s*(OR|AND)?\s*(.*)$$
        );
        exclude := parts[1];
        term := parts[2];
        cur_op := parts[3];
        rest := parts[4];

        IF starts_with(term, '"') THEN
            -- match as a phrase
            tsq := phraseto_tsquery(language, trim(both '"' from term));
        ELSE
            tsq := to_tsquery(language, term);
        END IF;

        IF exclude IS NOT NULL THEN
            tsq := !!tsq;
        END IF;

        -- figure out the operator between the current term and the next one
        IF rest = '' THEN
            -- base case, one one term left, so we ignore the cur_op even if
            -- present
            IF prev_op = 'OR' THEN
                -- explicit 'OR' terms are "should"
                should := array_append(should, tsq);
            ELSIF starts_with(term, '"')
               OR exclude IS NOT NULL
               OR prev_op = 'AND' THEN
                -- phrases, exclusions and 'AND' terms are "must"
                must := array_append(must, tsq);
            ELSE
                -- regular terms are "should" by default
                should := array_append(should, tsq);
            END IF;
        ELSE
            -- recursion

            IF prev_op = 'OR' OR cur_op = 'OR' THEN
                -- if at least one of the suprrounding operators is 'OR',
                -- then the phrase is put into "should" category
                should := array_append(should, tsq);
            ELSIF prev_op = 'AND' OR cur_op = 'AND' THEN
                -- if at least one of the suprrounding operators is 'AND',
                -- then the phrase is put into "must" category
                must := array_append(must, tsq);
            ELSIF starts_with(term, '"') OR exclude IS NOT NULL THEN
                -- phrases and exclusions are "must"
                must := array_append(must, tsq);
            ELSE
                -- regular terms are "should" by default
                should := array_append(should, tsq);
            END IF;

            RETURN edgedb_VER.fts_parse_query(
                rest, language, must, should, cur_op);
        END IF;

        FOREACH el IN ARRAY should
        LOOP
            result := result || el;
        END LOOP;

        FOREACH el IN ARRAY must
        LOOP
            result := result && el;
        END LOOP;

        RETURN result;

    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'fts_parse_query'),
            args=[
                ('q', ('text',)),
                ('language', ('regconfig',), "'english'"),
                ('must', ('tsquery[]',), 'array[]::tsquery[]'),
                ('should', ('tsquery[]',), 'array[]::tsquery[]'),
                ('prev_op', ('text',), 'NULL'),
            ],
            returns=('tsquery',),
            volatility='immutable',
            language='plpgsql',
            text=self.text,
        )


class FTSNormalizeWeightFunction(trampoline.VersionedFunction):
    """Normalize an array of weights to be a 4-value weight array."""

    text = r'''
    SELECT
        CASE COALESCE(array_length(weights, 1), 0)
            WHEN 0 THEN array[1, 1, 1, 1]::float4[]
            WHEN 1 THEN array[0, 0, 0, weights[1]]::float4[]
            WHEN 2 THEN array[0, 0, weights[2], weights[1]]::float4[]
            WHEN 3 THEN array[0, weights[3], weights[2], weights[1]]::float4[]
            ELSE (
                WITH raw as (
                    SELECT w
                    FROM UNNEST(weights) AS w
                    ORDER BY w DESC
                )
                SELECT array_prepend(rest.w, first.arrw)::float4[]
                FROM
                (
                    SELECT array_agg(rw1.w) as arrw
                    FROM (
                        SELECT w
                        FROM (SELECT w FROM raw LIMIT 3) as rw0
                        ORDER BY w ASC
                    ) as rw1
                ) AS first,
                (
                    SELECT avg(rw2.w) as w
                    FROM (SELECT w FROM raw OFFSET 3) as rw2
                ) AS rest
            )
        END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'fts_normalize_weights'),
            args=[
                ('weights', ('float8[]',)),
            ],
            returns=('float4[]',),
            volatility='immutable',
            text=self.text,
        )


class FTSNormalizeDocFunction(trampoline.VersionedFunction):
    """Normalize a document based on an array of weights."""

    text = r'''
    SELECT
        CASE COALESCE(array_length(doc, 1), 0)
            WHEN 0 THEN ''::tsvector
            WHEN 1 THEN setweight(to_tsvector(language, doc[1]), 'A')
            WHEN 2 THEN (
                setweight(to_tsvector(language, doc[1]), 'A') ||
                setweight(to_tsvector(language, doc[2]), 'B')
            )
            WHEN 3 THEN (
                setweight(to_tsvector(language, doc[1]), 'A') ||
                setweight(to_tsvector(language, doc[2]), 'B') ||
                setweight(to_tsvector(language, doc[3]), 'C')
            )
            ELSE (
                WITH raw as (
                    SELECT d.v as t
                    FROM UNNEST(doc) WITH ORDINALITY AS d(v, n)
                    LEFT JOIN UNNEST(weights) WITH ORDINALITY AS w(v, n)
                    ON d.n = w.n
                    ORDER BY w.v DESC
                )
                SELECT
                    setweight(to_tsvector(language, d.arr[1]), 'A') ||
                    setweight(to_tsvector(language, d.arr[2]), 'B') ||
                    setweight(to_tsvector(language, d.arr[3]), 'C') ||
                    setweight(to_tsvector(language,
                                          array_to_string(d.arr[4:], ' ')),
                              'D')
                FROM
                (
                    SELECT array_agg(raw.t) as arr
                    FROM raw
                ) AS d
            )
        END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'fts_normalize_doc'),
            args=[
                ('doc', ('text[]',)),
                ('weights', ('float8[]',)),
                ('language', ('regconfig',)),
            ],
            returns=('tsvector',),
            volatility='stable',
            text=self.text,
        )


class FTSToRegconfig(trampoline.VersionedFunction):
    """
    Converts ISO 639-3 language identifiers into a regconfig.
    Defaults to english.
    Identifiers prefixed with 'xxx_' have the prefix stripped and the remainder
    used as regconfg identifier.
    """

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'fts_to_regconfig'),
            args=[
                ('language', ('text',)),
            ],
            returns=('regconfig',),
            volatility='immutable',
            text='''
            SELECT CASE
                WHEN language ILIKE 'xxx_%' THEN SUBSTR(language, 4)
                ELSE (CASE LOWER(language)
                    WHEN 'ara' THEN 'arabic'
                    WHEN 'hye' THEN 'armenian'
                    WHEN 'eus' THEN 'basque'
                    WHEN 'cat' THEN 'catalan'
                    WHEN 'dan' THEN 'danish'
                    WHEN 'nld' THEN 'dutch'
                    WHEN 'eng' THEN 'english'
                    WHEN 'fin' THEN 'finnish'
                    WHEN 'fra' THEN 'french'
                    WHEN 'deu' THEN 'german'
                    WHEN 'ell' THEN 'greek'
                    WHEN 'hin' THEN 'hindi'
                    WHEN 'hun' THEN 'hungarian'
                    WHEN 'ind' THEN 'indonesian'
                    WHEN 'gle' THEN 'irish'
                    WHEN 'ita' THEN 'italian'
                    WHEN 'lit' THEN 'lithuanian'
                    WHEN 'npi' THEN 'nepali'
                    WHEN 'nor' THEN 'norwegian'
                    WHEN 'por' THEN 'portuguese'
                    WHEN 'ron' THEN 'romanian'
                    WHEN 'rus' THEN 'russian'
                    WHEN 'srp' THEN 'serbian'
                    WHEN 'spa' THEN 'spanish'
                    WHEN 'swe' THEN 'swedish'
                    WHEN 'tam' THEN 'tamil'
                    WHEN 'tur' THEN 'turkish'
                    WHEN 'yid' THEN 'yiddish'
                    ELSE 'english' END
                )
            END::pg_catalog.regconfig;
            ''',
        )


class UuidGenerateV1mcFunction(trampoline.VersionedFunction):
    def __init__(self, ext_schema: str) -> None:
        super().__init__(
            name=('edgedb', 'uuid_generate_v1mc'),
            args=[],
            returns=('uuid',),
            volatility='volatile',
            language='sql',
            strict=True,
            parallel_safe=True,
            text=f'SELECT "{ext_schema}".uuid_generate_v1mc();'
        )


class UuidGenerateV4Function(trampoline.VersionedFunction):
    def __init__(self, ext_schema: str) -> None:
        super().__init__(
            name=('edgedb', 'uuid_generate_v4'),
            args=[],
            returns=('uuid',),
            volatility='volatile',
            language='sql',
            strict=True,
            parallel_safe=True,
            text=f'SELECT "{ext_schema}".uuid_generate_v4();'
        )


class UuidGenerateV5Function(trampoline.VersionedFunction):
    def __init__(self, ext_schema: str) -> None:
        super().__init__(
            name=('edgedb', 'uuid_generate_v5'),
            args=[
                ('namespace', ('uuid',)),
                ('name', ('text',)),
            ],
            returns=('uuid',),
            volatility='immutable',
            language='sql',
            strict=True,
            parallel_safe=True,
            text=f'SELECT "{ext_schema}".uuid_generate_v5(namespace, name);'
        )


class PadBase64StringFunction(trampoline.VersionedFunction):
    text = r"""
        WITH
            l AS (SELECT pg_catalog.length("s") % 4 AS r),
            p AS (
                SELECT
                    (CASE WHEN l.r > 0 THEN repeat('=', (4 - l.r))
                    ELSE '' END) AS p
                FROM
                    l
            )
        SELECT
            "s" || p.p
        FROM
            p
    """

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'pad_base64_string'),
            args=[
                ('s', ('text',)),
            ],
            returns=('text',),
            volatility='immutable',
            language='sql',
            strict=True,
            parallel_safe=True,
            text=self.text,
        )


class ResetQueryStatsFunction(trampoline.VersionedFunction):
    text = r"""
    DECLARE
        tenant_id TEXT;
        other_tenant_exists BOOLEAN;
        db_oid OID;
        queryid bigint;
    BEGIN
        tenant_id := edgedb_VER.get_backend_tenant_id();
        IF id IS NULL THEN
            queryid := 0;
        ELSE
            queryid := edgedbext.edb_stat_queryid(id);
        END IF;

        SELECT EXISTS (
            SELECT 1
            FROM
                pg_database dat
                CROSS JOIN LATERAL (
                    SELECT
                        edgedb_VER.shobj_metadata(dat.oid, 'pg_database')
                            AS description
                ) AS d
            WHERE
                (d.description)->>'id' IS NOT NULL
                AND (d.description)->>'tenant_id' != tenant_id
        ) INTO other_tenant_exists;

        IF branch_name IS NULL THEN
            IF other_tenant_exists THEN
                RETURN edgedbext.edb_stat_statements_reset(
                    0,  -- userid
                    ARRAY(
                        SELECT
                            dat.oid
                        FROM
                            pg_database dat
                            CROSS JOIN LATERAL (
                                SELECT
                                    edgedb_VER.shobj_metadata(dat.oid,
                                                              'pg_database')
                                        AS description
                            ) AS d
                        WHERE
                            (d.description)->>'id' IS NOT NULL
                            AND (d.description)->>'tenant_id' = tenant_id
                    ),
                    queryid,
                    COALESCE(minmax_only, false)
                );
            ELSE
                RETURN edgedbext.edb_stat_statements_reset(
                    0,  -- userid
                    '{}',  -- database oid
                    queryid,
                    COALESCE(minmax_only, false)
                );
            END IF;
        ELSE
            SELECT
                dat.oid INTO db_oid
            FROM
                pg_database dat
                CROSS JOIN LATERAL (
                    SELECT
                        edgedb_VER.shobj_metadata(dat.oid, 'pg_database')
                            AS description
                ) AS d
            WHERE
                (d.description)->>'id' IS NOT NULL
                AND (d.description)->>'tenant_id' = tenant_id
                AND edgedb_VER.get_database_frontend_name(dat.datname) =
                    branch_name;

            IF db_oid IS NULL THEN
                RETURN NULL::edgedbt.timestamptz_t;
            END IF;

            RETURN edgedbext.edb_stat_statements_reset(
                0,  -- userid
                ARRAY[db_oid],
                queryid,
                COALESCE(minmax_only, false)
            );
        END IF;

        RETURN now()::edgedbt.timestamptz_t;
    END;
    """

    noop_text = r"""
        BEGIN
        RETURN NULL::edgedbt.timestamptz_t;
        END;
    """

    def __init__(self, enable_stats: bool) -> None:
        super().__init__(
            name=('edgedb', 'reset_query_stats'),
            args=[
                ('branch_name', ('text',)),
                ('id', ('uuid',)),
                ('minmax_only', ('bool',)),
            ],
            returns=('edgedbt', 'timestamptz_t'),
            volatility='volatile',
            language='plpgsql',
            text=self.text if enable_stats else self.noop_text,
        )


def _maybe_trampoline(
    cmd: dbops.Command, out: list[trampoline.Trampoline]
) -> None:
    namespace = V('')
    if (
        isinstance(cmd, dbops.CreateFunction)
        and cmd.function.name[0].endswith(namespace)
    ):
        out.append(trampoline.make_trampoline(cmd.function))
    elif (
        isinstance(cmd, dbops.CreateView)
        and cmd.view.name[0].endswith(namespace)
    ):
        out.append(trampoline.make_view_trampoline(cmd.view))
    elif (
        isinstance(cmd, dbops.CreateTable)
        and cmd.table.name[0].endswith(namespace)
    ):
        f, n = cmd.table.name
        out.append(trampoline.make_table_trampoline((f, n)))


def trampoline_functions(
    cmds: Sequence[dbops.Command]
) -> list[trampoline.Trampoline]:
    ncmds: list[trampoline.Trampoline] = []
    for cmd in cmds:
        _maybe_trampoline(cmd, ncmds)
    return ncmds


def trampoline_command(cmd: dbops.Command) -> list[trampoline.Trampoline]:
    ncmds: list[trampoline.Trampoline] = []

    def go(cmd: dbops.Command) -> None:
        if isinstance(cmd, dbops.CommandGroup):
            for subcmd in cmd.commands:
                go(subcmd)
        else:
            _maybe_trampoline(cmd, ncmds)

    go(cmd)

    return ncmds


def get_fixed_bootstrap_commands() -> dbops.CommandGroup:
    """Create metaschema objects that are truly global"""

    cmds = [
        dbops.CreateSchema(name='edgedb'),
        dbops.CreateSchema(name='edgedbt'),
        dbops.CreateSchema(name='edgedbpub'),
        dbops.CreateSchema(name='edgedbstd'),
        dbops.CreateSchema(name='edgedbinstdata'),

        dbops.CreateTable(
            DBConfigTable(),
        ),
        # TODO: SHOULD THIS BE VERSIONED?
        dbops.CreateTable(DMLDummyTable()),
        # TODO: SHOULD THIS BE VERSIONED?
        dbops.CreateTable(QueryCacheTable()),

        dbops.Query(DMLDummyTable.SETUP_QUERY),

        dbops.CreateDomain(BigintDomain()),
        dbops.CreateDomain(ConfigMemoryDomain()),
        dbops.CreateDomain(TimestampTzDomain()),
        dbops.CreateDomain(TimestampDomain()),
        dbops.CreateDomain(DateDomain()),
        dbops.CreateDomain(DurationDomain()),
        dbops.CreateDomain(RelativeDurationDomain()),
        dbops.CreateDomain(DateDurationDomain()),

        dbops.CreateEnum(SysConfigSourceType()),
        dbops.CreateEnum(SysConfigScopeType()),

        dbops.CreateCompositeType(SysConfigValueType()),
        dbops.CreateCompositeType(SysConfigEntryType()),
        dbops.CreateRange(Float32Range()),
        dbops.CreateRange(Float64Range()),
        dbops.CreateRange(DatetimeRange()),
        dbops.CreateRange(LocalDatetimeRange()),
    ]

    commands = dbops.CommandGroup()
    commands.add_commands(cmds)
    return commands


def get_instdata_commands(
) -> tuple[dbops.CommandGroup, list[trampoline.Trampoline]]:
    cmds = [
        dbops.CreateSchema(name=V('edgedbinstdata')),
        dbops.CreateTable(InstDataTable()),
    ]

    commands = dbops.CommandGroup()
    commands.add_commands(cmds)

    return commands, trampoline_functions(cmds)


async def generate_instdata_table(
    conn: PGConnection,
) -> list[trampoline.Trampoline]:
    commands, trampolines = get_instdata_commands()
    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)
    return trampolines


def get_bootstrap_commands(
    config_spec: edbconfig.Spec,
) -> tuple[dbops.CommandGroup, list[trampoline.Trampoline]]:
    cmds = [
        dbops.CreateSchema(name=V('edgedb')),
        dbops.CreateSchema(name=V('edgedbpub')),
        dbops.CreateSchema(name=V('edgedbstd')),
        dbops.CreateSchema(name=V('edgedbsql')),

        dbops.CreateView(NormalizedPgSettingsView()),
        dbops.CreateFunction(EvictQueryCacheFunction()),
        dbops.CreateFunction(ClearQueryCacheFunction()),
        dbops.CreateFunction(CreateTrampolineViewFunction()),
        dbops.CreateFunction(UuidGenerateV1mcFunction('edgedbext')),
        dbops.CreateFunction(UuidGenerateV4Function('edgedbext')),
        dbops.CreateFunction(UuidGenerateV5Function('edgedbext')),
        dbops.CreateFunction(IntervalToMillisecondsFunction()),
        dbops.CreateFunction(SafeIntervalCastFunction()),
        dbops.CreateFunction(QuoteIdentFunction()),
        dbops.CreateFunction(QuoteNameFunction()),
        dbops.CreateFunction(AlterCurrentDatabaseSetString()),
        dbops.CreateFunction(AlterCurrentDatabaseSetStringArray()),
        dbops.CreateFunction(AlterCurrentDatabaseSetNonArray()),
        dbops.CreateFunction(AlterCurrentDatabaseSetArray()),
        dbops.CreateFunction(CopyDatabaseConfigs()),
        dbops.CreateFunction(GetBackendCapabilitiesFunction()),
        dbops.CreateFunction(GetBackendTenantIDFunction()),
        dbops.CreateFunction(GetDatabaseBackendNameFunction()),
        dbops.CreateFunction(GetDatabaseFrontendNameFunction()),
        dbops.CreateFunction(GetRoleBackendNameFunction()),
        dbops.CreateFunction(GetUserSequenceBackendNameFunction()),
        dbops.CreateFunction(GetStdModulesFunction()),
        dbops.CreateFunction(GetObjectMetadata()),
        dbops.CreateFunction(GetColumnMetadata()),
        dbops.CreateFunction(GetSharedObjectMetadata()),
        dbops.CreateFunction(GetDatabaseMetadataFunction()),
        dbops.CreateFunction(GetCurrentDatabaseFunction()),
        dbops.CreateFunction(RaiseNoticeFunction()),
        dbops.CreateFunction(IndirectReturnFunction()),
        dbops.CreateFunction(RaiseExceptionFunction()),
        dbops.CreateFunction(RaiseExceptionOnNullFunction()),
        dbops.CreateFunction(RaiseExceptionOnNotNullFunction()),
        dbops.CreateFunction(RaiseExceptionOnEmptyStringFunction()),
        dbops.CreateFunction(AssertJSONTypeFunction()),
        dbops.CreateFunction(ExtractJSONScalarFunction()),
        dbops.CreateFunction(NormalizeNameFunction()),
        dbops.CreateFunction(GetNameModuleFunction()),
        dbops.CreateFunction(NullIfArrayNullsFunction()),
        dbops.CreateFunction(StrToConfigMemoryFunction()),
        dbops.CreateFunction(ConfigMemoryToStrFunction()),
        dbops.CreateFunction(StrToBigint()),
        dbops.CreateFunction(StrToDecimal()),
        dbops.CreateFunction(StrToInt64NoInline()),
        dbops.CreateFunction(StrToInt32NoInline()),
        dbops.CreateFunction(StrToInt16NoInline()),
        dbops.CreateFunction(StrToFloat64NoInline()),
        dbops.CreateFunction(StrToFloat32NoInline()),
        dbops.CreateFunction(NormalizeArrayIndexFunction()),
        dbops.CreateFunction(NormalizeArraySliceIndexFunction()),
        dbops.CreateFunction(IntOrNullFunction()),
        dbops.CreateFunction(ArrayIndexWithBoundsFunction()),
        dbops.CreateFunction(ArraySliceFunction()),
        dbops.CreateFunction(StringIndexWithBoundsFunction()),
        dbops.CreateFunction(LengthStringProxyFunction()),
        dbops.CreateFunction(LengthBytesProxyFunction()),
        dbops.CreateFunction(SubstrProxyFunction()),
        dbops.CreateFunction(StringSliceImplFunction()),
        dbops.CreateFunction(StringSliceFunction()),
        dbops.CreateFunction(BytesSliceFunction()),
        dbops.CreateFunction(JSONIndexByTextFunction()),
        dbops.CreateFunction(JSONIndexByIntFunction()),
        dbops.CreateFunction(JSONSliceFunction()),
        dbops.CreateFunction(DatetimeInFunction()),
        dbops.CreateFunction(DurationInFunction()),
        dbops.CreateFunction(DateDurationInFunction()),
        dbops.CreateFunction(LocalDatetimeInFunction()),
        dbops.CreateFunction(LocalDateInFunction()),
        dbops.CreateFunction(LocalTimeInFunction()),
        dbops.CreateFunction(ToTimestampTZCheck()),
        dbops.CreateFunction(ToDatetimeFunction()),
        dbops.CreateFunction(ToLocalDatetimeFunction()),
        dbops.CreateFunction(StrToBool()),
        dbops.CreateFunction(BytesIndexWithBoundsFunction()),
        dbops.CreateFunction(TypeIDToConfigType()),
        dbops.CreateFunction(ConvertPostgresConfigUnitsFunction()),
        dbops.CreateFunction(InterpretConfigValueToJsonFunction()),
        dbops.CreateFunction(PostgresConfigValueToJsonFunction()),
        dbops.CreateFunction(SysConfigFullFunction()),
        dbops.CreateFunction(SysConfigUncachedFunction()),
        dbops.Query(pgcon.SETUP_CONFIG_CACHE_SCRIPT),
        dbops.CreateFunction(SysConfigFunction()),
        dbops.CreateFunction(SysClearConfigCacheFunction()),
        dbops.CreateFunction(ResetSessionConfigFunction()),
        dbops.CreateFunction(ApplySessionConfigFunction(config_spec)),
        dbops.CreateFunction(SysGetTransactionIsolation()),
        dbops.CreateFunction(GetCachedReflection()),
        dbops.CreateFunction(GetBaseScalarTypeMap()),
        dbops.CreateFunction(GetTypeToRangeNameMap()),
        dbops.CreateFunction(GetTypeToMultiRangeNameMap()),
        dbops.CreateFunction(GetPgTypeForEdgeDBTypeFunction()),
        dbops.CreateFunction(DescribeRolesAsDDLFunctionForwardDecl()),
        dbops.CreateFunction(RangeToJsonFunction()),
        dbops.CreateFunction(MultiRangeToJsonFunction()),
        dbops.CreateFunction(RangeValidateFunction()),
        dbops.CreateFunction(RangeUnpackLowerValidateFunction()),
        dbops.CreateFunction(RangeUnpackUpperValidateFunction()),
        dbops.CreateFunction(FTSParseQueryFunction()),
        dbops.CreateFunction(FTSNormalizeWeightFunction()),
        dbops.CreateFunction(FTSNormalizeDocFunction()),
        dbops.CreateFunction(FTSToRegconfig()),
        dbops.CreateFunction(PadBase64StringFunction()),
        dbops.CreateFunction(ResetQueryStatsFunction(False)),
    ]

    commands = dbops.CommandGroup()
    commands.add_commands(cmds)

    return commands, trampoline_functions(cmds)


async def create_pg_extensions(
    conn: PGConnection,
    backend_params: params.BackendRuntimeParams,
) -> None:
    inst_params = backend_params.instance_params
    ext_schema = inst_params.ext_schema
    # Both the extension schema, and the desired extension
    # might already exist in a single database backend,
    # attempt to create things conditionally.
    commands = dbops.CommandGroup()
    commands.add_command(
        dbops.CreateSchema(name=ext_schema, conditional=True),
    )
    extensions = ["uuid-ossp"]
    if backend_params.has_stat_statements:
        extensions.append("edb_stat_statements")
    for ext in extensions:
        if (
            inst_params.existing_exts is None
            or inst_params.existing_exts.get(ext) is None
        ):
            commands.add_commands([
                dbops.CreateExtension(
                    dbops.Extension(name=ext, schema=ext_schema),
                ),
            ])
    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)


async def patch_pg_extensions(
    conn: PGConnection,
    backend_params: params.BackendRuntimeParams,
) -> None:
    # A single database backend might restrict creation of extensions
    # to a specific schema, or restrict creation of extensions altogether
    # and provide a way to register them using a different method
    # (e.g. a hosting panel UI).
    inst_params = backend_params.instance_params
    if inst_params.existing_exts is not None:
        uuid_ext_schema = inst_params.existing_exts.get("uuid-ossp")
        if uuid_ext_schema is None:
            uuid_ext_schema = inst_params.ext_schema
    else:
        uuid_ext_schema = inst_params.ext_schema

    commands = dbops.CommandGroup()

    if uuid_ext_schema != "edgedbext":
        commands.add_commands([
            dbops.CreateFunction(
                UuidGenerateV1mcFunction(uuid_ext_schema), or_replace=True),
            dbops.CreateFunction(
                UuidGenerateV4Function(uuid_ext_schema), or_replace=True),
            dbops.CreateFunction(
                UuidGenerateV5Function(uuid_ext_schema), or_replace=True),
        ])

    if len(commands) > 0:
        block = dbops.PLTopBlock()
        commands.generate(block)
        await _execute_block(conn, block)


classref_attr_aliases = {
    'links': 'pointers',
    'link_properties': 'pointers'
}


def tabname(
    schema: s_schema.Schema, obj: s_obj.QualifiedObject
) -> tuple[str, str]:
    return common.get_backend_name(
        schema,
        obj,
        aspect='table',
        catenate=False,
        versioned=True,
    )


def ptr_col_name(
    schema: s_schema.Schema,
    obj: s_sources.Source,
    propname: str,
) -> str:
    prop = obj.getptr(schema, s_name.UnqualName(propname))
    psi = types.get_pointer_storage_info(prop, schema=schema)
    return psi.column_name


def format_fields(
    schema: s_schema.Schema,
    obj: s_sources.Source,
    fields: dict[str, str],
) -> str:
    """Format a dictionary of column mappings for database views

    The reason we do it this way is because, since these views are
    overwriting existing temporary views, we need to put all the
    columns in the same order as the original view.
    """
    ptrs = [obj.getptr(schema, s_name.UnqualName(s)) for s in fields]

    # Sort by the order the pointers were added to the source.
    # N.B: This only works because we are using the original in-memory
    # schema. If it was loaded from reflection it probably wouldn't
    # work.
    ptr_indexes = {
        v: i for i, v in enumerate(obj.get_pointers(schema).objects(schema))
    }
    ptrs.sort(key=(
        lambda p: (not p.is_link_source_property(schema), ptr_indexes[p])
    ))

    cols = []
    for ptr in ptrs:
        name = ptr.get_shortname(schema).name
        val = fields[name]
        sname = qi(ptr_col_name(schema, obj, name))
        cols.append(f'            {val} AS {sname}')

    return ',\n'.join(cols)


def _generate_branch_views(schema: s_schema.Schema) -> List[dbops.View]:
    Branch = schema.get('sys::Branch', type=s_objtypes.ObjectType)
    annos = Branch.getptr(
        schema, s_name.UnqualName('annotations'), type=s_links.Link)
    int_annos = Branch.getptr(
        schema, s_name.UnqualName('annotations__internal'), type=s_links.Link)

    view_fields = {
        'id': "((d.description)->>'id')::uuid",
        'internal': f"""(CASE WHEN
                (edgedb_VER.get_backend_capabilities()
                 & {int(params.BackendCapabilities.CREATE_DATABASE)}) != 0
             THEN
                datname IN (
                    edgedb_VER.get_database_backend_name(
                        {ql(defines.EDGEDB_TEMPLATE_DB)}),
                    edgedb_VER.get_database_backend_name(
                        {ql(defines.EDGEDB_SYSTEM_DB)})
                )
             ELSE False END
        )""",
        'name': (
            'edgedb_VER.get_database_frontend_name(datname) COLLATE "default"'
        ),
        'name__internal': (
            'edgedb_VER.get_database_frontend_name(datname) COLLATE "default"'
        ),
        'computed_fields': 'ARRAY[]::text[]',
        'builtin': "((d.description)->>'builtin')::bool",
        'last_migration': "(d.description)->>'last_migration'",
    }

    view_query = f'''
        SELECT
            {format_fields(schema, Branch, view_fields)}
        FROM
            pg_database dat
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(dat.oid, 'pg_database')
                        AS description
            ) AS d
        WHERE
            (d.description)->>'id' IS NOT NULL
            AND (d.description)->>'tenant_id'
                = edgedb_VER.get_backend_tenant_id()
    '''

    annos_link_fields = {
        'source': "((d.description)->>'id')::uuid",
        'target': "(annotations->>'id')::uuid",
        'value': "(annotations->>'value')::text",
        'owned': "(annotations->>'owned')::bool",
    }

    annos_link_query = f'''
        SELECT
            {format_fields(schema, annos, annos_link_fields)}
        FROM
            pg_database dat
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(dat.oid, 'pg_database')
                        AS description
            ) AS d
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements((d.description)->'annotations')
                ) AS annotations
    '''

    int_annos_link_fields = {
        'source': "((d.description)->>'id')::uuid",
        'target': "(annotations->>'id')::uuid",
        'owned': "(annotations->>'owned')::bool",
    }

    int_annos_link_query = f'''
        SELECT
            {format_fields(schema, int_annos, int_annos_link_fields)}
        FROM
            pg_database dat
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(dat.oid, 'pg_database')
                        AS description
            ) AS d
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(
                        (d.description)->'annotations__internal'
                    )
                ) AS annotations
    '''

    objects = {
        Branch: view_query,
        annos: annos_link_query,
        int_annos: int_annos_link_query,
    }

    views: list[dbops.View] = []
    for obj, query in objects.items():
        tabview = trampoline.VersionedView(
            name=tabname(schema, obj), query=query)
        views.append(tabview)

    return views


def _generate_extension_views(schema: s_schema.Schema) -> List[dbops.View]:
    ExtPkg = schema.get('sys::ExtensionPackage', type=s_objtypes.ObjectType)
    annos = ExtPkg.getptr(
        schema, s_name.UnqualName('annotations'), type=s_links.Link)
    int_annos = ExtPkg.getptr(
        schema, s_name.UnqualName('annotations__internal'), type=s_links.Link)
    ver = ExtPkg.getptr(
        schema, s_name.UnqualName('version'), type=s_props.Property)
    ver_t = common.get_backend_name(
        schema,
        not_none(ver.get_target(schema)),
        catenate=False,
    )

    view_query_fields = {
        'id': "(e.value->>'id')::uuid",
        'name': "(e.value->>'name')",
        'name__internal': "(e.value->>'name__internal')",
        'script': "(e.value->>'script')",
        'sql_extensions': '''
            COALESCE(
                (SELECT
                    array_agg(edgedb_VER.jsonb_extract_scalar(q.v, 'string'))
                FROM jsonb_array_elements(
                    e.value->'sql_extensions'
                ) AS q(v)),
                ARRAY[]::text[]
            )
        ''',
        'dependencies': '''
            COALESCE(
                (SELECT
                    array_agg(edgedb_VER.jsonb_extract_scalar(q.v, 'string'))
                FROM jsonb_array_elements(
                    e.value->'dependencies'
                ) AS q(v)),
                ARRAY[]::text[]
            )
        ''',
        'ext_module': "(e.value->>'ext_module')",
        'sql_setup_script': "(e.value->>'sql_setup_script')",
        'sql_teardown_script': "(e.value->>'sql_teardown_script')",
        'computed_fields': 'ARRAY[]::text[]',
        'builtin': "(e.value->>'builtin')::bool",
        'internal': "(e.value->>'internal')::bool",
        'version': f'''
            (
                (e.value->'version'->>'major')::int,
                (e.value->'version'->>'minor')::int,
                (e.value->'version'->>'stage')::text,
                (e.value->'version'->>'stage_no')::int,
                COALESCE(
                    (SELECT array_agg(q.v::text)
                    FROM jsonb_array_elements(
                        e.value->'version'->'local'
                    ) AS q(v)),
                    ARRAY[]::text[]
                )
            )::{qt(ver_t)}
        ''',
    }

    view_query = f'''
        SELECT
            {format_fields(schema, ExtPkg, view_query_fields)}
        FROM
            jsonb_each(
                edgedb_VER.get_database_metadata(
                    {ql(defines.EDGEDB_TEMPLATE_DB)}
                ) -> 'ExtensionPackage'
            ) AS e
    '''

    annos_link_fields = {
        'source': "(e.value->>'id')::uuid",
        'target': "(annotations->>'id')::uuid",
        'value': "(annotations->>'value')::text",
        'owned': "(annotations->>'owned')::bool",
    }

    int_annos_link_fields = {
        'source': "(e.value->>'id')::uuid",
        'target': "(annotations->>'id')::uuid",
        'owned': "(annotations->>'owned')::bool",
    }

    annos_link_query = f'''
        SELECT
            {format_fields(schema, annos, annos_link_fields)}
        FROM
            jsonb_each(
                edgedb_VER.get_database_metadata(
                    {ql(defines.EDGEDB_TEMPLATE_DB)}
                ) -> 'ExtensionPackage'
            ) AS e
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(e.value->'annotations')
                ) AS annotations
    '''

    int_annos_link_query = f'''
        SELECT
            {format_fields(schema, int_annos, int_annos_link_fields)}
        FROM
            jsonb_each(
                edgedb_VER.get_database_metadata(
                    {ql(defines.EDGEDB_TEMPLATE_DB)}
                ) -> 'ExtensionPackage'
            ) AS e
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(e.value->'annotations__internal')
                ) AS annotations
    '''

    objects = {
        ExtPkg: view_query,
        annos: annos_link_query,
        int_annos: int_annos_link_query,
    }

    views: list[dbops.View] = []
    for obj, query in objects.items():
        tabview = trampoline.VersionedView(
            name=tabname(schema, obj), query=query)
        views.append(tabview)

    return views


def _generate_extension_migration_views(
    schema: s_schema.Schema
) -> List[dbops.View]:
    ExtPkgMigration = schema.get(
        'sys::ExtensionPackageMigration', type=s_objtypes.ObjectType)
    annos = ExtPkgMigration.getptr(
        schema, s_name.UnqualName('annotations'), type=s_links.Link)
    int_annos = ExtPkgMigration.getptr(
        schema, s_name.UnqualName('annotations__internal'), type=s_links.Link)
    from_ver = ExtPkgMigration.getptr(
        schema, s_name.UnqualName('from_version'), type=s_props.Property)
    ver_t = common.get_backend_name(
        schema,
        not_none(from_ver.get_target(schema)),
        catenate=False,
    )

    view_query_fields = {
        'id': "(e.value->>'id')::uuid",
        'name': "(e.value->>'name')",
        'name__internal': "(e.value->>'name__internal')",
        'script': "(e.value->>'script')",
        'sql_early_script': "(e.value->>'sql_early_script')",
        'sql_late_script': "(e.value->>'sql_late_script')",
        'computed_fields': 'ARRAY[]::text[]',
        'builtin': "(e.value->>'builtin')::bool",
        'internal': "(e.value->>'internal')::bool",
        # XXX: code duplication here
        'from_version': f'''
            (
                (e.value->'from_version'->>'major')::int,
                (e.value->'from_version'->>'minor')::int,
                (e.value->'from_version'->>'stage')::text,
                (e.value->'from_version'->>'stage_no')::int,
                COALESCE(
                    (SELECT array_agg(q.v::text)
                    FROM jsonb_array_elements(
                        e.value->'from_version'->'local'
                    ) AS q(v)),
                    ARRAY[]::text[]
                )
            )::{qt(ver_t)}
        ''',
        'to_version': f'''
            (
                (e.value->'to_version'->>'major')::int,
                (e.value->'to_version'->>'minor')::int,
                (e.value->'to_version'->>'stage')::text,
                (e.value->'to_version'->>'stage_no')::int,
                COALESCE(
                    (SELECT array_agg(q.v::text)
                    FROM jsonb_array_elements(
                        e.value->'to_version'->'local'
                    ) AS q(v)),
                    ARRAY[]::text[]
                )
            )::{qt(ver_t)}
        ''',
    }

    view_query = f'''
        SELECT
            {format_fields(schema, ExtPkgMigration, view_query_fields)}
        FROM
            jsonb_each(
                edgedb_VER.get_database_metadata(
                    {ql(defines.EDGEDB_TEMPLATE_DB)}
                ) -> 'ExtensionPackageMigration'
            ) AS e
    '''

    annos_link_fields = {
        'source': "(e.value->>'id')::uuid",
        'target': "(annotations->>'id')::uuid",
        'value': "(annotations->>'value')::text",
        'owned': "(annotations->>'owned')::bool",
    }

    int_annos_link_fields = {
        'source': "(e.value->>'id')::uuid",
        'target': "(annotations->>'id')::uuid",
        'owned': "(annotations->>'owned')::bool",
    }

    annos_link_query = f'''
        SELECT
            {format_fields(schema, annos, annos_link_fields)}
        FROM
            jsonb_each(
                edgedb_VER.get_database_metadata(
                    {ql(defines.EDGEDB_TEMPLATE_DB)}
                ) -> 'ExtensionPackageMigration'
            ) AS e
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(e.value->'annotations')
                ) AS annotations
    '''

    int_annos_link_query = f'''
        SELECT
            {format_fields(schema, int_annos, int_annos_link_fields)}
        FROM
            jsonb_each(
                edgedb_VER.get_database_metadata(
                    {ql(defines.EDGEDB_TEMPLATE_DB)}
                ) -> 'ExtensionPackageMigration'
            ) AS e
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(e.value->'annotations__internal')
                ) AS annotations
    '''

    objects = {
        ExtPkgMigration: view_query,
        annos: annos_link_query,
        int_annos: int_annos_link_query,
    }

    views: list[dbops.View] = []
    for obj, query in objects.items():
        tabview = trampoline.VersionedView(
            name=tabname(schema, obj), query=query)
        views.append(tabview)

    return views


def _generate_role_views(schema: s_schema.Schema) -> List[dbops.View]:
    Role = schema.get('sys::Role', type=s_objtypes.ObjectType)
    member_of = Role.getptr(
        schema, s_name.UnqualName('member_of'), type=s_links.Link)
    bases = Role.getptr(
        schema, s_name.UnqualName('bases'), type=s_links.Link)
    ancestors = Role.getptr(
        schema, s_name.UnqualName('ancestors'), type=s_links.Link)
    annos = Role.getptr(
        schema, s_name.UnqualName('annotations'), type=s_links.Link)
    int_annos = Role.getptr(
        schema, s_name.UnqualName('annotations__internal'), type=s_links.Link)

    superuser = f'''
        a.rolsuper OR EXISTS (
            SELECT
            FROM
                pg_auth_members m
                INNER JOIN pg_catalog.pg_roles g
                    ON (m.roleid = g.oid)
            WHERE
                m.member = a.oid
                AND g.rolname = edgedb_VER.get_role_backend_name(
                    {ql(defines.EDGEDB_SUPERGROUP)}
                )
        )
    '''

    view_query_fields = {
        'id': "((d.description)->>'id')::uuid",
        'name': "(d.description)->>'name'",
        'name__internal': "(d.description)->>'name'",
        'superuser': f'{superuser}',
        'abstract': 'False',
        'is_derived': 'False',
        'inherited_fields': 'ARRAY[]::text[]',
        'computed_fields': 'ARRAY[]::text[]',
        'builtin': "((d.description)->>'builtin')::bool",
        'internal': 'False',
        'password': "(d.description)->>'password_hash'",
    }

    view_query = f'''
        SELECT
            {format_fields(schema, Role, view_query_fields)}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
        WHERE
            (d.description)->>'id' IS NOT NULL
            AND
              (d.description)->>'tenant_id' = edgedb_VER.get_backend_tenant_id()
    '''

    member_of_link_query_fields = {
        'source': "((d.description)->>'id')::uuid",
        'target': "((md.description)->>'id')::uuid",
    }

    member_of_link_query = f'''
        SELECT
            {format_fields(schema, member_of, member_of_link_query_fields)}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
            INNER JOIN pg_auth_members m ON m.member = a.oid
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(m.roleid, 'pg_authid')
                        AS description
            ) AS md
    '''

    bases_link_query_fields = {
        'source': "((d.description)->>'id')::uuid",
        'target': "((md.description)->>'id')::uuid",
        'index': 'row_number() OVER (PARTITION BY a.oid ORDER BY m.roleid)',
    }

    bases_link_query = f'''
        SELECT
            {format_fields(schema, bases, bases_link_query_fields)}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
            INNER JOIN pg_auth_members m ON m.member = a.oid
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(m.roleid, 'pg_authid')
                        AS description
            ) AS md
    '''

    ancestors_link_query = f'''
        SELECT
            {format_fields(schema, ancestors, bases_link_query_fields)}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
            INNER JOIN pg_auth_members m ON m.member = a.oid
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(m.roleid, 'pg_authid')
                        AS description
            ) AS md
    '''

    annos_link_fields = {
        'source': "((d.description)->>'id')::uuid",
        'target': "(annotations->>'id')::uuid",
        'value': "(annotations->>'value')::text",
        'owned': "(annotations->>'owned')::bool",
    }

    annos_link_query = f'''
        SELECT
            {format_fields(schema, annos, annos_link_fields)}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(
                        (d.description)->'annotations'
                    )
                ) AS annotations
    '''

    int_annos_link_fields = {
        'source': "((d.description)->>'id')::uuid",
        'target': "(annotations->>'id')::uuid",
        'owned': "(annotations->>'owned')::bool",
    }

    int_annos_link_query = f'''
        SELECT
            {format_fields(schema, int_annos, int_annos_link_fields)}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(
                        (d.description)->'annotations__internal'
                    )
                ) AS annotations
    '''

    objects = {
        Role: view_query,
        member_of: member_of_link_query,
        bases: bases_link_query,
        ancestors: ancestors_link_query,
        annos: annos_link_query,
        int_annos: int_annos_link_query,
    }

    views: list[dbops.View] = []
    for obj, query in objects.items():
        tabview = trampoline.VersionedView(
            name=tabname(schema, obj), query=query)
        views.append(tabview)

    return views


def _generate_single_role_views(schema: s_schema.Schema) -> List[dbops.View]:
    Role = schema.get('sys::Role', type=s_objtypes.ObjectType)
    member_of = Role.getptr(
        schema, s_name.UnqualName('member_of'), type=s_links.Link)
    bases = Role.getptr(
        schema, s_name.UnqualName('bases'), type=s_links.Link)
    ancestors = Role.getptr(
        schema, s_name.UnqualName('ancestors'), type=s_links.Link)
    annos = Role.getptr(
        schema, s_name.UnqualName('annotations'), type=s_links.Link)
    int_annos = Role.getptr(
        schema, s_name.UnqualName('annotations__internal'), type=s_links.Link)
    view_query_fields = {
        'id': "(json->>'id')::uuid",
        'name': "json->>'name'",
        'name__internal': "json->>'name'",
        'superuser': 'True',
        'abstract': 'False',
        'is_derived': 'False',
        'inherited_fields': 'ARRAY[]::text[]',
        'computed_fields': 'ARRAY[]::text[]',
        'builtin': 'True',
        'internal': 'False',
        'password': "json->>'password_hash'",
    }

    view_query = f'''
        SELECT
            {format_fields(schema, Role, view_query_fields)}
        FROM
            edgedbinstdata_VER.instdata
        WHERE
            key = 'single_role_metadata'
            AND json->>'tenant_id' = edgedb_VER.get_backend_tenant_id()
    '''

    member_of_link_query_fields = {
        'source': "'00000000-0000-0000-0000-000000000000'::uuid",
        'target': "'00000000-0000-0000-0000-000000000000'::uuid",
    }

    member_of_link_query = f'''
        SELECT
            {format_fields(schema, member_of, member_of_link_query_fields)}
        LIMIT 0
    '''

    bases_link_query_fields = {
        'source': "'00000000-0000-0000-0000-000000000000'::uuid",
        'target': "'00000000-0000-0000-0000-000000000000'::uuid",
        'index': "0::bigint",
    }

    bases_link_query = f'''
        SELECT
            {format_fields(schema, bases, bases_link_query_fields)}
        LIMIT 0
    '''

    ancestors_link_query = f'''
        SELECT
            {format_fields(schema, ancestors, bases_link_query_fields)}
        LIMIT 0
    '''

    annos_link_fields = {
        'source': "(json->>'id')::uuid",
        'target': "(annotations->>'id')::uuid",
        'value': "(annotations->>'value')::text",
        'owned': "(annotations->>'owned')::bool",
    }

    annos_link_query = f'''
        SELECT
            {format_fields(schema, annos, annos_link_fields)}
        FROM
            edgedbinstdata_VER.instdata
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(json->'annotations')
                ) AS annotations
        WHERE
            key = 'single_role_metadata'
            AND json->>'tenant_id' = edgedb_VER.get_backend_tenant_id()
    '''

    int_annos_link_fields = {
        'source': "(json->>'id')::uuid",
        'target': "(annotations->>'id')::uuid",
        'owned': "(annotations->>'owned')::bool",
    }

    int_annos_link_query = f'''
        SELECT
            {format_fields(schema, int_annos, int_annos_link_fields)}
        FROM
            edgedbinstdata_VER.instdata
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(json->'annotations__internal')
                ) AS annotations
        WHERE
            key = 'single_role_metadata'
            AND json->>'tenant_id' = edgedb_VER.get_backend_tenant_id()
    '''

    objects = {
        Role: view_query,
        member_of: member_of_link_query,
        bases: bases_link_query,
        ancestors: ancestors_link_query,
        annos: annos_link_query,
        int_annos: int_annos_link_query,
    }

    views: list[dbops.View] = []
    for obj, query in objects.items():
        tabview = trampoline.VersionedView(
            name=tabname(schema, obj), query=query)
        views.append(tabview)

    return views


def _generate_schema_ver_views(schema: s_schema.Schema) -> List[dbops.View]:
    Ver = schema.get(
        'sys::GlobalSchemaVersion',
        type=s_objtypes.ObjectType,
    )

    view_fields = {
        'id': "(v.value->>'id')::uuid",
        'name': "(v.value->>'name')",
        'name__internal': "(v.value->>'name')",
        'version': "(v.value->>'version')::uuid",
        'builtin': "(v.value->>'builtin')::bool",
        'internal': "(v.value->>'internal')::bool",
        'computed_fields': 'ARRAY[]::text[]',
    }

    view_query = f'''
        SELECT
            {format_fields(schema, Ver, view_fields)}
        FROM
            jsonb_each(
                edgedb_VER.get_database_metadata(
                    {ql(defines.EDGEDB_TEMPLATE_DB)}
                ) -> 'GlobalSchemaVersion'
            ) AS v
    '''

    objects = {
        Ver: view_query
    }

    views: list[dbops.View] = []
    for obj, query in objects.items():
        tabview = trampoline.VersionedView(
            name=tabname(schema, obj), query=query)
        views.append(tabview)

    return views


def _generate_stats_views(schema: s_schema.Schema) -> List[dbops.View]:
    QueryStats = schema.get(
        'sys::QueryStats',
        type=s_objtypes.ObjectType,
    )
    pvd = common.get_backend_name(
        schema,
        QueryStats
            .getptr(schema, s_name.UnqualName("protocol_version"))
            .get_target(schema)  # type: ignore
    )
    QueryType = schema.get(
        'sys::QueryType',
        type=s_scalars.ScalarType,
    )
    query_type_domain = common.get_backend_name(schema, QueryType)
    type_mapping = {
        str(v): k for k, v in defines.QueryType.__members__.items()
    }
    output_format_domain = common.get_backend_name(
        schema, schema.get('sys::OutputFormat', type=s_scalars.ScalarType)
    )

    def float64_to_duration_t(val: str) -> str:
        return f"({val} * interval '1ms')::edgedbt.duration_t"

    query_stats_fields = {
        'id': "s.id",
        'name': "s.id::text",
        'name__internal': "s.queryid::text",
        'builtin': "false",
        'internal': "false",
        'computed_fields': 'ARRAY[]::text[]',

        'compilation_config': "s.extras->'cc'",
        'protocol_version': f"ROW(s.extras->'pv'->0, s.extras->'pv'->1)::{pvd}",
        'default_namespace': "s.extras->>'dn'",
        'namespace_aliases': "s.extras->'na'",
        'output_format': f"(s.extras->>'of')::{output_format_domain}",
        'expect_one': "(s.extras->'e1')::boolean",
        'implicit_limit': "(s.extras->'il')::bigint",
        'inline_typeids': "(s.extras->'ii')::boolean",
        'inline_typenames': "(s.extras->'in')::boolean",
        'inline_objectids': "(s.extras->'io')::boolean",

        'branch': "((d.description)->>'id')::uuid",
        'query': "s.query",
        'query_type': f"(t.mapping->>s.stmt_type::text)::{query_type_domain}",
        'tag': "s.tag",

        'plans': 's.plans',
        'total_plan_time': float64_to_duration_t('s.total_plan_time'),
        'min_plan_time': float64_to_duration_t('s.min_plan_time'),
        'max_plan_time': float64_to_duration_t('s.max_plan_time'),
        'mean_plan_time': float64_to_duration_t('s.mean_plan_time'),
        'stddev_plan_time': float64_to_duration_t('s.stddev_plan_time'),

        'calls': 's.calls',
        'total_exec_time': float64_to_duration_t('s.total_exec_time'),
        'min_exec_time': float64_to_duration_t('s.min_exec_time'),
        'max_exec_time': float64_to_duration_t('s.max_exec_time'),
        'mean_exec_time': float64_to_duration_t('s.mean_exec_time'),
        'stddev_exec_time': float64_to_duration_t('s.stddev_exec_time'),

        'rows': 's.rows',
        'stats_since': 's.stats_since::edgedbt.timestamptz_t',
        'minmax_stats_since': 's.minmax_stats_since::edgedbt.timestamptz_t',
    }

    query_stats_query = fr'''
        SELECT
            {format_fields(schema, QueryStats, query_stats_fields)}
        FROM
            edgedbext.edb_stat_statements AS s
            INNER JOIN pg_database dat ON s.dbid = dat.oid
            CROSS JOIN LATERAL (
                SELECT
                    edgedb_VER.shobj_metadata(dat.oid, 'pg_database')
                        AS description
            ) AS d
            CROSS JOIN LATERAL (
                SELECT {ql(json.dumps(type_mapping))}::jsonb AS mapping
            ) AS t
        WHERE
            s.id IS NOT NULL
            AND (d.description)->>'id' IS NOT NULL
            AND (d.description)->>'tenant_id'
                = edgedb_VER.get_backend_tenant_id()
            AND t.mapping ? s.stmt_type::text
    '''

    objects = {
        QueryStats: query_stats_query,
    }

    views: list[dbops.View] = []
    for obj, query in objects.items():
        tabview = trampoline.VersionedView(
            name=tabname(schema, obj), query=query)
        views.append(tabview)

    return views


def _make_json_caster(
    schema: s_schema.Schema,
    stype: s_types.Type,
    versioned: bool,
) -> Callable[[str], str]:
    cast_expr = qlast.TypeCast(
        expr=qlast.TypeCast(
            expr=qlast.Parameter(name="__replaceme__"),
            type=s_utils.typeref_to_ast(schema, schema.get('std::json')),
        ),
        type=s_utils.typeref_to_ast(schema, stype),
    )

    cast_ir = qlcompiler.compile_ast_fragment_to_ir(
        cast_expr,
        schema,
    )

    cast_sql_res = compiler.compile_ir_to_sql_tree(
        cast_ir,
        named_param_prefix=(),
        singleton_mode=True,
        versioned_singleton=versioned,
    )
    cast_sql = codegen.generate_source(cast_sql_res.ast)

    return lambda val: cast_sql.replace('__replaceme__', val)


def _generate_schema_alias_views(
    schema: s_schema.Schema,
    module: s_name.UnqualName,
) -> List[dbops.View]:
    views = []

    schema_objs = schema.get_objects(
        type=s_objtypes.ObjectType,
        included_modules=(module,),
    )

    for schema_obj in schema_objs:
        if not schema_obj.get_from_alias(schema):
            views.append(_generate_schema_alias_view(schema, schema_obj))

    return views


def _generate_schema_alias_view(
    schema: s_schema.Schema,
    obj: s_sources.Source | s_pointers.Pointer,
) -> dbops.View:

    name = _schema_alias_view_name(schema, obj)
    select = inheritance.get_inheritance_view(schema, obj)

    return trampoline.VersionedView(
        name=name,
        query=codegen.generate_source(select),
    )


def _schema_alias_view_name(
    schema: s_schema.Schema,
    obj: s_sources.Source | s_pointers.Pointer,
) -> tuple[str, str]:
    module = obj.get_name(schema).module
    prefix = module.capitalize()

    if isinstance(obj, s_links.Link):
        objtype = obj.get_source(schema)
        assert objtype is not None
        objname = objtype.get_name(schema).name
        lname = obj.get_shortname(schema).name
        name = f'_{prefix}{objname}__{lname}'
    else:
        name = f'_{prefix}{obj.get_name(schema).name}'

    return ('edgedb', name)


def _generate_sql_information_schema(
    backend_version: params.BackendVersion
) -> List[dbops.Command]:

    # Helper to create wrappers around materialized views.  For
    # performance, we use MATERIALIZED VIEW for some of our SQL
    # emulation tables. Unfortunately we can't use those directly,
    # since we need tableoid to match the real pg_catalog table.
    def make_wrapper_view(name: str) -> trampoline.VersionedView:
        return trampoline.VersionedView(
            name=("edgedbsql", name),
            query=f"""
            SELECT *,
            'pg_catalog.{name}'::regclass::oid as tableoid,
            xmin, cmin, xmax, cmax, ctid
            FROM edgedbsql_VER.{name}_
            """,
        )

    # A helper view that contains all data tables we expose over SQL, excluding
    # introspection tables.
    # It contains table & schema names and associated module id.
    virtual_tables = trampoline.VersionedView(
        name=('edgedbsql', 'virtual_tables'),
        materialized=True,
        query='''
        WITH obj_ty_pre AS (
            SELECT
                id,
                REGEXP_REPLACE(name, '::[^:]*$', '') AS module_name,
                REGEXP_REPLACE(name, '^.*::', '') as table_name
            FROM edgedb_VER."_SchemaObjectType"
            WHERE internal IS NOT TRUE
        ),
        obj_ty AS (
            SELECT
                id,
                REGEXP_REPLACE(module_name, '^default(?=::|$)', 'public')
                    AS schema_name,
                module_name,
                table_name
            FROM obj_ty_pre
        ),
        all_tables (id, schema_name, module_name, table_name) AS ((
            SELECT * FROM obj_ty
        ) UNION ALL (
            WITH qualified_links AS (
                -- multi links and links with at least one property
                -- (besides source and target)
                SELECT link.id
                FROM edgedb_VER."_SchemaLink" link
                JOIN edgedb_VER."_SchemaProperty" AS prop
                  ON link.id = prop.source
                WHERE prop.computable IS NOT TRUE AND prop.internal IS NOT TRUE
                GROUP BY link.id, link.cardinality
                HAVING link.cardinality = 'Many' OR COUNT(*) > 2
            )
            SELECT link.id, obj_ty.schema_name, obj_ty.module_name,
                CONCAT(obj_ty.table_name, '.', link.name) AS table_name
            FROM edgedb_VER."_SchemaLink" link
            JOIN obj_ty ON obj_ty.id = link.source
            WHERE link.id IN (SELECT * FROM qualified_links)
        ) UNION ALL (
            -- multi properties
            SELECT prop.id, obj_ty.schema_name, obj_ty.module_name,
                CONCAT(obj_ty.table_name, '.', prop.name) AS table_name
            FROM edgedb_VER."_SchemaProperty" AS prop
            JOIN obj_ty ON obj_ty.id = prop.source
            WHERE prop.computable IS NOT TRUE
            AND prop.internal IS NOT TRUE
            AND prop.cardinality = 'Many'
        ))
        SELECT
            at.id,
            schema_name,
            table_name,
            sm.id AS module_id,
            pt.oid AS pg_type_id
        FROM all_tables at
        JOIN edgedb_VER."_SchemaModule" sm ON sm.name = at.module_name
        LEFT JOIN pg_type pt ON pt.typname = at.id::text
        WHERE schema_name not in (
            'cfg', 'sys', 'schema', 'std', 'std::net', 'std::net::http'
        )
        '''
    )
    # A few tables in here were causing problems, so let's hide them as an
    # implementation detail.
    # To be more specific:
    # - following tables were missing from information_schema:
    #   Link.properties, ObjectType.links, ObjectType.properties
    # - even though introspection worked, I wasn't able to select from some
    #   tables in cfg and sys

    # For making up oids of schemas that represent modules
    uuid_to_oid = trampoline.VersionedFunction(
        name=('edgedbsql', 'uuid_to_oid'),
        args=(
            ('id', 'uuid'),
            # extra is two extra bits to throw into the oid, for now
            ('extra', 'int4', '0'),
        ),
        returns=('oid',),
        volatility='immutable',
        text="""
            SELECT (
                ('x' || substring(id::text, 2, 7))::bit(28)::bigint*4 + extra
                 + 40000)::oid;
        """
    )
    long_name = trampoline.VersionedFunction(
        name=('edgedbsql', '_long_name'),
        args=[
            ('origname', ('text',)),
            ('longname', ('text',)),
        ],
        returns=('text',),
        volatility='stable',
        text=r'''
            SELECT CASE WHEN length(longname) > 63
                THEN left(longname, 55) || left(origname, 8)
                ELSE longname
                END
        '''
    )
    type_rename = trampoline.VersionedFunction(
        name=('edgedbsql', '_pg_type_rename'),
        args=[
            ('typeoid', ('oid',)),
            ('typename', ('name',)),
        ],
        returns=('name',),
        volatility='stable',
        text=r'''
            SELECT COALESCE (
                -- is the name in virtual_tables?
                (
                    SELECT vt.table_name::name
                    FROM edgedbsql_VER.virtual_tables vt
                    WHERE vt.pg_type_id = typeoid
                ),
                -- is this a scalar or tuple?
                (
                    SELECT name::name
                    FROM (
                        -- get the built-in scalars
                        SELECT
                            split_part(name, '::', 2) AS name,
                            backend_id
                        FROM edgedb_VER."_SchemaScalarType"
                        WHERE NOT builtin AND arg_values IS NULL
                        UNION ALL
                        -- get the tuples
                        SELECT
                            edgedbsql_VER._long_name(typename, name),
                            backend_id
                        FROM edgedb_VER."_SchemaTuple"
                    ) x
                    WHERE x.backend_id = typeoid
                ),
                typename
            )
        '''
    )
    namespace_rename = trampoline.VersionedFunction(
        name=('edgedbsql', '_pg_namespace_rename'),
        args=[
            ('typeoid', ('oid',)),
            ('typens', ('oid',)),
        ],
        returns=('oid',),
        volatility='stable',
        text=r'''
            WITH
                nspub AS (
                    SELECT oid FROM pg_namespace WHERE nspname = 'edgedbpub'
                ),
                nsdef AS (
                    SELECT edgedbsql_VER.uuid_to_oid(id) AS oid
                    FROM edgedb_VER."_SchemaModule"
                    WHERE name = 'default'
                )
            SELECT COALESCE (
                (
                    SELECT edgedbsql_VER.uuid_to_oid(vt.module_id)
                    FROM edgedbsql_VER.virtual_tables vt
                    WHERE vt.pg_type_id = typeoid
                ),
                -- just replace "edgedbpub" with "public"
                (SELECT nsdef.oid WHERE typens = nspub.oid),
                typens
            )
            FROM
                nspub,
                nsdef
        '''
    )

    sql_ident = 'information_schema.sql_identifier'
    sql_str = 'information_schema.character_data'
    sql_bool = 'information_schema.yes_or_no'
    sql_card = 'information_schema.cardinal_number'
    tables_and_columns = [
        trampoline.VersionedView(
            name=('edgedbsql', 'tables'),
            query=(
                f'''
        SELECT
            edgedb_VER.get_current_database()::{sql_ident} AS table_catalog,
            vt.schema_name::{sql_ident} AS table_schema,
            vt.table_name::{sql_ident} AS table_name,
            ist.table_type,
            ist.self_referencing_column_name,
            ist.reference_generation,
            ist.user_defined_type_catalog,
            ist.user_defined_type_schema,
            ist.user_defined_type_name,
            ist.is_insertable_into,
            ist.is_typed,
            ist.commit_action
        FROM information_schema.tables ist
        JOIN edgedbsql_VER.virtual_tables vt ON vt.id::text = ist.table_name
            '''
            ),
        ),
        trampoline.VersionedView(
            name=('edgedbsql', 'columns'),
            query=(
                f'''
        SELECT
            edgedb_VER.get_current_database()::{sql_ident} AS table_catalog,
            vt_table_schema::{sql_ident} AS table_schema,
            vt_table_name::{sql_ident} AS table_name,
            v_column_name::{sql_ident} as column_name,
            ROW_NUMBER() OVER (
                PARTITION BY vt_table_schema, vt_table_name
                ORDER BY position, v_column_name
            ) AS ordinal_position,
            column_default,
            is_nullable,
            data_type,
            NULL::{sql_card} AS character_maximum_length,
            NULL::{sql_card} AS character_octet_length,
            NULL::{sql_card} AS numeric_precision,
            NULL::{sql_card} AS numeric_precision_radix,
            NULL::{sql_card} AS numeric_scale,
            NULL::{sql_card} AS datetime_precision,
            NULL::{sql_str} AS interval_type,
            NULL::{sql_card} AS interval_precision,
            NULL::{sql_ident} AS character_set_catalog,
            NULL::{sql_ident} AS character_set_schema,
            NULL::{sql_ident} AS character_set_name,
            NULL::{sql_ident} AS collation_catalog,
            NULL::{sql_ident} AS collation_schema,
            NULL::{sql_ident} AS collation_name,
            NULL::{sql_ident} AS domain_catalog,
            NULL::{sql_ident} AS domain_schema,
            NULL::{sql_ident} AS domain_name,
            edgedb_VER.get_current_database()::{sql_ident} AS udt_catalog,
            'pg_catalog'::{sql_ident} AS udt_schema,
            NULL::{sql_ident} AS udt_name,
            NULL::{sql_ident} AS scope_catalog,
            NULL::{sql_ident} AS scope_schema,
            NULL::{sql_ident} AS scope_name,
            NULL::{sql_card} AS maximum_cardinality,
            0::{sql_ident} AS dtd_identifier,
            'NO'::{sql_bool} AS is_self_referencing,
            'NO'::{sql_bool} AS is_identity,
            NULL::{sql_str} AS identity_generation,
            NULL::{sql_str} AS identity_start,
            NULL::{sql_str} AS identity_increment,
            NULL::{sql_str} AS identity_maximum,
            NULL::{sql_str} AS identity_minimum,
            'NO' ::{sql_bool} AS identity_cycle,
            'NEVER'::{sql_str} AS is_generated,
            NULL::{sql_str} AS generation_expression,
            'YES'::{sql_bool} AS is_updatable
        FROM (
        SELECT
            vt.schema_name AS vt_table_schema,
            vt.table_name AS vt_table_name,
            COALESCE(
                -- this happends for id and __type__
                spec.name,

                -- fallback to pointer name, with suffix '_id' for links
                sp.name || case when sl.id is not null then '_id' else '' end
            ) AS v_column_name,
            COALESCE(spec.position, 2) AS position,
            (sp.expr IS NOT NULL) AS is_computed,
            isc.column_default,
            CASE WHEN sp.required OR spec.k IS NOT NULL
                THEN 'NO' ELSE 'YES' END AS is_nullable,

            -- HACK: computeds don't have backing rows in isc,
            -- so we just default to 'text'. This is wrong.
            COALESCE(isc.data_type, 'text') AS data_type
        FROM edgedb_VER."_SchemaPointer" sp
        LEFT JOIN information_schema.columns isc ON (
            isc.table_name = sp.source::TEXT AND CASE
                WHEN length(isc.column_name) = 36 -- if column name is uuid
                THEN isc.column_name = sp.id::text -- compare uuids
                ELSE isc.column_name = sp.name -- for id, source, target
            END
        )

        -- needed for attaching `_id`
        LEFT JOIN edgedb_VER."_SchemaLink" sl ON sl.id = sp.id

        -- needed for determining table name
        JOIN edgedbsql_VER.virtual_tables vt ON vt.id = sp.source

        -- positions for special pointers
        -- duplicate id get both id and __type__ columns out of it
        LEFT JOIN (
            VALUES  ('id', 'id', 0),
                    ('id', '__type__', 1),
                    ('source', 'source', 0),
                    ('target', 'target', 1)
        ) spec(k, name, position) ON (spec.k = isc.column_name)

        WHERE isc.column_name IS NOT NULL -- normal pointers
           OR sp.expr IS NOT NULL AND sp.cardinality <> 'Many' -- computeds

        UNION ALL

        -- special case: multi properties source and target
        -- (this is needed, because schema does not create pointers for
        -- these two columns)
        SELECT
            vt.schema_name AS vt_table_schema,
            vt.table_name AS vt_table_name,
            isc.column_name AS v_column_name,
            spec.position as position,
            FALSE as is_computed,
            isc.column_default,
            'NO' as is_nullable,
            isc.data_type as data_type
        FROM edgedb_VER."_SchemaPointer" sp
        JOIN information_schema.columns isc ON isc.table_name = sp.id::TEXT

        -- needed for filtering out links
        LEFT JOIN edgedb_VER."_SchemaLink" sl ON sl.id = sp.id

        -- needed for determining table name
        JOIN edgedbsql_VER.virtual_tables vt ON vt.id = sp.id

        -- positions for special pointers
        JOIN (
            VALUES  ('source', 'source', 0),
                    ('target', 'target', 1)
        ) spec(k, name, position) ON (spec.k = isc.column_name)

        WHERE
            sl.id IS NULL -- property (non-link)
            AND sp.cardinality = 'Many' -- multi
            AND sp.expr IS NULL -- non-computed
        ) t
            '''
            ),
        ),
    ]

    pg_catalog_views = [
        trampoline.VersionedView(
            name=("edgedbsql", "pg_namespace_"),
            materialized=True,
            query="""
        -- system schemas
        SELECT
            oid,
            nspname,
            nspowner,
            nspacl
        FROM pg_namespace
        WHERE nspname IN ('pg_catalog', 'pg_toast', 'information_schema',
                          'edgedb', 'edgedbstd', 'edgedbt',
                          'edgedb_VER', 'edgedbstd_VER')
        UNION ALL

        -- virtual schemas
        SELECT
            edgedbsql_VER.uuid_to_oid(t.module_id)  AS oid,
            t.schema_name                       AS nspname,
            (SELECT oid
             FROM pg_roles
             WHERE rolname = CURRENT_USER
             LIMIT 1)                           AS nspowner,
            NULL AS nspacl
        FROM (
            SELECT schema_name, module_id
            FROM edgedbsql_VER.virtual_tables

            UNION

            -- always include the default module,
            -- because it is needed for tuple types
            SELECT 'public' AS schema_name, id AS module_id
            FROM edgedb_VER."_SchemaModule"
            WHERE name = 'default'
        ) t
        """,
        ),
        make_wrapper_view("pg_namespace"),
        trampoline.VersionedView(
            name=("edgedbsql", "pg_type_"),
            materialized=True,
            query="""
        SELECT
            pt.oid,
            edgedbsql_VER._pg_type_rename(pt.oid, pt.typname)
                AS typname,
            edgedbsql_VER._pg_namespace_rename(pt.oid, pt.typnamespace)
                AS typnamespace,
            {0}
        FROM pg_type pt
        JOIN pg_namespace pn ON pt.typnamespace = pn.oid
        WHERE
            nspname IN ('pg_catalog', 'pg_toast', 'information_schema',
                        'edgedb', 'edgedbstd', 'edgedb_VER', 'edgedbstd_VER',
                        'edgedbpub', 'edgedbt')
        """.format(
                ",".join(
                    f"pt.{col}"
                    for col, _, _ in sql_introspection.PG_CATALOG["pg_type"][3:]
                )
            ),
        ),
        make_wrapper_view("pg_type"),
        # pg_class that contains classes only for tables
        # This is needed so we can use it to filter pg_index to indexes only on
        # visible tables.
        trampoline.VersionedView(
            name=("edgedbsql", "pg_class_tables"),
            materialized=True,
            query="""
        -- Postgres tables
        SELECT pc.*
        FROM pg_class pc
        JOIN pg_namespace pn ON pc.relnamespace = pn.oid
        WHERE nspname IN ('pg_catalog', 'pg_toast', 'information_schema')

        UNION ALL

        -- user-defined tables
        SELECT
            oid,
            vt.table_name as relname,
            edgedbsql_VER.uuid_to_oid(vt.module_id) as relnamespace,
            reltype,
            reloftype,
            relowner,
            relam,
            relfilenode,
            reltablespace,
            relpages,
            reltuples,
            relallvisible,
            reltoastrelid,
            relhasindex,
            relisshared,
            relpersistence,
            relkind,
            relnatts,
            0 as relchecks, -- don't care about CHECK constraints
            relhasrules,
            relhastriggers,
            relhassubclass,
            relrowsecurity,
            relforcerowsecurity,
            relispopulated,
            relreplident,
            relispartition,
            relrewrite,
            relfrozenxid,
            relminmxid,
            relacl,
            reloptions,
            relpartbound
        FROM pg_class pc
        JOIN edgedbsql_VER.virtual_tables vt ON vt.pg_type_id = pc.reltype
        """,
        ),
        trampoline.VersionedView(
            name=("edgedbsql", "pg_index_"),
            materialized=True,
            query=f"""
        SELECT
            pi.indexrelid,
            pi.indrelid,
            pi.indnatts,
            pi.indnkeyatts,
            CASE
                WHEN COALESCE(is_id.t, FALSE) THEN TRUE
                ELSE pi.indisprimary
            END AS indisunique,
            {'pi.indnullsnotdistinct,' if backend_version.major >= 15 else ''}
            CASE
                WHEN COALESCE(is_id.t, FALSE) THEN TRUE
                ELSE pi.indisprimary
            END AS indisprimary,
            pi.indisexclusion,
            pi.indimmediate,
            pi.indisclustered,
            pi.indisvalid,
            pi.indcheckxmin,
            CASE
                WHEN COALESCE(is_id.t, FALSE) THEN TRUE
                ELSE FALSE -- override so pg_dump won't try to recreate them
            END AS indisready,
            pi.indislive,
            pi.indisreplident,
            CASE
                WHEN COALESCE(is_id.t, FALSE) THEN ARRAY[1]::int2vector -- id: 1
                ELSE pi.indkey
            END AS indkey,
            pi.indcollation,
            pi.indclass,
            pi.indoption,
            pi.indexprs,
            pi.indpred
        FROM pg_index pi

        -- filter by tables visible in pg_class
        INNER JOIN edgedbsql_VER.pg_class_tables pr ON pi.indrelid = pr.oid

        -- find indexes that are on virtual tables and on `id` columns
        LEFT JOIN LATERAL (
            SELECT TRUE AS t
            FROM pg_attribute pa
            WHERE pa.attrelid = pi.indrelid
              AND pa.attnum = ANY(pi.indkey)
              AND pa.attname = 'id'
        ) is_id ON TRUE

        -- for our tables show only primary key indexes
        LEFT JOIN edgedbsql_VER.virtual_tables vt ON vt.pg_type_id = pr.reltype
        WHERE vt.id IS NULL OR is_id.t IS NOT NULL
        """,
        ),
        make_wrapper_view('pg_index'),
        trampoline.VersionedView(
            name=("edgedbsql", "pg_class_"),
            materialized=True,
            query="""
        -- tables
        SELECT pc.*
        FROM edgedbsql_VER.pg_class_tables pc

        UNION

        -- indexes
        SELECT pc.*
        FROM pg_class pc
        JOIN pg_index pi ON pc.oid = pi.indexrelid

        UNION

        -- compound types (tuples)
        SELECT
            pc.oid,
            edgedbsql_VER._long_name(pc.reltype::text, tup.name) as relname,
            nsdef.oid as relnamespace,
            pc.reltype,
            pc.reloftype,
            pc.relowner,
            pc.relam,
            pc.relfilenode,
            pc.reltablespace,
            pc.relpages,
            pc.reltuples,
            pc.relallvisible,
            pc.reltoastrelid,
            pc.relhasindex,
            pc.relisshared,
            pc.relpersistence,
            pc.relkind,
            pc.relnatts,
            0 as relchecks, -- don't care about CHECK constraints
            pc.relhasrules,
            pc.relhastriggers,
            pc.relhassubclass,
            pc.relrowsecurity,
            pc.relforcerowsecurity,
            pc.relispopulated,
            pc.relreplident,
            pc.relispartition,
            pc.relrewrite,
            pc.relfrozenxid,
            pc.relminmxid,
            pc.relacl,
            pc.reloptions,
            pc.relpartbound
        FROM pg_class pc
        JOIN edgedb_VER."_SchemaTuple" tup ON tup.backend_id = pc.reltype
        JOIN (
            SELECT edgedbsql_VER.uuid_to_oid(id) AS oid
            FROM edgedb_VER."_SchemaModule"
            WHERE name = 'default'
        ) nsdef ON TRUE
        """,
        ),
        make_wrapper_view("pg_class"),
        # Because we hide some columns and
        # because pg_dump expects attnum to be sequential numbers
        # we have to invent new attnums with ROW_NUMBER().
        # Since attnum is used elsewhere, we need to know the mapping from
        # constructed attnum into underlying attnum.
        # To do that, we have pg_attribute_ext view with additional
        # attnum_internal column.
        trampoline.VersionedView(
            name=("edgedbsql", "pg_attribute_ext"),
            materialized=True,
            query=r"""
        SELECT attrelid,
            attname,
            atttypid,
            attstattarget,
            attlen,
            attnum,
            attnum as attnum_internal,
            attndims,
            attcacheoff,
            atttypmod,
            attbyval,
            attstorage,
            attalign,
            attnotnull,
            atthasdef,
            atthasmissing,
            attidentity,
            attgenerated,
            attisdropped,
            attislocal,
            attinhcount,
            attcollation,
            attacl,
            attoptions,
            attfdwoptions,
            null::int[] as attmissingval
        FROM pg_attribute pa
        JOIN pg_class pc ON pa.attrelid = pc.oid
        JOIN pg_namespace pn ON pc.relnamespace = pn.oid
        LEFT JOIN edgedb_VER."_SchemaTuple" tup ON tup.backend_id = pc.reltype
        WHERE
            nspname IN ('pg_catalog', 'pg_toast', 'information_schema')
            OR
            tup.backend_id IS NOT NULL

        UNION ALL

        SELECT pc_oid as attrelid,
            col_name as attname,
            COALESCE(atttypid, 25) as atttypid, -- defaults to TEXT
            COALESCE(attstattarget, -1) as attstattarget,
            COALESCE(attlen, -1) as attlen,
            (ROW_NUMBER() OVER (
                PARTITION BY pc_oid
                ORDER BY col_position, col_name
            ) - 6)::smallint AS attnum,
            t.attnum as attnum_internal,
            COALESCE(attndims, 0) as attndims,
            COALESCE(attcacheoff, -1) as attcacheoff,
            COALESCE(atttypmod, -1) as atttypmod,
            COALESCE(attbyval, FALSE) as attbyval,
            COALESCE(attstorage, 'x') as attstorage,
            COALESCE(attalign, 'i') as attalign,
            required as attnotnull,
            -- Always report no default, to avoid expr trouble
            false as atthasdef,
            COALESCE(atthasmissing, FALSE) as atthasmissing,
            COALESCE(attidentity, '') as attidentity,
            COALESCE(attgenerated, '') as attgenerated,
            COALESCE(attisdropped, FALSE) as attisdropped,
            COALESCE(attislocal, TRUE) as attislocal,
            COALESCE(attinhcount, 0) as attinhcount,
            COALESCE(attcollation, 0) as attcollation,
            attacl,
            attoptions,
            attfdwoptions,
            null::int[] as attmissingval
        FROM (
        SELECT
            COALESCE(
                spec.name, -- for special columns
                sp.name || case when sl.id is not null then '_id' else '' end,
                pa.attname -- for system columns
            ) as col_name,
            COALESCE(spec.position, 2) AS col_position,
            (sp.required IS TRUE OR spec.k IS NOT NULL) as required,
            pc.oid AS pc_oid,
            pa.*

        FROM edgedb_VER."_SchemaPointer" sp
        JOIN edgedbsql_VER.virtual_tables vt ON vt.id = sp.source
        JOIN pg_class pc ON pc.reltype = vt.pg_type_id

        -- try to find existing pg_attribute (it will not exist for computeds)
        LEFT JOIN pg_attribute pa ON (
            pa.attrelid = pc.oid AND CASE
                WHEN length(pa.attname) = 36 -- if column name is uuid
                THEN pa.attname = sp.id::text -- compare uuids
                ELSE pa.attname = sp.name -- for id, source, target
            END
        )

        -- positions for special pointers
        -- duplicate id get both id and __type__ columns out of it
        LEFT JOIN (
            VALUES  ('id', 'id', 0),
                    ('id', '__type__', 1),
                    ('source', 'source', 0),
                    ('target', 'target', 1)
        ) spec(k, name, position) ON (spec.k = pa.attname)

        -- needed for attaching `_id`
        LEFT JOIN edgedb_VER."_SchemaLink" sl ON sl.id = sp.id

        WHERE pa.attname IS NOT NULL -- non-computed pointers
           OR sp.expr IS NOT NULL AND sp.cardinality <> 'Many' -- computeds

        UNION ALL

        -- special case: multi properties source and target
        -- (this is needed, because schema does not create pointers for
        -- these two columns)
        SELECT
            pa.attname AS col_name,
            spec.position as position,
            TRUE as required,
            pa.attrelid as pc_oid,
            pa.*
        FROM edgedb_VER."_SchemaProperty" sp
        JOIN pg_class pc ON pc.relname = sp.id::TEXT
        JOIN pg_attribute pa ON pa.attrelid = pc.oid

        -- positions for special pointers
        JOIN (
            VALUES  ('source', 0),
                    ('target', 1)
        ) spec(k, position) ON (spec.k = pa.attname)

        WHERE
            sp.cardinality = 'Many' -- multi
            AND sp.expr IS NULL -- non-computed

        UNION ALL

        -- special case: system columns
        SELECT
            pa.attname AS col_name,
            pa.attnum as position,
            TRUE as required,
            pa.attrelid as pc_oid,
            pa.*
        FROM pg_attribute pa
        JOIN pg_class pc ON pc.oid = pa.attrelid
        JOIN edgedbsql_VER.virtual_tables vt ON vt.pg_type_id = pc.reltype
        WHERE pa.attnum < 0
        ) t
        """,
        ),
        trampoline.VersionedView(
            name=("edgedbsql", "pg_attribute"),
            query="""
        SELECT
          attrelid,
          attname,
          atttypid,
          attstattarget,
          attlen,
          attnum,
          attndims,
          attcacheoff,
          atttypmod,
          attbyval,
          attstorage,
          attalign,
          attnotnull,
          atthasdef,
          atthasmissing,
          attidentity,
          attgenerated,
          attisdropped,
          attislocal,
          attinhcount,
          attcollation,
          attacl,
          attoptions,
          attfdwoptions,
          attmissingval,
          'pg_catalog.pg_attribute'::regclass::oid as tableoid,
          xmin,
          cmin,
          xmax,
          cmax,
          ctid
        FROM edgedbsql_VER.pg_attribute_ext
        """,
        ),

        trampoline.VersionedView(
            name=("edgedbsql", "pg_database"),
            query="""
        SELECT
            oid,
            edgedb_VER.get_current_database()::name as datname,
            datdba,
            encoding,
            datcollate,
            datctype,
            datistemplate,
            datallowconn,
            datconnlimit,
            0::oid AS datlastsysoid,
            datfrozenxid,
            datminmxid,
            dattablespace,
            datacl,
            tableoid, xmin, cmin, xmax, cmax, ctid
        FROM pg_database
        WHERE datname LIKE '%_edgedb'
        """,
        ),

        # HACK: there were problems with pg_dump when exposing this table, so
        # I've added WHERE FALSE. The query could be simplified, but it may
        # be needed in the future. Its EXPLAIN cost is 0..0 anyway.
        trampoline.VersionedView(
            name=("edgedbsql", "pg_stats"),
            query="""
        SELECT n.nspname AS schemaname,
            c.relname AS tablename,
            a.attname,
            s.stainherit AS inherited,
            s.stanullfrac AS null_frac,
            s.stawidth AS avg_width,
            s.stadistinct AS n_distinct,
            NULL::real[] AS most_common_vals,
            s.stanumbers1 AS most_common_freqs,
            s.stanumbers1 AS histogram_bounds,
            s.stanumbers1[1] AS correlation,
            NULL::real[] AS most_common_elems,
            s.stanumbers1 AS most_common_elem_freqs,
            s.stanumbers1 AS elem_count_histogram
        FROM pg_statistic s
        JOIN pg_class c ON c.oid = s.starelid
        JOIN edgedbsql_VER.pg_attribute_ext a ON (
            c.oid = a.attrelid and a.attnum_internal = s.staattnum
        )
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE FALSE
        """,
        ),
        trampoline.VersionedView(
            name=("edgedbsql", "pg_constraint"),
            query=r"""
        -- primary keys for:
        --  - objects tables (that contains id)
        --  - link tables (that contains source and target)
        -- there exists a unique constraint for each of these
        SELECT
          pc.oid,
          vt.table_name || '_pk' AS conname,
          pc.connamespace,
          'p'::"char" AS contype,
          pc.condeferrable,
          pc.condeferred,
          pc.convalidated,
          pc.conrelid,
          pc.contypid,
          pc.conindid,
          pc.conparentid,
          NULL::oid AS confrelid,
          NULL::"char" AS confupdtype,
          NULL::"char" AS confdeltype,
          NULL::"char" AS confmatchtype,
          pc.conislocal,
          pc.coninhcount,
          pc.connoinherit,
          CASE WHEN pa.attname = 'id'
            THEN ARRAY[1]::int2[] -- id will always have attnum 1
            ELSE ARRAY[1, 2]::int2[] -- source and target
          END AS conkey,
          NULL::int2[] AS confkey,
          NULL::oid[] AS conpfeqop,
          NULL::oid[] AS conppeqop,
          NULL::oid[] AS conffeqop,
          NULL::int2[] AS confdelsetcols,
          NULL::oid[] AS conexclop,
          pc.conbin,
          pc.tableoid, pc.xmin, pc.cmin, pc.xmax, pc.cmax, pc.ctid
        FROM pg_constraint pc
        JOIN edgedbsql_VER.pg_class_tables pct ON pct.oid = pc.conrelid
        JOIN edgedbsql_VER.virtual_tables vt ON vt.pg_type_id = pct.reltype
        JOIN pg_attribute pa
          ON (pa.attrelid = pct.oid
              AND pa.attnum = ANY(conkey)
              AND pa.attname IN ('id', 'source')
             )
        WHERE contype = 'u' -- our ids and all links will have unique constraint

        UNION ALL

        -- foreign keys for object tables
        SELECT
          -- uuid_to_oid needs "extra" arg to disambiguate from the link table
          -- keys below
          edgedbsql_VER.uuid_to_oid(sl.id, 0) as oid,
          vt.table_name || '_fk_' || sl.name AS conname,
          edgedbsql_VER.uuid_to_oid(vt.module_id) AS connamespace,
          'f'::"char" AS contype,
          FALSE AS condeferrable,
          FALSE AS condeferred,
          TRUE AS convalidated,
          pc.oid AS conrelid,
          0::oid AS contypid,
          0::oid AS conindid, -- let's hope this is not needed
          0::oid AS conparentid,
          pc_target.oid AS confrelid,
          'a'::"char" AS confupdtype,
          'a'::"char" AS confdeltype,
          's'::"char" AS confmatchtype,
          TRUE AS conislocal,
          0::int2 AS coninhcount,
          TRUE AS connoinherit,
          ARRAY[pa.attnum]::int2[] AS conkey,
          ARRAY[1]::int2[] AS confkey, -- id will always have attnum 1
          ARRAY['uuid_eq'::regproc]::oid[] AS conpfeqop,
          ARRAY['uuid_eq'::regproc]::oid[] AS conppeqop,
          ARRAY['uuid_eq'::regproc]::oid[] AS conffeqop,
          NULL::int2[] AS confdelsetcols,
          NULL::oid[] AS conexclop,
          NULL::pg_node_tree AS conbin,
          pa.tableoid, pa.xmin, pa.cmin, pa.xmax, pa.cmax, pa.ctid
        FROM edgedbsql_VER.virtual_tables vt
        JOIN pg_class pc ON pc.reltype = vt.pg_type_id
        JOIN edgedb_VER."_SchemaLink" sl
          ON sl.source = vt.id -- AND COALESCE(sl.cardinality = 'One', TRUE)
        JOIN edgedbsql_VER.virtual_tables vt_target
          ON sl.target = vt_target.id
        JOIN pg_class pc_target ON pc_target.reltype = vt_target.pg_type_id
        JOIN edgedbsql_VER.pg_attribute pa
          ON pa.attrelid = pc.oid
         AND pa.attname = sl.name || '_id'

        UNION ALL

        -- foreign keys for:
        -- - multi link tables (source & target),
        -- - multi property tables (source),
        -- - single link with link properties (source & target),
        -- these constraints do not actually exist, so we emulate it entierly
        SELECT
            -- uuid_to_oid needs "extra" arg to disambiguate from other
            -- constraints using this pointer
            edgedbsql_VER.uuid_to_oid(sp.id, spec.attnum) AS oid,
            vt.table_name || '_fk_' || spec.name AS conname,
            edgedbsql_VER.uuid_to_oid(vt.module_id) AS connamespace,
            'f'::"char" AS contype,
            FALSE AS condeferrable,
            FALSE AS condeferred,
            TRUE AS convalidated,
            pc.oid AS conrelid,
            pc.reltype AS contypid,
            0::oid AS conindid, -- TODO
            0::oid AS conparentid,
            pcf.oid AS confrelid,
            'r'::"char" AS confupdtype,
            'r'::"char" AS confdeltype,
            's'::"char" AS confmatchtype,
            TRUE AS conislocal,
            0::int2 AS coninhcount,
            TRUE AS connoinherit,
            ARRAY[spec.attnum]::int2[] AS conkey,
            ARRAY[1]::int2[] AS confkey,     -- id will have attnum 1
            ARRAY['uuid_eq'::regproc]::oid[] AS conpfeqop,
            ARRAY['uuid_eq'::regproc]::oid[] AS conppeqop,
            ARRAY['uuid_eq'::regproc]::oid[] AS conffeqop,
            NULL::int2[] AS confdelsetcols,
            NULL::oid[] AS conexclop,
            pc.relpartbound AS conbin,
            pc.tableoid,
            pc.xmin,
            pc.cmin,
            pc.xmax,
            pc.cmax,
            pc.ctid
        FROM edgedb_VER."_SchemaPointer" sp

        -- find links with link properties
        LEFT JOIN LATERAL (
            SELECT sl.id
            FROM edgedb_VER."_SchemaLink" sl
            LEFT JOIN edgedb_VER."_SchemaProperty" AS slp ON slp.source = sl.id
            GROUP BY sl.id
            HAVING COUNT(*) > 2
        ) link_props ON link_props.id = sp.id

        JOIN pg_class pc ON pc.relname = sp.id::TEXT
        JOIN edgedbsql_VER.virtual_tables vt ON vt.pg_type_id = pc.reltype

        -- duplicate each row for source and target
        JOIN LATERAL (VALUES
            ('source', 1::int2, sp.source),
            ('target', 2::int2, sp.target)
        ) spec(name, attnum, foreign_id) ON TRUE
        JOIN edgedbsql_VER.virtual_tables vtf ON vtf.id = spec.foreign_id
        JOIN pg_class pcf ON pcf.reltype = vtf.pg_type_id

        WHERE
            sp.cardinality = 'Many' OR link_props.id IS NOT NULL
            AND sp.computable IS NOT TRUE
            AND sp.internal IS NOT TRUE
        """
        ),
        trampoline.VersionedView(
            name=("edgedbsql", "pg_statistic"),
            query="""
        SELECT
            starelid,
            a.attnum as staattnum,
            stainherit,
            stanullfrac,
            stawidth,
            stadistinct,
            stakind1,
            stakind2,
            stakind3,
            stakind4,
            stakind5,
            staop1,
            staop2,
            staop3,
            staop4,
            staop5,
            stacoll1,
            stacoll2,
            stacoll3,
            stacoll4,
            stacoll5,
            stanumbers1,
            stanumbers2,
            stanumbers3,
            stanumbers4,
            stanumbers5,
            NULL::real[] AS stavalues1,
            NULL::real[] AS stavalues2,
            NULL::real[] AS stavalues3,
            NULL::real[] AS stavalues4,
            NULL::real[] AS stavalues5,
            s.tableoid, s.xmin, s.cmin, s.xmax, s.cmax, s.ctid
        FROM pg_statistic s
        JOIN edgedbsql_VER.pg_attribute_ext a ON (
            a.attrelid = s.starelid AND a.attnum_internal = s.staattnum
        )
        """,
        ),
        trampoline.VersionedView(
            name=("edgedbsql", "pg_statistic_ext"),
            query="""
        SELECT
            oid,
            stxrelid,
            stxname,
            stxnamespace,
            stxowner,
            stxstattarget,
            stxkeys,
            stxkind,
            NULL::pg_node_tree as stxexprs,
            tableoid, xmin, cmin, xmax, cmax, ctid
        FROM pg_statistic_ext
        """,
        ),
        trampoline.VersionedView(
            name=("edgedbsql", "pg_statistic_ext_data"),
            query="""
        SELECT
            stxoid,
            stxdndistinct,
            stxddependencies,
            stxdmcv,
            NULL::oid AS stxdexpr,
            tableoid, xmin, cmin, xmax, cmax, ctid
        FROM pg_statistic_ext_data
        """,
        ),
        trampoline.VersionedView(
            name=("edgedbsql", "pg_rewrite"),
            query="""
        SELECT pr.*, pr.tableoid, pr.xmin, pr.cmin, pr.xmax, pr.cmax, pr.ctid
        FROM pg_rewrite pr
        JOIN edgedbsql_VER.pg_class pn ON pr.ev_class = pn.oid
        """,
        ),

        # HACK: Automatically generated cast function for ranges/multiranges
        # was causing issues for pg_dump. So at the end of the day we opt for
        # not exposing any casts at all here since there is no real reason for
        # this compatibility layer that is read-only to have elaborate casts
        # present.
        trampoline.VersionedView(
            name=("edgedbsql", "pg_cast"),
            query="""
        SELECT pc.*, pc.tableoid, pc.xmin, pc.cmin, pc.xmax, pc.cmax, pc.ctid
        FROM pg_cast pc
        WHERE FALSE
        """,
        ),
        # Omit all funcitons for now.
        trampoline.VersionedView(
            name=("edgedbsql", "pg_proc"),
            query="""
        SELECT *, tableoid, xmin, cmin, xmax, cmax, ctid
        FROM pg_proc
        WHERE FALSE
        """,
        ),
        # Omit all operators for now.
        trampoline.VersionedView(
            name=("edgedbsql", "pg_operator"),
            query="""
        SELECT *, tableoid, xmin, cmin, xmax, cmax, ctid
        FROM pg_operator
        WHERE FALSE
        """,
        ),
        # Omit all triggers for now.
        trampoline.VersionedView(
            name=("edgedbsql", "pg_trigger"),
            query="""
        SELECT *, tableoid, xmin, cmin, xmax, cmax, ctid
        FROM pg_trigger
        WHERE FALSE
        """,
        ),
        # Omit all subscriptions for now.
        # This table is queried by pg_dump with COUNT(*) when user does not
        # have permissions to access it. This should be allowed, but the
        # view expands the query to all columns, which is not allowed.
        # So we have to construct an empty view with correct signature that
        # does not reference pg_subscription.
        trampoline.VersionedView(
            name=("edgedbsql", "pg_subscription"),
            query="""
        SELECT
            NULL::oid AS oid,
            NULL::oid AS subdbid,
            NULL::name AS subname,
            NULL::oid AS subowner,
            NULL::boolean AS subenabled,
            NULL::text AS subconninfo,
            NULL::name AS subslotname,
            NULL::text AS subsynccommit,
            NULL::oid AS subpublications,
            tableoid, xmin, cmin, xmax, cmax, ctid
        FROM pg_namespace
        WHERE FALSE
        """,
        ),
        trampoline.VersionedView(
            name=("edgedbsql", "pg_tables"),
            query="""
        SELECT
            n.nspname AS schemaname,
            c.relname AS tablename,
            pg_get_userbyid(c.relowner) AS tableowner,
            t.spcname AS tablespace,
            c.relhasindex AS hasindexes,
            c.relhasrules AS hasrules,
            c.relhastriggers AS hastriggers,
            c.relrowsecurity AS rowsecurity
        FROM edgedbsql_VER.pg_class c
        LEFT JOIN edgedbsql_VER.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
        WHERE c.relkind = ANY (ARRAY['r'::"char", 'p'::"char"])
        """,
        ),
        trampoline.VersionedView(
            name=("edgedbsql", "pg_views"),
            query="""
        SELECT
            n.nspname AS schemaname,
            c.relname AS viewname,
            pg_get_userbyid(c.relowner) AS viewowner,
            pg_get_viewdef(c.oid) AS definition
        FROM edgedbsql_VER.pg_class c
        LEFT JOIN edgedbsql_VER.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'v'::"char"
        """,
        ),
        # Omit all descriptions (comments), becase all non-system comments
        # are our internal implementation details.
        trampoline.VersionedView(
            name=("edgedbsql", "pg_description"),
            query="""
        SELECT
            *,
            tableoid, xmin, cmin, xmax, cmax, ctid
        FROM pg_description
        WHERE FALSE
        """,
        ),
    ]

    # We expose most of the views as empty tables, just to prevent errors when
    # the tools do introspection.
    # For the tables that it turns out are actually needed, we handcraft the
    # views that expose the actual data.
    # I've been cautious about exposing too much data, for example limiting
    # pg_type to pg_catalog and pg_toast namespaces.
    views: list[dbops.View] = []
    views.extend(tables_and_columns)

    for table_name, columns in sql_introspection.INFORMATION_SCHEMA.items():
        if table_name in ["tables", "columns"]:
            continue
        views.append(
            trampoline.VersionedView(
                name=("edgedbsql", table_name),
                query="SELECT {} LIMIT 0".format(
                    ",".join(
                        f"NULL::information_schema.{type} AS {name}"
                        for name, type, _ver_since in columns
                    )
                ),
            )
        )

    PG_TABLES_SKIP = {
        'pg_type',
        'pg_attribute',
        'pg_namespace',
        'pg_class',
        'pg_database',
        'pg_proc',
        'pg_operator',
        'pg_pltemplate',
        'pg_stats',
        'pg_stats_ext_exprs',
        'pg_statistic',
        'pg_statistic_ext',
        'pg_statistic_ext_data',
        'pg_rewrite',
        'pg_cast',
        'pg_index',
        'pg_constraint',
        'pg_trigger',
        'pg_subscription',
        'pg_tables',
        'pg_views',
        'pg_description',
    }

    PG_TABLES_WITH_SYSTEM_COLS = {
        'pg_aggregate',
        'pg_am',
        'pg_amop',
        'pg_amproc',
        'pg_attrdef',
        'pg_attribute',
        'pg_auth_members',
        'pg_authid',
        'pg_cast',
        'pg_class',
        'pg_collation',
        'pg_constraint',
        'pg_conversion',
        'pg_database',
        'pg_db_role_setting',
        'pg_default_acl',
        'pg_depend',
        'pg_enum',
        'pg_event_trigger',
        'pg_extension',
        'pg_foreign_data_wrapper',
        'pg_foreign_server',
        'pg_foreign_table',
        'pg_index',
        'pg_inherits',
        'pg_init_privs',
        'pg_language',
        'pg_largeobject',
        'pg_largeobject_metadata',
        'pg_namespace',
        'pg_opclass',
        'pg_operator',
        'pg_opfamily',
        'pg_partitioned_table',
        'pg_policy',
        'pg_publication',
        'pg_publication_rel',
        'pg_range',
        'pg_replication_origin',
        'pg_rewrite',
        'pg_seclabel',
        'pg_sequence',
        'pg_shdepend',
        'pg_shdescription',
        'pg_shseclabel',
        'pg_statistic',
        'pg_statistic_ext',
        'pg_statistic_ext_data',
        'pg_subscription_rel',
        'pg_tablespace',
        'pg_transform',
        'pg_trigger',
        'pg_ts_config',
        'pg_ts_config_map',
        'pg_ts_dict',
        'pg_ts_parser',
        'pg_ts_template',
        'pg_type',
        'pg_user_mapping',
    }

    SYSTEM_COLUMNS = ['tableoid', 'xmin', 'cmin', 'xmax', 'cmax', 'ctid']

    def construct_pg_view(
        table_name: str, backend_version: params.BackendVersion
    ) -> Optional[dbops.View]:
        pg_columns = sql_introspection.PG_CATALOG[table_name]

        columns = []
        has_columns = False
        for c_name, c_typ, c_ver_since in pg_columns:
            if c_ver_since <= backend_version.major:
                columns.append('o.' + c_name)
                has_columns = True
            elif c_typ:
                columns.append(f'NULL::{c_typ} as {c_name}')
            else:
                columns.append(f'NULL as {c_name}')
        if not has_columns:
            return None

        if table_name in PG_TABLES_WITH_SYSTEM_COLS:
            for c_name in SYSTEM_COLUMNS:
                columns.append('o.' + c_name)

        return trampoline.VersionedView(
            name=("edgedbsql", table_name),
            query=f"SELECT {','.join(columns)} FROM pg_catalog.{table_name} o",
        )

    views.extend(pg_catalog_views)

    for table_name in sql_introspection.PG_CATALOG.keys():
        if table_name in PG_TABLES_SKIP:
            continue
        if v := construct_pg_view(table_name, backend_version):
            views.append(v)

    util_functions = [
        trampoline.VersionedFunction(
            name=('edgedbsql', 'has_schema_privilege'),
            args=(
                ('schema_name', 'text'),
                ('privilege', 'text'),
            ),
            returns=('bool',),
            text="""
            SELECT COALESCE((
                SELECT has_schema_privilege(oid, privilege)
                FROM edgedbsql_VER.pg_namespace
                WHERE nspname = schema_name
            ), TRUE);
            """
        ),
        trampoline.VersionedFunction(
            name=('edgedbsql', 'has_schema_privilege'),
            args=(
                ('schema_oid', 'oid'),
                ('privilege', 'text'),
            ),
            returns=('bool',),
            text="""
                SELECT COALESCE(
                    has_schema_privilege(schema_oid, privilege), TRUE
                )
            """
        ),
        trampoline.VersionedFunction(
            name=('edgedbsql', 'has_table_privilege'),
            args=(
                ('table_name', 'text'),
                ('privilege', 'text'),
            ),
            returns=('bool',),
            text="""
                SELECT has_table_privilege(oid, privilege)
                FROM edgedbsql_VER.pg_class
                WHERE relname = table_name;
            """
        ),
        trampoline.VersionedFunction(
            name=('edgedbsql', 'has_table_privilege'),
            args=(
                ('schema_oid', 'oid'),
                ('privilege', 'text'),
            ),
            returns=('bool',),
            text="""
                SELECT has_table_privilege(schema_oid, privilege)
            """
        ),

        trampoline.VersionedFunction(
            name=('edgedbsql', 'has_column_privilege'),
            args=(
                ('tbl', 'oid'),
                ('col', 'smallint'),
                ('privilege', 'text'),
            ),
            returns=('bool',),
            text="""
                SELECT has_column_privilege(tbl, col, privilege)
            """
        ),
        trampoline.VersionedFunction(
            name=('edgedbsql', 'has_column_privilege'),
            args=(
                ('tbl', 'text'),
                ('col', 'smallint'),
                ('privilege', 'text'),
            ),
            returns=('bool',),
            text="""
                SELECT has_column_privilege(oid, col, privilege)
                FROM edgedbsql_VER.pg_class
                WHERE relname = tbl;
            """
        ),
        trampoline.VersionedFunction(
            name=('edgedbsql', 'has_column_privilege'),
            args=(
                ('tbl', 'oid'),
                ('col', 'text'),
                ('privilege', 'text'),
            ),
            returns=('bool',),
            text="""
                SELECT has_column_privilege(tbl, attnum_internal, privilege)
                FROM edgedbsql_VER.pg_attribute_ext pa
                WHERE attrelid = tbl AND attname = col
            """
        ),
        trampoline.VersionedFunction(
            name=('edgedbsql', 'has_column_privilege'),
            args=(
                ('tbl', 'text'),
                ('col', 'text'),
                ('privilege', 'text'),
            ),
            returns=('bool',),
            text="""
                SELECT has_column_privilege(pc.oid, attnum_internal, privilege)
                FROM edgedbsql_VER.pg_class pc
                JOIN edgedbsql_VER.pg_attribute_ext pa ON pa.attrelid = pc.oid
                WHERE pc.relname = tbl AND pa.attname = col;
            """
        ),
        trampoline.VersionedFunction(
            name=('edgedbsql', '_pg_truetypid'),
            args=(
                ('att', ('edgedbsql_VER', 'pg_attribute')),
                ('typ', ('edgedbsql_VER', 'pg_type')),
            ),
            returns=('oid',),
            volatility='IMMUTABLE',
            strict=True,
            text="""
                SELECT CASE
                    WHEN typ.typtype = 'd' THEN typ.typbasetype
                    ELSE att.atttypid
                END
            """
        ),
        trampoline.VersionedFunction(
            name=('edgedbsql', '_pg_truetypmod'),
            args=(
                ('att', ('edgedbsql_VER', 'pg_attribute')),
                ('typ', ('edgedbsql_VER', 'pg_type')),
            ),
            returns=('int4',),
            volatility='IMMUTABLE',
            strict=True,
            text="""
                SELECT CASE
                    WHEN typ.typtype = 'd' THEN typ.typtypmod
                    ELSE att.atttypmod
                END
            """
        ),
        trampoline.VersionedFunction(
            name=('edgedbsql', 'pg_table_is_visible'),
            args=[
                ('id', ('oid',)),
                ('search_path', ('text[]',)),
            ],
            returns=('bool',),
            volatility='stable',
            text=r'''
                SELECT pc.relnamespace IN (
                    SELECT oid
                    FROM edgedbsql_VER.pg_namespace pn
                    WHERE pn.nspname IN (select * from unnest(search_path))
                )
                FROM edgedbsql_VER.pg_class pc
                WHERE id = pc.oid
            '''
        ),
        trampoline.VersionedFunction(
            # Used instead of pg_catalog.format_type in pg_dump.
            name=('edgedbsql', '_format_type'),
            args=[
                ('typeoid', ('oid',)),
                ('typemod', ('integer',)),
            ],
            returns=('text',),
            volatility='STABLE',
            text=r'''
                SELECT
                    CASE
                        -- arrays
                        WHEN t.typcategory = 'A' THEN (
                            SELECT
                                quote_ident(nspname) || '.' ||
                                quote_ident(el.typname) || tm.mod || '[]'
                            FROM edgedbsql_VER.pg_namespace
                            WHERE oid = el.typnamespace
                        )

                        -- composite (tuples) and types in irregular schemas
                        WHEN (
                            t.typcategory = 'C' OR COALESCE(tn.nspname IN (
                                'edgedb', 'edgedbt', 'edgedbpub', 'edgedbstd',
                                'edgedb_VER', 'edgedbstd_VER'
                            ), TRUE)
                        ) THEN (
                            SELECT
                                quote_ident(nspname) || '.' ||
                                quote_ident(t.typname) || tm.mod
                            FROM edgedbsql_VER.pg_namespace
                            WHERE oid = t.typnamespace
                        )
                        ELSE format_type(typeoid, typemod)
                    END
                FROM edgedbsql_VER.pg_type t
                LEFT JOIN pg_namespace tn ON t.typnamespace = tn.oid
                LEFT JOIN edgedbsql_VER.pg_type el ON t.typelem = el.oid

                CROSS JOIN (
                    SELECT
                        CASE
                            WHEN typemod >= 0 THEN '(' || typemod::text || ')'
                            ELSE ''
                        END AS mod
                ) as tm

                WHERE t.oid = typeoid
            ''',
        ),
        trampoline.VersionedFunction(
            name=("edgedbsql", "pg_get_constraintdef"),
            args=[
                ('conid', ('oid',)),
            ],
            returns=('text',),
            volatility='stable',
            text=r"""
                -- Wrap in a subquery SELECT so that we get a clear failure
                -- if something is broken and this returns multiple rows.
                -- (By default it would silently return the first.)
                SELECT (
                SELECT CASE
                    WHEN contype = 'p' THEN
                    'PRIMARY KEY(' || (
                        SELECT string_agg('"' || attname || '"', ', ')
                        FROM edgedbsql_VER.pg_attribute
                        WHERE attrelid = conrelid AND attnum = ANY(conkey)
                    ) || ')'
                    WHEN contype = 'f' THEN
                    'FOREIGN KEY ("' || (
                        SELECT attname
                        FROM edgedbsql_VER.pg_attribute
                        WHERE attrelid = conrelid AND attnum = ANY(conkey)
                    ) || '")' || ' REFERENCES "'
                    || pn.nspname || '"."' || pc.relname || '"(id)'
                    ELSE ''
                    END
                FROM edgedbsql_VER.pg_constraint con
                LEFT JOIN edgedbsql_VER.pg_class_tables pc ON pc.oid = confrelid
                LEFT JOIN edgedbsql_VER.pg_namespace pn
                  ON pc.relnamespace = pn.oid
                WHERE con.oid = conid
                )
            """
        ),
        trampoline.VersionedFunction(
            name=("edgedbsql", "pg_get_constraintdef"),
            args=[
                ('conid', ('oid',)),
                ('pretty', ('bool',)),
            ],
            returns=('text',),
            volatility='stable',
            text=r"""
                SELECT pg_get_constraintdef(conid)
            """
        ),
    ]

    return (
        [cast(dbops.Command, dbops.CreateFunction(uuid_to_oid))]
        + [dbops.CreateView(virtual_tables)]
        + [
            cast(dbops.Command, dbops.CreateFunction(long_name)),
            cast(dbops.Command, dbops.CreateFunction(type_rename)),
            cast(dbops.Command, dbops.CreateFunction(namespace_rename)),
        ]
        + [dbops.CreateView(view) for view in views]
        + [dbops.CreateFunction(func) for func in util_functions]
    )


@functools.cache
def generate_sql_information_schema_refresh(
    backend_version: params.BackendVersion
) -> dbops.Command:
    refresh = dbops.CommandGroup()
    for command in _generate_sql_information_schema(backend_version):
        if (
            isinstance(command, dbops.CreateView)
            and command.view.materialized
        ):
            refresh.add_command(dbops.Query(
                text=f'REFRESH MATERIALIZED VIEW {q(*command.view.name)}'
            ))
    return refresh


class ObjectAncestorsView(trampoline.VersionedView):
    """A trampolined and explicit version of _SchemaObjectType__ancestors"""

    query = r'''
        SELECT source, target, index
        FROM edgedb_VER."_SchemaObjectType__ancestors"
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_object_ancestors'),
            query=self.query,
        )


class LinksView(trampoline.VersionedView):
    """A trampolined and explicit version of _SchemaLink"""

    query = r'''
        SELECT id, name, source, target
        FROM edgedb_VER."_SchemaLink"
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_schema_links'),
            query=self.query,
        )


def get_config_type_views(
    schema: s_schema.Schema,
    conf: s_objtypes.ObjectType,
    scope: Optional[qltypes.ConfigScope],
    existing_view_columns: Optional[dict[str, list[str]]]=None,
) -> dbops.CommandGroup:
    commands = dbops.CommandGroup()

    cfg_views, _ = _generate_config_type_view(
        schema,
        conf,
        scope=scope,
        path=[],
        rptr=None,
        existing_view_columns=existing_view_columns,
    )
    commands.add_commands([
        dbops.CreateView(
            (trampoline.VersionedView if tn[0] == 'edgedbstd' else dbops.View)(
                name=tn, query=trampoline.fixup_query(q)
            ),
            or_replace=True,
        )
        for tn, q in cfg_views
    ])

    return commands


def get_config_views(
    schema: s_schema.Schema,
    existing_view_columns: Optional[dict[str, list[str]]]=None,
) -> dbops.CommandGroup:
    commands = dbops.CommandGroup()

    conf = schema.get('cfg::Config', type=s_objtypes.ObjectType)
    commands.add_command(
        get_config_type_views(
            schema, conf, scope=None,
            existing_view_columns=existing_view_columns,
        ),
    )

    conf = schema.get('cfg::InstanceConfig', type=s_objtypes.ObjectType)
    commands.add_command(
        get_config_type_views(
            schema, conf, scope=qltypes.ConfigScope.INSTANCE,
            existing_view_columns=existing_view_columns,
        ),
    )

    conf = schema.get('cfg::DatabaseConfig', type=s_objtypes.ObjectType)
    commands.add_command(
        get_config_type_views(
            schema, conf, scope=qltypes.ConfigScope.DATABASE,
            existing_view_columns=existing_view_columns,
        ),
    )

    return commands


def get_synthetic_type_views(
    schema: s_schema.Schema,
    backend_params: params.BackendRuntimeParams,
) -> dbops.CommandGroup:
    commands = dbops.CommandGroup()

    commands.add_command(get_config_views(schema))

    for dbview in _generate_branch_views(schema):
        commands.add_command(dbops.CreateView(dbview, or_replace=True))

    for extview in _generate_extension_views(schema):
        commands.add_command(dbops.CreateView(extview, or_replace=True))
    for extview in _generate_extension_migration_views(schema):
        commands.add_command(dbops.CreateView(extview, or_replace=True))

    if backend_params.has_create_role:
        role_views = _generate_role_views(schema)
    else:
        role_views = _generate_single_role_views(schema)
    for roleview in role_views:
        commands.add_command(dbops.CreateView(roleview, or_replace=True))

    for verview in _generate_schema_ver_views(schema):
        commands.add_command(dbops.CreateView(verview, or_replace=True))

    if backend_params.has_stat_statements:
        for stats_view in _generate_stats_views(schema):
            commands.add_command(dbops.CreateView(stats_view, or_replace=True))
        commands.add_command(
            dbops.CreateFunction(
                ResetQueryStatsFunction(True), or_replace=True
            )
        )

    return commands


def get_support_views(
    schema: s_schema.Schema,
    backend_params: params.BackendRuntimeParams,
) -> tuple[dbops.CommandGroup, list[trampoline.Trampoline]]:
    commands = dbops.CommandGroup()

    schema_alias_views = _generate_schema_alias_views(
        schema, s_name.UnqualName('schema'))

    InhObject = schema.get(
        'schema::InheritingObject', type=s_objtypes.ObjectType)
    InhObject__ancestors = InhObject.getptr(
        schema, s_name.UnqualName('ancestors'), type=s_links.Link)
    schema_alias_views.append(
        _generate_schema_alias_view(schema, InhObject__ancestors))

    ObjectType = schema.get(
        'schema::ObjectType', type=s_objtypes.ObjectType)
    ObjectType__ancestors = ObjectType.getptr(
        schema, s_name.UnqualName('ancestors'), type=s_links.Link)
    schema_alias_views.append(
        _generate_schema_alias_view(schema, ObjectType__ancestors))

    for alias_view in schema_alias_views:
        commands.add_command(dbops.CreateView(alias_view, or_replace=True))

    synthetic_types = get_synthetic_type_views(schema, backend_params)
    commands.add_command(synthetic_types)

    # Create some trampolined wrapper views around _Schema types we need
    # to reference from functions.
    wrapper_commands = dbops.CommandGroup()
    wrapper_commands.add_command(
        dbops.CreateView(ObjectAncestorsView(), or_replace=True))
    wrapper_commands.add_command(
        dbops.CreateView(LinksView(), or_replace=True))
    commands.add_command(wrapper_commands)

    sys_alias_views = _generate_schema_alias_views(
        schema, s_name.UnqualName('sys'))

    # Include sys::Role::member_of to support DescribeRolesAsDDLFunction
    SysRole = schema.get(
        'sys::Role', type=s_objtypes.ObjectType)
    SysRole__member_of = SysRole.getptr(
        schema, s_name.UnqualName('member_of'))
    sys_alias_views.append(
        _generate_schema_alias_view(schema, SysRole__member_of))

    for alias_view in sys_alias_views:
        commands.add_command(dbops.CreateView(alias_view, or_replace=True))

    commands.add_commands(
        _generate_sql_information_schema(
            backend_params.instance_params.version
        )
    )

    # The synthetic type views (cfg::, sys::) need to be trampolined
    trampolines = []
    trampolines.extend(trampoline_command(synthetic_types))
    trampolines.extend(trampoline_command(wrapper_commands))

    return commands, trampolines


async def generate_support_views(
    conn: PGConnection,
    schema: s_schema.Schema,
    backend_params: params.BackendRuntimeParams,
) -> list[trampoline.Trampoline]:
    commands, trampolines = get_support_views(schema, backend_params)
    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)
    return trampolines


async def generate_support_functions(
    conn: PGConnection,
    schema: s_schema.Schema,
) -> list[trampoline.Trampoline]:
    commands = dbops.CommandGroup()

    cmds = [
        dbops.CreateFunction(GetPgTypeForEdgeDBTypeFunction2(),
                             or_replace=True),
        dbops.CreateFunction(IssubclassFunction()),
        dbops.CreateFunction(IssubclassFunction2()),
        dbops.CreateFunction(GetSchemaObjectNameFunction()),
    ]
    commands.add_commands(cmds)

    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)
    return trampoline_functions(cmds)


async def generate_more_support_functions(
    conn: PGConnection,
    compiler: edbcompiler.Compiler,
    schema: s_schema.Schema,
    testmode: bool,
) -> list[trampoline.Trampoline]:
    commands = dbops.CommandGroup()

    cmds = [
        dbops.CreateFunction(
            DescribeRolesAsDDLFunction(schema), or_replace=True),
        dbops.CreateFunction(GetSequenceBackendNameFunction()),
        dbops.CreateFunction(DumpSequencesFunction()),
    ]
    commands.add_commands(cmds)

    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)
    return trampoline_functions(cmds)


def _build_key_source(
    schema: s_schema.Schema,
    exc_props: Iterable[s_pointers.Pointer],
    rptr: Optional[s_pointers.Pointer],
    source_idx: str,
) -> str:
    if exc_props:
        restargets = []
        for prop in exc_props:
            pname = prop.get_shortname(schema).name
            restarget = f'(q{source_idx}.val)->>{ql(pname)}'
            restargets.append(restarget)

        targetlist = ','.join(restargets)

        keysource = f'''
            (SELECT
                ARRAY[{targetlist}] AS key
            ) AS k{source_idx}'''
    else:
        assert rptr is not None
        rptr_name = rptr.get_shortname(schema).name
        keysource = f'''
            (SELECT
                ARRAY[
                    (CASE WHEN q{source_idx}.val = 'null'::jsonb
                     THEN NULL
                     ELSE {ql(rptr_name)}
                     END)
                ] AS key
            ) AS k{source_idx}'''

    return keysource


def _build_key_expr(
    key_components: List[str],
    versioned: bool,
) -> str:
    prefix = 'edgedb_VER' if versioned else 'edgedb'
    key_expr = ' || '.join(key_components)
    final_keysource = f'''
        (SELECT
            (CASE WHEN array_position(q.v, NULL) IS NULL
             THEN
                 {prefix}.uuid_generate_v5(
                     '{DATABASE_ID_NAMESPACE}'::uuid,
                     array_to_string(q.v, ';')
                 )
             ELSE NULL
             END) AS key
         FROM
            (SELECT {key_expr} AS v) AS q
        )'''

    return final_keysource


def _build_data_source(
    schema: s_schema.Schema,
    rptr: s_pointers.Pointer,
    source_idx: int,
    *,
    always_array: bool = False,
    alias: Optional[str] = None,
) -> str:

    rptr_name = rptr.get_shortname(schema).name
    rptr_card = rptr.get_cardinality(schema)
    rptr_multi = rptr_card.is_multi()

    if alias is None:
        alias = f'q{source_idx + 1}'
    else:
        alias = f'q{alias}'

    if rptr_multi:
        sourceN = f'''
            (SELECT jel.val
                FROM
                jsonb_array_elements(
                    (q{source_idx}.val)->{ql(rptr_name)}) AS jel(val)
            ) AS {alias}'''
    else:
        proj = '[0]' if always_array else ''
        sourceN = f'''
            (SELECT
                (q{source_idx}.val){proj}->{ql(rptr_name)} AS val
            ) AS {alias}'''

    return sourceN


def _escape_like(s: str) -> str:
    return s.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')


def _generate_config_type_view(
    schema: s_schema.Schema,
    stype: s_objtypes.ObjectType,
    *,
    scope: Optional[qltypes.ConfigScope],
    path: List[Tuple[s_pointers.Pointer, List[s_pointers.Pointer]]],
    rptr: Optional[s_pointers.Pointer],
    existing_view_columns: Optional[dict[str, list[str]]],
    override_exclusive_props: Optional[list[s_pointers.Pointer]] = None,
    _memo: Optional[Set[s_obj.Object]] = None,
) -> Tuple[
    List[Tuple[Tuple[str, str], str]],
    List[s_pointers.Pointer],
]:
    X = xdedent.escape

    exc = schema.get('std::exclusive', type=s_constr.Constraint)

    if scope is not None:
        if scope is qltypes.ConfigScope.INSTANCE:
            max_source = "'system override'"
        elif scope is qltypes.ConfigScope.DATABASE:
            max_source = "'database'"
        else:
            raise AssertionError(f'unexpected config scope: {scope!r}')
    else:
        max_source = 'NULL'

    if _memo is None:
        _memo = set()

    _memo.add(stype)

    views = []

    sources = []

    ext_cfg = schema.get('cfg::ExtensionConfig', type=s_objtypes.ObjectType)
    is_ext_cfg = stype.issubclass(schema, ext_cfg)
    if is_ext_cfg:
        rptr = None
    is_rptr_ext_cfg = False
    # For extension configs, we want to use the trampolined version,
    # since we know it must exist already and don't want to have to
    # recreate the views on update.
    versioned = not is_ext_cfg or stype == ext_cfg
    prefix = 'edgedb_VER' if versioned else 'edgedb'

    if not path:
        if is_ext_cfg:
            # Extension configs get one object per scope.
            cfg_name = str(stype.get_name(schema))

            escaped_name = _escape_like(cfg_name)
            source0 = f'''
                (SELECT
                    (SELECT jsonb_object_agg(
                      substr(name, {len(cfg_name) + 3}), value) AS val
                    FROM {prefix}._read_sys_config(
                      NULL, scope::edgedb._sys_config_source_t) cfg
                    WHERE name LIKE {ql(escaped_name + '%')}
                    ) AS val, scope::text AS scope, scope_id AS scope_id
                    FROM (VALUES
                        (NULL, '{CONFIG_ID[None]}'::uuid),
                        ('database',
                         '{CONFIG_ID[qltypes.ConfigScope.DATABASE]}'::uuid)
                    ) AS s(scope, scope_id)
                ) AS q0
            '''
        elif rptr is None:
            # This is the root config object.
            source0 = f'''
                (SELECT jsonb_object_agg(name, value) AS val
                FROM {prefix}._read_sys_config(NULL, {max_source}) cfg)
                AS q0'''
        else:
            rptr_name = rptr.get_shortname(schema).name
            rptr_source = not_none(rptr.get_source(schema))
            is_rptr_ext_cfg = rptr_source.issubclass(schema, ext_cfg)
            if is_rptr_ext_cfg:
                versioned = False
                prefix = 'edgedb'

                cfg_name = str(rptr_source.get_name(schema)) + '::' + rptr_name
                escaped_name = _escape_like(cfg_name)

                source0 = f'''
                    (SELECT el.val AS val, s.scope::text AS scope,
                            s.scope_id AS scope_id
                     FROM (VALUES
                         (NULL, '{CONFIG_ID[None]}'::uuid),
                         ('database',
                          '{CONFIG_ID[qltypes.ConfigScope.DATABASE]}'::uuid)
                     ) AS s(scope, scope_id),
                     LATERAL (
                         SELECT (value::jsonb) AS val
                         FROM {prefix}._read_sys_config(
                           NULL, scope::edgedb._sys_config_source_t) cfg
                         WHERE name LIKE {ql(escaped_name + '%')}
                     ) AS cfg,
                     LATERAL jsonb_array_elements(cfg.val) AS el(val)
                    ) AS q0
                '''

            else:
                source0 = f'''
                    (SELECT el.val
                     FROM
                        (SELECT (value::jsonb) AS val
                        FROM {prefix}._read_sys_config(NULL, {max_source})
                        WHERE name = {ql(rptr_name)}) AS cfg,
                        LATERAL jsonb_array_elements(cfg.val) AS el(val)
                    ) AS q0'''

        sources.append(source0)
        key_start = 0
    else:
        # XXX: The second level is broken for extension configs.
        # Can we solve this without code duplication?
        root = path[0][0]
        root_source = not_none(root.get_source(schema))
        is_root_ext_cfg = root_source.issubclass(schema, ext_cfg)
        assert not is_root_ext_cfg, (
            "nested conf objects not yet supported for ext configs")

        key_start = 0

        for i, (l, exc_props) in enumerate(path):
            l_card = l.get_cardinality(schema)
            l_multi = l_card.is_multi()
            l_name = l.get_shortname(schema).name

            if i == 0:
                if l_multi:
                    sourceN = f'''
                        (SELECT el.val
                        FROM
                            (SELECT (value::jsonb) AS val
                            FROM {prefix}._read_sys_config(NULL, {max_source})
                            WHERE name = {ql(l_name)}) AS cfg,
                            LATERAL jsonb_array_elements(cfg.val) AS el(val)
                        ) AS q{i}'''
                else:
                    sourceN = f'''
                        (SELECT (value::jsonb) AS val
                        FROM {prefix}._read_sys_config(NULL, {max_source}) cfg
                        WHERE name = {ql(l_name)}) AS q{i}'''
            else:
                sourceN = _build_data_source(schema, l, i - 1)

            sources.append(sourceN)
            sources.append(_build_key_source(schema, exc_props, l, str(i)))

            if exc_props:
                key_start = i

    exclusive_props = []
    single_links = []
    multi_links = []
    multi_props = []
    target_cols: dict[s_pointers.Pointer, str] = {}
    where = ''

    path_steps = [p.get_shortname(schema).name for p, _ in path]

    if rptr is not None:
        self_idx = len(path)

        # Generate a source rvar for _this_ target
        rptr_name = rptr.get_shortname(schema).name
        path_steps.append(rptr_name)

        if self_idx > 0:
            sourceN = _build_data_source(schema, rptr, self_idx - 1)
            sources.append(sourceN)
    else:
        self_idx = 0

    sval = f'(q{self_idx}.val)'

    for pp_name, pp in stype.get_pointers(schema).items(schema):
        pn = str(pp_name)
        if pn in ('id', '__type__'):
            continue

        pp_type = pp.get_target(schema)
        assert pp_type is not None
        pp_card = pp.get_cardinality(schema)
        pp_multi = pp_card.is_multi()
        pp_psi = types.get_pointer_storage_info(pp, schema=schema)
        pp_col = pp_psi.column_name

        if isinstance(pp, s_links.Link):
            if pp_multi:
                multi_links.append(pp)
            else:
                single_links.append(pp)
        else:
            pp_cast = _make_json_caster(
                schema, pp_type, versioned=versioned
            )

            if pp_multi:
                multi_props.append((pp, pp_cast))
            else:
                extract_col = (
                    f'{pp_cast(f"{sval}->{ql(pn)}")} AS {qi(pp_col)}')

                target_cols[pp] = extract_col

                constraints = pp.get_constraints(schema).objects(schema)
                if any(c.issubclass(schema, exc) for c in constraints):
                    exclusive_props.append(pp)

    if override_exclusive_props:
        exclusive_props = [
            stype.getptr(
                schema, s_name.UnqualName(p.get_shortname(schema).name)
            )
            for p in override_exclusive_props
        ]

    exclusive_props.sort(key=lambda p: p.get_shortname(schema).name)

    if is_ext_cfg:
        # Extension configs get custom keys based on their type name
        # and the scope, since we create one object per scope.
        key_components = [
            f'ARRAY[{ql(str(stype.get_name(schema)))}]',
            "ARRAY[coalesce(q0.scope, 'session')]"
        ]
        final_keysource = f'{_build_key_expr(key_components, versioned)} AS k'
        sources.append(final_keysource)

        key_expr = 'k.key'
        where = f"q0.val IS NOT NULL"

    elif exclusive_props or rptr:
        sources.append(
            _build_key_source(schema, exclusive_props, rptr, str(self_idx)))

        key_components = [f'k{i}.key' for i in range(key_start, self_idx + 1)]
        if is_rptr_ext_cfg:
            assert rptr_source
            key_components = [
                f'ARRAY[{ql(str(rptr_source.get_name(schema)))}]',
                "ARRAY[coalesce(q0.scope, 'session')]"
            ] + key_components

        final_keysource = f'{_build_key_expr(key_components, versioned)} AS k'
        sources.append(final_keysource)

        key_expr = 'k.key'

        tname = str(stype.get_name(schema))
        where = f"{key_expr} IS NOT NULL AND ({sval}->>'_tname') = {ql(tname)}"

    else:
        key_expr = f"'{CONFIG_ID[scope]}'::uuid"

        key_components = []

    id_ptr = stype.getptr(schema, s_name.UnqualName('id'))
    target_cols[id_ptr] = f'{X(key_expr)} AS id'

    base_sources = list(sources)

    for link in single_links:
        link_name = link.get_shortname(schema).name
        link_type = link.get_target(schema)
        link_psi = types.get_pointer_storage_info(link, schema=schema)
        link_col = link_psi.column_name

        if str(link_type.get_name(schema)) == 'cfg::AbstractConfig':
            target_cols[link] = f'q0.scope_id AS {qi(link_col)}'
            continue

        if rptr is not None:
            target_path = path + [(rptr, exclusive_props)]
        else:
            target_path = path

        target_views, target_exc_props = _generate_config_type_view(
            schema,
            link_type,
            scope=scope,
            path=target_path,
            rptr=link,
            existing_view_columns=existing_view_columns,
            _memo=_memo,
        )

        for descendant in link_type.descendants(schema):
            if descendant not in _memo:
                desc_views, _ = _generate_config_type_view(
                    schema,
                    descendant,
                    scope=scope,
                    path=target_path,
                    rptr=link,
                    existing_view_columns=existing_view_columns,
                    override_exclusive_props=target_exc_props,
                    _memo=_memo,
                )
                views.extend(desc_views)

        target_source = _build_data_source(
            schema, link, self_idx, alias=link_name,
            always_array=rptr is None,
        )
        sources.append(target_source)

        target_key_source = _build_key_source(
            schema, target_exc_props, link, source_idx=link_name)
        sources.append(target_key_source)

        target_key_components = key_components + [f'k{link_name}.key']

        target_key = _build_key_expr(target_key_components, versioned)
        target_cols[link] = f'({X(target_key)}) AS {qi(link_col)}'

        views.extend(target_views)

    # You can't change the order of a postgres view... so
    # we have to maintain the original order.
    #
    # If we are applying patches that modify the config views,
    # then we will have an existing_view_columns map that tells us
    # the existing order in postgres.
    # If it isn't already in that map, then we order based on
    # the order in the pointers refdict, which will be the order
    # the pointers were created, *if* they were added to the in-memory
    # schema in this process.  (If it was loaded from reflection, that
    # order won't be preserved, which is why we need existing_view_columns).
    #
    # FIXME: We should consider adding enough info to the schema to not need
    # this complication.
    existing_indexes = {
        v: i for i, v in enumerate(existing_view_columns.get(str(stype.id), []))
    } if existing_view_columns else {}
    ptr_indexes = {}
    for i, v in enumerate(stype.get_pointers(schema).objects(schema)):
        # First try the id
        if (eidx := existing_indexes.get(str(v.id))) is not None:
            idx = (0, eidx)
        # Certain columns use their actual names, so try the actual
        # name also.
        elif (
            eidx := existing_indexes.get(v.get_shortname(schema).name)
        ) is not None:
            idx = (0, eidx)
        # Not already in the database, use the order in pointers refdict
        else:
            idx = (1, i)
        ptr_indexes[v] = idx

    target_cols_sorted = sorted(
        target_cols.items(), key=lambda p: ptr_indexes[p[0]]
    )

    target_cols_str = ',\n'.join([x for _, x in target_cols_sorted if x])

    fromlist = ',\n'.join(f'LATERAL {X(s)}' for s in sources)

    target_query = xdedent.xdedent(f'''
        SELECT
            {X(target_cols_str)}
        FROM
            {X(fromlist)}
    ''')

    if where:
        target_query += f'\nWHERE\n    {where}'

    views.append((tabname(schema, stype), target_query))

    for link in multi_links:
        target_sources = list(base_sources)

        link_name = link.get_shortname(schema).name
        link_type = link.get_target(schema)

        if rptr is not None:
            target_path = path + [(rptr, exclusive_props)]
        else:
            target_path = path

        target_views, target_exc_props = _generate_config_type_view(
            schema,
            link_type,
            scope=scope,
            path=target_path,
            rptr=link,
            existing_view_columns=existing_view_columns,
            _memo=_memo,
        )
        views.extend(target_views)

        for descendant in link_type.descendants(schema):
            if descendant not in _memo:
                desc_views, _ = _generate_config_type_view(
                    schema,
                    descendant,
                    scope=scope,
                    path=target_path,
                    rptr=link,
                    existing_view_columns=existing_view_columns,
                    override_exclusive_props=target_exc_props,
                    _memo=_memo,
                )
                views.extend(desc_views)

        # HACK: For computable links (just extensions hopefully?), we
        # want to compile the targets as a side effect, but we don't
        # want to actually include them in the view.
        if link.get_computable(schema):
            continue

        target_source = _build_data_source(
            schema, link, self_idx, alias=link_name)
        target_sources.append(target_source)

        target_key_source = _build_key_source(
            schema, target_exc_props, link, source_idx=link_name)
        target_sources.append(target_key_source)

        target_key_components = key_components + [f'k{link_name}.key']
        target_key = _build_key_expr(target_key_components, versioned)

        target_fromlist = ',\n'.join(f'LATERAL {X(s)}' for s in target_sources)

        link_query = xdedent.xdedent(f'''\
            SELECT
                q.source,
                q.target
            FROM
                (SELECT
                    {X(key_expr)} AS source,
                    {X(target_key)} AS target
                FROM
                    {X(target_fromlist)}
                ) q
            WHERE
                q.target IS NOT NULL
            ''')

        views.append((tabname(schema, link), link_query))

    for prop, pp_cast in multi_props:
        target_sources = list(sources)

        pn = prop.get_shortname(schema).name

        target_source = _build_data_source(
            schema, prop, self_idx, alias=pn)
        target_sources.append(target_source)

        target_fromlist = ',\n'.join(f'LATERAL {X(s)}' for s in target_sources)

        link_query = xdedent.xdedent(f'''\
            SELECT
                {X(key_expr)} AS source,
                {pp_cast(f'q{pn}.val')} AS target
            FROM
                {X(target_fromlist)}
        ''')

        views.append((tabname(schema, prop), link_query))

    return views, exclusive_props


async def _execute_block(
    conn: PGConnection,
    block: dbops.SQLBlock,
) -> None:
    await execute_sql_script(conn, block.to_string())


async def execute_sql_script(
    conn: PGConnection,
    sql_text: str,
) -> None:
    from edb.server import pgcon

    if debug.flags.bootstrap:
        debug.header('Bootstrap Script')
        if len(sql_text) > 102400:
            # Make sure we don't hog CPU by attempting to highlight
            # huge scripts.
            print(sql_text)
        else:
            debug.dump_code(sql_text, lexer='sql')

    try:
        await conn.sql_execute(sql_text.encode("utf-8"))
    except pgcon.BackendError as e:
        position = e.get_field('P')
        internal_position = e.get_field('p')
        context = e.get_field('W')
        if context:
            pl_func_line_m = re.search(
                r'^PL/pgSQL function inline_code_block line (\d+).*',
                context, re.M)

            if pl_func_line_m:
                pl_func_line = int(pl_func_line_m.group(1))
        else:
            pl_func_line = None

        point = None
        text = None

        if position is not None:
            point = int(position) - 1
            text = sql_text

        elif internal_position is not None:
            point = int(internal_position) - 1
            text = e.get_field('q')

        elif pl_func_line:
            point = ql_parser.offset_of_line(sql_text, pl_func_line)
            text = sql_text

        if point is not None:
            span = qlast.Span(
                'query', text, start=point, end=point, context_lines=30
            )
            exceptions.replace_context(e, span)

        raise
