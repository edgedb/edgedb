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


"""Database structure and objects supporting EdgeDB metadata."""

from __future__ import annotations
from typing import *

import re
import textwrap

from edb import _edgeql_rust

from edb.common import context as parser_context
from edb.common import debug
from edb.common import exceptions
from edb.common import uuidgen

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
from edb.server import bootstrap as edbbootstrap

from . import common
from . import compiler
from . import dbops
from . import delta
from . import types
from . import params

if TYPE_CHECKING:
    import asyncpg


q = common.qname
qi = common.quote_ident
ql = common.quote_literal
qt = common.quote_type


DATABASE_ID_NAMESPACE = uuidgen.UUID('0e6fed66-204b-11e9-8666-cffd58a5240b')
CONFIG_ID_NAMESPACE = uuidgen.UUID('a48b38fa-349b-11e9-a6be-4f337f82f5ad')
CONFIG_ID = uuidgen.UUID('172097a4-39f4-11e9-b189-9321eb2f4b97')


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


class ExpressionType(dbops.CompositeType):
    def __init__(self) -> None:
        super().__init__(name=('edgedb', 'expression_t'))

        self.add_columns([
            dbops.Column(name='text', type='text'),
            dbops.Column(name='refs', type='uuid[]'),
        ])


class BigintDomain(dbops.Domain):
    """Bigint: a variant of numeric that enforces zero digits after the dot.

    We're using an explicit scale check as opposed to simply specifying
    the numeric bounds, because using bounds severly restricts the range
    of the numeric type (1000 vs 131072 digits).
    """
    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'bigint_t'),
            base='numeric',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedb', 'bigint_t'),
                    expr=("scale(VALUE) = 0 AND VALUE != 'NaN'"),
                ),
            ),
        )


class ConfigMemoryDomain(dbops.Domain):
    """Represents the cfg::memory type. Stores number of bytes.

    Defined just as edgedb.bigint_t:

    * numeric is used to ensure we can comfortably represent huge amounts
      of data beyond petabytes;
    * enforces zero digits after the dot.
    """
    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'memory_t'),
            base='int8',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedb', 'memory_t'),
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
            name=('edgedb', 'timestamptz_t'),
            base='timestamptz',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedb', 'timestamptz_t'),
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
            name=('edgedb', 'timestamp_t'),
            base='timestamp',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedb', 'timestamp_t'),
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
            name=('edgedb', 'date_t'),
            base='date',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedb', 'date_t'),
                    expr=("EXTRACT(years from VALUE) BETWEEN 1 AND 9999"),
                ),
            ),
        )


class DurationDomain(dbops.Domain):
    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'duration_t'),
            base='interval',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedb', 'duration_t'),
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
            name=('edgedb', 'relative_duration_t'),
            base='interval',
            constraints=(
                dbops.DomainCheckConstraint(
                    domain_name=('edgedb', 'relative_duration_t'),
                    expr="true",
                ),
            ),
        )


class StrToConfigMemoryFunction(dbops.Function):
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
                            edgedb.raise(
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
                            edgedb.raise(
                                NULL::int8,
                                'invalid_parameter_value',
                                msg => (
                                    'unable to parse memory size "' ||
                                    "val" || '"'
                                )
                            )
                    END
            END)::edgedb.memory_t
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
            returns=('edgedb', 'memory_t'),
            volatility='immutable',
            language='sql',
            text=self.text,
        )


class ConfigMemoryToStrFunction(dbops.Function):
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
                ('val', ('edgedb', 'memory_t')),
            ],
            returns=('text',),
            volatility='immutable',
            language='sql',
            text=self.text,
        )


class AlterCurrentDatabaseSetString(dbops.Function):
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


class AlterCurrentDatabaseSetStringArray(dbops.Function):
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


class AlterCurrentDatabaseSetNonArray(dbops.Function):
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


class AlterCurrentDatabaseSetArray(dbops.Function):
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


class StrToBigint(dbops.Function):
    """Parse bigint from text."""

    # The plpgsql execption handling nonsense is actually just so that
    # we can produce an exception that mentions edgedb.bigint_t
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
                RETURN v::edgedb.bigint_t;
            ELSE
                EXECUTE edgedb.raise(
                    NULL::numeric,
                    'invalid_text_representation',
                    msg => (
                        'invalid input syntax for type edgedb.bigint_t: '
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
            returns=('edgedb', 'bigint_t'),
            language='plpgsql',
            # Stable because it's raising exceptions.
            volatility='stable',
            strict=True,
            text=self.text)


class StrToDecimal(dbops.Function):
    """Parse decimal from text."""
    text = r'''
        SELECT
            (CASE WHEN v.column1 != 'NaN' THEN
                v.column1
            ELSE
                edgedb.raise(
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
            # Stable because it's raising exceptions.
            volatility='stable',
            strict=True,
            text=self.text,
        )


class StrToInt64NoInline(dbops.Function):
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
            volatility='stable',
            text=self.text,
        )


class StrToInt32NoInline(dbops.Function):
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
            volatility='stable',
            text=self.text,
        )


class StrToInt16NoInline(dbops.Function):
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
            volatility='stable',
            text=self.text,
        )


class StrToFloat64NoInline(dbops.Function):
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
            volatility='stable',
            text=self.text,
        )


class StrToFloat32NoInline(dbops.Function):
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
            volatility='stable',
            text=self.text,
        )


class GetBackendCapabilitiesFunction(dbops.Function):

    text = f'''
        SELECT
            (json ->> 'capabilities')::bigint
        FROM
            edgedbinstdata.instdata
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


class GetBackendTenantIDFunction(dbops.Function):

    text = f'''
        SELECT
            (json ->> 'tenant_id')::text
        FROM
            edgedbinstdata.instdata
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


class GetDatabaseBackendNameFunction(dbops.Function):

    text = f'''
    SELECT
        CASE
        WHEN
            (edgedb.get_backend_capabilities()
             & {int(params.BackendCapabilities.CREATE_DATABASE)}) != 0
        THEN
            edgedb.get_backend_tenant_id() || '_' || "db_name"
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


class GetRoleBackendNameFunction(dbops.Function):

    text = f'''
    SELECT
        CASE
        WHEN
            (edgedb.get_backend_capabilities()
             & {int(params.BackendCapabilities.CREATE_ROLE)}) != 0
        THEN
            edgedb.get_backend_tenant_id() || '_' || "role_name"
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


class GetUserSequenceBackendNameFunction(dbops.Function):

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


class GetSequenceBackendNameFunction(dbops.Function):

    text = f'''
        SELECT
            (CASE
                WHEN edgedb.get_name_module(st.name)
                     = any(edgedb.get_std_modules())
                THEN 'edgedbstd'
                ELSE 'edgedbpub'
             END),
            "sequence_type_id"::text || '_sequence'
        FROM
            edgedb."_SchemaScalarType" AS st
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


class GetStdModulesFunction(dbops.Function):

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


class GetObjectMetadata(dbops.Function):
    """Return EdgeDB metadata associated with a backend object."""
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


class GetColumnMetadata(dbops.Function):
    """Return EdgeDB metadata associated with a backend object."""
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


class GetSharedObjectMetadata(dbops.Function):
    """Return EdgeDB metadata associated with a backend object."""
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


class GetDatabaseMetadataFunction(dbops.Function):
    """Return EdgeDB metadata associated with a given database."""
    text = f'''
        SELECT
            CASE
            WHEN
                "dbname" = {ql(defines.EDGEDB_SUPERUSER_DB)}
                OR (edgedb.get_backend_capabilities()
                    & {int(params.BackendCapabilities.CREATE_DATABASE)}) != 0
            THEN
                edgedb.shobj_metadata(
                    (SELECT
                        oid
                     FROM
                        pg_database
                     WHERE
                        datname = edgedb.get_database_backend_name("dbname")
                    ),
                    'pg_database'
                )
            ELSE
                COALESCE(
                    (SELECT
                        json
                     FROM
                        edgedbinstdata.instdata
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


class GetCurrentDatabaseFunction(dbops.Function):

    text = f'''
        SELECT
            CASE
            WHEN
                (edgedb.get_backend_capabilities()
                 & {int(params.BackendCapabilities.CREATE_DATABASE)}) != 0
            THEN
                substr(
                    current_database(),
                    char_length(edgedb.get_backend_tenant_id()) + 2
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


class RaiseExceptionFunction(dbops.Function):
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


class RaiseExceptionOnNullFunction(dbops.Function):
    """Return the passed value or raise an exception if it's NULL."""
    text = '''
        SELECT coalesce(
            val,
            edgedb.raise(
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


class RaiseExceptionOnNotNullFunction(dbops.Function):
    """Return the passed value or raise an exception if it's NOT NULL."""
    text = '''
        SELECT
            CASE
            WHEN val IS NULL THEN
                val
            ELSE
                edgedb.raise(
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


class RaiseExceptionOnEmptyStringFunction(dbops.Function):
    """Return the passed string or raise an exception if it's empty."""
    text = '''
        SELECT
            CASE WHEN edgedb._length(val) = 0 THEN
                edgedb.raise(val, exc, msg => msg, detail => detail)
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


class AssertJSONTypeFunction(dbops.Function):
    """Assert that the JSON type matches what is expected."""
    text = '''
        SELECT
            CASE WHEN array_position(typenames, jsonb_typeof(val)) IS NULL THEN
                edgedb.raise(
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


class ExtractJSONScalarFunction(dbops.Function):
    """Convert a given JSON scalar value into a text value."""
    text = '''
        SELECT
            (to_jsonb(ARRAY[
                edgedb.jsonb_assert_type(
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
            volatility='stable',
            text=self.text,
        )


class GetSchemaObjectNameFunction(dbops.Function):
    text = '''
        SELECT coalesce(
            (SELECT name FROM edgedb."_SchemaObject"
             WHERE id = type::uuid),
            edgedb.raise(
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


class IssubclassFunction(dbops.Function):
    text = '''
        SELECT
            clsid = any(classes) OR (
                SELECT classes && q.ancestors
                FROM
                    (SELECT
                        array_agg(o.target) AS ancestors
                        FROM edgedb."_SchemaInheritingObject__ancestors" o
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


class IssubclassFunction2(dbops.Function):
    text = '''
        SELECT
            clsid = pclsid OR (
                SELECT
                    pclsid IN (
                        SELECT
                            o.target
                        FROM edgedb."_SchemaInheritingObject__ancestors" o
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


class NormalizeNameFunction(dbops.Function):
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


class GetNameModuleFunction(dbops.Function):
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


class NullIfArrayNullsFunction(dbops.Function):
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


class IndexDescType(dbops.CompositeType):
    """Introspected index description."""
    def __init__(self) -> None:
        super().__init__(name=('edgedb', 'intro_index_desc_t'))

        self.add_columns([
            dbops.Column(name='table_name', type='text[]'),
            dbops.Column(name='name', type='text'),
            dbops.Column(name='is_unique', type='bool'),
            dbops.Column(name='predicate', type='text'),
            dbops.Column(name='expression', type='text'),
            dbops.Column(name='columns', type='text[]'),
            dbops.Column(name='metadata', type='jsonb'),
        ])


class IntrospectIndexesFunction(dbops.Function):
    """Return set of indexes for each table."""

    text = '''
        SELECT
            i.table_name,
            i.index_name,
            i.index_is_unique,
            i.index_predicate,
            i.index_expression,
            i.index_columns,
            i.index_metadata
        FROM
            (SELECT
                *
             FROM
                (SELECT
                    ARRAY[ns.nspname::text, c.relname::text]
                                                    AS table_name,
                    ic.relname::text                AS index_name,
                    i.indisunique                   AS index_is_unique,
                    pg_get_expr(i.indpred, i.indrelid)::text
                                                    AS index_predicate,
                    pg_get_expr(i.indexprs, i.indrelid)::text
                                                    AS index_expression,

                    (SELECT
                        array_agg(ia.attname::text ORDER BY ia.attnum)
                     FROM
                        pg_attribute AS ia
                     WHERE
                        ia.attrelid = i.indexrelid
                        AND (ia.attnum IS NULL OR ia.attnum >= 1)
                    )                               AS index_columns,

                    edgedb.obj_metadata(i.indexrelid, 'pg_class')
                                                    AS index_metadata

                 FROM
                    pg_class AS c
                    INNER JOIN pg_namespace AS ns ON ns.oid = c.relnamespace
                    INNER JOIN pg_index AS i ON i.indrelid = c.oid
                    INNER JOIN pg_class AS ic ON i.indexrelid = ic.oid

                 WHERE
                    ($1::text IS NULL OR ns.nspname LIKE $1::text) AND
                    ($2::text IS NULL OR c.relname LIKE $2::text) AND
                    ($3::text[] IS NULL OR
                        ns.nspname || '.' || ic.relname = any($3::text[])) AND
                    ($4::text IS NULL OR ic.relname LIKE $4::text)
                ) AS q

             WHERE
                (NOT $5::bool OR
                    (index_metadata IS NOT NULL AND
                        (index_metadata->>'ddl:inherit')::bool))
                AND (
                    $6 OR
                    (
                        index_metadata IS NULL OR
                        NOT coalesce(
                            (index_metadata->>'ddl:inherited')::bool, false)
                    )
                )

            ) AS i
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'introspect_indexes'),
            args=[
                ('schema_pattern', 'text', 'NULL'),
                ('table_pattern', 'text', 'NULL'),
                ('table_list', 'text[]', 'NULL'),
                ('index_pattern', 'text', 'NULL'),
                ('inheritable_only', 'bool', 'FALSE'),
                ('include_inherited', 'bool', 'FALSE'),
            ],
            returns=('edgedb', 'intro_index_desc_t'),
            set_returning=True,
            volatility='stable',
            language='sql',
            text=self.__class__.text)


class TriggerDescType(dbops.CompositeType):
    """Introspected trigger description."""
    def __init__(self) -> None:
        super().__init__(name=('edgedb', 'intro_trigger_desc_t'))

        self.add_columns([
            dbops.Column(name='table_name', type='text[]'),
            dbops.Column(name='name', type='text'),
            dbops.Column(name='proc', type='text[]'),
            dbops.Column(name='is_constraint', type='bool'),
            dbops.Column(name='granularity', type='text'),
            dbops.Column(name='deferred', type='bool'),
            dbops.Column(name='timing', type='text'),
            dbops.Column(name='events', type='text[]'),
            dbops.Column(name='definition', type='text'),
            dbops.Column(name='condition', type='text'),
            dbops.Column(name='metadata', type='jsonb'),
        ])


class IntrospectTriggersFunction(dbops.Function):
    """Return a set of triggers for each table."""

    text = '''

        SELECT
            table_name,
            trg_name,
            trg_proc,
            trg_constraint,
            trg_granularity,
            trg_deferred,
            trg_timing,
            trg_events,
            trg_definition,
            NULL::text,
            trg_metadata
        FROM
            (SELECT
                *
             FROM
                (SELECT
                    ARRAY[ns.nspname::text, tc.relname::text]
                                                            AS table_name,

                    t.oid::int                              AS trg_id,
                    t.tgname::text                          AS trg_name,

                    (SELECT
                        ARRAY[nsp.nspname::text, p.proname::text]
                     FROM
                        pg_proc AS p
                        INNER JOIN pg_namespace AS nsp
                                ON nsp.oid = p.pronamespace
                     WHERE
                        t.tgfoid = p.oid
                    )                                       AS trg_proc,

                    t.tgconstraint != 0                     AS trg_constraint,

                    (CASE
                        WHEN (t.tgtype & (1 << 0)) != 0 THEN 'row'
                        ELSE 'statement'
                    END)                                    AS trg_granularity,

                    t.tginitdeferred                        AS trg_deferred,

                    (CASE
                        WHEN (t.tgtype & (1 << 1)) != 0 THEN 'before'
                        WHEN (t.tgtype & (1 << 6)) != 0 THEN 'instead'
                        ELSE 'after'
                    END)                                    AS trg_timing,

                    array_remove(ARRAY[
                        (CASE WHEN (t.tgtype & (1 << 2)) != 0 THEN 'insert'
                         ELSE NULL END),
                        (CASE WHEN (t.tgtype & (1 << 3)) != 0 THEN 'delete'
                         ELSE NULL END),
                        (CASE WHEN (t.tgtype & (1 << 4)) != 0 THEN 'update'
                         ELSE NULL END),
                        (CASE WHEN (t.tgtype & (1 << 5)) != 0 THEN 'truncate'
                         ELSE NULL END)
                    ]::text[], NULL)                        AS trg_events,

                    pg_get_triggerdef(t.oid)::text          AS trg_definition,

                    edgedb.obj_metadata(t.oid, 'pg_trigger') AS trg_metadata

                 FROM
                    pg_trigger AS t
                    INNER JOIN pg_class AS tc ON t.tgrelid = tc.oid
                    INNER JOIN pg_namespace AS ns ON ns.oid = tc.relnamespace

                 WHERE
                    ($1::text IS NULL OR ns.nspname LIKE $1::text) AND
                    ($2::text IS NULL OR tc.relname LIKE $2::text) AND
                    ($3::text[] IS NULL OR
                        ns.nspname || '.' || tc.relname = any($3::text[])) AND
                    ($4::text IS NULL OR t.tgname LIKE $4::text)
                ) AS q

             WHERE
                (NOT $5::bool OR
                    (trg_metadata IS NOT NULL AND
                        (trg_metadata->>'ddl:inherit')::bool))

                AND (
                    $6 OR
                    (
                        trg_metadata IS NULL OR
                        NOT coalesce(
                            (trg_metadata->>'ddl:inherited')::bool, false)
                    )
                )
            ) AS t
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'introspect_triggers'),
            args=[
                ('schema_pattern', 'text', 'NULL'),
                ('table_pattern', 'text', 'NULL'),
                ('table_list', 'text[]', 'NULL'),
                ('trigger_pattern', 'text', 'NULL'),
                ('inheritable_only', 'bool', 'FALSE'),
                ('include_inherited', 'bool', 'FALSE'),
            ],
            returns=('edgedb', 'intro_trigger_desc_t'),
            set_returning=True,
            volatility='stable',
            language='sql',
            text=self.__class__.text)


class TableInheritanceDescType(dbops.CompositeType):
    """Introspected table inheritance descriptor."""
    def __init__(self) -> None:
        super().__init__(name=('edgedb', 'intro_tab_inh_t'))

        self.add_columns([
            dbops.Column(name='name', type='text[]'),
            dbops.Column(name='depth', type='int'),
            dbops.Column(name='pos', type='int'),
        ])


class GetTableDescendantsFunction(dbops.Function):
    """Return a set of table descendants."""

    text = '''
        SELECT
            *
        FROM
            (WITH RECURSIVE
                inheritance(oid, name, ns, depth, path) AS (
                    SELECT
                        c.oid,
                        c.relname,
                        ns.nspname,
                        0,
                        ARRAY[c.relname]
                    FROM
                        pg_class c
                        INNER JOIN pg_namespace ns
                            ON c.relnamespace = ns.oid
                    WHERE
                        ($1::text IS NULL OR
                            ns.nspname LIKE $1::text) AND
                        ($2::text IS NULL OR
                            c.relname LIKE $2::text)

                    UNION ALL

                    SELECT
                        c.oid,
                        c.relname,
                        ns.nspname,
                        i.depth + 1,
                        i.path || c.relname
                    FROM
                        pg_class c,
                        inheritance i,
                        pg_inherits pgi,
                        pg_namespace ns
                    WHERE
                        i.oid = pgi.inhparent
                        AND c.oid = pgi.inhrelid
                        AND ns.oid = c.relnamespace
                        AND ($3::int IS NULL OR i.depth < $3::int)
            )
            SELECT DISTINCT ON (ns, name)
                ARRAY[ns::text, name::text], depth, 0 FROM inheritance) q
        WHERE
            depth > 0
        ORDER BY
            depth
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'get_table_descendants'),
            args=[
                ('schema_name', 'text'),
                ('table_name', 'text'),
                ('max_depth', 'int', 'NULL'),
            ],
            returns=('edgedb', 'intro_tab_inh_t'),
            set_returning=True,
            volatility='stable',
            language='sql',
            text=self.__class__.text)


class ParseTriggerConditionFunction(dbops.Function):
    """Return a set of table descendants."""

    text = '''
        DECLARE
            when_off integer;
            pos integer;
            brackets integer;
            chr text;
            def_len integer;
        BEGIN
            def_len := char_length(definition);
            when_off := strpos(definition, 'WHEN (');
            IF when_off IS NULL OR when_off = 0 THEN
                RETURN NULL;
            ELSE
                pos := when_off + 6;
                brackets := 1;
                WHILE brackets > 0 AND pos < def_len LOOP
                    chr := substr(definition, pos, 1);
                    IF chr = ')' THEN
                        brackets := brackets - 1;
                    ELSIF chr = '(' THEN
                        brackets := brackets + 1;
                    END IF;
                    pos := pos + 1;
                END LOOP;

                IF brackets != 0 THEN
                    RAISE EXCEPTION
                        'cannot parse trigger condition: %',
                        definition;
                END IF;

                RETURN substr(
                    definition,
                    when_off + 6,
                    pos - (when_off + 6) - 1
                );
            END IF;
        END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_parse_trigger_condition'),
            args=[
                ('definition', 'text'),
            ],
            returns='text',
            volatility='stable',
            language='plpgsql',
            text=self.__class__.text)


class NormalizeArrayIndexFunction(dbops.Function):
    """Convert an EdgeQL index to SQL index."""
    text = '''
        SELECT (
            CASE WHEN index < 0 THEN
                length + index + 1
            ELSE
                index + 1
            END
        )::int
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_normalize_array_index'),
            args=[('index', ('bigint',)), ('length', ('int',))],
            returns=('int',),
            volatility='immutable',
            strict=True,
            text=self.text)


class ArrayIndexWithBoundsFunction(dbops.Function):
    """Get an array element or raise an out-of-bounds exception."""
    text = '''
        SELECT edgedb.raise_on_null(
            val[edgedb._normalize_array_index(index, array_upper(val, 1))],
            'array_subscript_error',
            msg => 'array index ' || index::text || ' is out of bounds',
            detail => detail
        )
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('anyarray',)), ('index', ('bigint',)),
                  ('detail', ('text',))],
            returns=('anyelement',),
            # Same volatility as raise()
            volatility='stable',
            strict=True,
            text=self.text,
        )


class ArraySliceFunction(dbops.Function):
    """Get an array slice."""
    text = '''
        SELECT
            CASE
                WHEN start IS NULL THEN
                    val[:edgedb._normalize_array_index(
                            stop, array_upper(val, 1)) - 1]
                WHEN stop IS NULL THEN
                    val[edgedb._normalize_array_index(
                            start, array_upper(val, 1)):]
                ELSE
                    val[edgedb._normalize_array_index(
                            start, array_upper(val, 1)):
                        edgedb._normalize_array_index(
                            stop, array_upper(val, 1)) - 1]
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_slice'),
            args=[('val', ('anyarray',)), ('start', ('bigint',)),
                  ('stop', ('bigint',))],
            returns=('anyarray',),
            volatility='immutable',
            text=self.text,
        )


class StringIndexWithBoundsFunction(dbops.Function):
    """Get a string character or raise an out-of-bounds exception."""
    text = '''
        SELECT edgedb.raise_on_empty(
            substr(
                "val",
                edgedb._normalize_array_index("index", char_length("val")),
                1
            ),
            'invalid_parameter_value',
            "typename" || ' index ' || "index"::text || ' is out of bounds',
            "detail"
        )
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
            # Same volatility as raise_on_empty
            volatility='stable',
            strict=True,
            text=self.text,
        )


class BytesIndexWithBoundsFunction(dbops.Function):
    """Get a bytes character or raise an out-of-bounds exception."""
    text = '''
        SELECT edgedb.raise_on_empty(
            substr(
                "val",
                edgedb._normalize_array_index("index", length("val")),
                1
            ),
            'invalid_parameter_value',
            'byte string index ' || "index"::text || ' is out of bounds',
            "detail"
        )
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
            # Same volatility as raise_on_empty
            volatility='stable',
            strict=True,
            text=self.text,
        )


class SubstrProxyFunction(dbops.Function):
    """Same as substr, but interpret negative length as 0 instead."""
    text = r'''
        SELECT
            CASE
                WHEN length < 0 THEN ''
                ELSE substr(val, start::int, length)
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_substr'),
            args=[('val', ('anyelement',)), ('start', ('bigint',)),
                  ('length', ('int',))],
            returns=('anyelement',),
            volatility='immutable',
            strict=True,
            text=self.text)


class LengthStringProxyFunction(dbops.Function):
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


class LengthBytesProxyFunction(dbops.Function):
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


class StringSliceImplFunction(dbops.Function):
    """Get a string slice."""
    text = r'''
        SELECT
            CASE
                WHEN start IS NULL THEN
                    edgedb._substr(
                        val,
                        1,
                        edgedb._normalize_array_index(
                            stop, edgedb._length(val)) - 1
                    )
                WHEN stop IS NULL THEN
                    substr(
                        val,
                        edgedb._normalize_array_index(
                            start, edgedb._length(val))
                    )
                ELSE
                    edgedb._substr(
                        val,
                        edgedb._normalize_array_index(
                            start, edgedb._length(val)),
                        edgedb._normalize_array_index(
                            stop, edgedb._length(val)) -
                        edgedb._normalize_array_index(
                            start, edgedb._length(val))
                    )
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_str_slice'),
            args=[
                ('val', ('anyelement',)),
                ('start', ('bigint',)), ('stop', ('bigint',))
            ],
            returns=('anyelement',),
            volatility='immutable',
            text=self.text)


class StringSliceFunction(dbops.Function):
    """Get a string slice."""
    text = r'''
        SELECT edgedb._str_slice(val, start, stop)
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_slice'),
            args=[
                ('val', ('text',)),
                ('start', ('bigint',)), ('stop', ('bigint',))
            ],
            returns=('text',),
            volatility='immutable',
            text=self.text)


class BytesSliceFunction(dbops.Function):
    """Get a string slice."""
    text = r'''
        SELECT edgedb._str_slice(val, start, stop)
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_slice'),
            args=[
                ('val', ('bytea',)),
                ('start', ('bigint',)), ('stop', ('bigint',))
            ],
            returns=('bytea',),
            volatility='immutable',
            text=self.text)


class JSONIndexByTextFunction(dbops.Function):
    """Get a JSON element by text index or raise an exception."""
    text = r'''
        SELECT
            CASE jsonb_typeof(val)
            WHEN 'object' THEN (
                edgedb.raise_on_null(
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
                edgedb.raise(
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
                edgedb.raise(
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
            # Same volatility as exception helpers
            volatility='stable',
            strict=True,
            text=self.text,
        )


class JSONIndexByIntFunction(dbops.Function):
    """Get a JSON element by int index or raise an exception."""
    text = r'''
        SELECT
            CASE jsonb_typeof(val)
            WHEN 'object' THEN (
                edgedb.raise(
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
                edgedb.raise_on_null(
                    val -> index::int,
                    'invalid_parameter_value',
                    msg => 'JSON index ' || index::text || ' is out of bounds',
                    detail => detail
                )
            )
            WHEN 'string' THEN (
                to_jsonb(edgedb._index(
                    val#>>'{}',
                    index,
                    detail,
                    'JSON'
                ))
            )
            ELSE
                edgedb.raise(
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
            # Min volatility of exception helpers and pg_typeof (stable).
            volatility='stable',
            strict=True,
            text=self.text,
        )


class JSONSliceFunction(dbops.Function):
    """Get a JSON array slice."""
    text = r'''
        SELECT
            CASE jsonb_typeof(val)
            WHEN 'array' THEN (
                to_jsonb(_slice(
                    (
                        SELECT array_agg(value)
                        FROM jsonb_array_elements(val)
                    ),
                    start, stop
                ))
            )
            WHEN 'string' THEN (
                to_jsonb(_slice(val#>>'{}', start, stop))
            )
            ELSE
                edgedb.raise(
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
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_slice'),
            args=[('val', ('jsonb',)), ('start', ('bigint',)),
                  ('stop', ('bigint',))],
            returns=('jsonb',),
            # Same volatility as to_jsonb (stable)
            volatility='stable',
            text=self.text)


# We need custom casting functions for various datetime scalars in
# order to enforce correctness w.r.t. local vs time-zone-aware
# datetime. Postgres does a lot of magic and guessing for time zones
# and generally will accept text with or without time zone for any
# particular flavor of timestamp. In order to guarantee that we can
# detect time-zones we restrict the inputs to ISO8601 format.
#
# See issue #740.
class DatetimeInFunction(dbops.Function):
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
                edgedb.raise(
                    NULL::edgedb.timestamptz_t,
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
                val::edgedb.timestamptz_t
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'datetime_in'),
            args=[('val', ('text',))],
            returns=('edgedb', 'timestamptz_t'),
            # Same volatility as raise() (stable)
            volatility='stable',
            text=self.text)


class DurationInFunction(dbops.Function):
    """Cast text into duration, ensuring there is no days or months units"""
    text = r'''
        SELECT
            CASE WHEN
                EXTRACT(MONTH FROM v.column1) != 0 OR
                EXTRACT(YEAR FROM v.column1) != 0 OR
                EXTRACT(DAY FROM v.column1) != 0
            THEN
                edgedb.raise(
                    NULL::edgedb.duration_t,
                    'invalid_datetime_format',
                    msg => (
                        'invalid input syntax for type std::duration: '
                        || quote_literal(val)
                    ),
                    detail => (
                        '{"hint":"Units bigger than hours cannot be used '
                        || 'for std::duration."}'
                    )
                )
            ELSE v.column1::edgedb.duration_t
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
            returns=('edgedb', 'duration_t'),
            # Same volatility as raise() (stable)
            volatility='stable',
            text=self.text,
        )


class LocalDatetimeInFunction(dbops.Function):
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
                edgedb.raise(
                    NULL::edgedb.timestamp_t,
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
                val::edgedb.timestamp_t
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'local_datetime_in'),
            args=[('val', ('text',))],
            returns=('edgedb', 'timestamp_t'),
            # Same volatility as raise() (stable)
            volatility='stable',
            text=self.text)


class LocalDateInFunction(dbops.Function):
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
                edgedb.raise(
                    NULL::edgedb.date_t,
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
                val::edgedb.date_t
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'local_date_in'),
            args=[('val', ('text',))],
            returns=('edgedb', 'date_t'),
            # Same volatility as raise() (stable)
            volatility='stable',
            text=self.text)


class LocalTimeInFunction(dbops.Function):
    """Cast text into time using ISO8601 spec."""
    text = r'''
        SELECT
            CASE WHEN val !~ (
                    '^\s*(' ||
                        '(\d{2}(:\d{2}(:\d{2}(\.\d+)?)?)?|\d{2,6}(\.\d+)?)' ||
                    ')\s*$'
                )
            THEN
                edgedb.raise(
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
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'local_time_in'),
            args=[('val', ('text',))],
            returns=('time',),
            # Same volatility as raise() (stable)
            volatility='stable',
            text=self.text,
        )


class ToTimestampTZCheck(dbops.Function):
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

            RETURN result::edgedb.timestamptz_t;
        END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_to_timestamptz_check'),
            args=[('val', ('text',)), ('fmt', ('text',)),
                  ('hastz', ('bool',))],
            returns=('edgedb', 'timestamptz_t'),
            # We're relying on changing settings, so it's volatile.
            volatility='volatile',
            language='plpgsql',
            text=self.text)


class ToDatetimeFunction(dbops.Function):
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
                edgedb.raise(
                    NULL::edgedb.timestamptz_t,
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
                edgedb._to_timestamptz_check(val, fmt, true)
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'to_datetime'),
            args=[('val', ('text',)), ('fmt', ('text',))],
            returns=('edgedb', 'timestamptz_t'),
            # Same as _to_timestamptz_check.
            volatility='volatile',
            text=self.text)


class ToLocalDatetimeFunction(dbops.Function):
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
                edgedb.raise(
                    NULL::edgedb.timestamp_t,
                    'invalid_datetime_format',
                    msg => (
                        'unexpected time zone in format: '
                        || quote_literal(fmt)
                    )
                )
            ELSE
                edgedb._to_timestamptz_check(val, fmt, false)
                    ::edgedb.timestamp_t
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'to_local_datetime'),
            args=[('val', ('text',)), ('fmt', ('text',))],
            returns=('edgedb', 'timestamp_t'),
            # Same as _to_timestamptz_check.
            volatility='volatile',
            text=self.text)


class StrToBool(dbops.Function):
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
                edgedb.raise(
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
            # Stable because it's raising exceptions.
            volatility='stable',
            text=self.text)


class QuoteLiteralFunction(dbops.Function):
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


class QuoteIdentFunction(dbops.Function):
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


class QuoteNameFunction(dbops.Function):

    text = r"""
        SELECT
            string_agg(edgedb.quote_ident(np), '::')
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


class DescribeRolesAsDDLFunctionForwardDecl(dbops.Function):
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


class DescribeRolesAsDDLFunction(dbops.Function):
    """Describe roles as DDL"""

    def __init__(self, schema: s_schema.Schema) -> None:
        role_obj = schema.get("sys::Role", type=s_objtypes.ObjectType)
        roles = inhviewname(schema, role_obj)
        member_of = role_obj.getptr(schema, s_name.UnqualName('member_of'))
        members = inhviewname(schema, member_of)
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
                    role.{qi(name_col)} = { ql(defines.EDGEDB_SUPERUSER) } THEN
                    NULLIF(concat(
                        'ALTER ROLE { qi_superuser } {{',
                        NULLIF((SELECT
                            concat(
                                ' EXTENDING ',
                                string_agg(
                                    edgedb.quote_ident(parent.{qi(name_col)}),
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
                    ), 'ALTER ROLE { qi_superuser } {{}};')
                ELSE
                    concat(
                        'CREATE SUPERUSER ROLE ',
                        edgedb.quote_ident(role.{qi(name_col)}),
                        NULLIF((SELECT
                            concat(' EXTENDING ',
                                string_agg(
                                    edgedb.quote_ident(parent.{qi(name_col)}),
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


class DescribeInstanceConfigAsDDLFunctionForwardDecl(dbops.Function):

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_describe_system_config_as_ddl'),
            args=[],
            returns=('text'),
            volatility='stable',
            text='SELECT NULL::text',
        )


class DescribeDatabaseConfigAsDDLFunctionForwardDecl(dbops.Function):

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_describe_database_config_as_ddl'),
            args=[],
            returns=('text'),
            volatility='stable',
            text='SELECT NULL::text',
        )


class DumpSequencesFunction(dbops.Function):

    text = r"""
        SELECT
            string_agg(
                'SELECT std::sequence_reset('
                || 'INTROSPECT ' || edgedb.quote_name(seq.name)
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
                edgedb."_SchemaScalarType"
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
                        edgedb.get_sequence_backend_name(seq.id)
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
                'postgres command line',
                'postgres global',
                'system override',
                'database',
                'postgres client',
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


class IntervalToMillisecondsFunction(dbops.Function):
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


class SafeIntervalCastFunction(dbops.Function):
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


class ConvertPostgresConfigUnitsFunction(dbops.Function):
    """Convert duration/memory values to milliseconds/kilobytes.

    See https://www.postgresql.org/docs/12/config-setting.html
    for information about the units Postgres config system has.
    """

    text = r"""
    SELECT (
        CASE
            WHEN "unit" = any(ARRAY['us', 'ms', 's', 'min', 'h'])
            THEN to_jsonb(
                edgedb._interval_safe_cast(
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
            THEN trunc("value" * "multiplier")::text::jsonb

            ELSE edgedb.raise(
                NULL::jsonb,
                msg => (
                    'unknown configutation unit "' ||
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


class NormalizedPgSettingsView(dbops.View):
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


class InterpretConfigValueToJsonFunction(dbops.Function):
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
            THEN edgedb._convert_postgres_config_units(
                    "value"::numeric, "multiplier"::numeric, "unit"
                 )

            ELSE
                edgedb.raise(
                    NULL::jsonb,
                    msg => (
                        'unknown configutation type "' ||
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


class PostgresConfigValueToJsonFunction(dbops.Function):
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
                    edgedb._interpret_config_value_to_json(
                        parsed_value.val,
                        settings.vartype,
                        1,
                        parsed_value.unit
                    )

                ELSE
                    edgedb._interpret_config_value_to_json(
                        "setting_value",
                        settings.vartype,
                        settings.multiplier,
                        settings.unit
                    )

            END)
        FROM
            (
                SELECT
                    epg_settings.vartype AS vartype,
                    epg_settings.multiplier AS multiplier,
                    epg_settings.unit AS unit
                FROM
                    edgedb._normalized_pg_settings AS epg_settings
                WHERE
                    epg_settings.name = "setting_name"
            ) AS settings,

            LATERAL (
                SELECT regexp_match(
                    "setting_value", '^(\d+)\s*([a-zA-Z]{0,3})$') AS v
            ) AS _unit,

            LATERAL (
                SELECT
                    COALESCE(_unit.v[1], "setting_value") AS val,
                    COALESCE(_unit.v[2], '') AS unit
            ) AS parsed_value
    """

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_postgres_config_value_to_json'),
            args=[
                ('setting_name', ('text',)),
                ('setting_value', ('text',)),
            ],
            returns=('jsonb',),
            volatility='volatile',
            text=self.text,
        )


class SysConfigFullFunction(dbops.Function):

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
                jsonb_each(
                    (SELECT json
                    FROM edgedbinstdata.instdata
                    WHERE key = 'configspec')
                ) AS s
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

        config_sys AS (
            SELECT
                s.key AS name,
                s.value AS value,
                'system override' AS source,
                config_spec.backend_setting IS NOT NULL AS is_backend
            FROM
                jsonb_each(
                    edgedb.get_database_metadata(
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
                FALSE AS from_backend  -- 'C' is for non-backend settings
            FROM
                _edgecon_state s
            WHERE
                s.type = 'C'
        ),

        pg_db_setting AS (
            SELECT
                spec.name,
                edgedb._postgres_config_value_to_json(
                    spec.backend_setting, nameval.value
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
                        config_spec.backend_setting
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
                    edgedb._postgres_config_value_to_json(
                        spec.backend_setting, setting
                    ) AS value,
                    'postgres configuration file' AS source,
                    TRUE AS is_backend
                FROM
                    pg_file_settings,
                    LATERAL (
                        SELECT
                            config_spec.name,
                            config_spec.backend_setting
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
                    edgedb._postgres_config_value_to_json(
                        spec.backend_setting, setting
                    ) AS value,
                    'system override' AS source,
                    TRUE AS is_backend
                FROM
                    pg_file_settings,
                    LATERAL (
                        SELECT
                            config_spec.name,
                            config_spec.backend_setting
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
                edgedb._interpret_config_value_to_json(
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
                        edgedb._normalized_pg_settings AS epg_settings
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

        edge_all_settings AS (
            SELECT
                q.*
            FROM
                (
                    SELECT * FROM config_defaults UNION ALL
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
            pg_all_settings AS (
                SELECT
                    q.*
                FROM
                    (
                        SELECT * FROM pg_db_setting UNION ALL
                        SELECT * FROM pg_conf_settings UNION ALL
                        SELECT * FROM pg_auto_conf_settings UNION ALL
                        SELECT * FROM pg_config
                    ) AS q
                WHERE
                    q.is_backend
            )
        $$;
    ELSE
        query := query || $$
            pg_all_settings AS (
                SELECT
                    q.*
                FROM
                    (
                        -- config_sys is here, because there
                        -- is no other way to read instance-level
                        -- configuration overrides.
                        SELECT * FROM config_sys UNION ALL
                        SELECT * FROM pg_db_setting UNION ALL
                        SELECT * FROM pg_config
                    ) AS q
                WHERE
                    q.is_backend
            )
        $$;
    END IF;

    query := query || $$
        SELECT
            q.name,
            q.value,
            q.source,
            (CASE
                WHEN q.source < 'database'::edgedb._sys_config_source_t THEN
                    'INSTANCE'
                WHEN q.source = 'database'::edgedb._sys_config_source_t THEN
                    'DATABASE'
                ELSE
                    'SESSION'
            END)::edgedb._sys_config_scope_t AS scope
        FROM
            (SELECT
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
                    ($1 IS NULL OR
                        q.source::edgedb._sys_config_source_t = any($1)
                    )
                    AND ($2 IS NULL OR
                        q.source::edgedb._sys_config_source_t <= $2
                    )
                ) AS u
            ) AS q
        WHERE
            q.n = 1;
    $$;

    RETURN QUERY EXECUTE query USING source_filter, max_source;
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
                    'max_source',
                    ('edgedb', '_sys_config_source_t'),
                    'NULL',
                ),
                (
                    'fs_access',
                    ('bool',),
                    'TRUE',
                )
            ],
            returns=('edgedb', '_sys_config_val_t'),
            set_returning=True,
            language='plpgsql',
            volatility='volatile',
            text=self.text,
        )


class SysConfigFunction(dbops.Function):

    text = f'''
    DECLARE
        backend_caps bigint;
    BEGIN

    backend_caps := edgedb.get_backend_capabilities();
    IF (backend_caps
        & {int(params.BackendCapabilities.CONFIGFILE_ACCESS)}) != 0
    THEN
        RETURN QUERY
        SELECT *
        FROM edgedb._read_sys_config_full(source_filter, max_source, TRUE);
    ELSE
        RETURN QUERY
        SELECT *
        FROM edgedb._read_sys_config_full(source_filter, max_source, FALSE);
    END IF;

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


class ResetSessionConfigFunction(dbops.Function):

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


class ApplySessionConfigFunction(dbops.Function):
    """Apply an EdgeDB config setting to the backend, if possible.

    The function accepts any EdgeDB config name/value pair. If this
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
        for setting_name in backend_settings:
            setting = config_spec[setting_name]

            valql = '"value"->>0'
            if issubclass(setting.type, statypes.Duration):
                valql = f"""
                    edgedb._interval_to_ms(({valql})::interval)::text || 'ms'
                """

            variants_list.append(f'''
                WHEN "name" = {ql(setting_name)}
                THEN
                    pg_catalog.set_config(
                        {ql(setting.backend_setting)}::text,
                        {valql},
                        false
                    )
            ''')

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


class SysVersionFunction(dbops.Function):

    text = f'''
        BEGIN
        RETURN (
            SELECT value
            FROM _edgecon_state
            WHERE name = 'server_version' AND type = 'R'
        );
        END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_sys_version'),
            args=[],
            returns=('jsonb',),
            language='plpgsql',
            volatility='stable',
            text=self.text,
        )


class SysGetTransactionIsolation(dbops.Function):
    "Get transaction isolation value as text compatible with EdgeDB's enum."
    text = r'''
        SELECT
            CASE setting
                WHEN 'repeatable read' THEN 'RepeatableRead'
                WHEN 'serializable' THEN 'Serializable'
                ELSE (
                    SELECT edgedb.raise(
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


class GetCachedReflection(dbops.Function):
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


class GetBaseScalarTypeMap(dbops.Function):
    """Return a map of base EdgeDB scalar type ids to Postgres type names."""

    text = f'''
        VALUES
            {", ".join(
                f"""(
                    {ql(str(k))}::uuid,
                    {
                        ql(f'{v[0]}.{v[1]}') if len(v) == 2
                        else ql(f'pg_catalog.{v[0]}')
                    }
                )"""
            for k, v in types.base_type_name_map.items())}
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_get_base_scalar_type_map'),
            args=[],
            returns=('record',),
            set_returning=True,
            volatility='immutable',
            text=self.text,
        )


class GetPgTypeForEdgeDBTypeFunction(dbops.Function):
    """Return Postgres OID representing a given EdgeDB type."""

    text = f'''
        SELECT
            coalesce(
                (
                    SELECT
                        tn::regtype::oid
                    FROM
                        edgedb._get_base_scalar_type_map()
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
                        typ.typname = "elemid"::text || '_domain'
                        OR typ.typname = "elemid"::text || '_t'
                        OR typ.oid = (
                            SELECT
                                tn::regtype::oid
                            FROM
                                edgedb._get_base_scalar_type_map()
                                    AS m(tid uuid, tn text)
                            WHERE
                                tid = elemid
                        )
                ),
                edgedb.raise(
                    NULL::bigint,
                    'invalid_parameter_value',
                    msg => (
                        format(
                            'cannot determine OID of EdgeDB type %L',
                            typeid::text
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
                ('elemid', ('uuid',)),
            ],
            returns=('bigint',),
            volatility='stable',
            text=self.text,
        )


async def bootstrap(
    conn: asyncpg.Connection,
    config_spec: edbconfig.Spec
) -> None:
    commands = dbops.CommandGroup()
    commands.add_commands([
        dbops.CreateSchema(name='edgedb'),
        dbops.CreateSchema(name='edgedbss'),
        dbops.CreateSchema(name='edgedbpub'),
        dbops.CreateSchema(name='edgedbstd'),
        dbops.CreateCompositeType(ExpressionType()),
        dbops.CreateView(NormalizedPgSettingsView()),
        dbops.CreateTable(DBConfigTable()),
        dbops.CreateTable(DMLDummyTable()),
        dbops.CreateFunction(IntervalToMillisecondsFunction()),
        dbops.CreateFunction(SafeIntervalCastFunction()),
        dbops.CreateFunction(QuoteIdentFunction()),
        dbops.CreateFunction(QuoteNameFunction()),
        dbops.CreateFunction(AlterCurrentDatabaseSetString()),
        dbops.CreateFunction(AlterCurrentDatabaseSetStringArray()),
        dbops.CreateFunction(AlterCurrentDatabaseSetNonArray()),
        dbops.CreateFunction(AlterCurrentDatabaseSetArray()),
        dbops.CreateFunction(GetBackendCapabilitiesFunction()),
        dbops.CreateFunction(GetBackendTenantIDFunction()),
        dbops.CreateFunction(GetDatabaseBackendNameFunction()),
        dbops.CreateFunction(GetRoleBackendNameFunction()),
        dbops.CreateFunction(GetUserSequenceBackendNameFunction()),
        dbops.CreateFunction(GetStdModulesFunction()),
        dbops.CreateFunction(GetObjectMetadata()),
        dbops.CreateFunction(GetColumnMetadata()),
        dbops.CreateFunction(GetSharedObjectMetadata()),
        dbops.CreateFunction(GetDatabaseMetadataFunction()),
        dbops.CreateFunction(GetCurrentDatabaseFunction()),
        dbops.CreateFunction(RaiseExceptionFunction()),
        dbops.CreateFunction(RaiseExceptionOnNullFunction()),
        dbops.CreateFunction(RaiseExceptionOnNotNullFunction()),
        dbops.CreateFunction(RaiseExceptionOnEmptyStringFunction()),
        dbops.CreateFunction(AssertJSONTypeFunction()),
        dbops.CreateFunction(ExtractJSONScalarFunction()),
        dbops.CreateFunction(NormalizeNameFunction()),
        dbops.CreateFunction(GetNameModuleFunction()),
        dbops.CreateFunction(NullIfArrayNullsFunction()),
        dbops.CreateCompositeType(IndexDescType()),
        dbops.CreateFunction(IntrospectIndexesFunction()),
        dbops.CreateCompositeType(TriggerDescType()),
        dbops.CreateFunction(IntrospectTriggersFunction()),
        dbops.CreateCompositeType(TableInheritanceDescType()),
        dbops.CreateDomain(BigintDomain()),
        dbops.CreateDomain(ConfigMemoryDomain()),
        dbops.CreateDomain(TimestampTzDomain()),
        dbops.CreateDomain(TimestampDomain()),
        dbops.CreateDomain(DateDomain()),
        dbops.CreateDomain(DurationDomain()),
        dbops.CreateDomain(RelativeDurationDomain()),
        dbops.CreateFunction(StrToConfigMemoryFunction()),
        dbops.CreateFunction(ConfigMemoryToStrFunction()),
        dbops.CreateFunction(StrToBigint()),
        dbops.CreateFunction(StrToDecimal()),
        dbops.CreateFunction(StrToInt64NoInline()),
        dbops.CreateFunction(StrToInt32NoInline()),
        dbops.CreateFunction(StrToInt16NoInline()),
        dbops.CreateFunction(StrToFloat64NoInline()),
        dbops.CreateFunction(StrToFloat32NoInline()),
        dbops.CreateFunction(GetTableDescendantsFunction()),
        dbops.CreateFunction(ParseTriggerConditionFunction()),
        dbops.CreateFunction(NormalizeArrayIndexFunction()),
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
        dbops.CreateFunction(LocalDatetimeInFunction()),
        dbops.CreateFunction(LocalDateInFunction()),
        dbops.CreateFunction(LocalTimeInFunction()),
        dbops.CreateFunction(ToTimestampTZCheck()),
        dbops.CreateFunction(ToDatetimeFunction()),
        dbops.CreateFunction(ToLocalDatetimeFunction()),
        dbops.CreateFunction(StrToBool()),
        dbops.CreateFunction(BytesIndexWithBoundsFunction()),
        dbops.CreateEnum(SysConfigSourceType()),
        dbops.CreateEnum(SysConfigScopeType()),
        dbops.CreateCompositeType(SysConfigValueType()),
        dbops.CreateFunction(ConvertPostgresConfigUnitsFunction()),
        dbops.CreateFunction(InterpretConfigValueToJsonFunction()),
        dbops.CreateFunction(PostgresConfigValueToJsonFunction()),
        dbops.CreateFunction(SysConfigFullFunction()),
        dbops.CreateFunction(SysConfigFunction()),
        dbops.CreateFunction(SysVersionFunction()),
        dbops.CreateFunction(ResetSessionConfigFunction()),
        dbops.CreateFunction(ApplySessionConfigFunction(config_spec)),
        dbops.CreateFunction(SysGetTransactionIsolation()),
        dbops.CreateFunction(GetCachedReflection()),
        dbops.CreateFunction(GetBaseScalarTypeMap()),
        dbops.CreateFunction(GetPgTypeForEdgeDBTypeFunction()),
        dbops.CreateFunction(DescribeInstanceConfigAsDDLFunctionForwardDecl()),
        dbops.CreateFunction(DescribeDatabaseConfigAsDDLFunctionForwardDecl()),
        dbops.CreateFunction(DescribeRolesAsDDLFunctionForwardDecl()),
    ])

    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)


async def create_pg_extensions(conn: asyncpg.Connection) -> None:
    commands = dbops.CommandGroup()
    commands.add_commands([
        dbops.CreateSchema(name='edgedbext'),
        dbops.CreateExtension(
            dbops.Extension(name='uuid-ossp', schema='edgedbext'),
        ),
    ])
    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)


classref_attr_aliases = {
    'links': 'pointers',
    'link_properties': 'pointers'
}


def tabname(schema: s_schema.Schema, obj: s_obj.Object) -> Tuple[str, str]:
    return (
        'edgedbss',
        common.get_backend_name(
            schema,
            obj,
            aspect='table',
            catenate=False,
        )[1],
    )


def inhviewname(schema: s_schema.Schema, obj: s_obj.Object) -> Tuple[str, str]:
    return (
        'edgedbss',
        common.get_backend_name(
            schema,
            obj,
            aspect='inhview',
            catenate=False,
        )[1],
    )


def ptr_col_name(
    schema: s_schema.Schema,
    obj: s_sources.Source,
    propname: str,
) -> str:
    prop = obj.getptr(schema, s_name.UnqualName(propname))
    psi = types.get_pointer_storage_info(prop, schema=schema)
    return psi.column_name  # type: ignore[no-any-return]


def _generate_database_views(schema: s_schema.Schema) -> List[dbops.View]:
    Database = schema.get('sys::Database', type=s_objtypes.ObjectType)
    annos = Database.getptr(
        schema, s_name.UnqualName('annotations'), type=s_links.Link)
    int_annos = Database.getptr(
        schema, s_name.UnqualName('annotations__internal'), type=s_links.Link)

    view_query = f'''
        SELECT
            ((d.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, Database, 'id'))},
            (SELECT id FROM edgedb."_SchemaObjectType"
                 WHERE name = 'sys::Database')
                AS {qi(ptr_col_name(schema, Database, '__type__'))},
            (CASE WHEN
                (edgedb.get_backend_capabilities()
                 & {int(params.BackendCapabilities.CREATE_DATABASE)}) != 0
             THEN
                datname IN (
                    edgedb.get_database_backend_name(
                        {ql(defines.EDGEDB_TEMPLATE_DB)}),
                    edgedb.get_database_backend_name(
                        {ql(defines.EDGEDB_SYSTEM_DB)})
                )
             ELSE False END
            )
                AS {qi(ptr_col_name(schema, Database, 'internal'))},
            (d.description)->>'name'
                AS {qi(ptr_col_name(schema, Database, 'name'))},
            (d.description)->>'name'
                AS {qi(ptr_col_name(schema, Database, 'name__internal'))},
            ARRAY[]::text[]
                AS {qi(ptr_col_name(schema, Database, 'computed_fields'))},
            ((d.description)->>'builtin')::bool
                AS {qi(ptr_col_name(schema, Database, 'builtin'))}
        FROM
            pg_database dat
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(dat.oid, 'pg_database')
                        AS description
            ) AS d
        WHERE
            (d.description)->>'id' IS NOT NULL
            AND (d.description)->>'tenant_id' = edgedb.get_backend_tenant_id()
    '''

    annos_link_query = f'''
        SELECT
            ((d.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, annos, 'source'))},
            (annotations->>'id')::uuid
                AS {qi(ptr_col_name(schema, annos, 'target'))},
            (annotations->>'value')::text
                AS {qi(ptr_col_name(schema, annos, 'value'))},
            (annotations->>'owned')::bool
                AS {qi(ptr_col_name(schema, annos, 'owned'))}
        FROM
            pg_database dat
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(dat.oid, 'pg_database')
                        AS description
            ) AS d
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements((d.description)->'annotations')
                ) AS annotations
    '''

    int_annos_link_query = f'''
        SELECT
            ((d.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, int_annos, 'source'))},
            (annotations->>'id')::uuid
                AS {qi(ptr_col_name(schema, int_annos, 'target'))},
            (annotations->>'owned')::bool
                AS {qi(ptr_col_name(schema, int_annos, 'owned'))}
        FROM
            pg_database dat
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(dat.oid, 'pg_database')
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
        Database: view_query,
        annos: annos_link_query,
        int_annos: int_annos_link_query,
    }

    views = []
    for obj, query in objects.items():
        tabview = dbops.View(name=tabname(schema, obj), query=query)
        inhview = dbops.View(name=inhviewname(schema, obj), query=query)
        views.append(tabview)
        views.append(inhview)

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
        ver.get_target(schema),
        catenate=False,
    )

    view_query = f'''
        SELECT
            (e.value->>'id')::uuid
                AS {qi(ptr_col_name(schema, ExtPkg, 'id'))},
            (SELECT id FROM edgedb."_SchemaObjectType"
                 WHERE name = 'sys::ExtensionPackage')
                AS {qi(ptr_col_name(schema, ExtPkg, '__type__'))},
            (e.value->>'name')
                AS {qi(ptr_col_name(schema, ExtPkg, 'name'))},
            (e.value->>'name__internal')
                AS {qi(ptr_col_name(schema, ExtPkg, 'name__internal'))},
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
                AS {qi(ptr_col_name(schema, ExtPkg, 'version'))},
            (e.value->>'script')
                AS {qi(ptr_col_name(schema, ExtPkg, 'script'))},
            ARRAY[]::text[]
                AS {qi(ptr_col_name(schema, ExtPkg, 'computed_fields'))},
            (e.value->>'builtin')::bool
                AS {qi(ptr_col_name(schema, ExtPkg, 'builtin'))},
            (e.value->>'internal')::bool
                AS {qi(ptr_col_name(schema, ExtPkg, 'internal'))}
        FROM
            jsonb_each(
                edgedb.get_database_metadata(
                    {ql(defines.EDGEDB_TEMPLATE_DB)}
                ) -> 'ExtensionPackage'
            ) AS e
    '''

    annos_link_query = f'''
        SELECT
            (e.value->>'id')::uuid
                AS {qi(ptr_col_name(schema, annos, 'source'))},
            (annotations->>'id')::uuid
                AS {qi(ptr_col_name(schema, annos, 'target'))},
            (annotations->>'value')::text
                AS {qi(ptr_col_name(schema, annos, 'value'))},
            (annotations->>'is_owned')::bool
                AS {qi(ptr_col_name(schema, annos, 'owned'))}
        FROM
            jsonb_each(
                edgedb.get_database_metadata(
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
            (e.value->>'id')::uuid
                AS {qi(ptr_col_name(schema, int_annos, 'source'))},
            (annotations->>'id')::uuid
                AS {qi(ptr_col_name(schema, int_annos, 'target'))},
            (annotations->>'is_owned')::bool
                AS {qi(ptr_col_name(schema, int_annos, 'owned'))}
        FROM
            jsonb_each(
                edgedb.get_database_metadata(
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

    views = []
    for obj, query in objects.items():
        tabview = dbops.View(name=tabname(schema, obj), query=query)
        inhview = dbops.View(name=inhviewname(schema, obj), query=query)
        views.append(tabview)
        views.append(inhview)

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
                AND g.rolname = edgedb.get_role_backend_name(
                    {ql(defines.EDGEDB_SUPERGROUP)}
                )
        )
    '''

    view_query = f'''
        SELECT
            ((d.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, Role, 'id'))},
            (SELECT id FROM edgedb."_SchemaObjectType"
                 WHERE name = 'sys::Role')
                AS {qi(ptr_col_name(schema, Role, '__type__'))},
            (d.description)->>'name'
                AS {qi(ptr_col_name(schema, Role, 'name'))},
            (d.description)->>'name'
                AS {qi(ptr_col_name(schema, Role, 'name__internal'))},
            {superuser}
                AS {qi(ptr_col_name(schema, Role, 'superuser'))},
            False
                AS {qi(ptr_col_name(schema, Role, 'abstract'))},
            False
                AS {qi(ptr_col_name(schema, Role, 'is_derived'))},
            ARRAY[]::text[]
                AS {qi(ptr_col_name(schema, Role, 'inherited_fields'))},
            ARRAY[]::text[]
                AS {qi(ptr_col_name(schema, Role, 'computed_fields'))},
            ((d.description)->>'builtin')::bool
                AS {qi(ptr_col_name(schema, Role, 'builtin'))},
            False
                AS {qi(ptr_col_name(schema, Role, 'internal'))},
            (d.description)->>'password_hash'
                AS {qi(ptr_col_name(schema, Role, 'password'))}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
        WHERE
            (d.description)->>'id' IS NOT NULL
            AND (d.description)->>'tenant_id' = edgedb.get_backend_tenant_id()
    '''

    member_of_link_query = f'''
        SELECT
            ((d.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, member_of, 'source'))},
            ((md.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, member_of, 'target'))}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
            INNER JOIN pg_auth_members m ON m.member = a.oid
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(m.roleid, 'pg_authid')
                        AS description
            ) AS md
    '''

    bases_link_query = f'''
        SELECT
            ((d.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, bases, 'source'))},
            ((md.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, bases, 'target'))},
            row_number() OVER (PARTITION BY a.oid ORDER BY m.roleid)
                AS {qi(ptr_col_name(schema, bases, 'index'))}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
            INNER JOIN pg_auth_members m ON m.member = a.oid
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(m.roleid, 'pg_authid')
                        AS description
            ) AS md
    '''

    ancestors_link_query = f'''
        SELECT
            ((d.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, ancestors, 'source'))},
            ((md.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, ancestors, 'target'))},
            row_number() OVER (PARTITION BY a.oid ORDER BY m.roleid)
                AS {qi(ptr_col_name(schema, ancestors, 'index'))}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
            INNER JOIN pg_auth_members m ON m.member = a.oid
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(m.roleid, 'pg_authid')
                        AS description
            ) AS md
    '''

    annos_link_query = f'''
        SELECT
            ((d.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, annos, 'source'))},
            (annotations->>'id')::uuid
                AS {qi(ptr_col_name(schema, annos, 'target'))},
            (annotations->>'value')::text
                AS {qi(ptr_col_name(schema, annos, 'value'))},
            (annotations->>'owned')::bool
                AS {qi(ptr_col_name(schema, annos, 'owned'))}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(
                        (d.description)->'annotations'
                    )
                ) AS annotations
    '''

    int_annos_link_query = f'''
        SELECT
            ((d.description)->>'id')::uuid
                AS {qi(ptr_col_name(schema, int_annos, 'source'))},
            (annotations->>'id')::uuid
                AS {qi(ptr_col_name(schema, int_annos, 'target'))},
            (annotations->>'owned')::bool
                AS {qi(ptr_col_name(schema, int_annos, 'owned'))}
        FROM
            pg_catalog.pg_roles AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(a.oid, 'pg_authid')
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

    views = []
    for obj, query in objects.items():
        tabview = dbops.View(name=tabname(schema, obj), query=query)
        inhview = dbops.View(name=inhviewname(schema, obj), query=query)
        views.append(tabview)
        views.append(inhview)

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

    view_query = f'''
        SELECT
            (json->>'id')::uuid
                AS {qi(ptr_col_name(schema, Role, 'id'))},
            (SELECT id FROM edgedb."_SchemaObjectType"
                 WHERE name = 'sys::Role')
                AS {qi(ptr_col_name(schema, Role, '__type__'))},
            json->>'name'
                AS {qi(ptr_col_name(schema, Role, 'name'))},
            json->>'name'
                AS {qi(ptr_col_name(schema, Role, 'name__internal'))},
            True
                AS {qi(ptr_col_name(schema, Role, 'superuser'))},
            False
                AS {qi(ptr_col_name(schema, Role, 'abstract'))},
            False
                AS {qi(ptr_col_name(schema, Role, 'is_derived'))},
            ARRAY[]::text[]
                AS {qi(ptr_col_name(schema, Role, 'inherited_fields'))},
            ARRAY[]::text[]
                AS {qi(ptr_col_name(schema, Role, 'computed_fields'))},
            True
                AS {qi(ptr_col_name(schema, Role, 'builtin'))},
            False
                AS {qi(ptr_col_name(schema, Role, 'internal'))},
            json->>'password_hash'
                AS {qi(ptr_col_name(schema, Role, 'password'))}
        FROM
            edgedbinstdata.instdata
        WHERE
            key = 'single_role_metadata'
            AND json->>'tenant_id' = edgedb.get_backend_tenant_id()
    '''

    member_of_link_query = f'''
        SELECT
            '00000000-0000-0000-0000-000000000000'::uuid
                AS {qi(ptr_col_name(schema, member_of, 'source'))},
            '00000000-0000-0000-0000-000000000000'::uuid
                AS {qi(ptr_col_name(schema, member_of, 'target'))}
        LIMIT 0
    '''

    bases_link_query = f'''
        SELECT
            '00000000-0000-0000-0000-000000000000'::uuid
                AS {qi(ptr_col_name(schema, bases, 'source'))},
            '00000000-0000-0000-0000-000000000000'::uuid
                AS {qi(ptr_col_name(schema, bases, 'target'))},
            0 AS {qi(ptr_col_name(schema, bases, 'index'))}
        LIMIT 0
    '''

    ancestors_link_query = f'''
        SELECT
            '00000000-0000-0000-0000-000000000000'::uuid
                AS {qi(ptr_col_name(schema, ancestors, 'source'))},
            '00000000-0000-0000-0000-000000000000'::uuid
                AS {qi(ptr_col_name(schema, ancestors, 'target'))},
            0 AS {qi(ptr_col_name(schema, ancestors, 'index'))}
        LIMIT 0
    '''

    annos_link_query = f'''
        SELECT
            (json->>'id')::uuid
                AS {qi(ptr_col_name(schema, annos, 'source'))},
            (annotations->>'id')::uuid
                AS {qi(ptr_col_name(schema, annos, 'target'))},
            (annotations->>'value')::text
                AS {qi(ptr_col_name(schema, annos, 'value'))},
            (annotations->>'owned')::bool
                AS {qi(ptr_col_name(schema, annos, 'owned'))}
        FROM
            edgedbinstdata.instdata
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(json->'annotations')
                ) AS annotations
        WHERE
            key = 'single_role_metadata'
            AND json->>'tenant_id' = edgedb.get_backend_tenant_id()
    '''

    int_annos_link_query = f'''
        SELECT
            (json->>'id')::uuid
                AS {qi(ptr_col_name(schema, int_annos, 'source'))},
            (annotations->>'id')::uuid
                AS {qi(ptr_col_name(schema, int_annos, 'target'))},
            (annotations->>'owned')::bool
                AS {qi(ptr_col_name(schema, int_annos, 'owned'))}
        FROM
            edgedbinstdata.instdata
            CROSS JOIN LATERAL
                ROWS FROM (
                    jsonb_array_elements(json->'annotations__internal')
                ) AS annotations
        WHERE
            key = 'single_role_metadata'
            AND json->>'tenant_id' = edgedb.get_backend_tenant_id()
    '''

    objects = {
        Role: view_query,
        member_of: member_of_link_query,
        bases: bases_link_query,
        ancestors: ancestors_link_query,
        annos: annos_link_query,
        int_annos: int_annos_link_query,
    }

    views = []
    for obj, query in objects.items():
        tabview = dbops.View(name=tabname(schema, obj), query=query)
        inhview = dbops.View(name=inhviewname(schema, obj), query=query)
        views.append(tabview)
        views.append(inhview)

    return views


def _generate_schema_ver_views(schema: s_schema.Schema) -> List[dbops.View]:
    Ver = schema.get(
        'sys::GlobalSchemaVersion',
        type=s_objtypes.ObjectType,
    )

    view_query = f'''
        SELECT
            (v.value->>'id')::uuid
                AS {qi(ptr_col_name(schema, Ver, 'id'))},
            (SELECT id FROM edgedb."_SchemaObjectType"
                 WHERE name = 'sys::GlobalSchemaVersion')
                AS {qi(ptr_col_name(schema, Ver, '__type__'))},
            (v.value->>'name')
                AS {qi(ptr_col_name(schema, Ver, 'name'))},
            (v.value->>'name')
                AS {qi(ptr_col_name(schema, Ver, 'name__internal'))},
            (v.value->>'version')::uuid
                AS {qi(ptr_col_name(schema, Ver, 'version'))},
            (v.value->>'builtin')::bool
                AS {qi(ptr_col_name(schema, Ver, 'builtin'))},
            (v.value->>'internal')::bool
                AS {qi(ptr_col_name(schema, Ver, 'internal'))},
            ARRAY[]::text[]
                AS {qi(ptr_col_name(schema, Ver, 'computed_fields'))}
        FROM
            jsonb_each(
                edgedb.get_database_metadata(
                    {ql(defines.EDGEDB_TEMPLATE_DB)}
                ) -> 'GlobalSchemaVersion'
            ) AS v
    '''

    objects = {
        Ver: view_query
    }

    views = []
    for obj, query in objects.items():
        tabview = dbops.View(name=tabname(schema, obj), query=query)
        inhview = dbops.View(name=inhviewname(schema, obj), query=query)
        views.append(tabview)
        views.append(inhview)

    return views


def _make_json_caster(
    schema: s_schema.Schema,
    stype: s_types.Type,
    context: str,
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

    cast_sql, _ = compiler.compile_ir_to_sql(
        cast_ir,
        use_named_params=True,
        singleton_mode=True,
    )

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
        views.append(_generate_schema_alias_view(schema, schema_obj))

    return views


def _generate_schema_alias_view(
    schema: s_schema.Schema,
    obj: s_sources.Source,
) -> dbops.View:

    module = obj.get_name(schema).module
    bn = common.get_backend_name(
        schema,
        obj,
        aspect='inhview',
        catenate=False,
    )

    if module == 'sys' and not obj.get_abstract(schema):
        bn = ('edgedbss', bn[1])

    targets = []

    if isinstance(obj, s_links.Link):
        expected_tt = "link"
    else:
        expected_tt = "ObjectType"

    for ptr in obj.get_pointers(schema).objects(schema):
        if ptr.is_pure_computable(schema):
            continue
        psi = types.get_pointer_storage_info(ptr, schema=schema)
        if psi.table_type == expected_tt:
            ptr_name = ptr.get_shortname(schema).name
            col_name = psi.column_name
            if col_name != ptr_name:
                targets.append(f'{qi(col_name)} AS {qi(ptr_name)}')
            targets.append(f'{qi(col_name)} AS {qi(col_name)}')

    prefix = module.capitalize()

    if isinstance(obj, s_links.Link):
        objtype = obj.get_source(schema)
        assert objtype is not None
        objname = objtype.get_name(schema).name
        lname = obj.get_shortname(schema).name
        name = f'_{prefix}{objname}__{lname}'
    else:
        name = f'_{prefix}{obj.get_name(schema).name}'

    return dbops.View(
        name=('edgedb', name),
        query=(f'SELECT {", ".join(targets)} FROM {q(*bn)}')
    )


async def generate_support_views(
    conn: asyncpg.Connection,
    schema: s_schema.Schema,
    backend_params: params.BackendRuntimeParams,
) -> None:
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

    conf = schema.get('cfg::Config', type=s_objtypes.ObjectType)
    cfg_views, _ = _generate_config_type_view(
        schema, conf, scope=None, path=[], rptr=None)
    commands.add_commands([
        dbops.CreateView(dbops.View(name=tn, query=q), or_replace=True)
        for tn, q in cfg_views
    ])

    conf = schema.get('cfg::InstanceConfig', type=s_objtypes.ObjectType)
    cfg_views, _ = _generate_config_type_view(
        schema, conf, scope=qltypes.ConfigScope.INSTANCE, path=[], rptr=None)
    commands.add_commands([
        dbops.CreateView(dbops.View(name=tn, query=q), or_replace=True)
        for tn, q in cfg_views
    ])

    conf = schema.get('cfg::DatabaseConfig', type=s_objtypes.ObjectType)
    cfg_views, _ = _generate_config_type_view(
        schema, conf, scope=qltypes.ConfigScope.DATABASE, path=[], rptr=None)
    commands.add_commands([
        dbops.CreateView(dbops.View(name=tn, query=q), or_replace=True)
        for tn, q in cfg_views
    ])

    abstract_conf = delta.CompositeMetaCommand.get_inhview(
        schema,
        schema.get('cfg::AbstractConfig', type=s_objtypes.ObjectType),
        exclude_self=True,
        pg_schema='edgedbss',
    )

    commands.add_command(
        dbops.CreateView(abstract_conf, or_replace=True)
    )

    for dbview in _generate_database_views(schema):
        commands.add_command(dbops.CreateView(dbview, or_replace=True))

    for extview in _generate_extension_views(schema):
        commands.add_command(dbops.CreateView(extview, or_replace=True))

    if backend_params.has_create_role:
        role_views = _generate_role_views(schema)
    else:
        role_views = _generate_single_role_views(schema)
    for roleview in role_views:
        commands.add_command(dbops.CreateView(roleview, or_replace=True))

    for verview in _generate_schema_ver_views(schema):
        commands.add_command(dbops.CreateView(verview, or_replace=True))

    sys_alias_views = _generate_schema_alias_views(
        schema, s_name.UnqualName('sys'))
    for alias_view in sys_alias_views:
        commands.add_command(dbops.CreateView(alias_view, or_replace=True))

    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)


async def generate_support_functions(
    conn: asyncpg.Connection,
    schema: s_schema.Schema,
) -> None:
    commands = dbops.CommandGroup()

    commands.add_commands([
        dbops.CreateFunction(IssubclassFunction()),
        dbops.CreateFunction(IssubclassFunction2()),
        dbops.CreateFunction(GetSchemaObjectNameFunction()),
    ])

    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)


async def generate_more_support_functions(
    conn: asyncpg.Connection,
    compiler: edbcompiler.Compiler,
    schema: s_schema.Schema,
    testmode: bool,
) -> None:
    commands = dbops.CommandGroup()

    _, text = edbbootstrap.compile_bootstrap_script(
        compiler,
        schema,
        _describe_config(
            schema, source='system override', testmode=testmode),
        output_format=edbcompiler.IoFormat.BINARY,
    )

    DescribeInstanceConfigAsDDLFunction = dbops.Function(
        name=('edgedb', '_describe_system_config_as_ddl'),
        args=[],
        returns=('text'),
        # Stable because it's raising exceptions.
        volatility='stable',
        text=text,
    )

    _, text = edbbootstrap.compile_bootstrap_script(
        compiler,
        schema,
        _describe_config(
            schema, source='database', testmode=testmode),
        output_format=edbcompiler.IoFormat.BINARY,
    )

    DescribeDatabaseConfigAsDDLFunction = dbops.Function(
        name=('edgedb', '_describe_database_config_as_ddl'),
        args=[],
        returns=('text'),
        # Stable because it's raising exceptions.
        volatility='stable',
        text=text,
    )

    commands.add_commands([
        dbops.CreateFunction(
            DescribeInstanceConfigAsDDLFunction, or_replace=True),
        dbops.CreateFunction(
            DescribeDatabaseConfigAsDDLFunction, or_replace=True),
        dbops.CreateFunction(
            DescribeRolesAsDDLFunction(schema), or_replace=True),
        dbops.CreateFunction(GetSequenceBackendNameFunction()),
        dbops.CreateFunction(DumpSequencesFunction()),
    ])

    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)


def _describe_config(
    schema: s_schema.Schema,
    source: str,
    testmode: bool,
) -> str:
    """Generate an EdgeQL query to render config as DDL."""

    if source == 'system override':
        scope = qltypes.ConfigScope.INSTANCE
        config_object_name = 'cfg::InstanceConfig'
    elif source == 'database':
        scope = qltypes.ConfigScope.DATABASE
        config_object_name = 'cfg::DatabaseConfig'
    else:
        raise AssertionError(f'unexpected configuration source: {source!r}')

    cfg = schema.get(config_object_name, type=s_objtypes.ObjectType)
    items = []
    for ptr_name, p in cfg.get_pointers(schema).items(schema):
        pn = str(ptr_name)
        if pn in ('id', '__type__'):
            continue

        is_internal = (
            p.get_annotation(
                schema,
                s_name.QualName('cfg', 'internal')
            ) == 'true'
        )
        if is_internal and not testmode:
            continue

        ptype = p.get_target(schema)
        assert ptype is not None
        ptr_card = p.get_cardinality(schema)
        mult = ptr_card.is_multi()
        if isinstance(ptype, s_objtypes.ObjectType):
            item = textwrap.indent(
                _render_config_object(
                    schema=schema,
                    valtype=ptype,
                    value_expr=str(ptype.get_name(schema)),
                    scope=scope,
                    join_term='',
                    level=1,
                ),
                ' ' * 4,
            )
        else:
            psource = f'{config_object_name}.{ qlquote.quote_ident(pn) }'
            renderer = _render_config_set if mult else _render_config_scalar
            item = textwrap.indent(
                renderer(
                    schema=schema,
                    valtype=ptype,
                    value_expr=psource,
                    name=pn,
                    scope=scope,
                    level=1,
                ),
                ' ' * 4,
            )

        condition = f'EXISTS json_get(conf, {ql(pn)})'
        if is_internal:
            condition = f'({condition}) AND testmode'
        items.append(f"(\n{item}\n    IF {condition} ELSE ''\n  )")

    testmode_check = (
        "<bool>json_get(cfg::get_config_json(),'__internal_testmode','value')"
        " ?? false"
    )
    query = (
        f"FOR conf IN {{cfg::get_config_json(sources := [{ql(source)}])}} "
        + "UNION (\n"
        + (f"FOR testmode IN {{{testmode_check}}} UNION (\n"
           if testmode else "")
        + "SELECT\n  " + ' ++ '.join(items)
        + (")" if testmode else "")
        + ")"
    )
    return query


def _render_config_value(
    *,
    schema: s_schema.Schema,
    valtype: s_types.Type,
    value_expr: str,
) -> str:
    if valtype.issubclass(
        schema,
        schema.get('std::anyreal', type=s_scalars.ScalarType),
    ):
        val = f'<str>{value_expr}'
    elif valtype.issubclass(
        schema,
        schema.get('std::bool', type=s_scalars.ScalarType),
    ):
        val = f'<str>{value_expr}'
    elif valtype.issubclass(
        schema,
        schema.get('std::duration', type=s_scalars.ScalarType),
    ):
        val = f'"<std::duration>" ++ cfg::_quote(<str>{value_expr})'
    elif valtype.issubclass(
        schema,
        schema.get('cfg::memory', type=s_scalars.ScalarType),
    ):
        val = f'"<cfg::memory>" ++ cfg::_quote(<str>{value_expr})'
    elif valtype.issubclass(
        schema,
        schema.get('std::str', type=s_scalars.ScalarType),
    ):
        val = f'cfg::_quote({value_expr})'
    elif valtype.is_enum(schema):
        tn = valtype.get_name(schema)
        val = f'"<{str(tn)}>" ++ cfg::_quote(<str>{value_expr})'
    else:
        raise AssertionError(
            f'unexpected configuration value type: '
            f'{valtype.get_displayname(schema)}'
        )

    return val


def _render_config_set(
    *,
    schema: s_schema.Schema,
    valtype: s_types.Type,
    value_expr: str,
    scope: qltypes.ConfigScope,
    name: str,
    level: int,
) -> str:
    assert isinstance(valtype, s_scalars.ScalarType)
    v = _render_config_value(
        schema=schema, valtype=valtype, value_expr=value_expr)
    if level == 1:
        return (
            f"'CONFIGURE {scope.to_edgeql()} "
            f"SET { qlquote.quote_ident(name) } := {{' ++ "
            f"array_join(array_agg({v}), ', ') ++ '}};'"
        )
    else:
        indent = ' ' * (4 * (level - 1))
        return (
            f"'{indent}{ qlquote.quote_ident(name) } := {{' ++ "
            f"array_join(array_agg({v}), ', ') ++ '}},'"
        )


def _render_config_scalar(
    *,
    schema: s_schema.Schema,
    valtype: s_types.Type,
    value_expr: str,
    scope: qltypes.ConfigScope,
    name: str,
    level: int,
) -> str:
    assert isinstance(valtype, s_scalars.ScalarType)
    v = _render_config_value(
        schema=schema, valtype=valtype, value_expr=value_expr)
    if level == 1:
        return (
            f"'CONFIGURE {scope.to_edgeql()} "
            f"SET { qlquote.quote_ident(name) } := ' ++ {v} ++ ';'"
        )
    else:
        indent = ' ' * (4 * (level - 1))
        return f"'{indent}{ qlquote.quote_ident(name) } := ' ++ {v} ++ ','"


def _render_config_object(
    *,
    schema: s_schema.Schema,
    valtype: s_objtypes.ObjectType,
    value_expr: str,
    scope: qltypes.ConfigScope,
    join_term: str,
    level: int,
) -> str:
    # Generate a valid `CONFIGURE <SCOPE> INSERT ConfigObject`
    # shape for a given configuration object type or
    # `INSERT ConfigObject` for a nested configuration type.
    sub_layouts = _describe_config_object(
        schema=schema, valtype=valtype, level=level + 1, scope=scope)
    sub_layouts_items = []
    if level == 1:
        decor = [f'CONFIGURE {scope.to_edgeql()} INSERT ', ';\\n']
    else:
        decor = ['(INSERT ', ')']

    indent = ' ' * (4 * (level - 1))

    for type_name, type_layout in sub_layouts.items():
        if type_layout:
            sub_layout_item = (
                f"'{indent}{decor[0]}{type_name} {{\\n'\n++ "
                + "\n++ ".join(type_layout)
                + f" ++ '{indent}}}{decor[1]}'"
            )
        else:
            sub_layout_item = (
                f"'{indent}{decor[0]}{type_name}{decor[1]}'"
            )

        if len(sub_layouts) > 1:
            if type_layout:
                sub_layout_item = (
                    f'(WITH item := item[IS {type_name}]'
                    f' SELECT {sub_layout_item}) '
                    f'IF item.__type__.name = {ql(str(type_name))}'
                )
            else:
                sub_layout_item = (
                    f'{sub_layout_item} '
                    f'IF item.__type__.name = {ql(str(type_name))}'
                )

        sub_layouts_items.append(sub_layout_item)

    if len(sub_layouts_items) > 1:
        sli_render = '\nELSE '.join(sub_layouts_items) + "\nELSE ''"
    else:
        sli_render = sub_layouts_items[0]

    return '\n'.join((
        f"array_join(array_agg((",
        f"  FOR item IN {{ {value_expr} }}",
        f"  UNION (",
        f"{textwrap.indent(sli_render, ' ' * 4)}",
        f"  )",
        f")), {ql(join_term)})",
    ))


def _describe_config_object(
    *,
    schema: s_schema.Schema,
    valtype: s_objtypes.ObjectType,
    level: int,
    scope: qltypes.ConfigScope,
) -> Dict[s_name.QualName, List[str]]:
    cfg_types = [valtype]
    cfg_types.extend(cfg_types[0].descendants(schema))
    layouts = {}
    for cfg in cfg_types:
        items = []
        for ptr_name, p in cfg.get_pointers(schema).items(schema):
            pn = str(ptr_name)
            if (
                pn in ('id', '__type__')
                or p.get_annotation(
                    schema,
                    s_name.QualName('cfg', 'internal'),
                ) == 'true'
            ):
                continue

            ptype = p.get_target(schema)
            assert ptype is not None
            ptr_card = p.get_cardinality(schema)
            mult = ptr_card.is_multi()
            psource = f'item.{ qlquote.quote_ident(pn) }'

            if isinstance(ptype, s_objtypes.ObjectType):
                rval = textwrap.indent(
                    _render_config_object(
                        schema=schema,
                        valtype=ptype,
                        value_expr=psource,
                        scope=scope,
                        join_term=' UNION ',
                        level=level + 1,
                    ),
                    ' ' * 2,
                ).strip()
                indent = ' ' * (4 * (level - 1))
                item = (
                    f"'{indent}{qlquote.quote_ident(pn)} "
                    f":= (\\n'\n++ {rval} ++ '\\n{indent}),\\n'"
                )
                condition = None
            else:
                render = _render_config_set if mult else _render_config_scalar
                item = render(
                    schema=schema,
                    valtype=ptype,
                    value_expr=psource,
                    scope=scope,
                    name=pn,
                    level=level,
                )
                condition = f'EXISTS {psource}'

            if condition is not None:
                item = f"({item} ++ '\\n' IF {condition} ELSE '')"

            items.append(item)

        layouts[cfg.get_name(schema)] = items

    return layouts


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

        keysource = textwrap.dedent(f'''\
            (SELECT
                ARRAY[{targetlist}] AS key
            ) AS k{source_idx}''')
    else:
        assert rptr is not None
        rptr_name = rptr.get_shortname(schema).name
        keysource = textwrap.dedent(f'''\
            (SELECT
                ARRAY[
                    (CASE WHEN q{source_idx}.val = 'null'::jsonb
                     THEN NULL
                     ELSE {ql(rptr_name)}
                     END)
                ] AS key
            ) AS k{source_idx}''')

    return keysource


def _build_key_expr(key_components: List[str]) -> str:
    key_expr = ' || '.join(key_components)
    final_keysource = textwrap.dedent(f'''\
        (SELECT
            (CASE WHEN array_position(q.v, NULL) IS NULL
             THEN
                 edgedbext.uuid_generate_v5(
                     '{DATABASE_ID_NAMESPACE}'::uuid,
                     array_to_string(q.v, ';')
                 )
             ELSE NULL
             END) AS key
         FROM
            (SELECT {key_expr} AS v) AS q
        )''')

    return final_keysource


def _build_data_source(
    schema: s_schema.Schema,
    rptr: s_pointers.Pointer,
    source_idx: int,
    *,
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
        sourceN = textwrap.dedent(f'''\
            (SELECT jel.val
                FROM
                jsonb_array_elements(
                    (q{source_idx}.val)->{ql(rptr_name)}) AS jel(val)
            ) AS {alias}''')
    else:
        sourceN = textwrap.dedent(f'''\
            (SELECT
                (q{source_idx}.val)->{ql(rptr_name)} AS val
            ) AS {alias}''')

    return sourceN


def _generate_config_type_view(
    schema: s_schema.Schema,
    stype: s_objtypes.ObjectType,
    *,
    scope: Optional[qltypes.ConfigScope],
    path: List[Tuple[s_pointers.Pointer, List[s_pointers.Pointer]]],
    rptr: Optional[s_pointers.Pointer],
    _memo: Optional[Set[s_obj.Object]] = None,
) -> Tuple[
    List[Tuple[Tuple[str, str], str]],
    List[s_pointers.Pointer],
]:
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

    tname = stype.get_name(schema)
    views = []

    sources = []

    if not path:
        # This is the root config object.
        if rptr is None:
            source0 = textwrap.dedent(f'''\
                (SELECT jsonb_object_agg(name, value) AS val
                FROM edgedb._read_sys_config(NULL, {max_source}) cfg) AS q0''')
        else:
            rptr_card = rptr.get_cardinality(schema)
            rptr_multi = rptr_card.is_multi()
            rptr_name = rptr.get_shortname(schema).name

            if rptr_multi:
                source0 = textwrap.dedent(f'''\
                    (SELECT el.val
                     FROM
                        (SELECT (value::jsonb) AS val
                        FROM edgedb._read_sys_config(NULL, {max_source})
                        WHERE name = {ql(rptr_name)}) AS cfg,
                        LATERAL jsonb_array_elements(cfg.val) AS el(val)
                    ) AS q0''')
            else:
                source0 = textwrap.dedent(f'''\
                    (SELECT (value::jsonb) AS val
                    FROM edgedb._read_sys_config(NULL, {max_source}) cfg
                    WHERE name = {ql(rptr_name)}) AS q0''')

        sources.append(source0)
        key_start = 0
    else:
        key_start = 0

        for i, (l, exc_props) in enumerate(path):
            l_card = l.get_cardinality(schema)
            l_multi = l_card.is_multi()
            l_name = l.get_shortname(schema).name

            if i == 0:
                if l_multi:
                    sourceN = textwrap.dedent(f'''\
                        (SELECT el.val
                        FROM
                            (SELECT (value::jsonb) AS val
                            FROM edgedb._read_sys_config(NULL, {max_source})
                            WHERE name = {ql(l_name)}) AS cfg,
                            LATERAL jsonb_array_elements(cfg.val) AS el(val)
                        ) AS q{i}''')
                else:
                    sourceN = textwrap.dedent(f'''\
                        (SELECT (value::jsonb) AS val
                        FROM edgedb._read_sys_config(NULL, {max_source}) cfg
                        WHERE name = {ql(l_name)}) AS q{i}''')
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
    target_cols = []
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
                schema, pp_type, f'cfg::Config.{".".join(path_steps)}')

            if pp_multi:
                multi_props.append((pp, pp_cast))
            else:
                extract_col = (
                    f'{pp_cast(f"{sval}->{ql(pn)}")}'
                    f' AS {qi(pp_col)}')

                target_cols.append(extract_col)

                constraints = pp.get_constraints(schema).objects(schema)
                if any(c.issubclass(schema, exc) for c in constraints):
                    exclusive_props.append(pp)

    exclusive_props.sort(key=lambda p: p.get_shortname(schema).name)

    if exclusive_props or rptr:
        sources.append(
            _build_key_source(schema, exclusive_props, rptr, str(self_idx)))

        key_components = [f'k{i}.key' for i in range(key_start, self_idx + 1)]
        final_keysource = f'{_build_key_expr(key_components)} AS k'
        sources.append(final_keysource)

        key_expr = 'k.key'
        target_cols.append(f'{key_expr} AS id')

        where = f'{key_expr} IS NOT NULL'

        target_cols.append(textwrap.dedent(f'''\
            (SELECT id
            FROM edgedb."_SchemaObjectType"
            WHERE name = 'cfg::' || ({sval}->>'_tname')) AS __type__'''))

    else:
        key_expr = f"'{CONFIG_ID}'::uuid"

        target_cols.extend([
            f"{key_expr} AS id",
            f'(SELECT id FROM edgedb."_SchemaObjectType" '
            f"WHERE name = {ql(str(tname))}) AS __type__",
        ])

        key_components = []

    for link in single_links:
        link_name = link.get_shortname(schema).name
        link_type = link.get_target(schema)
        link_psi = types.get_pointer_storage_info(link, schema=schema)
        link_col = link_psi.column_name

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
                    _memo=_memo,
                )
                views.extend(desc_views)

        target_source = _build_data_source(
            schema, link, self_idx, alias=link_name)
        sources.append(target_source)

        target_key_source = _build_key_source(
            schema, target_exc_props, link, source_idx=link_name)
        sources.append(target_key_source)

        if target_exc_props:
            target_key_components = [f'k{link_name}.key']
        else:
            target_key_components = key_components + [f'k{link_name}.key']

        target_key = _build_key_expr(target_key_components)
        target_cols.append(f'({target_key}) AS {qi(link_col)}')

        views.extend(target_views)

    target_cols_str = ',\n'.join(target_cols)

    fromlist = ',\n'.join(f'LATERAL {s}' for s in sources)

    target_query = textwrap.dedent(f'''\
        SELECT
            {textwrap.indent(target_cols_str, ' ' * 4).strip()}
        FROM
            {fromlist}
    ''')

    if where:
        target_query += f'\nWHERE\n    {where}'

    views.append((tabname(schema, stype), target_query))
    views.append((inhviewname(schema, stype), target_query))

    for link in multi_links:
        target_sources = list(sources)

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
                    _memo=_memo,
                )
                views.extend(desc_views)

        target_source = _build_data_source(
            schema, link, self_idx, alias=link_name)
        target_sources.append(target_source)

        target_key_source = _build_key_source(
            schema, target_exc_props, link, source_idx=link_name)
        target_sources.append(target_key_source)

        target_key_components = key_components + [f'k{link_name}.key']
        target_key = _build_key_expr(target_key_components)

        target_fromlist = ',\n'.join(f'LATERAL {s}' for s in target_sources)

        link_query = textwrap.dedent(f'''\
            SELECT
                q.source,
                q.target
            FROM
                (SELECT
                    {key_expr} AS source,
                    {target_key} AS target
                FROM
                    {target_fromlist}
                ) q
            WHERE
                q.target IS NOT NULL
            ''')

        views.append((tabname(schema, link), link_query))
        views.append((inhviewname(schema, link), link_query))

    for prop, pp_cast in multi_props:
        target_sources = list(sources)

        pn = prop.get_shortname(schema).name

        target_source = _build_data_source(
            schema, prop, self_idx, alias=pn)
        target_sources.append(target_source)

        target_fromlist = ',\n'.join(f'LATERAL {s}' for s in target_sources)

        link_query = textwrap.dedent(f'''\
            SELECT
                {key_expr} AS source,
                {pp_cast(f'q{pn}.val')} AS target
            FROM
                {target_fromlist}
        ''')

        views.append((tabname(schema, prop), link_query))
        views.append((inhviewname(schema, prop), link_query))

    return views, exclusive_props


async def _execute_block(
    conn: asyncpg.Connection,
    block: dbops.SQLBlock,
) -> None:
    await _execute_sql_script(conn, block.to_string())


async def _execute_sql_script(
    conn: asyncpg.Connection,
    sql_text: str,
) -> None:
    if debug.flags.bootstrap:
        debug.header('Bootstrap Script')
        if len(sql_text) > 102400:
            # Make sure we don't hog CPU by attempting to highlight
            # huge scripts.
            print(sql_text)
        else:
            debug.dump_code(sql_text, lexer='sql')

    try:
        await conn.execute(sql_text)
    except Exception as e:
        position = getattr(e, 'position', None)
        internal_position = getattr(e, 'internal_position', None)
        context = getattr(e, 'context', '')
        pl_func_line: Optional[int]
        if context:
            pl_func_line_m = re.search(
                r'^PL/pgSQL function inline_code_block line (\d+).*',
                context, re.M)

            if pl_func_line_m:
                pl_func_line = int(pl_func_line_m.group(1))
        else:
            pl_func_line = None
        point = None

        if position is not None:
            point = int(position)
            text = getattr(e, 'query', None)
            if text is None:
                # Parse errors
                text = sql_text

        elif internal_position is not None:
            point = int(internal_position)
            text = getattr(e, 'internal_query', None)

        elif pl_func_line:
            point = _edgeql_rust.offset_of_line(sql_text, pl_func_line)
            text = sql_text

        if point is not None:
            context = parser_context.ParserContext(
                'query', text, start=point, end=point)
            exceptions.replace_context(e, context)
        raise
