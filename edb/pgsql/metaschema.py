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

import collections
import re
import textwrap
import uuid

from edb.common import adapter, checked, debug
from edb.common import context as parser_context
from edb.common import exceptions

from edb.edgeql import qltypes

from edb.schema import abc as s_abc
from edb.schema import constraints as s_constraints
from edb.schema import database as s_db
from edb.schema import expr as s_expr
from edb.schema import inheriting as s_inheriting
from edb.schema import migrations  # NoQA
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import pseudo as s_pseudo
from edb.schema import types as s_types

from edb.server import defines

from . import common
from . import dbops
from . import types


q = common.qname
qi = common.quote_ident
ql = common.quote_literal
qt = common.quote_type


DATABASE_ID_NAMESPACE = uuid.UUID('0e6fed66-204b-11e9-8666-cffd58a5240b')
CONFIG_ID_NAMESPACE = uuid.UUID('a48b38fa-349b-11e9-a6be-4f337f82f5ad')
CONFIG_ID = uuid.UUID('172097a4-39f4-11e9-b189-9321eb2f4b97')


class Context:
    def __init__(self, conn):
        self.db = conn


class TypeDescNodeType(dbops.CompositeType):
    def __init__(self) -> None:
        super().__init__(name=('edgedb', 'type_desc_node_t'))

        self.add_columns([
            dbops.Column(name='id', type='uuid'),
            dbops.Column(name='maintype', type='uuid'),
            dbops.Column(name='name', type='text'),
            dbops.Column(name='position', type='smallint'),
            dbops.Column(name='collection', type='text'),
            dbops.Column(name='subtypes', type='uuid[]'),
            dbops.Column(name='dimensions', type='smallint[]'),
        ])


class TypeDescType(dbops.CompositeType):
    def __init__(self) -> None:
        super().__init__(name=('edgedb', 'typedesc_t'))

        self.add_columns([
            dbops.Column(name='types', type='edgedb.type_desc_node_t[]'),
        ])


class ExpressionType(dbops.CompositeType):
    def __init__(self) -> None:
        super().__init__(name=('edgedb', 'expression_t'))

        self.add_columns([
            dbops.Column(name='text', type='text'),
            dbops.Column(name='origtext', type='text'),
            dbops.Column(name='refs', type='uuid[]'),
        ])


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


class RaiseExceptionFunction(dbops.Function):
    text = '''
    BEGIN
        RAISE EXCEPTION '%', msg;
        RETURN rtype;
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_raise_exception'),
            args=[('msg', ('text',)), ('rtype', ('anyelement',))],
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
            text=self.text)


class RaiseSpecificExceptionFunction(dbops.Function):
    text = '''
    BEGIN
        RAISE EXCEPTION USING
            ERRCODE = exc,
            MESSAGE = msg,
            DETAIL = COALESCE(det, '');
        RETURN rtype;
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_raise_specific_exception'),
            args=[('exc', ('text',)), ('msg', ('text',)), ('det', ('text',)),
                  ('rtype', ('anyelement',))],
            returns=('anyelement',),
            # See NOTE for the _raise_exception for reason why this is
            # stable and not immutable.
            volatility='stable',
            language='plpgsql',
            text=self.text)


class RaiseSpecificExceptionFunctionArray(dbops.Function):
    text = '''
    BEGIN
        RAISE EXCEPTION USING
            ERRCODE = exc,
            MESSAGE = msg,
            DETAIL = COALESCE(det, '');
        RETURN rtype;
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_raise_specific_exception_array'),
            args=[('exc', ('text',)), ('msg', ('text',)), ('det', ('text',)),
                  ('rtype', ('anyarray',))],
            returns=('anyarray',),
            # See NOTE for the _raise_exception for reason why this is
            # stable and not immutable.
            volatility='stable',
            language='plpgsql',
            text=self.text)


class RaiseExceptionOnNullFunction(dbops.Function):
    """Return the passed value or raise an exception if it's NULL."""
    text = '''
        SELECT
            coalesce(val, edgedb._raise_specific_exception(exc, msg, det, val))
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_raise_exception_on_null'),
            args=[('val', ('anyelement',)), ('exc', ('text',)),
                  ('msg', ('text',)), ('det', ('text',))],
            returns=('anyelement',),
            # Same volatility as _raise_specific_exception
            volatility='stable',
            text=self.text)


class RaiseExceptionOnEmptyStringFunction(dbops.Function):
    """Return the passed string or raise an exception if it's empty."""
    text = '''
        SELECT
            CASE WHEN edgedb._length(val) = 0 THEN
                edgedb._raise_specific_exception(exc, msg, det, val)
            ELSE
                val
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_raise_exception_on_empty'),
            args=[('val', ('anyelement',)), ('exc', ('text',)),
                  ('msg', ('text',)), ('det', ('text',))],
            returns=('anyelement',),
            # Same volatility as _raise_specific_exception
            volatility='stable',
            text=self.text)


class AssertJSONTypeFunction(dbops.Function):
    """Assert that the JSON type matches what is expected."""
    text = '''
        SELECT
            CASE WHEN array_position(typenames, jsonb_typeof(val)) IS NULL THEN
                edgedb._raise_specific_exception(
                    'wrong_object_type',
                    coalesce(
                        msg,
                        'expected json ' || array_to_string(typenames, ', ') ||
                        '; got json ' || coalesce(jsonb_typeof(val), 'UNKNOWN')
                    ),
                    det,
                    NULL::jsonb
                )
            ELSE
                val
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'jsonb_assert_type'),
            args=[('val', ('jsonb',)), ('typenames', ('text[]',)),
                  ('msg', ('text',), 'NULL'), ('det', ('text',), "''")],
            returns=('jsonb',),
            # Max volatility of _raise_specific_exception and
            # array_to_string (stable)
            volatility='stable',
            text=self.text)


class ExtractJSONScalarFunction(dbops.Function):
    """Convert a given JSON scalar value into a text value."""
    text = '''
        SELECT
            (to_jsonb(ARRAY[
                edgedb.jsonb_assert_type(coalesce(val, 'null'::jsonb),
                                         ARRAY[json_typename, 'null'])
            ])->>0)
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'jsonb_extract_scalar'),
            args=[('val', ('jsonb',)), ('json_typename', ('text',)),
                  ('msg', ('text',), 'NULL'), ('det', ('text',), "''")],
            returns=('text',),
            # Same volatility as jsonb_assert_type
            volatility='stable',
            text=self.text)


class DeriveUUIDFunction(dbops.Function):
    text = '''
        WITH
            i AS (
                SELECT uuid_send(id) AS id
            ),
            b AS (
                SELECT
                    (variant >> 8 & 255) AS hi_8,
                    (variant & 255) AS low_8
            )
            SELECT
                substr(set_byte(
                    set_byte(
                        set_byte(
                            i.id, 6, (get_byte(i.id, 6) & 240)),
                        7, b.hi_8),
                    4, b.low_8)::text, 3)::uuid
            FROM
                i, b
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_derive_uuid'),
            args=[('id', ('uuid',)), ('variant', ('smallint',))],
            returns=('uuid',),
            volatility='immutable',
            text=self.text)


class ResolveTypeNameFunction(dbops.Function):
    text = '''
        SELECT edgedb._resolve_type_name((type.types[1]).maintype)
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_resolve_type_name'),
            args=[('type', ('edgedb', 'typedesc_t'))],
            returns=('text',),
            # Same volatility as _resolve_type_name(uuid)
            volatility='stable',
            text=self.text,
            strict=True)


class ResolveSimpleTypeNameFunction(dbops.Function):
    text = '''
        SELECT coalesce(
            (SELECT name FROM edgedb.Object
             WHERE id = type::uuid),
            edgedb._raise_exception(
                'resolve_type_name: unknown type: "' || type || '"',
                NULL::text
            )
        )
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_resolve_type_name'),
            args=[('type', ('uuid',))],
            returns=('text',),
            # Max volatility of _raise_exception and a SELECT from a
            # table (stable).
            volatility='stable',
            text=self.text,
            strict=True)


class ResolveSimpleTypeNameListFunction(dbops.Function):
    text = '''
        SELECT
            array_agg(edgedb._resolve_type_name(t.id) ORDER BY t.ordinality)
        FROM
            UNNEST(type_data) WITH ORDINALITY AS t(id)
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_resolve_type_name'),
            args=[('type_data', ('uuid[]',))],
            returns=('text[]',),
            # Same volatility as _resolve_type_name(uuid)
            volatility='stable',
            text=self.text,
            strict=True)


class EdgeDBNameToPGNameFunction(dbops.Function):
    text = '''
        SELECT
            CASE WHEN char_length(name) > 63 THEN
                (SELECT
                    hash.v || ':' ||
                        substr(name, char_length(name) - (61 - hash.l))
                FROM
                    (SELECT
                        q.v AS v,
                        char_length(q.v) AS l
                     FROM
                        (SELECT
                            rtrim(encode(decode(
                                md5(name), 'hex'), 'base64'), '=')
                            AS v
                        ) AS q
                    ) AS hash
                )
            ELSE
                name
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'edgedb_name_to_pg_name'),
            args=[('name', 'text')],
            returns='text',
            volatility='immutable',
            text=self.__class__.text)


class ConvertNameFunction(dbops.Function):
    text = '''
        SELECT
            quote_ident(edgedb.edgedb_name_to_pg_name(prefix || module))
                || '.' ||
                quote_ident(edgedb.edgedb_name_to_pg_name(name || suffix));
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'convert_name'),
            args=[('module', 'text'), ('name', 'text'), ('suffix', 'text'),
                  ('prefix', 'text', "'edgedb_'")],
            returns='text',
            volatility='immutable',
            text=self.__class__.text)


class ObjectTypeNameToTableNameFunction(dbops.Function):
    text = '''
        SELECT convert_name(module, name, '_data', prefix);
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'objtype_name_to_table_name'),
            args=[('module', 'text'), ('name', 'text'),
                  ('prefix', 'text', "'edgedb_'")],
            returns='text',
            volatility='immutable',
            text=self.__class__.text)


class LinkNameToTableNameFunction(dbops.Function):
    text = '''
        SELECT convert_name(module, name, '_link', prefix);
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'link_name_to_table_name'),
            args=[('module', 'text'), ('name', 'text'),
                  ('prefix', 'text', "'edgedb_'")],
            returns='text',
            volatility='immutable',
            text=self.__class__.text)


class IssubclassFunction(dbops.Function):
    text = '''
        SELECT
            clsid = any(classes) OR (
                SELECT classes && o.ancestors
                FROM edgedb.InheritingObject o
                WHERE o.id = clsid
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
                SELECT pclsid = any(o.ancestors)
                FROM edgedb.InheritingObject o
                WHERE o.id = clsid
            );
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'issubclass'),
            args=[('clsid', 'uuid'), ('pclsid', 'uuid')],
            returns='bool',
            volatility='stable',
            text=self.__class__.text)


class IsinstanceFunction(dbops.Function):
    text = '''
    DECLARE
        ptabname text;
        clsid uuid;
    BEGIN
        ptabname := (
            SELECT
                edgedb.objtype_name_to_table_name(split_part(name, '::', 1),
                                                  split_part(name, '::', 2))
            FROM
                edgedb.ObjectType
            WHERE
                id = pclsid
        );

        EXECUTE
            'SELECT "__type__" FROM ' ||
                ptabname || ' WHERE "id" = $1'
            INTO clsid
            USING objid;

        RETURN clsid IS NOT NULL;
    END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'isinstance'),
            args=[('objid', 'uuid'), ('pclsid', 'uuid')],
            returns='bool',
            volatility='stable',
            language='plpgsql',
            text=self.__class__.text)


class NormalizeNameFunction(dbops.Function):
    text = '''
        SELECT
            CASE WHEN strpos(name, '@@') = 0 THEN
                name
            ELSE
                CASE WHEN strpos(name, '::') = 0 THEN
                    replace(split_part(name, '@@', 1), '|', '::')
                ELSE
                    replace(
                        split_part(
                            -- "reverse" calls are to emulate "rsplit"
                            reverse(split_part(reverse(name), '::', 1)),
                            '@@', 1),
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
        SELECT edgedb._raise_exception_on_null(
            val[edgedb._normalize_array_index(index, array_upper(val, 1))],
            'array_subscript_error',
            'array index ' || index::text || ' is out of bounds',
            det
        )
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('anyarray',)), ('index', ('bigint',)),
                  ('det', ('text',))],
            returns=('anyelement',),
            # Same volatility as _raise_exception_on_null
            volatility='stable',
            strict=True,
            text=self.text)


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
            text=self.text)


class StringIndexWithBoundsFunction(dbops.Function):
    """Get a string character or raise an out-of-bounds exception."""
    text = '''
        SELECT edgedb._raise_exception_on_empty(
            substr(
                val,
                edgedb._normalize_array_index(index, char_length(val)),
                1),
            'invalid_parameter_value',
            'string index ' || index::text || ' is out of bounds',
            det
        )
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('text',)), ('index', ('bigint',)),
                  ('det', ('text',))],
            returns=('text',),
            # Same volatility as _raise_exception_on_empty
            volatility='stable',
            strict=True,
            text=self.text)


class BytesIndexWithBoundsFunction(dbops.Function):
    """Get a bytes character or raise an out-of-bounds exception."""
    text = '''
        SELECT edgedb._raise_exception_on_empty(
            substr(
                val,
                edgedb._normalize_array_index(index, length(val)),
                1),
            'invalid_parameter_value',
            'byte string index ' || index::text || ' is out of bounds',
            det
        )
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('bytea',)), ('index', ('bigint',)),
                  ('det', ('text',))],
            returns=('bytea',),
            # Same volatility as _raise_exception_on_empty
            volatility='stable',
            strict=True,
            text=self.text)


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
                edgedb._raise_exception_on_null(
                    val -> index,
                    'invalid_parameter_value',
                    'json index ' || quote_literal(index) ||
                    ' is out of bounds',
                    det
                )
            )
            WHEN 'array' THEN (
                edgedb._raise_specific_exception(
                    'wrong_object_type',
                    'cannot index json ' || jsonb_typeof(val) ||
                    ' by ' || pg_typeof(index)::text,
                    det,
                    NULL::jsonb
                )
            )
            ELSE
                edgedb._raise_specific_exception(
                    'wrong_object_type',
                    'cannot index json ' || coalesce(jsonb_typeof(val),
                                                     'UNKNOWN'),
                    det,
                    NULL::jsonb
                )
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('jsonb',)), ('index', ('text',)),
                  ('det', ('text',))],
            returns=('jsonb',),
            # Same volatility as exception helpers
            volatility='stable',
            strict=True,
            text=self.text)


class JSONIndexByIntFunction(dbops.Function):
    """Get a JSON element by int index or raise an exception."""
    text = r'''
        SELECT
            CASE jsonb_typeof(val)
            WHEN 'object' THEN (
                edgedb._raise_specific_exception(
                    'wrong_object_type',
                    'cannot index json ' || jsonb_typeof(val) ||
                    ' by ' || pg_typeof(index)::text,
                    det,
                    NULL::jsonb
                )
            )
            WHEN 'array' THEN (
                edgedb._raise_exception_on_null(
                    val -> index::int,
                    'invalid_parameter_value',
                    'json index ' || index::text || ' is out of bounds',
                    det
                )
            )
            ELSE
                edgedb._raise_specific_exception(
                    'wrong_object_type',
                    'cannot index json ' || coalesce(jsonb_typeof(val),
                                                     'UNKNOWN'),
                    det,
                    NULL::jsonb
                )
            END
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('jsonb',)), ('index', ('bigint',)),
                  ('det', ('text',))],
            returns=('jsonb',),
            # Min volatility of exception helpers and pg_typeof (stable).
            volatility='stable',
            strict=True,
            text=self.text)


class JSONSliceFunction(dbops.Function):
    """Get a JSON array slice."""
    text = r'''
        SELECT to_jsonb(_slice(
            (
                SELECT array_agg(value)
                FROM jsonb_array_elements(
                    jsonb_assert_type(val, ARRAY['array']))
            ),
            start, stop
        ))
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
                edgedb._raise_specific_exception(
                    'invalid_datetime_format',
                    'invalid input syntax for type timestamptz: ' ||
                    quote_literal(val),
                    '{"hint":"Please use ISO8601 format. Alternatively ' ||
                    '\"to_datetime\" function provides custom ' ||
                    'formatting options."}',
                    NULL::timestamptz
                )
            ELSE
                val::timestamptz
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'datetime_in'),
            args=[('val', ('text',))],
            returns=('timestamptz',),
            # Same volatility as _raise_specific_exception (stable)
            volatility='stable',
            text=self.text)


class LocalDatetimeInFunction(dbops.Function):
    """Cast text into timestamp using ISO8601 spec."""
    text = r'''
        SELECT
            CASE WHEN val !~ (
                    '^\s*(' ||
                        '(\d{4}-\d{2}-\d{2}|\d{8})' ||
                        '[ tT]' ||
                        '(\d{2}(:\d{2}(:\d{2}(\.\d+)?)?)?|\d{2,6}(\.\d+)?)' ||
                    ')\s*$'
                )
            THEN
                edgedb._raise_specific_exception(
                    'invalid_datetime_format',
                    'invalid input syntax for type timestamp: ' ||
                    quote_literal(val),
                    '{"hint":"Please use ISO8601 format. Alternatively ' ||
                    '\"to_local_datetime\" function provides custom ' ||
                    'formatting options."}',
                    NULL::timestamp
                )
            ELSE
                val::timestamp
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'local_datetime_in'),
            args=[('val', ('text',))],
            returns=('timestamp',),
            # Same volatility as _raise_specific_exception (stable)
            volatility='stable',
            text=self.text)


class LocalDateInFunction(dbops.Function):
    """Cast text into date using ISO8601 spec."""
    text = r'''
        SELECT
            CASE WHEN val !~ (
                    '^\s*(' ||
                        '(\d{4}-\d{2}-\d{2}|\d{8})' ||
                    ')\s*$'
                )
            THEN
                edgedb._raise_specific_exception(
                    'invalid_datetime_format',
                    'invalid input syntax for type date: ' ||
                    quote_literal(val),
                    '{"hint":"Please use ISO8601 format. Alternatively ' ||
                    '\"to_local_date\" function provides custom ' ||
                    'formatting options."}',
                    NULL::date
                )
            ELSE
                val::date
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'local_date_in'),
            args=[('val', ('text',))],
            returns=('date',),
            # Same volatility as _raise_specific_exception (stable)
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
                edgedb._raise_specific_exception(
                    'invalid_datetime_format',
                    'invalid input syntax for type time: ' ||
                    quote_literal(val),
                    '{"hint":"Please use ISO8601 format. Alternatively ' ||
                    '\"to_local_time\" function provides custom ' ||
                    'formatting options."}',
                    NULL::time
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
            # Same volatility as _raise_specific_exception (stable)
            volatility='stable',
            text=self.text)


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

            RETURN result;
        END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_to_timestamptz_check'),
            args=[('val', ('text',)), ('fmt', ('text',)),
                  ('hastz', ('bool',))],
            returns=('timestamptz',),
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
                edgedb._raise_specific_exception(
                    'invalid_datetime_format',
                    'missing required time zone in format: ' ||
                    quote_literal(fmt),
                    $h${"hint":"Use one or both of the following: $h$ ||
                    $h$'TZH', 'TZM'"}$h$,
                    NULL::timestamptz
                )
            ELSE
                edgedb._to_timestamptz_check(val, fmt, true)
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'to_datetime'),
            args=[('val', ('text',)), ('fmt', ('text',))],
            returns=('timestamptz',),
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
                edgedb._raise_specific_exception(
                    'invalid_datetime_format',
                    'unexpected time zone in format: ' ||
                    quote_literal(fmt),
                    '',
                    NULL::timestamp
                )
            ELSE
                edgedb._to_timestamptz_check(val, fmt, false)::timestamp
            END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', 'to_local_datetime'),
            args=[('val', ('text',)), ('fmt', ('text',))],
            returns=('timestamp',),
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
    #
    # Also, we can't re-use `_raise_specific_exception` or functions
    # derived from it because we're passing an array as argument
    # instead of an element.
    text = r'''
        SELECT (
            coalesce(
                regexp_match(val, '^\s*(?:(true)|(false))\s*$', 'i')::text[],
                edgedb._raise_specific_exception_array(
                    'invalid_text_representation',
                    'invalid syntax for bool: ' || quote_literal(val),
                    '',
                    NULL::text[])
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


class SysConfigValueType(dbops.CompositeType):
    """Type of values returned by _read_sys_config."""
    def __init__(self) -> None:
        super().__init__(name=('edgedb', '_sys_config_val_t'))

        self.add_columns([
            dbops.Column(name='name', type='text'),
            dbops.Column(name='value', type='jsonb'),
            dbops.Column(name='source', type='text'),
        ])


class SysConfigFunction(dbops.Function):

    # This is a function because "_edgecon_state" is a temporary table
    # and therefore cannot be used in a view.

    text = f'''
        BEGIN
        RETURN QUERY EXECUTE $$
            WITH
                data_dir AS
                    (SELECT setting AS dir FROM pg_settings
                     WHERE name = 'data_directory'),

                config_spec AS
                    (SELECT
                        s.key AS name,
                        s.value->'default' AS default,
                        (s.value->>'internal')::bool AS internal,
                        (s.value->>'system')::bool AS system,
                        (s.value->>'typeid')::uuid AS typeid,
                        (s.value->>'typemod') AS typemod
                    FROM
                        jsonb_each(edgedb.__syscache_configspec()) AS s
                    ),

                config_defaults AS
                    (SELECT
                        s.name AS name,
                        s.default AS value,
                        'default' AS source,
                        0 AS priority
                    FROM
                        config_spec s
                    ),

                config_sys AS
                    (SELECT
                        s.key AS name,
                        s.value AS value,
                        'system override' AS source,
                        10 AS priority
                    FROM
                        jsonb_each(
                            shobj_metadata(
                               (SELECT oid FROM pg_database
                                WHERE
                                    datname = {ql(defines.EDGEDB_TEMPLATE_DB)}
                               ),
                               'pg_database'
                            ) -> 'sysconfig'
                        ) s
                    ),

                config_sess AS
                    (SELECT
                        s.name AS name,
                        s.value::jsonb AS value,
                        'session' AS source,
                        20 AS priority
                    FROM
                        _edgecon_state s
                    WHERE
                        s.type = 'C'
                    ),

                config_backend AS
                    (SELECT
                        name,
                        to_jsonb(CASE WHEN u.v[1] IS NOT NULL
                         THEN (setting::int * (u.v[1])::int)::text || u.v[2]
                         ELSE setting || COALESCE(unit, '')
                         END
                        ) AS value,
                        'backend' AS source,
                        30 AS priority
                     FROM
                        pg_settings,
                        LATERAL
                        (SELECT
                            regexp_match(pg_settings.unit, '(\\d+)(\\w+)') AS v
                        ) AS u
                     WHERE name = any(ARRAY[
                         'shared_buffers',
                         'work_mem',
                         'effective_cache_size',
                         'effective_io_concurrency',
                         'default_statistics_target'
                     ])
                    )

            SELECT
                q.name,
                q.value,
                q.source
            FROM
                (SELECT
                    u.name,
                    u.value,
                    u.source,
                    row_number() OVER (
                        PARTITION BY u.name ORDER BY u.priority DESC) AS n
                FROM
                    (
                        SELECT * FROM config_defaults UNION ALL
                        SELECT * FROM config_sys UNION ALL
                        SELECT * FROM config_sess UNION ALL
                        SELECT * FROM config_backend
                    ) AS u
                ) AS q
            WHERE
                q.n = 1;
        $$;
        END;
    '''

    def __init__(self) -> None:
        super().__init__(
            name=('edgedb', '_read_sys_config'),
            args=[],
            returns=('edgedb', '_sys_config_val_t'),
            set_returning=True,
            language='plpgsql',
            volatility='volatile',
            text=self.text,
        )


def _field_to_column(field):
    ftype = field.type
    coltype = None
    required = False
    default = None

    if issubclass(ftype, (s_obj.ObjectSet, s_obj.ObjectList)):
        # ObjectSet and ObjectList are exempt from typedesc_t encoding,
        # as they always represent only non-collection types, and
        # keeping the encoding simple is important for performance
        # reasons.
        coltype = 'uuid[]'

    elif issubclass(ftype, (s_obj.Object, s_obj.ObjectCollection)):
        coltype = 'edgedb.typedesc_t'

    elif issubclass(ftype, s_expr.Expression):
        coltype = 'edgedb.expression_t'

    elif issubclass(ftype, s_expr.ExpressionList):
        coltype = 'edgedb.expression_t[]'

    elif (issubclass(ftype, (checked.CheckedList, checked.FrozenCheckedList))
            and issubclass(ftype.type, str)):
        coltype = 'text[]'

    elif (issubclass(ftype, (checked.CheckedList, checked.FrozenCheckedList))
            and issubclass(ftype.type, int)):
        coltype = 'int[]'

    elif issubclass(ftype, collections.abc.Mapping):
        coltype = 'jsonb'

    elif issubclass(ftype, str):
        coltype = 'text'

    elif issubclass(ftype, bool):
        coltype = 'bool'

    elif issubclass(ftype, int):
        coltype = 'bigint'

    elif issubclass(ftype, uuid.UUID):
        coltype = 'uuid'
        if field.name == 'id':
            required = True
            default = 'edgedb.uuid_generate_v1mc()'

    else:
        coltype = 'text'

    return dbops.Column(
        name=field.name,
        type=coltype,
        required=required,
        default=default,
    )


metaclass_tables = collections.OrderedDict()


def get_interesting_metaclasses():
    metaclasses = []

    for mcls in s_obj.ObjectMeta.get_schema_metaclasses():
        if issubclass(mcls, (s_obj.ObjectRef, s_db.Database)):
            continue

        if isinstance(mcls, adapter.Adapter):
            continue

        if (issubclass(mcls, s_abc.Collection)
                and not issubclass(mcls, s_types.SchemaCollection)):
            continue

        metaclasses.append(mcls)

    return metaclasses


def init_metaclass_tables():
    # The first MetaCLass is the abstract Object, which we created
    # manually above.
    metaclasses = get_interesting_metaclasses()

    for mcls in metaclasses:
        table = dbops.Table(name=('edgedb', mcls.__name__.lower()))

        bases = []
        seen_bases = set()
        for parent in mcls.__mro__[1:-1]:
            if not issubclass(parent, s_obj.Object):
                continue

            if any(issubclass(b, parent) for b in seen_bases):
                continue

            parent_tab = metaclass_tables.get(parent)
            if parent_tab is None:
                continue

            bases.append(parent_tab)
            seen_bases.add(parent)

        table.add_bases(bases)

        fields = mcls.get_ownfields()

        cols = []

        for fn in fields:
            field = mcls.get_field(fn)
            if field.ephemeral:
                continue

            cols.append(_field_to_column(field))

        table.add_columns(cols)

        if mcls is s_obj.Object:
            table.add_constraint(
                dbops.PrimaryKey(('edgedb', 'object'), columns=('id',)))

        table.add_constraint(
            dbops.UniqueConstraint(table, columns=('name',)))

        metaclass_tables[mcls] = table


init_metaclass_tables()


def get_metaclass_table(mcls):
    return metaclass_tables[mcls]


def make_register_any_command():
    pseudo_type_table = get_metaclass_table(s_pseudo.PseudoType)

    anytype = pseudo_type_table.record
    anytype.ancestors = None
    anytype.id = s_obj.get_known_type_id('anytype')
    anytype.name = 'anytype'

    anytuple = pseudo_type_table.record
    anytuple.ancestors = None
    anytuple.id = s_obj.get_known_type_id('anytuple')
    anytuple.name = 'anytuple'

    return dbops.Insert(table=pseudo_type_table, records=[anytype, anytuple])


async def bootstrap(conn):
    commands = dbops.CommandGroup()
    commands.add_commands([
        dbops.DropSchema(name='public'),
        dbops.CreateSchema(name='edgedb'),
        dbops.CreateExtension(dbops.Extension(name='uuid-ossp')),
        dbops.CreateCompositeType(TypeDescNodeType()),
        dbops.CreateCompositeType(TypeDescType()),
        dbops.CreateCompositeType(ExpressionType()),
    ])

    commands.add_commands(
        dbops.CreateTable(table)
        for table in list(metaclass_tables.values()))

    commands.add_commands(
        dbops.Comment(table, f'schema::{mcls.__name__}')
        for mcls, table in list(metaclass_tables.items())[1:])

    commands.add_commands([
        dbops.CreateFunction(GetObjectMetadata()),
        dbops.CreateFunction(GetSharedObjectMetadata()),
        dbops.CreateFunction(RaiseExceptionFunction()),
        dbops.CreateFunction(RaiseSpecificExceptionFunction()),
        dbops.CreateFunction(RaiseSpecificExceptionFunctionArray()),
        dbops.CreateFunction(RaiseExceptionOnNullFunction()),
        dbops.CreateFunction(RaiseExceptionOnEmptyStringFunction()),
        dbops.CreateFunction(AssertJSONTypeFunction()),
        dbops.CreateFunction(ExtractJSONScalarFunction()),
        dbops.CreateFunction(DeriveUUIDFunction()),
        dbops.CreateFunction(ResolveSimpleTypeNameFunction()),
        dbops.CreateFunction(ResolveSimpleTypeNameListFunction()),
        dbops.CreateFunction(ResolveTypeNameFunction()),
        dbops.CreateFunction(EdgeDBNameToPGNameFunction()),
        dbops.CreateFunction(ConvertNameFunction()),
        dbops.CreateFunction(ObjectTypeNameToTableNameFunction()),
        dbops.CreateFunction(LinkNameToTableNameFunction()),
        dbops.CreateFunction(IssubclassFunction()),
        dbops.CreateFunction(IssubclassFunction2()),
        dbops.CreateFunction(IsinstanceFunction()),
        dbops.CreateFunction(NormalizeNameFunction()),
        dbops.CreateFunction(NullIfArrayNullsFunction()),
        dbops.CreateCompositeType(IndexDescType()),
        dbops.CreateFunction(IntrospectIndexesFunction()),
        dbops.CreateCompositeType(TriggerDescType()),
        dbops.CreateFunction(IntrospectTriggersFunction()),
        dbops.CreateCompositeType(TableInheritanceDescType()),
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
        dbops.CreateFunction(LocalDatetimeInFunction()),
        dbops.CreateFunction(LocalDateInFunction()),
        dbops.CreateFunction(LocalTimeInFunction()),
        dbops.CreateFunction(ToTimestampTZCheck()),
        dbops.CreateFunction(ToDatetimeFunction()),
        dbops.CreateFunction(ToLocalDatetimeFunction()),
        dbops.CreateFunction(StrToBool()),
        dbops.CreateFunction(BytesIndexWithBoundsFunction()),
        dbops.CreateCompositeType(SysConfigValueType()),
        dbops.CreateFunction(SysConfigFunction()),
    ])

    # Register "any" pseudo-type.
    commands.add_command(make_register_any_command())

    block = dbops.PLTopBlock(disable_ddl_triggers=True)
    commands.generate(block)
    await _execute_block(conn, block)


classref_attr_aliases = {
    'links': 'pointers',
    'link_properties': 'pointers'
}


dbname = lambda n: \
    common.quote_ident(common.edgedb_name_to_pg_name(sn.Name(n)))
tabname = lambda schema, obj: \
    ('edgedbss', common.get_backend_name(schema, obj, catenate=False)[1])


def _get_link_view(mcls, schema_cls, field, ptr, refdict, schema):
    pn = ptr.get_shortname(schema)

    if refdict:
        props = []

        if ptr.issubclass(schema, schema.get('schema::reference')):
            props.append('COALESCE(is_local, false) AS is_local')

        if props:
            props_q = ',' + ',\n'.join(props)
        else:
            props_q = ''

        link_query = '''
            SELECT
                (({refattr}).types[1]).maintype AS {src},
                id                              AS {tgt}
                {props_q}
            FROM
                {reftab}
        '''.format(
            reftab='edgedb.{}'.format(refdict.ref_cls.__name__),
            refattr=qi(refdict.backref_attr),
            src='source',
            tgt='target',
            props_q=props_q,
        )

        if pn.name == 'annotations':
            link_query = '''
                SELECT
                    q.{src} AS {src},
                    ((av.annotation).types[1]).maintype AS {tgt},
                    av.value    AS {valprop}
                FROM
                    ({query}
                    ) AS q
                    INNER JOIN edgedb.AnnotationValue av ON q.{tgt} = av.id
            '''.format(
                query=link_query,
                src='source',
                tgt='target',
                valprop='value',
            )

    else:
        link_query = None
        if field is not None:
            ftype = field.type
        else:
            ftype = type(None)

        if issubclass(ftype, (s_obj.ObjectSet, s_obj.ObjectList)):
            if ptr.singular(schema):
                raise RuntimeError(
                    'introspection schema error: {!r} must not be '
                    'singular'.format(
                        '(' + schema_cls.name + ')' + '.' + pn.name))

            # ObjectSet and ObjectList fields are stored as uuid[],
            # so we just need to unnest the array here.
            refattr = 'UNNEST(' + qi(pn.name) + ')'

        elif pn.name == 'args' and mcls is s_constraints.Constraint:
            # Constraint args need special handling.
            link_query = f'''
                SELECT
                    q.id            AS source,
                    q.param_id      AS target,
                    q.value         AS value
                FROM
                    edgedb.{mcls.__name__} AS s,

                    LATERAL (
                        SELECT
                            s.id        AS id,
                            p.param_id  AS param_id,
                            tv.value    AS value
                        FROM
                            UNNEST(s.params)
                                WITH ORDINALITY AS p(param_id, num)

                            INNER JOIN edgedb.Parameter AS param
                                ON param.id = p.param_id

                            INNER JOIN
                                UNNEST(s.args)
                                    WITH ORDINALITY AS tv(_, value, _, num)
                                ON (p.num = tv.num + 1)
                        WHERE
                            param.kind != 'VARIADIC'

                        UNION ALL

                        SELECT
                            s.id            AS id,
                            p.param_id      AS param_id,
                            (SELECT '[' || string_agg(tv.value, ', ') || ']'
                             FROM
                                UNNEST(s.args[p.num - 1:]) AS tv(_, value, _)
                            ) AS value
                        FROM
                            UNNEST(s.params)
                                WITH ORDINALITY AS p(param_id, num)

                            INNER JOIN edgedb.Parameter AS param
                                ON param.id = p.param_id

                        WHERE
                            param.kind = 'VARIADIC'

                    ) AS q
                WHERE
                    s.subject IS NOT NULL
            '''

        elif issubclass(ftype, (s_obj.Object, s_obj.ObjectCollection)):
            # All other type fields are encoded as typedesc_t.
            link_query = f'''
                SELECT
                    s.id        AS source,
                    t.maintype  AS target
                FROM
                    edgedb.{mcls.__name__} AS s,
                    LATERAL UNNEST ((s.{qi(pn.name)}).types) AS t(
                        id, maintype, name, position, collection,
                        subtypes, dimensions
                    )
                WHERE
                    t.position IS NULL
            '''

        else:
            if not ptr.singular(schema):
                raise RuntimeError(
                    'introspection schema error: {!r} must be '
                    'singular'.format(
                        '(' + schema_cls.name + ')' + '.' + pn.name))

            refattr = qi(pn.name)

        if link_query is None:
            link_query = '''
                SELECT
                    id         AS {src},
                    {refattr}  AS {tgt}
                FROM
                    {schematab}
            '''.format(
                schematab='edgedb.{}'.format(mcls.__name__),
                refattr=refattr,
                src='source',
                tgt='target',
            )

    return dbops.View(name=tabname(schema, ptr), query=link_query)


def _generate_database_view(schema):
    Database = schema.get('sys::Database')

    view_query = f'''
        SELECT
            edgedb.uuid_generate_v5(
                '{DATABASE_ID_NAMESPACE}'::uuid,
                pg_database.oid::text)
                            AS id,
            datname         AS name,
            (SELECT id FROM edgedb.Object
                 WHERE name = 'sys::Database') AS __type__
        FROM
            pg_database
        WHERE
            datname NOT IN ('postgres', 'template0', 'template1')
    '''

    return dbops.View(name=tabname(schema, Database), query=view_query)


def _generate_role_views(schema):
    Role = schema.get('sys::Role')

    view_query = f'''
        SELECT
            ((d.description)->>'id')::uuid              AS id,
            (SELECT id FROM edgedb.Object
                 WHERE name = 'sys::Role')              AS __type__,
            a.rolname                                   AS name,
            a.rolsuper                                  AS is_superuser,
            a.rolcanlogin                               AS allow_login,
            a.rolpassword                               AS password
        FROM
            pg_authid AS a
            CROSS JOIN LATERAL (
                SELECT
                    edgedb.shobj_metadata(a.oid, 'pg_authid')
                        AS description
            ) AS d
        WHERE
            (d.description)->>'__edgedb__' = '1';
    '''

    link_query = f'''
        SELECT
            ((d.description)->>'id')::uuid              AS source,
            ((md.description)->>'id')::uuid             AS target
        FROM
            pg_authid AS a
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

    return [
        dbops.View(name=tabname(schema, Role),
                   query=view_query),
        dbops.View(name=tabname(schema, Role.getptr(schema, 'member_of')),
                   query=link_query),
    ]


def _lookup_type(qual):
    return f'''(
        SELECT
            types.maintype AS id
        FROM
            types
        WHERE
            types.id = {qual}
        LIMIT
            1
    )'''


def _lookup_types(qual):
    return f'''(
        SELECT
            types.maintype AS id
        FROM
            types
        WHERE
            types.id = any({qual})
    )'''


def _get_type_source(schema, type_fields):

    source = '\nUNION\n'.join(f'''
        (SELECT
            t.*
         FROM
            {table},
            LATERAL UNNEST (({table}.{qi(field)}).types)
                AS t(
                    id, maintype, name, position,
                    collection, subtypes, dimensions
                )
        )
    ''' for table, field in type_fields)

    source = f'(SELECT DISTINCT ON (q.id) q.* FROM ({source}) AS q)'

    return source


def _generate_type_element_view(schema, type_fields):
    TypeElement = schema.get('schema::TypeElement')

    source = _get_type_source(schema, type_fields)

    view_query = f'''
        WITH
            types AS ({source})
        SELECT
            q.id            AS id,
            (SELECT id FROM edgedb.Object
                 WHERE name = 'schema::TypeElement')
                            AS __type__,
            {_lookup_type('q.id')}
                            AS type,
            q.name          AS name,
            q.position      AS num
        FROM
            types AS q
        WHERE
            q.position IS NOT NULL
    '''

    return dbops.View(name=tabname(schema, TypeElement), query=view_query)


def _generate_types_views(schema, type_fields):
    views = []
    link_views = []

    Array = schema.get('schema::Array')
    Tuple = schema.get('schema::Tuple')

    source = _get_type_source(schema, type_fields)

    view_query = f'''
        WITH
            types AS ({source})
        SELECT DISTINCT ON (q.maintype)
            q.maintype      AS id,
            q.collection    AS name,
            (SELECT id FROM edgedb.Object
                 WHERE name = 'schema::Array')
                            AS __type__,
            {_lookup_type('q.subtypes[1]')}
                            AS element_type,
            q.dimensions    AS dimensions
        FROM
            types AS q
        WHERE
            q.collection = 'array'
    '''

    views.append(dbops.View(name=tabname(schema, Array), query=view_query))

    view_query = f'''
        WITH
            types AS ({source})
        SELECT DISTINCT ON (q.maintype)
            q.maintype      AS id,
            q.collection    AS name,
            (SELECT id FROM edgedb.Object
                 WHERE name = 'schema::Tuple')
                            AS __type__
        FROM
            types AS q
        WHERE
            q.collection = 'tuple'
    '''

    views.append(dbops.View(name=tabname(schema, Tuple), query=view_query))

    link_view_query = f'''
        WITH
            types AS ({source})
        SELECT
            q.maintype      AS source,
            st.id           AS target
        FROM
            types AS q,
            LATERAL UNNEST (q.subtypes) WITH ORDINALITY AS st(id)
        WHERE
            q.collection = 'tuple'
    '''

    link_views.append(
        dbops.View(
            name=tabname(schema, Tuple.getptr(schema, 'element_types')),
            query=link_view_query,
        )
    )

    return views, link_views


def _make_json_caster(schema, json_casts, stype, context):
    cast = json_casts.get(stype)

    if cast is None:
        raise RuntimeError(
            f'there is no direct cast from std::json to '
            f'the type of {context!r} '
            f'({stype.get_displayname(schema)})'
        )

    if cast.get_from_cast(schema):
        pgtype = types.pg_type_from_object(schema, stype)

        def _cast(val):
            return f'({val})::{q(*pgtype)}'
    else:
        if cast.get_code(schema):
            cast_name = cast.get_name(schema)
            cast_module = schema.get_global(s_mod.Module, cast_name.module)
            func_name = common.get_cast_backend_name(
                cast_name, cast_module.id, aspect='function')
        else:
            func_name = cast.get_from_function(schema)

        def _cast(val):
            return f'{q(*func_name)}({val})'

    return _cast


async def generate_support_views(conn, schema):
    commands = dbops.CommandGroup()

    conf = schema.get('cfg::Config')

    views, _ = _generate_config_type_view(schema, conf, path=[], rptr=None)

    commands.add_commands([
        dbops.CreateView(dbops.View(name=tn, query=q))
        for tn, q in views
    ])

    block = dbops.PLTopBlock(disable_ddl_triggers=True)
    commands.generate(block)
    await _execute_block(conn, block)


def _build_key_source(schema, exc_props, rptr, source_idx):
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


def _build_key_expr(key_components):
    key_expr = ' || '.join(key_components)
    final_keysource = textwrap.dedent(f'''\
        (SELECT
            (CASE WHEN array_position(q.v, NULL) IS NULL
             THEN
                 edgedb.uuid_generate_v5(
                     '{DATABASE_ID_NAMESPACE}'::uuid,
                     array_to_string(q.v, ';')
                 )
             ELSE NULL
             END) AS key
         FROM
            (SELECT {key_expr} AS v) AS q
        )''')

    return final_keysource


def _build_data_source(schema, rptr, source_idx, *, alias=None):

    rptr_name = rptr.get_shortname(schema).name
    rptr_multi = rptr.get_cardinality(schema) is qltypes.Cardinality.MANY

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


def _generate_config_type_view(schema, stype, *, path, rptr, _memo=None):
    exc = schema.get('std::exclusive')
    json_t = schema.get('std::json')

    if _memo is None:
        _memo = set()

    _memo.add(stype)

    views = []
    json_casts = {
        c.get_to_type(schema): c
        for c in schema.get_casts_from_type(json_t)
    }

    sources = []

    if not path:
        # This is the root config object.
        if rptr is None:
            source0 = textwrap.dedent(f'''\
                (SELECT jsonb_object_agg(name, value) AS val
                FROM edgedb._read_sys_config() cfg) AS q0''')
        else:
            rptr_multi = (
                rptr.get_cardinality(schema) is qltypes.Cardinality.MANY)

            rptr_name = rptr.get_shortname(schema).name

            if rptr_multi:
                source0 = textwrap.dedent(f'''\
                    (SELECT el.val
                     FROM
                        (SELECT (value::jsonb) AS val
                        FROM edgedb._read_sys_config()
                        WHERE name = {ql(rptr_name)}) AS cfg,
                        LATERAL jsonb_array_elements(cfg.val) AS el(val)
                    ) AS q0''')
            else:
                source0 = textwrap.dedent(f'''\
                    (SELECT (value::jsonb) AS val
                    FROM edgedb._read_sys_config() cfg
                    WHERE name = {ql(rptr_name)}) AS q0''')

        sources.append(source0)
        key_start = 0
    else:
        key_start = 0

        for i, (l, exc_props) in enumerate(path):
            l_multi = l.get_cardinality(schema) is qltypes.Cardinality.MANY
            l_name = l.get_shortname(schema).name

            if i == 0:
                if l_multi:
                    sourceN = textwrap.dedent(f'''\
                        (SELECT el.val
                        FROM
                            (SELECT (value::jsonb) AS val
                            FROM edgedb._read_sys_config()
                            WHERE name = {ql(l_name)}) AS cfg,
                            LATERAL jsonb_array_elements(cfg.val) AS el(val)
                        ) AS q{i}''')
                else:
                    sourceN = textwrap.dedent(f'''\
                        (SELECT (value::jsonb) AS val
                        FROM edgedb._read_sys_config() cfg
                        WHERE name = {ql(l_name)}) AS q{i}''')
            else:
                sourceN = _build_data_source(schema, l, i - 1)

            sources.append(sourceN)
            sources.append(_build_key_source(schema, exc_props, l, i))

            if exc_props:
                key_start = i

    target_tab = tabname(schema, stype)
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
        if pp_name in ('id', '__type__'):
            continue

        pp_type = pp.get_target(schema)
        pp_multi = (
            pp.get_cardinality(schema) is qltypes.Cardinality.MANY
        )

        if pp_type.is_object_type():
            if pp_multi:
                multi_links.append(pp)
            else:
                single_links.append(pp)
        else:
            pp_cast = _make_json_caster(
                schema, json_casts, pp_type,
                f'cfg::Config.{".".join(path_steps)}')

            if pp_multi:
                multi_props.append((pp, pp_cast))
            else:
                extract_col = (
                    f'{pp_cast(f"{sval}->{ql(pp_name)}")}'
                    f' AS {qi(pp_name)}')

                target_cols.append(extract_col)

                constraints = pp.get_constraints(schema).objects(schema)
                if any(c.issubclass(schema, exc) for c in constraints):
                    exclusive_props.append(pp)

    exclusive_props.sort(key=lambda p: p.get_shortname(schema).name)

    if exclusive_props or rptr:
        sources.append(
            _build_key_source(schema, exclusive_props, rptr, self_idx))

        key_components = [f'k{i}.key' for i in range(key_start, self_idx + 1)]
        final_keysource = f'{_build_key_expr(key_components)} AS k'
        sources.append(final_keysource)

        key_expr = 'k.key'
        target_cols.append(f'{key_expr} AS id')

        where = f'{key_expr} IS NOT NULL'

        target_cols.append(textwrap.dedent(f'''\
            (SELECT id
            FROM edgedb.Object
            WHERE name = 'cfg::' || ({sval}->>'_tname')) AS __type__'''))

    else:
        key_expr = f"'{CONFIG_ID}'::uuid"

        target_cols.extend([
            f"{key_expr} AS id",
            f"(SELECT id FROM edgedb.Object "
            f"WHERE name = 'cfg::Config') AS __type__",
        ])

        key_components = []

    for link in single_links:
        link_name = link.get_shortname(schema).name
        link_type = link.get_target(schema)

        if rptr is not None:
            target_path = path + [(rptr, exclusive_props)]
        else:
            target_path = path

        target_views, target_exc_props = _generate_config_type_view(
            schema, link_type, path=target_path, rptr=link, _memo=_memo)

        for descendant in link_type.descendants(schema):
            if descendant not in _memo:
                desc_views, _ = _generate_config_type_view(
                    schema, descendant, path=target_path,
                    rptr=link, _memo=_memo,
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
        target_cols.append(f'({target_key}) AS {qi(link_name)}')

        views.extend(target_views)

    target_cols = ',\n'.join(target_cols)

    fromlist = ',\n'.join(f'LATERAL {s}' for s in sources)

    target_query = textwrap.dedent(f'''\
        SELECT
            {textwrap.indent(target_cols, ' ' * 4).strip()}
        FROM
            {fromlist}
    ''')

    if where:
        target_query += f'\nWHERE\n    {where}'

    views.append((target_tab, target_query))

    for link in multi_links:
        target_sources = list(sources)

        link_name = link.get_shortname(schema).name
        link_type = link.get_target(schema)

        if rptr is not None:
            target_path = path + [(rptr, exclusive_props)]
        else:
            target_path = path

        target_views, target_exc_props = _generate_config_type_view(
            schema, link_type, path=target_path, rptr=link, _memo=_memo)
        views.extend(target_views)

        for descendant in link_type.descendants(schema):
            if descendant not in _memo:
                desc_views, _ = _generate_config_type_view(
                    schema, descendant, path=target_path,
                    rptr=link, _memo=_memo,
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

    for prop, pp_cast in multi_props:
        target_sources = list(sources)

        pp_name = prop.get_shortname(schema).name

        target_source = _build_data_source(
            schema, prop, self_idx, alias=pp_name)
        target_sources.append(target_source)

        target_fromlist = ',\n'.join(f'LATERAL {s}' for s in target_sources)

        link_query = textwrap.dedent(f'''\
            SELECT
                {key_expr} AS source,
                {pp_cast(f'q{pp_name}.val')} AS target
            FROM
                {target_fromlist}
        ''')

        views.append((tabname(schema, prop), link_query))

    return views, exclusive_props


async def generate_views(conn, schema):
    """Setup views the introspection schema.

    The introspection views emulate regular type and link tables
    for the classes in the "schema" module by querying the actual
    metadata tables.
    """
    commands = dbops.CommandGroup()

    # We use a separate schema to make it easy to redirect queries.
    commands.add_command(dbops.CreateSchema(name='edgedbss'))

    metaclasses = get_interesting_metaclasses()
    views = collections.OrderedDict()
    type_fields = []
    non_intro_fields = set()

    for mcls in metaclasses:
        non_intro_fields.update(
            fn for fn, f in mcls.get_fields().items() if not f.introspectable
        )

    non_intro_fields_list = ','.join(ql(f) for f in non_intro_fields)
    non_intro_fields_expr = f'({non_intro_fields_list})'

    for mcls in metaclasses:
        if mcls is s_obj.Object:
            schema_name = 'Object'
        else:
            schema_name = mcls.__name__

        schema_cls = schema.get(
            sn.Name(module='schema', name=schema_name), default=None)

        if schema_cls is None:
            # Not all schema metaclasses are represented in the
            # introspection schema, just ignore them.
            continue

        cols = []

        for pn, ptr in schema_cls.get_pointers(schema).items(schema):
            if ptr.is_pure_computable(schema):
                continue

            field = mcls.get_field(pn)
            if field is not None and (field.ephemeral or
                                      not field.introspectable):
                field = None

            refdict = None
            if field is None:
                fn = classref_attr_aliases.get(pn, pn)
                refdict = mcls.get_refdict(fn)
                if refdict is not None and ptr.singular(schema):
                    # This is nether a field, nor a refdict, that's
                    # not expected.
                    raise RuntimeError(
                        'introspection schema error: {!r} must not be '
                        'singular'.format(
                            '(' + schema_cls.name + ')' + '.' + pn))

            if field is None and refdict is None:
                if pn == 'id':
                    # Id is present implicitly in schema tables.
                    pass
                elif pn == '__type__':
                    continue
                else:
                    # This is nether a field, nor a refdict, that's
                    # not expected.
                    raise RuntimeError(
                        f'introspection schema error: cannot resolve '
                        f'{schema_cls.get_name(schema)}.{pn} '
                        f'into metadata reference')

            if field is not None:
                ft = field.type
                if (issubclass(ft, (s_obj.Object, s_obj.ObjectCollection)) and
                        not issubclass(ft, (s_obj.ObjectSet,
                                            s_obj.ObjectList))):
                    type_fields.append(
                        (f'edgedb.{mcls.__name__}', pn)
                    )

            ptrstor = types.get_pointer_storage_info(ptr, schema=schema)
            ptrstor_link = types.get_pointer_storage_info(
                ptr, link_bias=True, schema=schema)

            if ptrstor.table_type == 'ObjectType':
                col_default_expr = None

                if pn == 'name':
                    name_expr = f't.{qi(ptrstor.column_name)}'

                    shortname_expr = (
                        f'edgedb.shortname_from_fullname({name_expr})'
                    )

                    if issubclass(mcls, s_inheriting.InheritingObject):
                        ptr = (
                            "(SELECT ARRAY[id] FROM edgedb.Object "
                            " WHERE name = 'schema::Pointer')"
                        )

                        col_expr = (
                            f'(CASE WHEN no.ancestors @> {ptr} '
                            f'AND strpos({name_expr}, \'@@\') > 0'
                            f'THEN split_part({shortname_expr}, \'::\', 2) '
                            f'ELSE {shortname_expr} END)'
                        )
                    else:
                        col_expr = shortname_expr

                elif pn == 'inherited_fields':
                    col_expr = f'''
                        (SELECT array_agg(key)
                         FROM jsonb_each(t.{qi(ptrstor.column_name)})
                         WHERE key NOT IN {non_intro_fields_expr}
                               AND value = 'true'::jsonb)
                    '''

                    col_default_expr = 'ARRAY[]::text[]'
                elif field.type is s_expr.Expression:
                    col_expr = f'(t.{qi(ptrstor.column_name)}).origtext'
                elif field.type is s_expr.ExpressionList:
                    col_expr = f'''
                        (SELECT array_agg(q.origtext)
                         FROM unnest(t.{qi(ptrstor.column_name)})
                                AS qi(text, _, _, _))
                    '''
                elif issubclass(field.type, s_obj.Object):
                    col_expr = textwrap.dedent(f'''\
                        ((t.{qi(ptrstor.column_name)}).types[1]).maintype
                    ''')
                else:
                    col_expr = f't.{qi(ptrstor.column_name)}'

                coltype = ptrstor.column_type
                col_expr = f'({col_expr})::{qt(coltype)}'

                if (getattr(field, 'default', None) is not None
                        and not field.required):
                    if col_default_expr is not None:
                        col_default = col_default_expr
                    else:
                        col_default = (
                            f'{ql(str(field.default))}::{qt(coltype)}')
                    col_expr = f'COALESCE({col_expr}, {col_default})'

                cols.append((col_expr, pn))

            if ptrstor_link is not None:
                view = _get_link_view(mcls, schema_cls, mcls.get_field(pn),
                                      ptr, refdict, schema)
                if view.name not in views:
                    views[view.name] = view

        coltext = textwrap.indent(
            ',\n'.join(('{} AS {}'.format(*c) for c in cols)), ' ' * 16)

        if issubclass(mcls, s_inheriting.InheritingObject):
            objtab = 'edgedb.InheritingObject'
        else:
            objtab = 'edgedb.Object'

        view_query = f'''
            SELECT
                {coltext.strip()},
                no.id AS "__type__"
            FROM
                edgedb.{mcls.__name__} AS t
                INNER JOIN pg_class AS c
                    ON (t.tableoid = c.oid)
                INNER JOIN pg_description AS cmt
                    ON (c.oid = cmt.objoid AND c.tableoid = cmt.classoid)
                INNER JOIN {objtab} AS no
                    ON (no.name = cmt.description)
        '''

        view = dbops.View(name=tabname(schema, schema_cls), query=view_query)

        views[view.name] = view

    type_views, type_link_views = _generate_types_views(schema, type_fields)
    views.update({v.name: v for v in type_views})
    views.update({v.name: v for v in type_link_views})
    for v in type_views + type_link_views:
        views.move_to_end(v.name, last=False)

    te_view = _generate_type_element_view(schema, type_fields)
    views[te_view.name] = te_view

    db_view = _generate_database_view(schema)
    views[db_view.name] = db_view

    role_views = _generate_role_views(schema)
    for role_view in role_views:
        views[role_view.name] = role_view

    types_view = views[tabname(schema, schema.get('schema::Type'))]
    types_view.query += '\nUNION ALL\n' + '\nUNION ALL\n'.join(f'''
        (
            SELECT
                "id",
                "name",
                "__type__"
            FROM
                {common.qname(*view.name)}
        )
    ''' for view in type_views)

    for view in views.values():
        commands.add_command(dbops.CreateView(view))

    block = dbops.PLTopBlock()
    commands.generate(block)
    await _execute_block(conn, block)


async def _execute_block(conn, block):
    sql_text = block.to_string()
    if debug.flags.bootstrap:
        debug.header('Bootstrap Script')
        debug.dump_code(sql_text, lexer='sql')

    try:
        await conn.execute(sql_text)
    except Exception as e:
        import edb.common.debug
        edb.common.debug.dump(e.__dict__)
        position = getattr(e, 'position', None)
        internal_position = getattr(e, 'internal_position', None)
        context = getattr(e, 'context', '')
        if context:
            pl_func_line = re.search(
                r'^PL/pgSQL function inline_code_block line (\d+).*',
                context, re.M)

            if pl_func_line:
                pl_func_line = int(pl_func_line.group(1))
        else:
            pl_func_line = None
        point = None

        if position is not None:
            position = int(position)
            point = parser_context.SourcePoint(
                None, None, position)
            text = e.query
            if text is None:
                # Parse errors
                text = sql_text

        elif internal_position is not None:
            internal_position = int(internal_position)
            point = parser_context.SourcePoint(
                None, None, internal_position)
            text = e.internal_query

        elif pl_func_line:
            point = parser_context.SourcePoint(
                pl_func_line, None, None
            )
            text = sql_text

        if point is not None:
            context = parser_context.ParserContext(
                'query', text, start=point, end=point)
            exceptions.replace_context(e, context)
        raise
