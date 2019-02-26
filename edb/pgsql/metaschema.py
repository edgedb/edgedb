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

import collections
import re
import textwrap
import uuid

from edb.common import adapter, debug, typed
from edb.common import context as parser_context
from edb.common import exceptions

from edb.edgeql import qltypes

from edb.schema import abc as s_abc
from edb.schema import constraints as s_constraints
from edb.schema import database as s_db
from edb.schema import deltas  # NoQA
from edb.schema import expr as s_expr
from edb.schema import inheriting as s_inheriting
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import pseudo as s_pseudo

from . import common
from . import dbops
from . import types


q = common.qname
qi = common.quote_ident
ql = common.quote_literal


DATABASE_ID_NAMESPACE = uuid.UUID('0e6fed66-204b-11e9-8666-cffd58a5240b')
CONFIG_ID_NAMESPACE = uuid.UUID('a48b38fa-349b-11e9-a6be-4f337f82f5ad')
CONFIG_ID = uuid.UUID('172097a4-39f4-11e9-b189-9321eb2f4b97')


class Context:
    def __init__(self, conn):
        self.db = conn


class TypeNodeType(dbops.CompositeType):
    """The node of the type_t, which is a forest of type_node_t."""
    def __init__(self):
        super().__init__(name=('edgedb', 'type_node_t'))

        self.add_columns([
            dbops.Column(name='id', type='uuid'),
            dbops.Column(name='maintype', type='uuid'),
            dbops.Column(name='name', type='text'),
            dbops.Column(name='collection', type='text'),
            dbops.Column(name='subtypes', type='uuid[]'),
            dbops.Column(name='dimensions', type='int[]'),
            dbops.Column(name='is_root', type='bool'),
        ])


class TypeType(dbops.CompositeType):
    """A common at-rest type description structure.

    edgedb.type_t is used to describe any type, including composites,
    lists of types and dicts of types.  The type information is represented
    by a forest of type_node_t.
    """
    def __init__(self):
        super().__init__(name=('edgedb', 'type_t'))

        self.add_columns([
            dbops.Column(name='types', type='edgedb.type_node_t[]'),
        ])


class TypeDescNodeType(dbops.CompositeType):
    def __init__(self):
        super().__init__(name=('edgedb', 'type_desc_node_t'))

        self.add_columns([
            dbops.Column(name='id', type='uuid'),
            dbops.Column(name='maintype', type='text'),
            dbops.Column(name='name', type='text'),
            dbops.Column(name='collection', type='text'),
            dbops.Column(name='subtypes', type='uuid[]'),
            dbops.Column(name='dimensions', type='int[]'),
            dbops.Column(name='is_root', type='bool'),
        ])


class TypeDescType(dbops.CompositeType):
    def __init__(self):
        super().__init__(name=('edgedb', 'typedesc_t'))

        self.add_columns([
            dbops.Column(name='types', type='edgedb.type_desc_node_t[]'),
        ])


class ExpressionType(dbops.CompositeType):
    def __init__(self):
        super().__init__(name=('edgedb', 'expression_t'))

        self.add_columns([
            dbops.Column(name='text', type='text'),
            dbops.Column(name='qlast', type='bytea'),
            dbops.Column(name='irast', type='bytea'),
            dbops.Column(name='refs', type='uuid[]'),
        ])


class GetObjectMetadata(dbops.Function):
    """Return EdgeDB metadata associated with a backend object."""
    text = '''
        SELECT
            CASE WHEN substr(d, 1, 5) = '$EDB:'
            THEN substr(d, 6)::jsonb
            ELSE '{}'::jsonb
            END
        FROM
            obj_description("objoid", "objclass") AS d
    '''

    def __init__(self):
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
            CASE WHEN substr(d, 1, 5) = '$EDB:'
            THEN substr(d, 6)::jsonb
            ELSE '{}'::jsonb
            END
        FROM
            shobj_description("objoid", "objclass") AS d
    '''

    def __init__(self):
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

    def __init__(self):
        super().__init__(
            name=('edgedb', '_raise_exception'),
            args=[('msg', ('text',)), ('rtype', ('anyelement',))],
            returns=('anyelement',),
            volatility='volatile',
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

    def __init__(self):
        super().__init__(
            name=('edgedb', '_raise_specific_exception'),
            args=[('exc', ('text',)), ('msg', ('text',)), ('det', ('text',)),
                  ('rtype', ('anyelement',))],
            returns=('anyelement',),
            volatility='volatile',
            language='plpgsql',
            text=self.text)


class RaiseExceptionOnNullFunction(dbops.Function):
    """Return the passed value or raise an exception if it's NULL."""
    text = '''
        SELECT
            coalesce(val, edgedb._raise_specific_exception(exc, msg, det, val))
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_raise_exception_on_null'),
            args=[('val', ('anyelement',)), ('exc', ('text',)),
                  ('msg', ('text',)), ('det', ('text',))],
            returns=('anyelement',),
            volatility='volatile',
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

    def __init__(self):
        super().__init__(
            name=('edgedb', '_raise_exception_on_empty'),
            args=[('val', ('anyelement',)), ('exc', ('text',)),
                  ('msg', ('text',)), ('det', ('text',))],
            returns=('anyelement',),
            volatility='volatile',
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
                        '; got json ' || jsonb_typeof(val)
                    ),
                    det,
                    NULL::jsonb
                )
            ELSE
                val
            END
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', 'jsonb_assert_type'),
            args=[('val', ('jsonb',)), ('typenames', ('text[]',)),
                  ('msg', ('text',), 'NULL'), ('det', ('text',), "''")],
            returns=('jsonb',),
            volatility='volatile',
            text=self.text)


class ExtractJSONScalarFunction(dbops.Function):
    """Convert a given JSON scalar value into a text value."""
    text = '''
        SELECT
            (to_jsonb(ARRAY[
                edgedb.jsonb_assert_type(val, ARRAY[json_typename, 'null'])
            ])->>0)
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', 'jsonb_extract_scalar'),
            args=[('val', ('jsonb',)), ('json_typename', ('text',)),
                  ('msg', ('text',), 'NULL'), ('det', ('text',), "''")],
            returns=('text',),
            volatility='volatile',
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

    def __init__(self):
        super().__init__(
            name=('edgedb', '_derive_uuid'),
            args=[('id', ('uuid',)), ('variant', ('smallint',))],
            returns=('uuid',),
            volatility='immutable',
            text=self.text)


class EncodeTypeFunction(dbops.Function):
    text = '''
        SELECT
            ROW(
                (SELECT
                    array_agg(ROW(
                        st.id,
                        edgedb._resolve_type_id(st.maintype),
                        st.name,
                        st.collection,
                        st.subtypes,
                        st.dimensions,
                        st.is_root
                    )::edgedb.type_node_t ORDER BY st.i)
                 FROM
                    UNNEST(type.types)
                        WITH ORDINALITY
                            AS st(id, maintype, name, collection,
                                  subtypes, dimensions, is_root, i)
                )
            )::edgedb.type_t
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_encode_type'),
            args=[('type', ('edgedb', 'typedesc_t'))],
            returns=('edgedb', 'type_t'),
            volatility='stable',
            text=self.text,
            strict=True)


class ResolveTypeFunction(dbops.Function):
    text = '''
        SELECT
            ROW(
                (SELECT
                    array_agg(ROW(
                        st.id,
                        edgedb._resolve_type_name(st.maintype),
                        st.name,
                        st.collection,
                        st.subtypes,
                        st.dimensions,
                        st.is_root
                    )::edgedb.type_desc_node_t ORDER BY st.i)
                 FROM
                    UNNEST(type.types)
                        WITH ORDINALITY
                            AS st(id, maintype, name, collection,
                                  subtypes, dimensions, is_root, i)
                )
            )::edgedb.typedesc_t
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_type'),
            args=[('type', ('edgedb', 'type_t'))],
            returns=('edgedb', 'typedesc_t'),
            volatility='stable',
            text=self.text,
            strict=True)


class ResolveTypeNameFunction(dbops.Function):
    text = '''
        SELECT ((edgedb._resolve_type(type)).types[1]).maintype
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_type_name'),
            args=[('type', ('edgedb', 'type_t'))],
            returns=('text',),
            volatility='stable',
            text=self.text,
            strict=True)


class ResolveSimpleTypeIdFunction(dbops.Function):
    text = '''
        SELECT coalesce(
            (SELECT id FROM edgedb.Object
             WHERE name = type::text),
            edgedb._raise_exception(
                'resolve_type_id: unknown type: "' || type || '"',
                NULL::uuid
            )
        )
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_type_id'),
            args=[('type', ('text',))],
            returns=('uuid',),
            volatility='volatile',
            text=self.text,
            strict=True)


class ResolveSimpleTypeIdListFunction(dbops.Function):
    text = '''
        SELECT
            array_agg(edgedb._resolve_type_id(t.name) ORDER BY t.ordinality)
        FROM
            UNNEST(types) WITH ORDINALITY AS t(name)
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_type_id'),
            args=[('types', ('text[]',))],
            returns=('uuid[]',),
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

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_type_name'),
            args=[('type', ('uuid',))],
            returns=('text',),
            volatility='volatile',
            text=self.text,
            strict=True)


class ResolveSimpleTypeNameListFunction(dbops.Function):
    text = '''
        SELECT
            array_agg(edgedb._resolve_type_name(t.id) ORDER BY t.ordinality)
        FROM
            UNNEST(type_data) WITH ORDINALITY AS t(id)
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_type_name'),
            args=[('type_data', ('uuid[]',))],
            returns=('text[]',),
            volatility='volatile',
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

    def __init__(self):
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

    def __init__(self):
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

    def __init__(self):
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

    def __init__(self):
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
                SELECT classes && o.mro
                FROM edgedb.InheritingObject o
                WHERE o.id = clsid
            );
    '''

    def __init__(self):
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
                SELECT pclsid = any(o.mro)
                FROM edgedb.InheritingObject o
                WHERE o.id = clsid
            );
    '''

    def __init__(self):
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

    def __init__(self):
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

    def __init__(self):
        super().__init__(
            name=('edgedb', 'shortname_from_fullname'),
            args=[('name', 'text')],
            returns='text',
            volatility='immutable',
            language='sql',
            text=self.__class__.text)


class NullIfArrayNullsFunction(dbops.Function):
    """Check if array contains NULLs and if so, return NULL."""
    def __init__(self):
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
    def __init__(self):
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

    def __init__(self):
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
    def __init__(self):
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

    def __init__(self):
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
    def __init__(self):
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

    def __init__(self):
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

    def __init__(self):
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

    def __init__(self):
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

    def __init__(self):
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('anyarray',)), ('index', ('bigint',)),
                  ('det', ('text',))],
            returns=('anyelement',),
            volatility='volatile',
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

    def __init__(self):
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

    def __init__(self):
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('text',)), ('index', ('bigint',)),
                  ('det', ('text',))],
            returns=('text',),
            volatility='volatile',
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

    def __init__(self):
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('bytea',)), ('index', ('bigint',)),
                  ('det', ('text',))],
            returns=('bytea',),
            volatility='volatile',
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

    def __init__(self):
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

    def __init__(self):
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

    def __init__(self):
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

    def __init__(self):
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

    def __init__(self):
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

    def __init__(self):
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
                    'cannot index json ' || jsonb_typeof(val),
                    det,
                    NULL::jsonb
                )
            END
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('jsonb',)), ('index', ('text',)),
                  ('det', ('text',))],
            returns=('jsonb',),
            volatility='volatile',
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
                    'cannot index json ' || jsonb_typeof(val),
                    det,
                    NULL::jsonb
                )
            END
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_index'),
            args=[('val', ('jsonb',)), ('index', ('bigint',)),
                  ('det', ('text',))],
            returns=('jsonb',),
            volatility='volatile',
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

    def __init__(self):
        super().__init__(
            name=('edgedb', '_slice'),
            args=[('val', ('jsonb',)), ('start', ('bigint',)),
                  ('stop', ('bigint',))],
            returns=('jsonb',),
            volatility='volatile',
            text=self.text)


class SysConfigValueType(dbops.CompositeType):
    """Type of values returned by _read_sys_config."""
    def __init__(self):
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
                        jsonb_each(
                            (SELECT pg_read_file(
                                (SELECT d.dir || '/config_spec.json'
                                 FROM data_dir d)
                            )::jsonb)
                        ) s
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
                            (SELECT pg_read_file(
                                (SELECT d.dir || '/config_sys.json'
                                 FROM data_dir d)
                            )::jsonb)
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
                        SELECT * FROM config_sess
                    ) AS u
                ) AS q
            WHERE
                q.n = 1;
        $$;
        END;
    '''

    def __init__(self):
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
        # ObjectSet and ObjectList are exempt from type_t encoding,
        # as they always represent only non-collection types, and
        # keeping the encoding simple is important for performance
        # reasons.
        coltype = 'uuid[]'

    elif issubclass(ftype, (s_obj.Object, s_obj.ObjectCollection)):
        coltype = 'edgedb.type_t'

    elif issubclass(ftype, s_expr.Expression):
        coltype = 'edgedb.expression_t'

    elif issubclass(ftype, s_expr.ExpressionList):
        coltype = 'edgedb.expression_t[]'

    elif issubclass(ftype, typed.TypedList) and issubclass(ftype.type, str):
        coltype = 'text[]'

    elif issubclass(ftype, dict):
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
    metaclasses = s_obj.ObjectMeta.get_schema_metaclasses()

    metaclasses = [
        mcls for mcls in metaclasses
        if (not issubclass(mcls, (s_obj.ObjectRef, s_abc.Collection)) and
            not isinstance(mcls, adapter.Adapter) and
            not issubclass(mcls, (s_db.Database)))
    ]

    return metaclasses


def init_metaclass_tables():
    # The first MetaCLass is the abstract Object, which we created
    # manually above.
    metaclasses = get_interesting_metaclasses()

    for mcls in metaclasses:
        table = dbops.Table(name=('edgedb', mcls.__name__.lower()))

        bases = []
        for parent in mcls.__bases__:
            if not issubclass(parent, s_obj.Object):
                continue

            parent_tab = metaclass_tables.get(parent)
            if parent_tab is None:
                raise RuntimeError(
                    'cannot determine schema metaclass table hierarchy')

            bases.append(parent_tab)

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
    anytype.mro = None
    anytype.id = s_obj.get_known_type_id('anytype')
    anytype.name = 'anytype'

    anytuple = pseudo_type_table.record
    anytuple.mro = None
    anytuple.id = s_obj.get_known_type_id('anytuple')
    anytuple.name = 'anytuple'

    return dbops.Insert(table=pseudo_type_table, records=[anytype, anytuple])


async def bootstrap(conn):
    commands = dbops.CommandGroup()
    commands.add_commands([
        dbops.DropSchema(name='public'),
        dbops.CreateSchema(name='edgedb'),
        dbops.CreateExtension(dbops.Extension(name='uuid-ossp')),
        dbops.CreateExtension(dbops.Extension(name='edbsys')),
        dbops.CreateCompositeType(TypeNodeType()),
        dbops.CreateCompositeType(TypeType()),
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
        dbops.CreateFunction(RaiseExceptionOnNullFunction()),
        dbops.CreateFunction(RaiseExceptionOnEmptyStringFunction()),
        dbops.CreateFunction(AssertJSONTypeFunction()),
        dbops.CreateFunction(ExtractJSONScalarFunction()),
        dbops.CreateFunction(DeriveUUIDFunction()),
        dbops.CreateFunction(ResolveSimpleTypeIdFunction()),
        dbops.CreateFunction(ResolveSimpleTypeIdListFunction()),
        dbops.CreateFunction(ResolveSimpleTypeNameFunction()),
        dbops.CreateFunction(ResolveSimpleTypeNameListFunction()),
        dbops.CreateFunction(ResolveTypeFunction()),
        dbops.CreateFunction(ResolveTypeNameFunction()),
        dbops.CreateFunction(EncodeTypeFunction()),
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
        if issubclass(mcls, s_inheriting.InheritingObject):
            schematab = 'edgedb.{}'.format(mcls.__name__)

            non_inh_link_query = None
        else:
            schematab = 'edgedb.InheritingObject'

            non_inh_link_query = '''
                SELECT
                    (({refattr}).types[1]).maintype AS {src},
                    id                              AS {tgt}
                FROM
                    {reftab}
            '''.format(
                reftab='edgedb.{}'.format(refdict.ref_cls.__name__),
                refattr=qi(refdict.backref_attr),
                src='source',
                tgt='target',
            )

        inh_link_query = '''
            SELECT DISTINCT ON ((cls.id, r.bases[1]))
                cls.id  AS {src},
                r.id    AS {tgt}
            FROM
                (SELECT
                    s.id                AS id,
                    ancestry.ancestor   AS ancestor,
                    ancestry.depth      AS depth
                    FROM
                    {schematab} s
                    LEFT JOIN LATERAL
                        UNNEST(s.mro) WITH ORDINALITY
                                    AS ancestry(ancestor, depth) ON true

                    UNION ALL
                    SELECT
                    s.id                AS id,
                    s.id                AS ancestor,
                    0                   AS depth
                    FROM
                    {schematab} s
                ) AS cls

                INNER JOIN {reftab} r
                    ON (((r.{refattr}).types[1]).maintype = cls.ancestor)
            ORDER BY
                (cls.id, r.bases[1]), cls.depth
        '''.format(
            schematab=schematab,
            reftab='edgedb.{}'.format(refdict.ref_cls.__name__),
            refattr=qi(refdict.backref_attr),
            src='source',
            tgt='target',
        )

        if non_inh_link_query:
            link_query = (
                f'({inh_link_query})\nUNION\n'
                f'({non_inh_link_query})'
            )
        else:
            link_query = inh_link_query

        if pn.name == 'attributes':
            link_query = '''
                SELECT
                    q.{src} AS {src},
                    ((av.attribute).types[1]).maintype AS {tgt},
                    av.value    AS {valprop}
                FROM
                    ({query}
                    ) AS q
                    INNER JOIN edgedb.AttributeValue av ON q.{tgt} = av.id
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
                    (SELECT
                        s.id        AS id,
                        p.param_id  AS param_id,
                        tv.value    AS value
                     FROM
                        edgedb.{mcls.__name__} AS s,

                        LATERAL UNNEST(s.params)
                            WITH ORDINALITY AS p(param_id, num)

                        LEFT JOIN
                            LATERAL UNNEST(s.args)
                                WITH ORDINALITY AS tv(value, _, _, _, num)
                            ON (p.num = tv.num)
                    ) AS q
            '''

        elif issubclass(ftype, (s_obj.Object, s_obj.ObjectCollection)):
            # All other type fields are encoded as type_t.
            link_query = f'''
                SELECT
                    s.id        AS source,
                    (CASE WHEN t.collection IS NULL
                     THEN t.maintype ELSE t.id END)
                                AS target
                FROM
                    edgedb.{mcls.__name__} AS s,
                    LATERAL UNNEST ((s.{qi(pn.name)}).types) AS t(
                        id, maintype, name, collection,
                        subtypes, dimensions, is_root
                    )
                WHERE
                    t.is_root
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
    Database = schema.get('schema::Database')

    view_query = f'''
        SELECT
            edgedb.uuid_generate_v5(
                '{DATABASE_ID_NAMESPACE}'::uuid,
                pg_database.oid::text)
                            AS id,
            datname         AS name
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
            (CASE WHEN types.collection IS NULL
            THEN types.maintype
            ELSE types.id
            END) AS id
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
            (CASE WHEN types.collection IS NULL
            THEN types.maintype
            ELSE types.id
            END) AS id
        FROM
            types
        WHERE
            types.id = any({qual})
    )'''


def _generate_type_element_view(schema, type_fields):
    TypeElement = schema.get('schema::TypeElement')

    source = '\nUNION\n'.join(f'''
        (SELECT
            t.*
        FROM
            {table},
            LATERAL UNNEST (({table}.{qi(field)}).types)
                WITH ORDINALITY AS t(
                    id, maintype, name, collection, subtypes,
                    dimensions, is_root, num
                ))
    ''' for table, field in type_fields)

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
            q.num           AS num
        FROM
            types AS q
        WHERE
            q.name IS NOT NULL
    '''

    return dbops.View(name=tabname(schema, TypeElement), query=view_query)


def _generate_types_views(schema, type_fields):
    views = []

    Array = schema.get('schema::Array')
    Tuple = schema.get('schema::Tuple')

    source = '\nUNION\n'.join(f'''
        (SELECT
            t.*
        FROM
            {table},
            LATERAL UNNEST (({table}.{qi(field)}).types)
                AS t(
                    id, maintype, name, collection, subtypes,
                    dimensions, is_root
                ))
    ''' for table, field in type_fields)

    view_query = f'''
        WITH
            types AS ({source})
        SELECT
            q.id            AS id,
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
        SELECT
            q.id            AS id,
            q.collection    AS name,
            (SELECT id FROM edgedb.Object
                 WHERE name = 'schema::Array')
                            AS __type__,
            (SELECT array_agg(t.id)
             FROM ({_lookup_types('q.subtypes')}) AS t)
                            AS element_types
        FROM
            types AS q
        WHERE
            q.collection = 'tuple'
    '''

    views.append(dbops.View(name=tabname(schema, Tuple), query=view_query))

    return views


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
            cast_module = schema.get(cast_name.module)
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
    exc = schema.get('std::exclusive')
    json_t = schema.get('std::json')

    cols = [
        f"'{CONFIG_ID}'::uuid AS id",
        f"(SELECT id FROM edgedb.Object "
        f"WHERE name = 'cfg::Config') AS __type__",
    ]

    views = []
    json_casts = {
        c.get_to_type(schema): c
        for c in schema.get_casts_from_type(json_t)
    }

    for pn, p in conf.get_pointers(schema).items(schema):
        if pn in ('id', '__type__'):
            continue

        ptype = p.get_target(schema)
        multi = p.get_cardinality(schema) is qltypes.Cardinality.MANY

        if not ptype.is_object_type() and not multi:
            cast = _make_json_caster(
                schema, json_casts, ptype, f'cfg::Config.{pn}')

            cols.append(
                f'(SELECT {cast("value")} FROM _sysconfig '
                f'WHERE name = {ql(pn)}) AS {qi(pn)}'
            )
        else:
            if ptype.is_object_type():
                target_tab = tabname(schema, ptype)
                exclusive_props = []
                multi_props = []
                target_cols = []
                target_extract_cols = []
                exclusive_extract_cols = []

                for pp_name, pp in ptype.get_pointers(schema).items(schema):
                    if pp_name in ('id', '__type__'):
                        continue

                    pp_type = pp.get_target(schema)
                    pp_cast = _make_json_caster(
                        schema, json_casts, pp_type,
                        f'cfg::Config.{pn}.{pp_name}')

                    pp_multi = (
                        pp.get_cardinality(schema) is qltypes.Cardinality.MANY
                    )

                    extract_col = (
                        f'{pp_cast(f"q->{ql(pp_name)}")}'
                        f' AS {qi(pp_name)}')

                    if pp_multi:
                        multi_props.append((pp, pp_cast))
                    else:
                        constraints = pp.get_constraints(schema).objects(
                            schema)
                        if any(c.issubclass(schema, exc) for c in constraints):
                            exclusive_props.append(pp_name)
                            exclusive_extract_cols.append(extract_col)

                        target_cols.append(qi(pp_name))
                        target_extract_cols.append(extract_col)

                target_cols = ',\n'.join(target_cols)
                target_extract_cols = ',\n'.join(target_extract_cols)
                exclusive_extract_cols = ',\n'.join(exclusive_extract_cols)

                exclusive_props.sort()
                id_string = ';'.join(exclusive_props)

                target_query = textwrap.dedent(f'''\
                    SELECT
                        edgedb.uuid_generate_v5(
                            '{DATABASE_ID_NAMESPACE}'::uuid,
                            {ql(id_string)}) AS id,
                        (SELECT id FROM edgedb.Object
                         WHERE name = {ql(ptype.get_name(schema))})
                                         AS __type__,
                        {textwrap.indent(target_cols, ' ' * 24).strip()}
                    FROM
                        (SELECT
                            {textwrap.indent(
                                target_extract_cols, ' ' * 28).strip()}
                         FROM
                            jsonb_array_elements(
                                (SELECT value::jsonb
                                 FROM edgedb._read_sys_config() cfg
                                 WHERE cfg.name = {ql(pn)})
                            ) AS q
                        ) AS q
                ''')

                views.append((target_tab, target_query))

                link_query = textwrap.dedent(f'''\
                    SELECT
                        '{CONFIG_ID}'::uuid AS source,
                        edgedb.uuid_generate_v5(
                            '{DATABASE_ID_NAMESPACE}'::uuid,
                            {ql(id_string)}) AS target
                    FROM
                        (SELECT
                            {textwrap.indent(
                                target_extract_cols, ' ' * 28).strip()}
                         FROM
                            jsonb_array_elements(
                                (SELECT value::jsonb
                                 FROM edgedb._read_sys_config() cfg
                                 WHERE cfg.name = {ql(pn)})
                            ) AS q
                        ) AS q
                ''')

                views.append((tabname(schema, p), link_query))

                for prop, pp_cast in multi_props:
                    pp_name = prop.get_shortname(schema).name

                    link_query = textwrap.dedent(f'''\
                        SELECT
                            edgedb.uuid_generate_v5(
                                '{DATABASE_ID_NAMESPACE}'::uuid,
                                {ql(id_string)}) AS source,
                            {pp_cast('v')}       AS target
                        FROM
                            (SELECT
                                {textwrap.indent(
                                    exclusive_extract_cols, ' ' * 32).strip()},
                                q->{ql(pp_name)} AS {qi(pp_name)}
                            FROM
                                jsonb_array_elements(
                                    (SELECT value::jsonb
                                    FROM edgedb._read_sys_config() cfg
                                    WHERE cfg.name = {ql(pn)})
                                ) AS q
                            ) AS q,
                            LATERAL
                                jsonb_array_elements(q.{qi(pp_name)})
                                AS v
                    ''')

                    views.append((tabname(schema, prop), link_query))

            else:
                # MULTI PROPERTY
                cast = _make_json_caster(
                    schema, json_casts, ptype, f'cfg::Config.{pn}')

                link_query = textwrap.dedent(f'''\
                    SELECT
                        '{CONFIG_ID}'::uuid AS source,
                        {cast('q')}         AS target
                    FROM
                        jsonb_array_elements(
                            (SELECT value::jsonb
                                FROM edgedb._read_sys_config() cfg
                                WHERE cfg.name = {ql(pn)})
                        ) AS q
                ''')

                views.append((tabname(schema, p), link_query))

    view_query = textwrap.dedent('''\
        WITH _sysconfig AS (SELECT * FROM edgedb._read_sys_config())
        SELECT
            {cols}
    ''').format(
        cols='\n,    '.join(cols)
    )

    views.append((
        tabname(schema, conf),
        view_query,
    ))

    commands.add_commands([
        dbops.CreateView(dbops.View(name=tn, query=q))
        for tn, q in views
    ])

    block = dbops.PLTopBlock(disable_ddl_triggers=True)
    commands.generate(block)
    await _execute_block(conn, block)


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

            if ptrstor.table_type == 'ObjectType':
                if pn == 'name' and issubclass(mcls, s_obj.Object):
                    col_expr = 'edgedb.shortname_from_fullname(t.{})'.format(
                        qi(ptrstor.column_name))
                elif field.type is s_expr.Expression:
                    col_expr = f'(t.{qi(ptrstor.column_name)}).text'
                elif field.type is s_expr.ExpressionList:
                    col_expr = f'''
                        (SELECT array_agg(q.text)
                         FROM unnest(t.{qi(ptrstor.column_name)})
                                AS qi(text, _, _, _))
                    '''
                else:
                    col_expr = f't.{qi(ptrstor.column_name)}'

                cols.append((col_expr, pn))
            else:
                view = _get_link_view(mcls, schema_cls, field, ptr, refdict,
                                      schema)
                if view.name not in views:
                    views[view.name] = view

        coltext = textwrap.indent(
            ',\n'.join(('{} AS {}'.format(*c) for c in cols)), ' ' * 16)

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
                INNER JOIN edgedb.Object AS no
                    ON (no.name = cmt.description)
        '''

        view = dbops.View(name=tabname(schema, schema_cls), query=view_query)

        views[view.name] = view

    type_views = _generate_types_views(schema, type_fields)
    views.update({v.name: v for v in type_views})
    for v in type_views:
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
                "name",
                "id",
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
