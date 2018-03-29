##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

"""Database structure and objects supporting EdgeDB metadata."""

import collections
import textwrap

from edgedb.lang.common import adapter, nlang, typed

from edgedb.lang.schema import attributes as s_attrs
from edgedb.lang.schema import constraints as s_constraints
from edgedb.lang.schema import expr as s_expr
from edgedb.lang.schema import functions as s_funcs
from edgedb.lang.schema import inheriting as s_inheriting
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import named as s_named
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import referencing as s_ref
from edgedb.lang.schema import types as s_types

from . import common
from . import dbops
from . import types


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


class ObjectTable(dbops.Table):
    def __init__(self):
        super().__init__(
            name=('edgedb', 'class'),
            columns=[
                dbops.Column(
                    name='id', type='uuid', required=True, readonly=True,
                    default='uuid_generate_v1mc()')
            ],
            constraints=[
                dbops.PrimaryKey(('edgedb', 'object'), columns=('id', ))
            ]
        )


class RaiseExceptionFunction(dbops.Function):
    text = '''
    BEGIN
        RAISE EXCEPTION '%', msg;
        RETURN 'foo';
    END;
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_raise_exception'),
            args=[('msg', ('text',))],
            returns=('text',),
            volatility='immutable',
            language='plpgsql',
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
        SELECT ((_resolve_type(type)).types[1]).maintype
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
            (SELECT id FROM edgedb.NamedObject
             WHERE name = type::text),
            edgedb._raise_exception(
                'resolve_type_id: unknown type: "' || type || '"'
            )::uuid
        )
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_type_id'),
            args=[('type', ('text',))],
            returns=('uuid',),
            volatility='stable',
            text=self.text,
            strict=True)


class ResolveSimpleTypeNameFunction(dbops.Function):
    text = '''
        SELECT coalesce(
            (SELECT name FROM edgedb.NamedObject
             WHERE id = type::uuid),
            edgedb._raise_exception(
                'resolve_type_name: unknown type: "' || type || '"'
            )::text
        )
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_type_name'),
            args=[('type', ('uuid',))],
            returns=('text',),
            volatility='stable',
            text=self.text,
            strict=True)


class ResolveSimpleTypeNameListFunction(dbops.Function):
    text = '''
        SELECT
            array_agg(_resolve_type_name(t.id) ORDER BY t.ordinality)
        FROM
            UNNEST(type_data) WITH ORDINALITY AS t(id)
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_type_name'),
            args=[('type_data', ('uuid[]',))],
            returns=('text[]',),
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
            'SELECT "std::__type__" FROM ' ||
                ptabname || ' WHERE "std::id" = $1'
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
            name=('edgedb', 'get_shortname'),
            args=[('name', 'text')],
            returns='text',
            volatility='immutable',
            language='sql',
            text=self.__class__.text)


class OrFilterFunction(dbops.Function):
    """Special version of boolean OR that returns NULL on NULL input.

    Unlike SQL, EdgeQL does not have the three-valued boolean logic,
    and boolean operators must obey the same rules as all other
    operators: they must yield an empty set if any of the operands
    is an empty set.  To achieve this, we convert the boolean op
    into an equivalent bitwise OR/AND expression (the shortest and
    fastest equivalent):

        a OR b --> (a::int | b::int)::bool

    This transformation may break bitmap index scan optimization
    when inside a WHERE clause, so we must use the original
    boolean expression in conjunction, which makes the operands appear
    twice in an expression, which may lead to unexpected side-effects,
    like repeated evaluation of calls to volatile functions.  To avoid
    this, we replace the boolean OR in WHERE clauses with a call to this
    function.
    """
    def __init__(self):
        super().__init__(
            name=('edgedb', '_or'),
            args=[('a', 'bool'), ('b', 'bool')],
            returns='bool',
            volatility='immutable',
            language='sql',
            text='SELECT (a OR b) AND (a::int | b::int)::bool')


def _field_to_column(field):
    ftype = field.type[0]
    coltype = None

    if issubclass(ftype, (s_obj.ObjectSet, s_obj.ObjectList)):
        # ObjectSet and ObjectList are exempt from type_t encoding,
        # as they always represent only non-collection types, and
        # keeping the encoding simple is important for performance
        # reasons.
        coltype = 'uuid[]'

    elif issubclass(ftype, (s_obj.Object, s_obj.ObjectCollection)):
        coltype = 'edgedb.type_t'

    elif issubclass(ftype, s_expr.ExpressionList):
        coltype = 'text[]'

    elif issubclass(ftype, typed.TypedList) and issubclass(ftype.type, str):
        coltype = 'text[]'

    elif issubclass(ftype, s_expr.ExpressionDict):
        coltype = 'jsonb'

    elif issubclass(ftype, nlang.WordCombination):
        coltype = 'jsonb'

    elif issubclass(ftype, dict):
        coltype = 'jsonb'

    elif issubclass(ftype, str):
        coltype = 'text'

    elif issubclass(ftype, bool):
        coltype = 'bool'

    elif issubclass(ftype, int):
        coltype = 'bigint'

    else:
        coltype = 'text'

    return dbops.Column(
        name=field.name,
        type=coltype,
        required=field.required
    )


metaclass_tables = collections.OrderedDict()


def get_interesting_metaclasses():
    metaclasses = s_obj.ObjectMeta.get_schema_metaclasses()

    metaclasses = [
        mcls for mcls in metaclasses
        if (not issubclass(mcls, (s_obj.ObjectRef, s_types.Collection)) and
            not isinstance(mcls, adapter.Adapter))
    ]

    return metaclasses[1:]


def init_metaclass_tables():
    # The first MetaCLass is the abstract Object, which we created
    # manually above.
    metaclasses = get_interesting_metaclasses()
    metaclass_tables[s_obj.Object] = ObjectTable()

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
            cols.append(_field_to_column(field))

        table.add_columns(cols)
        metaclass_tables[mcls] = table


init_metaclass_tables()


def get_metaclass_table(mcls):
    return metaclass_tables[mcls]


async def bootstrap(conn):
    commands = dbops.CommandGroup()
    commands.add_commands([
        dbops.DropSchema(name='public'),
        dbops.CreateSchema(name='edgedb'),
        dbops.CreateExtension(dbops.Extension(name='uuid-ossp')),
        dbops.CreateCompositeType(TypeNodeType()),
        dbops.CreateCompositeType(TypeType()),
        dbops.CreateCompositeType(TypeDescNodeType()),
        dbops.CreateCompositeType(TypeDescType()),
        dbops.CreateDomain(('edgedb', 'known_record_marker_t'), 'text'),
        dbops.CreateTable(ObjectTable()),
    ])

    commands.add_commands(
        dbops.CreateTable(table)
        for table in list(metaclass_tables.values())[1:])

    commands.add_commands([
        dbops.CreateFunction(RaiseExceptionFunction()),
        dbops.CreateFunction(DeriveUUIDFunction()),
        dbops.CreateFunction(ResolveSimpleTypeIdFunction()),
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
        dbops.CreateFunction(OrFilterFunction()),
    ])

    await commands.execute(Context(conn))


classref_attr_aliases = {
    'links': 'pointers',
    'link_properties': 'pointers'
}


dbname = lambda n: \
    common.quote_ident(common.edgedb_name_to_pg_name(sn.Name(n)))
tabname = lambda obj: \
    ('edgedbss', common.get_table_name(obj, catenate=False)[1])
q = common.quote_ident
ql = common.quote_literal


def _get_link_view(mcls, schema_cls, field, ptr, refdict, schema):
    pn = ptr.shortname

    if refdict:
        if (issubclass(mcls, s_inheriting.InheritingObject) or
                mcls is s_named.NamedObject):

            if mcls is s_named.NamedObject:
                schematab = 'edgedb.InheritingObject'
            else:
                schematab = 'edgedb.{}'.format(mcls.__name__)

            link_query = '''
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
                refattr=q(refdict.backref_attr),
                src=dbname(sn.Name('std::source')),
                tgt=dbname(sn.Name('std::target')),
            )
        else:
            link_query = '''
                SELECT
                    {refattr}  AS {src},
                    id         AS {tgt}
                FROM
                    {reftab}
            '''.format(
                reftab='edgedb.{}'.format(refdict.ref_cls.__name__),
                refattr=q(refdict.backref_attr),
                src=dbname(sn.Name('std::source')),
                tgt=dbname(sn.Name('std::target')),
            )

        if pn.name == 'attributes':
            link_query = '''
                SELECT
                    q.*,
                    av.value    AS {valprop}
                FROM
                    ({query}
                    ) AS q
                    INNER JOIN edgedb.AttributeValue av
                        ON (q.{tgt} = ((av.attribute).types[1]).maintype AND
                            q.{src} = ((av.subject).types[1]).maintype)
            '''.format(
                query=link_query,
                src=dbname(sn.Name('std::source')),
                tgt=dbname(sn.Name('std::target')),
                valprop=dbname(sn.Name('schema::value')),
            )

            # In addition to custom attributes returned by the
            # generic refdict query above, collect and return
            # standard system attributes.
            partitions = []

            for metaclass in get_interesting_metaclasses():
                fields = metaclass.get_ownfields()
                attrs = []
                for fn in fields:
                    field = metaclass.get_field(fn)
                    if not field.introspectable:
                        continue

                    ftype = field.type[0]
                    if issubclass(ftype,
                                  (s_obj.Object,
                                   s_obj.ObjectCollection,
                                   typed.AbstractTypedCollection,
                                   list, dict)):
                        continue

                    aname = 'stdattrs::{}'.format(fn)
                    attrcls = schema.get(aname, default=None)
                    if attrcls is None:
                        raise RuntimeError(
                            'introspection schema error: {}.{} is not '
                            'defined as `stdattrs` Attribute'.format(
                                metaclass.__name__, fn))
                    aname = ql(aname) + '::text'
                    aval = q(fn) + '::text'
                    if fn == 'name':
                        aval = 'edgedb.get_shortname({})'.format(aval)
                    attrs.append([aname, aval])

                if attrs:
                    values = ', '.join(
                        '({}, {})'.format(k, v) for k, v in attrs)

                    qry = '''
                        SELECT
                            id AS subject_id,
                            a.*
                        FROM
                            {schematab},
                            UNNEST(ARRAY[{values}]) AS a(
                                attr_name text,
                                attr_value text
                            )
                    '''.format(
                        schematab='edgedb.{}'.format(metaclass.__name__),
                        values=values
                    )

                    partitions.append(qry)

            if partitions:
                union = ('\n' + (' ' * 16) + 'UNION \n').join(partitions)

                stdattrs = '''
                    SELECT
                        vals.subject_id     AS {src},
                        attrs.id            AS {tgt},
                        vals.attr_value     AS {valprop}
                    FROM
                        ({union}
                        ) AS vals
                        INNER JOIN edgedb.Attribute attrs
                            ON (vals.attr_name = attrs.name)
                '''.format(
                    union=union,
                    src=dbname(sn.Name('std::source')),
                    tgt=dbname(sn.Name('std::target')),
                    valprop=dbname(sn.Name('schema::value')),
                )

                link_query += (
                    '\n' + (' ' * 16) + 'UNION ALL (\n' + stdattrs +
                    '\n' + (' ' * 16) + ')'
                )

    else:
        link_query = None
        if field is not None:
            ftype = field.type[0]
        else:
            ftype = type(None)

        if issubclass(ftype, (s_obj.ObjectSet, s_obj.ObjectList)):
            if ptr.singular():
                raise RuntimeError(
                    'introspection schema error: {!r} must not be '
                    'singular'.format(
                        '(' + schema_cls.name + ')' + '.' + pn.name))

            # ObjectSet and ObjectList fields are stored as uuid[],
            # so we just need to unnest the array here.
            refattr = 'UNNEST(' + q(pn.name) + ')'

        elif pn.name == 'params' and (
                mcls is s_funcs.Function or
                mcls is s_constraints.Constraint):
            # Func params need special handling as they are defined
            # in three separate fields.
            link_query = f'''
                SELECT
                    q.id            AS {dbname('std::source')},
                    edgedb._derive_uuid(q.id, q.num::smallint)
                                    AS {dbname('std::target')}
                FROM
                    (SELECT
                        s.id        AS id,
                        t.num       AS num
                     FROM
                        edgedb.{mcls.__name__} AS s,
                        LATERAL UNNEST((s.paramtypes).types)
                            WITH ORDINALITY AS
                                t(id, maintype, name, collection, subtypes,
                                  dimensions, is_root, num)
                     WHERE
                        t.is_root
                    ) AS q
            '''

        elif issubclass(ftype, (s_obj.Object, s_obj.ObjectCollection)):
            # All other type fields are encoded as type_t.
            link_query = f'''
                SELECT
                    s.id        AS {dbname('std::source')},
                    (CASE WHEN t.collection IS NULL
                     THEN t.maintype ELSE t.id END)
                                AS {dbname('std::target')}
                FROM
                    edgedb.{mcls.__name__} AS s,
                    LATERAL UNNEST ((s.{q(pn.name)}).types) AS t(
                        id, maintype, name, collection,
                        subtypes, dimensions, is_root
                    )
                WHERE
                    t.is_root
            '''

        else:
            if not ptr.singular():
                raise RuntimeError(
                    'introspection schema error: {!r} must be '
                    'singular'.format(
                        '(' + schema_cls.name + ')' + '.' + pn.name))

            refattr = q(pn.name)

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
                src=dbname(sn.Name('std::source')),
                tgt=dbname(sn.Name('std::target')),
            )

    return dbops.View(name=tabname(ptr), query=link_query)


def _generate_param_view(schema):
    FuncParam = schema.get('schema::Parameter')

    view_query = f'''
        SELECT
            edgedb._derive_uuid(id, q.num::smallint)
                            AS {dbname('std::id')},
            (SELECT id FROM edgedb.NamedObject
                 WHERE name = 'schema::Parameter')
                            AS {dbname('std::__type__')},
            q.type_id       AS {dbname('schema::type')},
            q.kind          AS {dbname('schema::kind')},
            q.num           AS {dbname('schema::num')},
            q.name          AS {dbname('schema::name')},
            q.def           AS {dbname('schema::default')},
            q.varparam=q.num AS {dbname('schema::variadic')}
        FROM
            (SELECT
                f.id        AS id,
                f.varparam  AS varparam,
                t.num       AS num,
                (CASE WHEN t.collection IS NULL
                 THEN t.maintype
                 ELSE t.id END)
                            AS type_id,
                tn.name     AS name,
                td.expr     AS def,
                tk.kind     AS kind
             FROM
                (SELECT
                    id, paramtypes, paramnames, paramdefaults, paramkinds,
                    varparam
                 FROM edgedb.Function
                 UNION ALL
                 SELECT
                    id, paramtypes, NULL as paramnames, NULL as paramdefaults,
                    NULL as paramkinds, NULL as varparam
                 FROM edgedb.Constraint
                ) AS f,
                LATERAL UNNEST((f.paramtypes).types)
                    WITH ORDINALITY AS
                        t(id, maintype, name, collection,
                          subtypes, dimensions, is_root, num)
                LEFT JOIN
                    LATERAL UNNEST(f.paramnames)
                        WITH ORDINALITY AS tn(name, num)
                    ON (t.num = tn.num)
                LEFT JOIN
                    LATERAL UNNEST(f.paramdefaults)
                        WITH ORDINALITY AS td(expr, num)
                    ON (t.num = td.num)
                LEFT JOIN
                    LATERAL UNNEST(f.paramkinds)
                        WITH ORDINALITY AS tk(kind, num)
                    ON (t.num = tk.num)
            ) AS q
    '''

    return dbops.View(name=tabname(FuncParam), query=view_query)


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
            LATERAL UNNEST (({table}.{q(field)}).types)
                WITH ORDINALITY AS t(
                    id, maintype, name, collection, subtypes,
                    dimensions, is_root, num
                ))
    ''' for table, field in type_fields)

    view_query = f'''
        WITH
            types AS ({source})
        SELECT
            q.id            AS {dbname('std::id')},
            (SELECT id FROM edgedb.NamedObject
                 WHERE name = 'schema::TypeElement')
                            AS {dbname('std::__type__')},
            {_lookup_type('q.id')}
                            AS {dbname('schema::type')},
            q.name          AS {dbname('schema::name')},
            q.num           AS {dbname('schema::num')}
        FROM
            types AS q
        WHERE
            q.name IS NOT NULL
    '''

    return dbops.View(name=tabname(TypeElement), query=view_query)


def _generate_types_views(schema, type_fields):
    views = []

    Array = schema.get('schema::Array')
    Map = schema.get('schema::Map')
    Tuple = schema.get('schema::Tuple')

    source = '\nUNION\n'.join(f'''
        (SELECT
            t.*
        FROM
            {table},
            LATERAL UNNEST (({table}.{q(field)}).types)
                AS t(
                    id, maintype, name, collection, subtypes,
                    dimensions, is_root
                ))
    ''' for table, field in type_fields)

    view_query = f'''
        WITH
            types AS ({source})
        SELECT
            q.id            AS {dbname('std::id')},
            q.collection    AS {dbname('schema::name')},
            NULL            AS {dbname('schema::description')},
            (SELECT id FROM edgedb.NamedObject
                 WHERE name = 'schema::Array')
                            AS {dbname('std::__type__')},
            {_lookup_type('q.subtypes[1]')}
                            AS {dbname('schema::element_type')},
            q.dimensions    AS {dbname('schema::dimensions')}
        FROM
            types AS q
        WHERE
            q.collection = 'array'
    '''

    views.append(dbops.View(name=tabname(Array), query=view_query))

    view_query = f'''
        WITH
            types AS ({source})
        SELECT
            q.id            AS {dbname('std::id')},
            q.collection    AS {dbname('schema::name')},
            NULL            AS {dbname('schema::description')},
            (SELECT id FROM edgedb.NamedObject
                 WHERE name = 'schema::Array')
                            AS {dbname('std::__type__')},
            {_lookup_type('q.subtypes[1]')}
                            AS {dbname('schema::key_type')},
            {_lookup_type('q.subtypes[2]')}
                            AS {dbname('schema::element_type')}
        FROM
            types AS q
        WHERE
            q.collection = 'map'
    '''

    views.append(dbops.View(name=tabname(Map), query=view_query))

    view_query = f'''
        WITH
            types AS ({source})
        SELECT
            q.id            AS {dbname('std::id')},
            q.collection    AS {dbname('schema::name')},
            NULL            AS {dbname('schema::description')},
            (SELECT id FROM edgedb.NamedObject
                 WHERE name = 'schema::Array')
                            AS {dbname('std::__type__')},
            (SELECT array_agg(t.id)
             FROM ({_lookup_types('q.subtypes')}) AS t)
                            AS {dbname('schema::element_types')}
        FROM
            types AS q
        WHERE
            q.collection = 'tuple'
    '''

    views.append(dbops.View(name=tabname(Tuple), query=view_query))

    return views


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
        if mcls is s_named.NamedObject:
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

        for pn, ptr in schema_cls.pointers.items():
            field = mcls.get_field(pn.name)
            refdict = None
            if field is None:
                if pn.name == 'attributes':
                    # Special hack to allow generic introspection of
                    # both generic and standard attributes, so we
                    # pretend all classes are AttributeSubjects.
                    refdict = s_attrs.AttributeSubject.attributes

                elif issubclass(mcls, s_ref.ReferencingObject):
                    fn = classref_attr_aliases.get(pn.name, pn.name)
                    refdict = mcls.get_refdict(fn)
                    if refdict is not None and ptr.singular():
                        # This is nether a field, nor a refdict, that's
                        # not expected.
                        raise RuntimeError(
                            'introspection schema error: {!r} must not be '
                            'singular'.format(
                                '(' + schema_cls.name + ')' + '.' + pn.name))

            if field is None and refdict is None:
                if pn.name == 'id':
                    # Id is present implicitly in schema tables.
                    pass
                elif pn.name == '__type__':
                    continue
                elif pn.name == 'params' and (
                        mcls is s_funcs.Function or
                        mcls is s_constraints.Constraint):
                    # Function params need special handling as
                    # they are defined as three separate fields.
                    pass
                else:
                    # This is nether a field, nor a refdict, that's
                    # not expected.
                    raise RuntimeError(
                        'introspection schema error: cannot resolve '
                        '{!r} into metadata reference'.format(
                            '(' + schema_cls.name + ')' +
                            '.' + pn.name))

            if field is not None:
                ft = field.type[0]
                if (issubclass(ft, (s_obj.Object, s_obj.ObjectCollection)) and
                        not issubclass(ft, (s_obj.ObjectSet,
                                            s_obj.ObjectList))):
                    type_fields.append(
                        (f'edgedb.{mcls.__name__}', pn.name)
                    )

            ptrstor = types.get_pointer_storage_info(ptr, schema=schema)

            if ptrstor.table_type == 'ObjectType':
                if (pn.name == 'name' and
                        issubclass(mcls, (s_inheriting.InheritingObject,
                                          s_funcs.Function))):
                    col_expr = 'edgedb.get_shortname({})'.format(q(pn.name))
                else:
                    col_expr = q(pn.name)

                cols.append((col_expr, dbname(ptr.shortname)))
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
                (SELECT id FROM edgedb.NamedObject
                 WHERE name = '{schema_cls.name}') AS "std::__type__"
            FROM
                edgedb.{mcls.__name__}
        '''

        view = dbops.View(name=tabname(schema_cls), query=view_query)

        views[view.name] = view

    type_views = _generate_types_views(schema, type_fields)
    views.update({v.name: v for v in type_views})
    for v in type_views:
        views.move_to_end(v.name, last=False)

    te_view = _generate_type_element_view(schema, type_fields)
    views[te_view.name] = te_view

    fp_view = _generate_param_view(schema)
    views[fp_view.name] = fp_view

    types_view = views[tabname(schema.get('schema::Type'))]
    types_view.query += '\nUNION ALL\n' + '\nUNION ALL\n'.join(f'''
        (
            SELECT
                "schema::name",
                "schema::description",
                "std::id",
                "std::__type__"
            FROM
                {common.qname(*view.name)}
        )
    ''' for view in type_views)

    for view in views.values():
        commands.add_command(dbops.CreateView(view))

    await commands.execute(Context(conn))
