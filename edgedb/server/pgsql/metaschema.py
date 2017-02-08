##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

"""Database structure and objects supporting EdgeDB metadata."""

import collections
import textwrap

from edgedb.lang.common import adapter, nlang

from edgedb.lang.schema import attributes as s_attrs
from edgedb.lang.schema import derivable as s_derivable
from edgedb.lang.schema import expr as s_expr
from edgedb.lang.schema import functions as s_funcs
from edgedb.lang.schema import inheriting as s_inheriting
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import named as s_named
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import referencing as s_ref

from . import common
from . import dbops
from . import types


class Context:
    def __init__(self, conn):
        self.db = conn


class TypeType(dbops.CompositeType):
    def __init__(self):
        super().__init__(name=('edgedb', 'type_t'))

        self.add_columns([
            dbops.Column(name='type', type='uuid'),
            dbops.Column(name='collection', type='text'),
            dbops.Column(name='subtypes', type='uuid[]'),
        ])


class TypeDescType(dbops.CompositeType):
    def __init__(self):
        super().__init__(name=('edgedb', 'typedesc_t'))

        self.add_columns([
            dbops.Column(name='type', type='text'),
            dbops.Column(name='collection', type='text'),
            dbops.Column(name='subtypes', type='text[]'),
        ])


class ClassTable(dbops.Table):
    def __init__(self):
        super().__init__(
            name=('edgedb', 'class'),
            columns=[
                dbops.Column(
                    name='id', type='uuid', required=True, readonly=True,
                    default='uuid_generate_v1mc()')
            ],
            constraints=[
                dbops.PrimaryKey(('edgedb', 'class'), columns=('id', ))
            ]
        )


class ResolveTypeFunction(dbops.Function):
    text = '''
        SELECT
            ROW(
                (SELECT name FROM edgedb.NamedClass
                 WHERE id = (type.type)::uuid),

                type.collection,

                (SELECT
                    array_agg(o.name ORDER BY st.i)
                 FROM
                    edgedb.NamedClass AS o,
                    UNNEST(type.subtypes)
                        WITH ORDINALITY AS st(t, i)
                 WHERE
                    o.id = st.t::uuid)
            )::edgedb.typedesc_t
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_type'),
            args=[('type', ('edgedb', 'type_t'))],
            returns=('edgedb', 'typedesc_t'),
            volatility='stable',
            text=self.text)


class ResolveSimpleTypeFunction(dbops.Function):
    text = '''
        SELECT name FROM edgedb.NamedClass
        WHERE id = type::uuid
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_type'),
            args=[('type', ('uuid',))],
            returns=('text',),
            volatility='stable',
            text=self.text)


class ResolveTypeDictFunction(dbops.Function):
    text = '''
        SELECT
            jsonb_object_agg(
                key,
                ROW(
                    (SELECT name FROM edgedb.NamedClass
                     WHERE id = (value->>'type')::uuid),

                    value->>'collection',

                    CASE WHEN jsonb_typeof(value->'subtypes') = 'null' THEN
                        NULL
                    ELSE
                        (SELECT
                                array_agg(o.name ORDER BY st.i)
                         FROM
                            edgedb.NamedClass AS o,
                            jsonb_array_elements_text(value->'subtypes')
                                WITH ORDINALITY AS st(t, i)
                         WHERE
                            o.id = st.t::uuid)
                    END
                )::edgedb.typedesc_t
            )
        FROM
            jsonb_each(type_data)
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_types'),
            args=[('type_data', 'jsonb')],
            returns='jsonb',
            volatility='stable',
            text=self.text)


class ResolveTypeListFunction(dbops.Function):
    text = '''
        SELECT
            array_agg(
                _resolve_type(
                    ROW(t.type, t.collection, t.subtypes)::edgedb.type_t)
            ORDER BY t.ordinality)
        FROM
            UNNEST(type_data) WITH ORDINALITY AS t
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_types'),
            args=[('type_data', ('edgedb', 'type_t[]'))],
            returns=('edgedb', 'typedesc_t[]'),
            volatility='stable',
            text=self.text)


class ResolveSimpleTypeListFunction(dbops.Function):
    text = '''
        SELECT
            array_agg(_resolve_type(t.id) ORDER BY t.ordinality)
        FROM
            UNNEST(type_data) WITH ORDINALITY AS t(id)
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', '_resolve_types'),
            args=[('type_data', ('uuid[]',))],
            returns=('text[]',),
            volatility='stable',
            text=self.text)


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


class ConceptNameToTableNameFunction(dbops.Function):
    text = '''
        SELECT convert_name(module, name, '_data', prefix);
    '''

    def __init__(self):
        super().__init__(
            name=('edgedb', 'concept_name_to_table_name'),
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
                FROM edgedb.InheritingClass o
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
                FROM edgedb.InheritingClass o
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
                edgedb.concept_name_to_table_name(split_part(name, '::', 1),
                                                  split_part(name, '::', 2))
            FROM
                edgedb.concept
            WHERE
                id = pclsid
        );

        EXECUTE
            'SELECT "std::__class__" FROM ' ||
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


def _field_to_column(field):
    ftype = field.type[0]
    coltype = None

    # This is a hack since there is no way
    # to indicate that something is either
    # a Class or a Collection.
    if field.name in {'type', 'returntype'}:
        coltype = 'edgedb.type_t'

    elif issubclass(ftype, (s_obj.Collection, s_obj.NodeClass)):
        coltype = 'edgedb.type_t'

    elif issubclass(ftype, s_obj.Class):
        coltype = 'uuid'

    elif issubclass(ftype, s_obj.TypeList):
        coltype = 'edgedb.type_t[]'

    elif issubclass(ftype, (s_obj.StringList, s_expr.ExpressionList)):
        coltype = 'text[]'

    elif issubclass(ftype, (s_obj.ClassSet, s_obj.ClassList)):
        coltype = 'uuid[]'

    elif issubclass(ftype, (s_obj.ClassDict, s_obj.ArgDict,
                            s_expr.ExpressionDict)):
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
    metaclasses = s_obj.MetaClass.get_schema_metaclasses()

    metaclasses = [
        mcls for mcls in metaclasses
        if (not issubclass(mcls, (s_obj.ClassRef, s_obj.Collection)) and
            not isinstance(mcls, adapter.Adapter))
    ]

    return metaclasses[1:]


def init_metaclass_tables():
    # The first MetaCLass is the abstract Class, which we created
    # manually above.
    metaclasses = get_interesting_metaclasses()
    metaclass_tables[s_obj.Class] = ClassTable()

    for mcls in metaclasses:
        table = dbops.Table(name=('edgedb', mcls.__name__.lower()))

        bases = []
        for parent in mcls.__bases__:
            if not issubclass(parent, s_obj.Class):
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
        dbops.CreateCompositeType(TypeType()),
        dbops.CreateCompositeType(TypeDescType()),
        dbops.CreateDomain(('edgedb', 'known_record_marker_t'), 'text'),
        dbops.CreateTable(ClassTable()),
    ])

    commands.add_commands(
        dbops.CreateTable(table)
        for table in list(metaclass_tables.values())[1:])

    commands.add_commands([
        dbops.CreateFunction(ResolveTypeFunction()),
        dbops.CreateFunction(ResolveSimpleTypeFunction()),
        dbops.CreateFunction(ResolveTypeDictFunction()),
        dbops.CreateFunction(ResolveTypeListFunction()),
        dbops.CreateFunction(ResolveSimpleTypeListFunction()),
        dbops.CreateFunction(EdgeDBNameToPGNameFunction()),
        dbops.CreateFunction(ConvertNameFunction()),
        dbops.CreateFunction(ConceptNameToTableNameFunction()),
        dbops.CreateFunction(LinkNameToTableNameFunction()),
        dbops.CreateFunction(IssubclassFunction()),
        dbops.CreateFunction(IssubclassFunction2()),
        dbops.CreateFunction(IsinstanceFunction()),
        dbops.CreateFunction(NormalizeNameFunction()),
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
        if (issubclass(mcls, s_inheriting.InheritingClass) or
                mcls is s_named.NamedClass):

            if mcls is s_named.NamedClass:
                schematab = 'edgedb.InheritingClass'
            else:
                schematab = 'edgedb.{}'.format(mcls.__name__)

            link_query = '''
                SELECT DISTINCT ON ((cls.id, r.id))
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
                        ON (r.{refattr} = cls.ancestor)
                ORDER BY
                    (cls.id, r.id), cls.depth
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
                        ON (q.{tgt} = av.attribute AND q.{src} = av.subject)
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
                                  (s_obj.Class, s_obj.NodeClass,
                                   s_obj.ClassCollection,
                                   s_obj.ArgDict, s_expr.ExpressionDict,
                                   s_expr.ExpressionList, s_obj.StringList,
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

        # This is a hack since there is no way
        # to indicate that something is either
        # a Class or a Collection.
        if pn.name in {'type', 'target'}:
            link_query = '''
                SELECT
                    id          AS {src},
                    (CASE WHEN
                        (t.{refattr}).collection IS NOT NULL
                        THEN (t.{refattr}).subtypes[0]
                        ELSE (t.{refattr}).type
                    END)        AS {tgt},
                    (t.{refattr}).collection AS {collprop}
                FROM
                    {schematab} AS t
            '''.format(
                schematab='edgedb.{}'.format(mcls.__name__),
                refattr=q(pn.name),
                collprop=dbname(sn.Name('schema::collection')),
                src=dbname(sn.Name('std::source')),
                tgt=dbname(sn.Name('std::target')),
            )

        elif pn.name == 'params' and mcls is s_funcs.Function:
            # Function params need special handling as
            # they are defined as three separate fields.
            link_query = f'''
                SELECT
                    id              AS {dbname('std::source')},
                    (CASE WHEN
                        q.collection IS NOT NULL
                        THEN (q.subtypes[0])::uuid
                        ELSE q.type
                    END)            AS {dbname('std::target')},
                    q.num           AS {dbname('schema::paramnum')},
                    q.collection    AS {dbname('schema::paramcollection')},
                    q.name          AS {dbname('schema::paramname')},
                    q.def           AS {dbname('schema::paramdefault')},
                    q.varparam=q.num AS {dbname('schema::paramvariadic')}
                FROM
                    (SELECT
                        id,
                        type,
                        collection,
                        subtypes,
                        varparam,
                        t.num       AS num,
                        tn.name     AS name,
                        td.expr     AS def
                     FROM
                        edgedb.{mcls.__name__} AS f,
                        LATERAL UNNEST(f.paramtypes)
                            WITH ORDINALITY AS
                                t(type, collection, subtypes, num)
                        INNER JOIN
                            LATERAL UNNEST(f.paramnames)
                                WITH ORDINALITY AS tn(name, num)
                            ON (t.num = tn.num)
                        INNER JOIN
                            LATERAL UNNEST(f.paramdefaults)
                                WITH ORDINALITY AS td(expr, num)
                            ON (t.num = td.num)
                    ) AS q
            '''

        elif issubclass(field.type[0], (s_obj.ClassSet, s_obj.ClassList)):
            if ptr.singular():
                raise RuntimeError(
                    'introspection schema error: {!r} must not be '
                    'singular'.format(
                        '(' + schema_cls.name + ')' + '.' + pn.name))

            refattr = 'UNNEST(' + q(pn.name) + ')'

        elif issubclass(field.type[0], s_obj.ClassDict):
            if ptr.singular():
                raise RuntimeError(
                    'introspection schema error: {!r} must not be '
                    'singular'.format(
                        '(' + schema_cls.name + ')' + '.' + pn.name))

            for propname, prop in ptr.pointers.items():
                if propname.endswith('name'):
                    nameprop = propname
                    break
            else:
                raise RuntimeError(
                    'introspection schema error: {!r} must define '
                    'a @...name link property'.format(
                        '(' + schema_cls.name + ')' + '.' + pn.name))

            link_query = '''
                SELECT
                    id          AS {src},
                    (CASE WHEN (q.{refattr}).value->>'collection' IS NOT NULL
                        THEN ((q.{refattr}).value->'subtypes'->>0)::uuid
                        ELSE ((q.{refattr}).value->>'type')::uuid
                    END)        AS {tgt},
                    (q.{refattr}).value->>'collection'
                                AS {collprop},
                    (q.{refattr}).key::text
                                AS {nameprop}
                FROM
                    (SELECT
                        id,
                        jsonb_each({refattr}) AS {refattr}
                     FROM
                        {schematab}
                    ) AS q
            '''.format(
                schematab='edgedb.{}'.format(mcls.__name__),
                refattr=q(pn.name),
                src=dbname(sn.Name('std::source')),
                tgt=dbname(sn.Name('std::target')),
                collprop=dbname(sn.Name('schema::collection')),
                nameprop=dbname(nameprop),
            )

        elif issubclass(field.type[0], s_obj.ArgDict):
            if ptr.singular():
                raise RuntimeError(
                    'introspection schema error: {!r} must not be '
                    'singular'.format(
                        '(' + schema_cls.name + ')' + '.' + pn.name))

            for propname, prop in ptr.pointers.items():
                if propname.endswith('name'):
                    nameprop = propname
                    break
            else:
                raise RuntimeError(
                    'introspection schema error: {!r} must define '
                    'a @...name link property'.format(
                        '(' + schema_cls.name + ')' + '.' + pn.name))

            link_query = '''
                SELECT
                    id          AS {src},
                    (q.{refattr}).value::text
                                AS {tgt},
                    (q.{refattr}).key::text
                                AS {nameprop}
                FROM
                    (SELECT
                        id,
                        jsonb_each({refattr}) AS {refattr}
                     FROM
                        {schematab}
                    ) AS q
            '''.format(
                schematab='edgedb.{}'.format(mcls.__name__),
                refattr=q(pn.name),
                src=dbname(sn.Name('std::source')),
                tgt=dbname(sn.Name('std::target')),
                nameprop=dbname(nameprop),
            )

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


async def generate_views(conn, schema):
    """Setup views the introspection schema.

    The introspection views emulate regular concept and link tables
    for the classes in the "schema" module by querying the actual
    metadata tables.
    """
    commands = dbops.CommandGroup()

    # We use a separate schema to make it easy to redirect queries.
    commands.add_command(dbops.CreateSchema(name='edgedbss'))

    metaclasses = get_interesting_metaclasses()
    views = collections.OrderedDict()

    for mcls in metaclasses:
        if mcls is s_named.NamedClass:
            schema_name = 'Class'
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

                elif issubclass(mcls, s_ref.ReferencingClass):
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
                elif pn.name == '__class__':
                    continue
                elif pn.name == 'params' and mcls is s_funcs.Function:
                    # Function params need special handling as
                    # they are defined as threee separate fields.
                    pass
                else:
                    # This is nether a field, nor a refdict, that's
                    # not expected.
                    raise RuntimeError(
                        'introspection schema error: cannot resolve '
                        '{!r} into metadata reference'.format(
                            '(' + schema_cls.name + ')' +
                            '.' + pn.name))

            ptrstor = types.get_pointer_storage_info(ptr, schema=schema)

            if ptrstor.table_type == 'concept':
                if (pn.name == 'name' and
                        issubclass(mcls, (s_derivable.DerivableClass,
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

        view_query = '''
            SELECT
                {cols},
                (SELECT id FROM edgedb.NamedClass
                 WHERE name = 'schema::Concept') AS "std::__class__"
            FROM
                {schematab}
        '''.format(
            cols=coltext.strip(),
            schematab='edgedb.{}'.format(mcls.__name__)
        )

        view = dbops.View(name=tabname(schema_cls), query=view_query)

        views[view.name] = view

    for view in views.values():
        commands.add_command(dbops.CreateView(view))

    await commands.execute(Context(conn))
