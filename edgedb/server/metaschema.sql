CREATE SCHEMA edgedb;

SET search_path = edgedb;

DROP SCHEMA public;


CREATE EXTENSION hstore WITH SCHEMA edgedb;
CREATE EXTENSION "uuid-ossp" WITH SCHEMA edgedb;


CREATE DOMAIN known_record_marker_t AS text;


CREATE TYPE type_t AS (
    type integer,
    collection text,
    subtypes integer[]
);


CREATE TYPE typedesc_t AS (
    type text,
    collection text,
    subtypes text[]
);


CREATE AGGREGATE agg_product(double precision) (
    SFUNC = float8mul,
    STYPE = double precision,
    INITCOND = '1'
);


CREATE AGGREGATE agg_product(numeric) (
    SFUNC = numeric_mul,
    STYPE = numeric,
    INITCOND = '1'
);


CREATE TABLE object (
    id serial NOT NULL PRIMARY KEY,
    name text NOT NULL UNIQUE
);


CREATE TABLE module (
    schema_name text NOT NULL,
    imports character varying[],

    PRIMARY KEY (id)
) INHERITS (object);


CREATE TABLE delta (
    module_id integer NOT NULL REFERENCES module(id),
    parents character varying[],
    deltabin bytea NOT NULL,
    deltasrc text NOT NULL,
    checksum character varying NOT NULL,
    commitdate timestamp with time zone DEFAULT now() NOT NULL,
    comment text,

    PRIMARY KEY (id)
) INHERITS (object);


CREATE TABLE primaryobject (
    is_abstract boolean DEFAULT false NOT NULL,
    is_final boolean DEFAULT false NOT NULL,
    title hstore,
    description text,

    PRIMARY KEY (id)
)
INHERITS (object);


CREATE TABLE function (
    paramtypes jsonb,
    paramkinds jsonb,
    paramdefaults jsonb,
    returntype integer,

    PRIMARY KEY (id),
    UNIQUE (name)
)
INHERITS (primaryobject);


CREATE TABLE action (
    PRIMARY KEY (id)
)
INHERITS (primaryobject);


CREATE TABLE inheritingobject (
    bases integer[],
    mro integer[]
)
INHERITS (primaryobject);


CREATE TABLE atom (
    constraints hstore,
    "default" text,
    attributes hstore,

    PRIMARY KEY (id)
)
INHERITS (inheritingobject);


CREATE TABLE attribute (
    type type_t NOT NULL,

    PRIMARY KEY (id)
)
INHERITS (inheritingobject);


CREATE TABLE attribute_value (
    subject integer NOT NULL,
    attribute integer NOT NULL,
    value bytea,

    PRIMARY KEY (id)
)
INHERITS (primaryobject);


CREATE TABLE concept (
    PRIMARY KEY (id)
)
INHERITS (inheritingobject);


CREATE TABLE "constraint" (
    subject integer,
    expr text,
    subjectexpr text,
    localfinalexpr text,
    finalexpr text,
    errmessage text,
    paramtypes jsonb,
    inferredparamtypes jsonb,
    args jsonb
)
INHERITS (inheritingobject);


CREATE TABLE event (
)
INHERITS (inheritingobject);


CREATE TABLE link (
    source integer,
    target integer,
    mapping character(2),
    exposed_behaviour text,
    required boolean DEFAULT false NOT NULL,
    readonly boolean DEFAULT false NOT NULL,
    loading text,
    "default" text,
    constraints hstore,
    abstract_constraints hstore,
    spectargets integer[]
)
INHERITS (inheritingobject);


CREATE TABLE link_property (
    source integer,
    target integer,
    required boolean DEFAULT false NOT NULL,
    readonly boolean DEFAULT false NOT NULL,
    loading text,
    "default" text,
    constraints hstore,
    abstract_constraints hstore
)
INHERITS (inheritingobject);


CREATE TABLE policy (
    subject integer NOT NULL,
    event integer,
    actions integer[]
)
INHERITS (primaryobject);

SET search_path = DEFAULT;


CREATE TABLE feature (
    name text NOT NULL,
    class_name text NOT NULL
);


CREATE TABLE backend_info (
    format_version integer NOT NULL
);


INSERT INTO backend_info (format_version) VALUES (30);


CREATE FUNCTION _resolve_type(type edgedb.type_t)
RETURNS edgedb.typedesc_t AS $$
    SELECT
        ROW(
            (SELECT name FROM edgedb.object
             WHERE id = (type.type)::int),

            type.collection,

            (SELECT
                array_agg(o.name ORDER BY st.i)
             FROM
                edgedb.object AS o,
                UNNEST(type.subtypes)
                    WITH ORDINALITY AS st(t, i)
             WHERE
                o.id = st.t::int)
        )::edgedb.typedesc_t

$$ LANGUAGE SQL STABLE;


CREATE FUNCTION _resolve_type_dict(type_data jsonb) RETURNS jsonb AS $$
    SELECT
        jsonb_object_agg(
            key,
            ROW(
                (SELECT name FROM edgedb.object
                 WHERE id = (value->>'type')::int),

                value->>'collection',

                CASE WHEN jsonb_typeof(value->'subtypes') = 'null' THEN
                    NULL
                ELSE
                    (SELECT
                            array_agg(o.name ORDER BY st.i)
                     FROM
                        edgedb.object AS o,
                        jsonb_array_elements_text(value->'subtypes')
                            WITH ORDINALITY AS st(t, i)
                     WHERE
                        o.id = st.t::int)
                END
            )::edgedb.typedesc_t
        )
    FROM
        jsonb_each(type_data)

$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION edgedb_name_to_pg_name(name text) RETURNS text AS $$
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
                        rtrim(encode(decode(md5(name), 'hex'), 'base64'), '=')
                            AS v
                    ) AS q
                ) AS hash
            )
        ELSE
            name
        END;
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION convert_name(
    module text, name text, suffix text, prefix text = 'edgedb_'
) RETURNS text AS $$

    SELECT
        quote_ident(edgedb.edgedb_name_to_pg_name(prefix || module)) || '.' ||
            quote_ident(edgedb.edgedb_name_to_pg_name(name || suffix));

$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION concept_name_to_table_name(
    module text, name text, prefix text = 'edgedb_'
) RETURNS text AS $$

    SELECT convert_name(module, name, '_data', prefix);

$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION link_name_to_table_name(
    module text, name text, prefix text = 'edgedb_'
) RETURNS text AS $$

    SELECT convert_name(module, name, '_link', prefix);

$$ LANGUAGE SQL IMMUTABLE;


CREATE FUNCTION issubclass(clsid int, classes int[]) RETURNS bool AS $$
    SELECT
        clsid = any(classes) OR (
            SELECT classes && o.mro
            FROM edgedb.inheritingobject o
            WHERE o.id = clsid
        )
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION issubclass(clsid int, pclsid int) RETURNS bool AS $$
    SELECT
        clsid = pclsid OR (
            SELECT pclsid = any(o.mro)
            FROM edgedb.inheritingobject o
            WHERE o.id = clsid
        )
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION isinstance(objid uuid, pclsid int) RETURNS bool AS $$
DECLARE
    ptabname text;
    clsid int;
BEGIN
    ptabname := (
        SELECT
            concept_name_to_table_name(split_part(name, '::', 1),
                                       split_part(name, '::', 2))
        FROM
            edgedb.concept
        WHERE
            id = pclsid
    );

    EXECUTE 'SELECT concept_id FROM ' || ptabname || ' WHERE "std::id" = $1'
        INTO clsid
        USING objid;

    RETURN clsid IS NOT NULL;
END;
$$ LANGUAGE PLPGSQL STABLE;


CREATE FUNCTION tgrf_validate_link_insert() RETURNS trigger AS $$
BEGIN
    PERFORM
        True
    FROM
        edgedb.link l
    WHERE
        l.id = NEW.link_type_id
        AND edgedb.isinstance(NEW."std::target", l.target);

    IF NOT FOUND THEN
        DECLARE
            srcname text;
            ptrname text;
            tgtnames text;
            inserted text;
            detail text;
        BEGIN
            SELECT INTO srcname, ptrname, tgtnames
                c.name,
                l2.name,
                (SELECT
                    string_agg('"' || c2.name || '"', ', ')
                 FROM
                    edgedb.concept c2
                 WHERE
                    c2.id = any(COALESCE(l.spectargets, ARRAY[l.target]))
                )
            FROM
                edgedb.link l,
                edgedb.link l2,
                edgedb.concept c
            WHERE
                l.id = NEW.link_type_id
                AND l2.id = l.bases[1]
                AND c.id = l.source;

            inserted := (
                SELECT
                    c.name
                FROM
                    "edgedb_std"."Object_data" o,
                    edgedb.concept c
                WHERE
                    o."std::id" = NEW."std::target"
                    AND c.id = o.concept_id
            );

            detail := (
                SELECT
                    format('{
                                "source": "%s",
                                "pointer": "%s",
                                "target": "%s",
                                "expected": [%s]
                            }', srcname, ptrname, inserted, tgtnames)
            );

            RAISE EXCEPTION
                'new row for relation "%" violates link target constraint',
                TG_TABLE_NAME
                USING
                    ERRCODE = 'check_violation',
                    COLUMN = 'std::target',
                    TABLE = TG_TABLE_NAME,
                    DETAIL = detail,
                    SCHEMA = TG_TABLE_SCHEMA;
        END;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL STABLE;
