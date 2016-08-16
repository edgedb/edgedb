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
    bases integer[]
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
    mapping character(2) NOT NULL,
    exposed_behaviour text,
    required boolean DEFAULT false NOT NULL,
    readonly boolean DEFAULT false NOT NULL,
    loading text,
    "default" text,
    constraints hstore,
    abstract_constraints hstore,
    spectargets text[]
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
