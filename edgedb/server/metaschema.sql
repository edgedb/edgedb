CREATE SCHEMA edgedb;

SET search_path = edgedb;

DROP SCHEMA public;


CREATE EXTENSION hstore WITH SCHEMA edgedb;
CREATE EXTENSION "uuid-ossp" WITH SCHEMA edgedb;


CREATE DOMAIN known_record_marker_t AS text;


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


CREATE TABLE metaobject (
    id serial NOT NULL PRIMARY KEY,
    name text NOT NULL UNIQUE,
    is_abstract boolean DEFAULT false NOT NULL,
    is_final boolean DEFAULT false NOT NULL,
    title hstore,
    description text
);


CREATE TABLE action (
)
INHERITS (metaobject);


CREATE TABLE atom (
    base text,
    constraints hstore,
    "default" text,
    attributes hstore
)
INHERITS (metaobject);


CREATE TABLE attribute (
    type bytea NOT NULL
)
INHERITS (metaobject);


CREATE TABLE attribute_value (
    subject integer NOT NULL,
    attribute integer NOT NULL,
    value bytea
)
INHERITS (metaobject);


CREATE TABLE backend_info (
    format_version integer NOT NULL
);


INSERT INTO backend_info (format_version) VALUES (30);


CREATE TABLE concept (
)
INHERITS (metaobject);


CREATE TABLE "constraint" (
    base text[],
    subject integer,
    expr text,
    subjectexpr text,
    localfinalexpr text,
    finalexpr text,
    errmessage text,
    paramtypes hstore,
    inferredparamtypes hstore,
    args bytea
)
INHERITS (metaobject);


CREATE TABLE deltalog (
    id character varying NOT NULL,
    parents character varying[],
    checksum character varying NOT NULL,
    commit_date timestamp with time zone DEFAULT now() NOT NULL,
    committer text NOT NULL,
    comment text
);


CREATE TABLE deltaref (
    id character varying NOT NULL,
    ref text NOT NULL
);


CREATE TABLE event (
    base text[]
)
INHERITS (metaobject);


CREATE TABLE feature (
    name text NOT NULL,
    class_name text NOT NULL
);


CREATE TABLE link (
    source_id integer,
    target_id integer,
    mapping character(2) NOT NULL,
    exposed_behaviour text,
    required boolean DEFAULT false NOT NULL,
    readonly boolean DEFAULT false NOT NULL,
    loading text,
    base text[],
    "default" text,
    constraints hstore,
    abstract_constraints hstore,
    spectargets text[]
)
INHERITS (metaobject);


CREATE TABLE link_property (
    source_id integer,
    target_id integer,
    required boolean DEFAULT false NOT NULL,
    readonly boolean DEFAULT false NOT NULL,
    loading text,
    base text[],
    "default" text,
    constraints hstore,
    abstract_constraints hstore
)
INHERITS (metaobject);


CREATE TABLE module (
    name text NOT NULL,
    schema_name text NOT NULL,
    imports character varying[]
);


CREATE TABLE policy (
    subject integer NOT NULL,
    event integer,
    actions integer[]
)
INHERITS (metaobject);

SET search_path = DEFAULT;
