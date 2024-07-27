#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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


create extension package postgis version '3.4.2' {
    set ext_module := "ext::postgis";
    set sql_extensions := ["postgis >=3.4.2,<4.0.0"];

    set sql_setup_script := $$
        -- Make it possible to have `!=`, `?!=`, and `not in` for geometry
        CREATE FUNCTION edgedb.geo_neq(l edgedb.geometry, r edgedb.geometry)
        RETURNS bool
            LANGUAGE sql
            STRICT
            IMMUTABLE
            AS 'SELECT not(l = r)';

        CREATE OPERATOR <> (
                LEFTARG = edgedb.geometry, RIGHTARG = edgedb.geometry,
                PROCEDURE = edgedb.geo_neq,
                COMMUTATOR = '<>'
        );

        -- All comparisons between box3d values need a cast to geometry, but
        -- implicit cast to box creates ambiguity and prevents the implicit
        -- cast to geometry for comparisons.
        ALTER EXTENSION postgis DROP CAST (edgedb.box3d AS box);
        DROP CAST (edgedb.box3d AS box);
    $$;
    set sql_teardown_script := $$
        DROP FUNCTION edgedb.geo_neq(l edgedb.geometry, r edgedb.geometry);
        DROP OPERATOR <> (edgedb.geometry, edgedb.geometry);
    $$;

    create module ext::postgis;

    create scalar type ext::postgis::geometry extending std::anyscalar {
        set id := <uuid>"44c901c0-d922-4894-83c8-061bd05e4840";
        set sql_type := "geometry";
    };

    create scalar type ext::postgis::geography extending std::anyscalar {
        set id := <uuid>"4d738878-3a5f-4821-ab76-9d8e7d6b32c4";
        set sql_type := "geography";
    };

    create scalar type ext::postgis::box2d extending std::anyscalar {
        set id := <uuid>"7fae5536-6311-4f60-8eb9-096a5d972f48";
        set sql_type := "box2d";
    };

    create scalar type ext::postgis::box3d extending std::anyscalar {
        set id := <uuid>"c1a50ff8-fded-48b0-85c2-4905a8481433";
        set sql_type := "box3d";
    };

    create cast from ext::postgis::geometry to std::json {
        set volatility := 'Immutable';
        using sql $$
        SELECT to_jsonb(ST_AsText(val))
        $$;
    };

    create cast from std::json to ext::postgis::geometry {
        set volatility := 'Immutable';
        using sql $$
        SELECT edgedb.jsonb_extract_scalar(
            val, 'string', detail => detail
        )::geometry;
        $$;
    };

    create cast from ext::postgis::geometry to std::str {
        set volatility := 'Immutable';
        using sql $$
        SELECT ST_AsText(val)
        $$;
    };

    create cast from std::str to ext::postgis::geometry {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from ext::postgis::geography to std::json {
        set volatility := 'Immutable';
        using sql $$
        SELECT to_jsonb(ST_AsText(val))
        $$;
    };

    create cast from std::json to ext::postgis::geography {
        set volatility := 'Immutable';
        using sql $$
        SELECT edgedb.jsonb_extract_scalar(
            val, 'string', detail => detail
        )::geography;
        $$;
    };

    create cast from ext::postgis::geography to std::str {
        set volatility := 'Immutable';
        using sql $$
        SELECT ST_AsText(val)
        $$;
    };

    create cast from std::str to ext::postgis::geography {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from ext::postgis::box2d to std::json {
        set volatility := 'Immutable';
        using sql $$
        SELECT to_jsonb(val::text)
        $$;
    };

    create cast from std::json to ext::postgis::box2d {
        set volatility := 'Immutable';
        using sql $$
        SELECT edgedb.jsonb_extract_scalar(
            val, 'string', detail => detail
        )::box2d;
        $$;
    };

    create cast from ext::postgis::box2d to std::str {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::text
        $$;
    };

    create cast from std::str to ext::postgis::box2d {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from ext::postgis::box3d to std::json {
        set volatility := 'Immutable';
        using sql $$
        SELECT to_jsonb(val::text)
        $$;
    };

    create cast from std::json to ext::postgis::box3d {
        set volatility := 'Immutable';
        using sql $$
        SELECT edgedb.jsonb_extract_scalar(
            val, 'string', detail => detail
        )::box3d;
        $$;
    };

    create cast from ext::postgis::box3d to std::str {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::text
        $$;
    };

    create cast from std::str to ext::postgis::box3d {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from ext::postgis::geometry to ext::postgis::geography {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::geography;
        $$;
        allow assignment;
    };

    create cast from ext::postgis::geometry to ext::postgis::box2d {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::box2d;
        $$;
    };

    create cast from ext::postgis::geometry to ext::postgis::box3d {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::box3d;
        $$;
    };

    create cast from ext::postgis::geography to ext::postgis::geometry {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::geometry;
        $$;
    };

    create cast from ext::postgis::box2d to ext::postgis::geometry {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::geometry;
        $$;
    };

    create cast from ext::postgis::box2d to ext::postgis::box3d {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::box3d;
        $$;
        allow assignment;
    };

    create cast from ext::postgis::box3d to ext::postgis::geometry {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::geometry;
        $$;
    };

    create cast from ext::postgis::box3d to ext::postgis::box2d {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::box2d;
        $$;
    };

    create cast from ext::postgis::geometry to std::bytes {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::bytea;
        $$;
    };

    create cast from ext::postgis::geography to std::bytes {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::bytea;
        $$;
    };

    create cast from std::bytes to ext::postgis::geometry {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::geometry;
        $$;
    };

    create cast from std::bytes to ext::postgis::geography {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::geography;
        $$;
    };

    create function ext::postgis::letters(letters: std::str, font: optional std::json = {}) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args:  letters,  font - Returns the input letters rendered as geometry with a default start position at the origin and default text height of 100.';
        set impl_is_strict := false;
        using sql $$
        SELECT st_letters(letters, font::json);
        $$;
    };

### REFLECT: OPERATORS

### REFLECT: FUNCTIONS

### REFLECT: AGGREGATES
};