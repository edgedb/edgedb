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
        using sql $$
        SELECT val::geometry;
        $$;
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
        using sql $$
        SELECT val::geography;
        $$;
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
        using sql $$
        SELECT val::box2d;
        $$;
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
        using sql $$
        SELECT val::box3d;
        $$;
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

    # total operators: 35
    ##################################################
    create function ext::postgis::op_overlaps(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a && b$$;
    };

    create function ext::postgis::op_same(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a ~= b$$;
    };

    create function ext::postgis::op_distance_centroid(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a <-> b$$;
    };

    create function ext::postgis::op_distance_box(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a <#> b$$;
    };

    create function ext::postgis::op_within(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a @ b$$;
    };

    create function ext::postgis::op_contains(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a ~ b$$;
    };

    create function ext::postgis::op_left(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a << b$$;
    };

    create function ext::postgis::op_overleft(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a &< b$$;
    };

    create function ext::postgis::op_below(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a <<| b$$;
    };

    create function ext::postgis::op_overbelow(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a &<| b$$;
    };

    create function ext::postgis::op_overright(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a &> b$$;
    };

    create function ext::postgis::op_right(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a >> b$$;
    };

    create function ext::postgis::op_overabove(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a |&> b$$;
    };

    create function ext::postgis::op_above(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a |>> b$$;
    };

    create function ext::postgis::op_overlaps_nd(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a &&& b$$;
    };

    create function ext::postgis::op_contains_nd(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a ~~ b$$;
    };

    create function ext::postgis::op_within_nd(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a @@ b$$;
    };

    create function ext::postgis::op_same_nd(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a ~~= b$$;
    };

    create function ext::postgis::op_distance_centroid_nd(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a <<->> b$$;
    };

    create function ext::postgis::op_distance_cpa(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a |=| b$$;
    };

    create function ext::postgis::op_overlaps(a: ext::postgis::geography, b: ext::postgis::geography) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a && b$$;
    };

    create function ext::postgis::op_distance_knn(a: ext::postgis::geography, b: ext::postgis::geography) ->  std::float64 {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a <-> b$$;
    };

    create function ext::postgis::op_contains_2d(a: ext::postgis::box2d, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a ~ b$$;
    };

    create function ext::postgis::op_is_contained_2d(a: ext::postgis::box2d, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a @ b$$;
    };

    create function ext::postgis::op_overlaps_2d(a: ext::postgis::box2d, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a && b$$;
    };

    create function ext::postgis::op_contains_2d(a: ext::postgis::geometry, b: ext::postgis::box2d) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a ~ b$$;
    };

    create function ext::postgis::op_is_contained_2d(a: ext::postgis::geometry, b: ext::postgis::box2d) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a @ b$$;
    };

    create function ext::postgis::op_overlaps_2d(a: ext::postgis::geometry, b: ext::postgis::box2d) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a && b$$;
    };

    create function ext::postgis::op_overlaps_2d(a: ext::postgis::box2d, b: ext::postgis::box2d) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a && b$$;
    };

    create function ext::postgis::op_is_contained_2d(a: ext::postgis::box2d, b: ext::postgis::box2d) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a @ b$$;
    };

    create function ext::postgis::op_contains_2d(a: ext::postgis::box2d, b: ext::postgis::box2d) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a ~ b$$;
    };

    create function ext::postgis::op_overlaps_3d(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a &/& b$$;
    };

    create function ext::postgis::op_contains_3d(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a @>> b$$;
    };

    create function ext::postgis::op_contained_3d(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a <<@ b$$;
    };

    create function ext::postgis::op_same_3d(a: ext::postgis::geometry, b: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set prefer_subquery_args := true;
        using sql $$SELECT a ~== b$$;
    };

    # total functions: 481
    ##################################################
    create function ext::postgis::to_geometry(a0: ext::postgis::geometry, a1: std::int64, a2: std::bool) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT geometry("a0", "a1"::int4, "a2")$$;
    };

    create function ext::postgis::to_geometry(a0: ext::postgis::box2d) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT geometry("a0")$$;
    };

    create function ext::postgis::to_geometry(a0: ext::postgis::box3d) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT geometry("a0")$$;
    };

    create function ext::postgis::to_geometry(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT geometry("a0")$$;
    };

    create function ext::postgis::to_geometry(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT geometry("a0")$$;
    };

    create function ext::postgis::to_geometry(a0: ext::postgis::geography) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT geometry("a0")$$;
    };

    create function ext::postgis::x(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_point - Returns the X coordinate of a Point.';
        using sql function 'st_x';
    };

    create function ext::postgis::y(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_point - Returns the Y coordinate of a Point.';
        using sql function 'st_y';
    };

    create function ext::postgis::z(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_point - Returns the Z coordinate of a Point.';
        using sql function 'st_z';
    };

    create function ext::postgis::m(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_point - Returns the M coordinate of a Point.';
        using sql function 'st_m';
    };

    create function ext::postgis::geometry_cmp(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'geometry_cmp';
    };

    create function ext::postgis::geometry_hash(a0: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'geometry_hash';
    };

    create function ext::postgis::shiftlongitude(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Shifts the longitude coordinates of a geometry between -180..180 and 0..360.';
        using sql function 'st_shiftlongitude';
    };

    create function ext::postgis::wrapx(geom: ext::postgis::geometry, wrap: std::float64, `move`: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, wrap, move - Wrap a geometry around an X value.';
        using sql function 'st_wrapx';
    };

    create function ext::postgis::xmin(a0: ext::postgis::box3d) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: aGeomorBox2DorBox3D - Returns the X minima of a 2D or 3D bounding box or a geometry.';
        using sql function 'st_xmin';
    };

    create function ext::postgis::ymin(a0: ext::postgis::box3d) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: aGeomorBox2DorBox3D - Returns the Y minima of a 2D or 3D bounding box or a geometry.';
        using sql function 'st_ymin';
    };

    create function ext::postgis::zmin(a0: ext::postgis::box3d) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: aGeomorBox2DorBox3D - Returns the Z minima of a 2D or 3D bounding box or a geometry.';
        using sql function 'st_zmin';
    };

    create function ext::postgis::xmax(a0: ext::postgis::box3d) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: aGeomorBox2DorBox3D - Returns the X maxima of a 2D or 3D bounding box or a geometry.';
        using sql function 'st_xmax';
    };

    create function ext::postgis::ymax(a0: ext::postgis::box3d) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: aGeomorBox2DorBox3D - Returns the Y maxima of a 2D or 3D bounding box or a geometry.';
        using sql function 'st_ymax';
    };

    create function ext::postgis::zmax(a0: ext::postgis::box3d) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: aGeomorBox2DorBox3D - Returns the Z maxima of a 2D or 3D bounding box or a geometry.';
        using sql function 'st_zmax';
    };

    create function ext::postgis::expand(a0: ext::postgis::box2d, a1: std::float64) ->  ext::postgis::box2d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, units_to_expand - Returns a bounding box expanded from another bounding box or a geometry.';
        using sql function 'st_expand';
    };

    create function ext::postgis::expand(box: ext::postgis::box2d, dx: std::float64, dy: std::float64) ->  ext::postgis::box2d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, units_to_expand - Returns a bounding box expanded from another bounding box or a geometry.';
        using sql function 'st_expand';
    };

    create function ext::postgis::expand(a0: ext::postgis::box3d, a1: std::float64) ->  ext::postgis::box3d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, units_to_expand - Returns a bounding box expanded from another bounding box or a geometry.';
        using sql function 'st_expand';
    };

    create function ext::postgis::expand(box: ext::postgis::box3d, dx: std::float64, dy: std::float64, dz: std::float64 = 0) ->  ext::postgis::box3d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, units_to_expand - Returns a bounding box expanded from another bounding box or a geometry.';
        using sql function 'st_expand';
    };

    create function ext::postgis::expand(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, units_to_expand - Returns a bounding box expanded from another bounding box or a geometry.';
        using sql function 'st_expand';
    };

    create function ext::postgis::expand(geom: ext::postgis::geometry, dx: std::float64, dy: std::float64, dz: std::float64 = 0, dm: std::float64 = 0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, units_to_expand - Returns a bounding box expanded from another bounding box or a geometry.';
        using sql function 'st_expand';
    };

    create function ext::postgis::postgis_getbbox(a0: ext::postgis::geometry) ->  ext::postgis::box2d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'postgis_getbbox';
    };

    create function ext::postgis::makebox2d(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::box2d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: pointLowLeft, pointUpRight - Creates a BOX2D defined by two 2D point geometries.';
        using sql function 'st_makebox2d';
    };

    create function ext::postgis::estimatedextent(a0: std::str, a1: std::str, a2: std::str, a3: std::bool) ->  ext::postgis::box2d {
        set volatility := 'Stable';
        set force_return_cast := true;
        create annotation description := 'args: schema_name, table_name, geocolumn_name, parent_only - Returns the estimated extent of a spatial table.';
        using sql function 'st_estimatedextent';
    };

    create function ext::postgis::estimatedextent(a0: std::str, a1: std::str, a2: std::str) ->  ext::postgis::box2d {
        set volatility := 'Stable';
        set force_return_cast := true;
        create annotation description := 'args: schema_name, table_name, geocolumn_name, parent_only - Returns the estimated extent of a spatial table.';
        using sql function 'st_estimatedextent';
    };

    create function ext::postgis::estimatedextent(a0: std::str, a1: std::str) ->  ext::postgis::box2d {
        set volatility := 'Stable';
        set force_return_cast := true;
        create annotation description := 'args: schema_name, table_name, geocolumn_name, parent_only - Returns the estimated extent of a spatial table.';
        using sql function 'st_estimatedextent';
    };

    create function ext::postgis::findextent(a0: std::str, a1: std::str, a2: std::str) ->  ext::postgis::box2d {
        set volatility := 'Stable';
        set force_return_cast := true;
        using sql function 'st_findextent';
    };

    create function ext::postgis::findextent(a0: std::str, a1: std::str) ->  ext::postgis::box2d {
        set volatility := 'Stable';
        set force_return_cast := true;
        using sql function 'st_findextent';
    };

    create function ext::postgis::postgis_addbbox(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Add bounding box to the geometry.';
        using sql function 'postgis_addbbox';
    };

    create function ext::postgis::postgis_dropbbox(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Drop the bounding box cache from the geometry.';
        using sql function 'postgis_dropbbox';
    };

    create function ext::postgis::postgis_hasbbox(a0: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns TRUE if the bbox of this geometry is cached, FALSE otherwise.';
        using sql function 'postgis_hasbbox';
    };

    create function ext::postgis::quantizecoordinates(g: optional ext::postgis::geometry, prec_x: optional std::int64, prec_y: optional std::int64 = {}, prec_z: optional std::int64 = {}, prec_m: optional std::int64 = {}) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g, prec_x, prec_y, prec_z, prec_m - Sets least significant bits of coordinates to zero';
        set impl_is_strict := false;
        using sql $$SELECT st_quantizecoordinates("g", "prec_x"::int4, "prec_y"::int4, "prec_z"::int4, "prec_m"::int4)$$;
    };

    create function ext::postgis::memsize(a0: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns the amount of memory space a geometry takes.';
        using sql function 'st_memsize';
    };

    create function ext::postgis::summary(a0: ext::postgis::geometry) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Returns a text summary of the contents of a geometry.';
        using sql function 'st_summary';
    };

    create function ext::postgis::summary(a0: ext::postgis::geography) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Returns a text summary of the contents of a geometry.';
        using sql function 'st_summary';
    };

    create function ext::postgis::npoints(a0: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the number of points (vertices) in a geometry.';
        using sql function 'st_npoints';
    };

    create function ext::postgis::nrings(a0: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns the number of rings in a polygonal geometry.';
        using sql function 'st_nrings';
    };

    create function ext::postgis::length3d(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_3dlinestring - Returns the 3D length of a linear geometry.';
        using sql function 'st_3dlength';
    };

    create function ext::postgis::length2d(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_2dlinestring - Returns the 2D length of a linear geometry. Alias for ST_Length';
        using sql function 'st_length2d';
    };

    create function ext::postgis::length(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_2dlinestring - Returns the 2D length of a linear geometry.';
        using sql function 'st_length';
    };

    create function ext::postgis::length(geog: ext::postgis::geography, use_spheroid: std::bool = true) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_2dlinestring - Returns the 2D length of a linear geometry.';
        using sql function 'st_length';
    };

    create function ext::postgis::length(a0: std::str) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_2dlinestring - Returns the 2D length of a linear geometry.';
        using sql function 'st_length';
    };

    create function ext::postgis::perimeter3d(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns the 3D perimeter of a polygonal geometry.';
        using sql function 'st_3dperimeter';
    };

    create function ext::postgis::perimeter2d(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns the 2D perimeter of a polygonal geometry. Alias for ST_Perimeter.';
        using sql function 'st_perimeter2d';
    };

    create function ext::postgis::perimeter(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the length of the boundary of a polygonal geometry or geography.';
        using sql function 'st_perimeter';
    };

    create function ext::postgis::perimeter(geog: ext::postgis::geography, use_spheroid: std::bool = true) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the length of the boundary of a polygonal geometry or geography.';
        using sql function 'st_perimeter';
    };

    create function ext::postgis::area2d(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_area2d';
    };

    create function ext::postgis::area(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the area of a polygonal geometry.';
        using sql function 'st_area';
    };

    create function ext::postgis::area(geog: ext::postgis::geography, use_spheroid: std::bool = true) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the area of a polygonal geometry.';
        using sql function 'st_area';
    };

    create function ext::postgis::area(a0: std::str) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the area of a polygonal geometry.';
        using sql function 'st_area';
    };

    create function ext::postgis::ispolygoncw(a0: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Tests if Polygons have exterior rings oriented clockwise and interior rings oriented counter-clockwise.';
        using sql function 'st_ispolygoncw';
    };

    create function ext::postgis::ispolygonccw(a0: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Tests if Polygons have exterior rings oriented counter-clockwise and interior rings oriented clockwise.';
        using sql function 'st_ispolygonccw';
    };

    create function ext::postgis::distancespheroid(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomlonlatA, geomlonlatB, measurement_spheroid=WGS84 - Returns the minimum distance between two lon/lat geometries using a spheroidal earth model.';
        using sql function 'st_distancespheroid';
    };

    create function ext::postgis::distance(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the distance between two geometry or geography values.';
        using sql function 'st_distance';
    };

    create function ext::postgis::distance(geog1: ext::postgis::geography, geog2: ext::postgis::geography, use_spheroid: std::bool = true) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the distance between two geometry or geography values.';
        using sql function 'st_distance';
    };

    create function ext::postgis::distance(a0: std::str, a1: std::str) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the distance between two geometry or geography values.';
        using sql function 'st_distance';
    };

    create function ext::postgis::pointinsidecircle(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64, a3: std::float64) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_pointinsidecircle';
    };

    create function ext::postgis::azimuth(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: origin, target - Returns the north-based azimuth of a line between two points.';
        using sql function 'st_azimuth';
    };

    create function ext::postgis::azimuth(geog1: ext::postgis::geography, geog2: ext::postgis::geography) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: origin, target - Returns the north-based azimuth of a line between two points.';
        using sql function 'st_azimuth';
    };

    create function ext::postgis::project(geom1: ext::postgis::geometry, distance: std::float64, azimuth: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, distance, azimuth - Returns a point projected from a start point by a distance and bearing (azimuth).';
        using sql function 'st_project';
    };

    create function ext::postgis::project(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, distance: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, distance, azimuth - Returns a point projected from a start point by a distance and bearing (azimuth).';
        using sql function 'st_project';
    };

    create function ext::postgis::project(geog: optional ext::postgis::geography, distance: optional std::float64, azimuth: optional std::float64) -> optional ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, distance, azimuth - Returns a point projected from a start point by a distance and bearing (azimuth).';
        set impl_is_strict := false;
        using sql function 'st_project';
    };

    create function ext::postgis::project(geog_from: ext::postgis::geography, geog_to: ext::postgis::geography, distance: std::float64) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, distance, azimuth - Returns a point projected from a start point by a distance and bearing (azimuth).';
        using sql function 'st_project';
    };

    create function ext::postgis::angle(pt1: ext::postgis::geometry, pt2: ext::postgis::geometry, pt3: ext::postgis::geometry, pt4: ext::postgis::geometry = <ext::postgis::geometry>'POINT EMPTY') ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: point1, point2, point3, point4 - Returns the angle between two vectors defined by 3 or 4 points, or 2 lines.';
        using sql function 'st_angle';
    };

    create function ext::postgis::angle(line1: ext::postgis::geometry, line2: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: point1, point2, point3, point4 - Returns the angle between two vectors defined by 3 or 4 points, or 2 lines.';
        using sql function 'st_angle';
    };

    create function ext::postgis::lineextend(geom: ext::postgis::geometry, distance_forward: std::float64, distance_backward: std::float64 = 0.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: line, distance_forward, distance_backward=0.0 - Returns a line with the last and first segments extended the specified distance(s).';
        using sql function 'st_lineextend';
    };

    create function ext::postgis::force2d(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Force the geometries into a "2-dimensional mode".';
        using sql function 'st_force2d';
    };

    create function ext::postgis::force3dz(geom: ext::postgis::geometry, zvalue: std::float64 = 0.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, Zvalue = 0.0 - Force the geometries into XYZ mode.';
        using sql function 'st_force3dz';
    };

    create function ext::postgis::force3d(geom: ext::postgis::geometry, zvalue: std::float64 = 0.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, Zvalue = 0.0 - Force the geometries into XYZ mode. This is an alias for ST_Force3DZ.';
        using sql function 'st_force3d';
    };

    create function ext::postgis::force3dm(geom: ext::postgis::geometry, mvalue: std::float64 = 0.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, Mvalue = 0.0 - Force the geometries into XYM mode.';
        using sql function 'st_force3dm';
    };

    create function ext::postgis::force4d(geom: ext::postgis::geometry, zvalue: std::float64 = 0.0, mvalue: std::float64 = 0.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, Zvalue = 0.0, Mvalue = 0.0 - Force the geometries into XYZM mode.';
        using sql function 'st_force4d';
    };

    create function ext::postgis::forcecollection(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Convert the geometry into a GEOMETRYCOLLECTION.';
        using sql function 'st_forcecollection';
    };

    create function ext::postgis::collectionextract(a0: ext::postgis::geometry, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: collection - Given a geometry collection, returns a multi-geometry containing only elements of a specified type.';
        using sql $$SELECT st_collectionextract("a0", "a1"::int4)$$;
    };

    create function ext::postgis::collectionextract(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: collection - Given a geometry collection, returns a multi-geometry containing only elements of a specified type.';
        using sql $$SELECT st_collectionextract("a0")$$;
    };

    create function ext::postgis::collectionhomogenize(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: collection - Returns the simplest representation of a geometry collection.';
        using sql function 'st_collectionhomogenize';
    };

    create function ext::postgis::multi(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Return the geometry as a MULTI* geometry.';
        using sql function 'st_multi';
    };

    create function ext::postgis::forcecurve(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Upcast a geometry into its curved type, if applicable.';
        using sql function 'st_forcecurve';
    };

    create function ext::postgis::forcesfs(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Force the geometries to use SFS 1.1 geometry types only.';
        using sql function 'st_forcesfs';
    };

    create function ext::postgis::forcesfs(a0: ext::postgis::geometry, version: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Force the geometries to use SFS 1.1 geometry types only.';
        using sql function 'st_forcesfs';
    };

    create function ext::postgis::envelope(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns a geometry representing the bounding box of a geometry.';
        using sql function 'st_envelope';
    };

    create function ext::postgis::boundingdiagonal(geom: ext::postgis::geometry, fits: std::bool = false) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, fits=false - Returns the diagonal of a geometrys bounding box.';
        using sql function 'st_boundingdiagonal';
    };

    create function ext::postgis::reverse(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Return the geometry with vertex order reversed.';
        using sql function 'st_reverse';
    };

    create function ext::postgis::scroll(a0: ext::postgis::geometry, a1: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: linestring, point - Change start point of a closed LineString.';
        using sql function 'st_scroll';
    };

    create function ext::postgis::forcepolygoncw(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Orients all exterior rings clockwise and all interior rings counter-clockwise.';
        using sql function 'st_forcepolygoncw';
    };

    create function ext::postgis::forcepolygonccw(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Orients all exterior rings counter-clockwise and all interior rings clockwise.';
        using sql function 'st_forcepolygonccw';
    };

    create function ext::postgis::forcerhr(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Force the orientation of the vertices in a polygon to follow the Right-Hand-Rule.';
        using sql function 'st_forcerhr';
    };

    create function ext::postgis::postgis_noop(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'postgis_noop';
    };

    create function ext::postgis::postgis_geos_noop(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'postgis_geos_noop';
    };

    create function ext::postgis::normalize(geom: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Return the geometry in its canonical form.';
        using sql function 'st_normalize';
    };

    create function ext::postgis::zmflag(a0: ext::postgis::geometry) ->  std::int16 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns a code indicating the ZM coordinate dimension of a geometry.';
        using sql function 'st_zmflag';
    };

    create function ext::postgis::ndims(a0: ext::postgis::geometry) ->  std::int16 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the coordinate dimension of a geometry.';
        using sql function 'st_ndims';
    };

    create function ext::postgis::asewkt(a0: ext::postgis::geometry) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asewkt("a0")$$;
    };

    create function ext::postgis::asewkt(a0: ext::postgis::geometry, a1: std::int64) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asewkt("a0", "a1"::int4)$$;
    };

    create function ext::postgis::asewkt(a0: ext::postgis::geography) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asewkt("a0")$$;
    };

    create function ext::postgis::asewkt(a0: ext::postgis::geography, a1: std::int64) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asewkt("a0", "a1"::int4)$$;
    };

    create function ext::postgis::asewkt(a0: std::str) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asewkt("a0")$$;
    };

    create function ext::postgis::astwkb(geom: optional ext::postgis::geometry, prec: optional std::int64 = {}, prec_z: optional std::int64 = {}, prec_m: optional std::int64 = {}, with_sizes: optional std::bool = {}, with_boxes: optional std::bool = {}) -> optional std::bytes {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql $$SELECT st_astwkb("geom", "prec"::int4, "prec_z"::int4, "prec_m"::int4, "with_sizes", "with_boxes")$$;
    };

    # FIXME: array<geometry> is causing an issue
    # create function ext::postgis::astwkb(geom: optional array<ext::postgis::geometry>, ids: optional array<std::int64>, prec: optional std::int64 = {}, prec_z: optional std::int64 = {}, prec_m: optional std::int64 = {}, with_sizes: optional std::bool = {}, with_boxes: optional std::bool = {}) -> optional std::bytes {
    #     set volatility := 'Immutable';
    #     set force_return_cast := true;
    #     set impl_is_strict := false;
    #     using sql $$SELECT st_astwkb("geom"::geometry[], "ids"::int8[], "prec"::int4, "prec_z"::int4, "prec_m"::int4, "with_sizes", "with_boxes")$$;
    # };

    create function ext::postgis::asewkb(a0: ext::postgis::geometry) ->  std::bytes {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_asewkb';
    };

    create function ext::postgis::asewkb(a0: ext::postgis::geometry, a1: std::str) ->  std::bytes {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_asewkb';
    };

    create function ext::postgis::ashexewkb(a0: ext::postgis::geometry) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_ashexewkb';
    };

    create function ext::postgis::ashexewkb(a0: ext::postgis::geometry, a1: std::str) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_ashexewkb';
    };

    create function ext::postgis::aslatlontext(geom: ext::postgis::geometry, tmpl: std::str = '') ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_aslatlontext';
    };

    create function ext::postgis::geomfromewkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_geomfromewkb';
    };

    create function ext::postgis::geomfromtwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_geomfromtwkb';
    };

    create function ext::postgis::geomfromewkt(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_geomfromewkt';
    };

    create function ext::postgis::makepoint(a0: std::float64, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: x, y - Creates a 2D, 3DZ or 4D Point.';
        using sql function 'st_makepoint';
    };

    create function ext::postgis::makepoint(a0: std::float64, a1: std::float64, a2: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: x, y - Creates a 2D, 3DZ or 4D Point.';
        using sql function 'st_makepoint';
    };

    create function ext::postgis::makepoint(a0: std::float64, a1: std::float64, a2: std::float64, a3: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: x, y - Creates a 2D, 3DZ or 4D Point.';
        using sql function 'st_makepoint';
    };

    create function ext::postgis::makepointm(a0: std::float64, a1: std::float64, a2: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: x, y, m - Creates a Point from X, Y and M values.';
        using sql function 'st_makepointm';
    };

    create function ext::postgis::makebox3d(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::box3d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: point3DLowLeftBottom, point3DUpRightTop - Creates a BOX3D defined by two 3D point geometries.';
        using sql function 'st_3dmakebox';
    };

    # FIXME: array<geometry> is causing an issue
    # create function ext::postgis::makeline(a0: array<ext::postgis::geometry>) ->  ext::postgis::geometry {
    #     set volatility := 'Immutable';
    #     set force_return_cast := true;
    #     create annotation description := 'args: geom1, geom2 - Creates a LineString from Point, MultiPoint, or LineString geometries.';
    #     using sql $$SELECT st_makeline("a0"::geometry[])$$;
    # };

    create function ext::postgis::makeline(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom1, geom2 - Creates a LineString from Point, MultiPoint, or LineString geometries.';
        using sql $$SELECT st_makeline("geom1", "geom2")$$;
    };

    create function ext::postgis::linefrommultipoint(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: aMultiPoint - Creates a LineString from a MultiPoint geometry.';
        using sql function 'st_linefrommultipoint';
    };

    create function ext::postgis::addpoint(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: linestring, point - Add a point to a LineString.';
        using sql $$SELECT st_addpoint("geom1", "geom2")$$;
    };

    create function ext::postgis::addpoint(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, a2: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: linestring, point - Add a point to a LineString.';
        using sql $$SELECT st_addpoint("geom1", "geom2", "a2"::int4)$$;
    };

    create function ext::postgis::removepoint(a0: ext::postgis::geometry, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: linestring, offset - Remove a point from a linestring.';
        using sql $$SELECT st_removepoint("a0", "a1"::int4)$$;
    };

    create function ext::postgis::setpoint(a0: ext::postgis::geometry, a1: std::int64, a2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: linestring, zerobasedposition, point - Replace point of a linestring with a given point.';
        using sql $$SELECT st_setpoint("a0", "a1"::int4, "a2")$$;
    };

    create function ext::postgis::makeenvelope(a0: std::float64, a1: std::float64, a2: std::float64, a3: std::float64, a4: std::int64 = 0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: xmin, ymin, xmax, ymax, srid=unknown - Creates a rectangular Polygon from minimum and maximum coordinates.';
        using sql $$SELECT st_makeenvelope("a0", "a1", "a2", "a3", "a4"::int4)$$;
    };

    create function ext::postgis::tileenvelope(zoom: std::int64, x: std::int64, y: std::int64, bounds: ext::postgis::geometry = <ext::postgis::geometry>'SRID=3857;LINESTRING(-20037508.342789244 -20037508.342789244, 20037508.342789244 20037508.342789244)', margin: std::float64 = 0.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: tileZoom, tileX, tileY, bounds=SRID=3857;LINESTRING(-20037508.342789 -20037508.342789,20037508.342789 20037508.342789), margin=0.0 - Creates a rectangular Polygon in Web Mercator (SRID:3857) using the XYZ tile system.';
        using sql $$SELECT st_tileenvelope("zoom"::int4, "x"::int4, "y"::int4, "bounds", "margin")$$;
    };

    # FIXME: array<geometry> is causing an issue
    # create function ext::postgis::makepolygon(a0: ext::postgis::geometry, a1: array<ext::postgis::geometry>) ->  ext::postgis::geometry {
    #     set volatility := 'Immutable';
    #     set force_return_cast := true;
    #     create annotation description := 'args: linestring - Creates a Polygon from a shell and optional list of holes.';
    #     using sql $$SELECT st_makepolygon("a0", "a1"::geometry[])$$;
    # };

    create function ext::postgis::makepolygon(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: linestring - Creates a Polygon from a shell and optional list of holes.';
        using sql $$SELECT st_makepolygon("a0")$$;
    };

    create function ext::postgis::buildarea(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Creates a polygonal geometry formed by the linework of a geometry.';
        using sql function 'st_buildarea';
    };

    # FIXME: array<geometry> is causing an issue
    # create function ext::postgis::polygonize(a0: array<ext::postgis::geometry>) ->  ext::postgis::geometry {
    #     set volatility := 'Immutable';
    #     set force_return_cast := true;
    #     create annotation description := 'args: geom_array - Computes a collection of polygons formed from the linework of a set of geometries.';
    #     using sql $$SELECT st_polygonize("a0"::geometry[])$$;
    # };

    # FIXME: array<geometry> is causing an issue
    # create function ext::postgis::clusterintersecting(a0: array<ext::postgis::geometry>) ->  array<ext::postgis::geometry> {
    #     set volatility := 'Immutable';
    #     set force_return_cast := true;
    #     using sql $$SELECT st_clusterintersecting("a0"::geometry[])$$;
    # };

    # FIXME: array<geometry> is causing an issue
    # create function ext::postgis::clusterwithin(a0: array<ext::postgis::geometry>, a1: std::float64) ->  array<ext::postgis::geometry> {
    #     set volatility := 'Immutable';
    #     set force_return_cast := true;
    #     using sql $$SELECT st_clusterwithin("a0"::geometry[], "a1")$$;
    # };

    create function ext::postgis::linemerge(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: amultilinestring - Return the lines formed by sewing together a MultiLineString.';
        using sql function 'st_linemerge';
    };

    create function ext::postgis::linemerge(a0: ext::postgis::geometry, a1: std::bool) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: amultilinestring - Return the lines formed by sewing together a MultiLineString.';
        using sql function 'st_linemerge';
    };

    create function ext::postgis::affine(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64, a3: std::float64, a4: std::float64, a5: std::float64, a6: std::float64, a7: std::float64, a8: std::float64, a9: std::float64, a10: std::float64, a11: std::float64, a12: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, a, b, c, d, e, f, g, h, i, xoff, yoff, zoff - Apply a 3D affine transformation to a geometry.';
        using sql function 'st_affine';
    };

    create function ext::postgis::affine(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64, a3: std::float64, a4: std::float64, a5: std::float64, a6: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, a, b, c, d, e, f, g, h, i, xoff, yoff, zoff - Apply a 3D affine transformation to a geometry.';
        using sql function 'st_affine';
    };

    create function ext::postgis::rotate(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, rotRadians - Rotates a geometry about an origin point.';
        using sql function 'st_rotate';
    };

    create function ext::postgis::rotate(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64, a3: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, rotRadians - Rotates a geometry about an origin point.';
        using sql function 'st_rotate';
    };

    create function ext::postgis::rotate(a0: ext::postgis::geometry, a1: std::float64, a2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, rotRadians - Rotates a geometry about an origin point.';
        using sql function 'st_rotate';
    };

    create function ext::postgis::rotatez(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, rotRadians - Rotates a geometry about the Z axis.';
        using sql function 'st_rotatez';
    };

    create function ext::postgis::rotatex(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, rotRadians - Rotates a geometry about the X axis.';
        using sql function 'st_rotatex';
    };

    create function ext::postgis::rotatey(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, rotRadians - Rotates a geometry about the Y axis.';
        using sql function 'st_rotatey';
    };

    create function ext::postgis::translate(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64, a3: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, deltax, deltay - Translates a geometry by given offsets.';
        using sql function 'st_translate';
    };

    create function ext::postgis::translate(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, deltax, deltay - Translates a geometry by given offsets.';
        using sql function 'st_translate';
    };

    create function ext::postgis::scale(a0: ext::postgis::geometry, a1: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, XFactor, YFactor, ZFactor - Scales a geometry by given factors.';
        using sql function 'st_scale';
    };

    create function ext::postgis::scale(a0: ext::postgis::geometry, a1: ext::postgis::geometry, origin: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, XFactor, YFactor, ZFactor - Scales a geometry by given factors.';
        using sql function 'st_scale';
    };

    create function ext::postgis::scale(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64, a3: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, XFactor, YFactor, ZFactor - Scales a geometry by given factors.';
        using sql function 'st_scale';
    };

    create function ext::postgis::scale(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, XFactor, YFactor, ZFactor - Scales a geometry by given factors.';
        using sql function 'st_scale';
    };

    create function ext::postgis::transscale(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64, a3: std::float64, a4: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, deltaX, deltaY, XFactor, YFactor - Translates and scales a geometry by given offsets and factors.';
        using sql function 'st_transscale';
    };

    create function ext::postgis::get_proj4_from_srid(a0: std::int64) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT get_proj4_from_srid("a0"::int4)$$;
    };

    create function ext::postgis::setsrid(geom: ext::postgis::geometry, srid: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, srid - Set the SRID on a geometry.';
        using sql $$SELECT st_setsrid("geom", "srid"::int4)$$;
    };

    create function ext::postgis::setsrid(geog: ext::postgis::geography, srid: std::int64) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, srid - Set the SRID on a geometry.';
        using sql $$SELECT st_setsrid("geog", "srid"::int4)$$;
    };

    create function ext::postgis::srid(geom: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the spatial reference identifier for a geometry.';
        using sql function 'st_srid';
    };

    create function ext::postgis::srid(geog: ext::postgis::geography) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the spatial reference identifier for a geometry.';
        using sql function 'st_srid';
    };

    create function ext::postgis::postgis_transform_geometry(geom: ext::postgis::geometry, a1: std::str, a2: std::str, a3: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT postgis_transform_geometry("geom", "a1", "a2", "a3"::int4)$$;
    };

    create function ext::postgis::postgis_srs_codes(auth_name: std::str) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: auth_name - Return the list of SRS codes associated with the given authority.';
        using sql function 'postgis_srs_codes';
    };

    create function ext::postgis::transform(a0: ext::postgis::geometry, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, srid - Return a new geometry with coordinates transformed to a different spatial reference system.';
        using sql $$SELECT st_transform("a0", "a1"::int4)$$;
    };

    create function ext::postgis::transform(geom: ext::postgis::geometry, to_proj: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, srid - Return a new geometry with coordinates transformed to a different spatial reference system.';
        using sql $$SELECT st_transform("geom", "to_proj")$$;
    };

    create function ext::postgis::transform(geom: ext::postgis::geometry, from_proj: std::str, to_proj: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, srid - Return a new geometry with coordinates transformed to a different spatial reference system.';
        using sql $$SELECT st_transform("geom", "from_proj", "to_proj")$$;
    };

    create function ext::postgis::transform(geom: ext::postgis::geometry, from_proj: std::str, to_srid: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, srid - Return a new geometry with coordinates transformed to a different spatial reference system.';
        using sql $$SELECT st_transform("geom", "from_proj", "to_srid"::int4)$$;
    };

    create function ext::postgis::postgis_transform_pipeline_geometry(geom: ext::postgis::geometry, pipeline: std::str, forward: std::bool, to_srid: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT postgis_transform_pipeline_geometry("geom", "pipeline", "forward", "to_srid"::int4)$$;
    };

    create function ext::postgis::transformpipeline(geom: ext::postgis::geometry, pipeline: std::str, to_srid: std::int64 = 0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, pipeline, to_srid - Return a new geometry with coordinates transformed to a different spatial reference system using a defined coordinate transformation pipeline.';
        using sql $$SELECT st_transformpipeline("geom", "pipeline", "to_srid"::int4)$$;
    };

    create function ext::postgis::inversetransformpipeline(geom: ext::postgis::geometry, pipeline: std::str, to_srid: std::int64 = 0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, pipeline, to_srid - Return a new geometry with coordinates transformed to a different spatial reference system using the inverse of a defined coordinate transformation pipeline.';
        using sql $$SELECT st_inversetransformpipeline("geom", "pipeline", "to_srid"::int4)$$;
    };

    create function ext::postgis::postgis_version() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns PostGIS version number and compile-time options.';
        set impl_is_strict := false;
        using sql function 'postgis_version';
    };

    create function ext::postgis::postgis_liblwgeom_version() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns the version number of the liblwgeom library. This should match the version of PostGIS.';
        set impl_is_strict := false;
        using sql function 'postgis_liblwgeom_version';
    };

    create function ext::postgis::postgis_proj_version() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns the version number of the PROJ4 library.';
        set impl_is_strict := false;
        using sql function 'postgis_proj_version';
    };

    create function ext::postgis::postgis_wagyu_version() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns the version number of the internal Wagyu library.';
        set impl_is_strict := false;
        using sql function 'postgis_wagyu_version';
    };

    create function ext::postgis::postgis_scripts_installed() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns version of the PostGIS scripts installed in this database.';
        set impl_is_strict := false;
        using sql function 'postgis_scripts_installed';
    };

    create function ext::postgis::postgis_lib_version() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns the version number of the PostGIS library.';
        set impl_is_strict := false;
        using sql function 'postgis_lib_version';
    };

    create function ext::postgis::postgis_scripts_released() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns the version number of the postgis.sql script released with the installed PostGIS lib.';
        set impl_is_strict := false;
        using sql function 'postgis_scripts_released';
    };

    create function ext::postgis::postgis_geos_version() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns the version number of the GEOS library.';
        set impl_is_strict := false;
        using sql function 'postgis_geos_version';
    };

    create function ext::postgis::postgis_geos_compiled_version() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns the version number of the GEOS library against which PostGIS was built.';
        set impl_is_strict := false;
        using sql function 'postgis_geos_compiled_version';
    };

    create function ext::postgis::postgis_lib_revision() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'postgis_lib_revision';
    };

    create function ext::postgis::postgis_svn_version() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'postgis_svn_version';
    };

    create function ext::postgis::postgis_libxml_version() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns the version number of the libxml2 library.';
        set impl_is_strict := false;
        using sql function 'postgis_libxml_version';
    };

    create function ext::postgis::postgis_scripts_build_date() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns build date of the PostGIS scripts.';
        set impl_is_strict := false;
        using sql function 'postgis_scripts_build_date';
    };

    create function ext::postgis::postgis_lib_build_date() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Returns build date of the PostGIS library.';
        set impl_is_strict := false;
        using sql function 'postgis_lib_build_date';
    };

    create function ext::postgis::postgis_extensions_upgrade(target_version: optional std::str = {}) -> optional std::str {
        set volatility := 'Volatile';
        set force_return_cast := true;
        create annotation description := 'args: target_version=null - Packages and upgrades PostGIS extensions (e.g. postgis_raster,postgis_topology, postgis_sfcgal) to given or latest version.';
        set impl_is_strict := false;
        using sql function 'postgis_extensions_upgrade';
    };

    create function ext::postgis::postgis_full_version() -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'Reports full PostGIS version and build configuration infos.';
        set impl_is_strict := false;
        using sql function 'postgis_full_version';
    };

    create function ext::postgis::to_box2d(a0: ext::postgis::geometry) ->  ext::postgis::box2d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Returns a BOX2D representing the 2D extent of a geometry.';
        using sql function 'box2d';
    };

    create function ext::postgis::to_box2d(a0: ext::postgis::box3d) ->  ext::postgis::box2d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Returns a BOX2D representing the 2D extent of a geometry.';
        using sql function 'box2d';
    };

    create function ext::postgis::to_box3d(a0: ext::postgis::geometry) ->  ext::postgis::box3d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Returns a BOX3D representing the 3D extent of a geometry.';
        using sql function 'box3d';
    };

    create function ext::postgis::to_box3d(a0: ext::postgis::box2d) ->  ext::postgis::box3d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Returns a BOX3D representing the 3D extent of a geometry.';
        using sql function 'box3d';
    };

    create function ext::postgis::text(a0: ext::postgis::geometry) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'text';
    };

    create function ext::postgis::bytea(a0: ext::postgis::geometry) ->  std::bytes {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'bytea';
    };

    create function ext::postgis::bytea(a0: ext::postgis::geography) ->  std::bytes {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'bytea';
    };

    create function ext::postgis::simplify(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, tolerance - Returns a simplified version of a geometry, using the Douglas-Peucker algorithm.';
        using sql function 'st_simplify';
    };

    create function ext::postgis::simplify(a0: ext::postgis::geometry, a1: std::float64, a2: std::bool) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, tolerance - Returns a simplified version of a geometry, using the Douglas-Peucker algorithm.';
        using sql function 'st_simplify';
    };

    create function ext::postgis::simplifyvw(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, tolerance - Returns a simplified version of a geometry, using the Visvalingam-Whyatt algorithm';
        using sql function 'st_simplifyvw';
    };

    create function ext::postgis::seteffectivearea(a0: ext::postgis::geometry, a1: std::float64 = -1, a2: std::int64 = 1) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, threshold = 0, set_area = 1 - Sets the effective area for each vertex, using the Visvalingam-Whyatt algorithm.';
        using sql $$SELECT st_seteffectivearea("a0", "a1", "a2"::int4)$$;
    };

    create function ext::postgis::filterbym(a0: optional ext::postgis::geometry, a1: optional std::float64, a2: optional std::float64 = {}, a3: optional std::bool = false) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, min, max = null, returnM = false - Removes vertices based on their M value';
        set impl_is_strict := false;
        using sql function 'st_filterbym';
    };

    create function ext::postgis::chaikinsmoothing(a0: ext::postgis::geometry, a1: std::int64 = 1, a2: std::bool = false) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, nIterations = 1, preserveEndPoints = false - Returns a smoothed version of a geometry, using the Chaikin algorithm';
        using sql $$SELECT st_chaikinsmoothing("a0", "a1"::int4, "a2")$$;
    };

    create function ext::postgis::snaptogrid(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64, a3: std::float64, a4: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, originX, originY, sizeX, sizeY - Snap all points of the input geometry to a regular grid.';
        using sql function 'st_snaptogrid';
    };

    create function ext::postgis::snaptogrid(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, originX, originY, sizeX, sizeY - Snap all points of the input geometry to a regular grid.';
        using sql function 'st_snaptogrid';
    };

    create function ext::postgis::snaptogrid(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, originX, originY, sizeX, sizeY - Snap all points of the input geometry to a regular grid.';
        using sql function 'st_snaptogrid';
    };

    create function ext::postgis::snaptogrid(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, a2: std::float64, a3: std::float64, a4: std::float64, a5: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, originX, originY, sizeX, sizeY - Snap all points of the input geometry to a regular grid.';
        using sql function 'st_snaptogrid';
    };

    create function ext::postgis::segmentize(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, max_segment_length - Returns a modified geometry/geography having no segment longer than a given distance.';
        using sql function 'st_segmentize';
    };

    create function ext::postgis::segmentize(geog: ext::postgis::geography, max_segment_length: std::float64) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, max_segment_length - Returns a modified geometry/geography having no segment longer than a given distance.';
        using sql function 'st_segmentize';
    };

    create function ext::postgis::lineinterpolatepoint(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, a_fraction - Returns a point interpolated along a line at a fractional location.';
        using sql function 'st_lineinterpolatepoint';
    };

    create function ext::postgis::lineinterpolatepoint(a0: ext::postgis::geography, a1: std::float64, use_spheroid: std::bool = true) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, a_fraction - Returns a point interpolated along a line at a fractional location.';
        using sql function 'st_lineinterpolatepoint';
    };

    create function ext::postgis::lineinterpolatepoint(a0: optional std::str, a1: optional std::float64) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, a_fraction - Returns a point interpolated along a line at a fractional location.';
        set impl_is_strict := false;
        using sql function 'st_lineinterpolatepoint';
    };

    create function ext::postgis::lineinterpolatepoints(a0: ext::postgis::geometry, a1: std::float64, repeat: std::bool = true) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, a_fraction, repeat - Returns points interpolated along a line at a fractional interval.';
        using sql function 'st_lineinterpolatepoints';
    };

    create function ext::postgis::lineinterpolatepoints(a0: ext::postgis::geography, a1: std::float64, use_spheroid: std::bool = true, repeat: std::bool = true) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, a_fraction, repeat - Returns points interpolated along a line at a fractional interval.';
        using sql function 'st_lineinterpolatepoints';
    };

    create function ext::postgis::lineinterpolatepoints(a0: optional std::str, a1: optional std::float64) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, a_fraction, repeat - Returns points interpolated along a line at a fractional interval.';
        set impl_is_strict := false;
        using sql function 'st_lineinterpolatepoints';
    };

    create function ext::postgis::linesubstring(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, startfraction, endfraction - Returns the part of a line between two fractional locations.';
        using sql function 'st_linesubstring';
    };

    create function ext::postgis::linesubstring(a0: ext::postgis::geography, a1: std::float64, a2: std::float64) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, startfraction, endfraction - Returns the part of a line between two fractional locations.';
        using sql function 'st_linesubstring';
    };

    create function ext::postgis::linesubstring(a0: optional std::str, a1: optional std::float64, a2: optional std::float64) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, startfraction, endfraction - Returns the part of a line between two fractional locations.';
        set impl_is_strict := false;
        using sql function 'st_linesubstring';
    };

    create function ext::postgis::linelocatepoint(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, a_point - Returns the fractional location of the closest point on a line to a point.';
        using sql function 'st_linelocatepoint';
    };

    create function ext::postgis::linelocatepoint(a0: ext::postgis::geography, a1: ext::postgis::geography, use_spheroid: std::bool = true) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, a_point - Returns the fractional location of the closest point on a line to a point.';
        using sql function 'st_linelocatepoint';
    };

    create function ext::postgis::linelocatepoint(a0: optional std::str, a1: optional std::str) -> optional std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, a_point - Returns the fractional location of the closest point on a line to a point.';
        set impl_is_strict := false;
        using sql function 'st_linelocatepoint';
    };

    create function ext::postgis::addmeasure(a0: ext::postgis::geometry, a1: std::float64, a2: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom_mline, measure_start, measure_end - Interpolates measures along a linear geometry.';
        using sql function 'st_addmeasure';
    };

    create function ext::postgis::closestpointofapproach(a0: ext::postgis::geometry, a1: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: track1, track2 - Returns a measure at the closest point of approach of two trajectories.';
        using sql function 'st_closestpointofapproach';
    };

    create function ext::postgis::distancecpa(a0: ext::postgis::geometry, a1: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: track1, track2 - Returns the distance between the closest point of approach of two trajectories.';
        using sql function 'st_distancecpa';
    };

    create function ext::postgis::cpawithin(a0: ext::postgis::geometry, a1: ext::postgis::geometry, a2: std::float64) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: track1, track2, dist - Tests if the closest point of approach of two trajectoriesis within the specified distance.';
        using sql function 'st_cpawithin';
    };

    create function ext::postgis::isvalidtrajectory(a0: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: line - Tests if the geometry is a valid trajectory.';
        using sql function 'st_isvalidtrajectory';
    };

    create function ext::postgis::intersection(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, gridsize: std::float64 = -1) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, geomB, gridSize = -1 - Computes a geometry representing the shared portion of geometries A and B.';
        using sql function 'st_intersection';
    };

    create function ext::postgis::intersection(a0: ext::postgis::geography, a1: ext::postgis::geography) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, geomB, gridSize = -1 - Computes a geometry representing the shared portion of geometries A and B.';
        using sql function 'st_intersection';
    };

    create function ext::postgis::intersection(a0: std::str, a1: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, geomB, gridSize = -1 - Computes a geometry representing the shared portion of geometries A and B.';
        using sql function 'st_intersection';
    };

    create function ext::postgis::buffer(geom: ext::postgis::geometry, radius: std::float64, options: std::str = '') ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := "args: g1, radius_of_buffer, buffer_style_parameters = ' - Computes a geometry covering all points within a given distance from a geometry.";
        using sql $$SELECT st_buffer("geom", "radius", "options")$$;
    };

    create function ext::postgis::buffer(geom: ext::postgis::geometry, radius: std::float64, quadsegs: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := "args: g1, radius_of_buffer, buffer_style_parameters = ' - Computes a geometry covering all points within a given distance from a geometry.";
        using sql $$SELECT st_buffer("geom", "radius", "quadsegs"::int4)$$;
    };

    create function ext::postgis::buffer(a0: ext::postgis::geography, a1: std::float64) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := "args: g1, radius_of_buffer, buffer_style_parameters = ' - Computes a geometry covering all points within a given distance from a geometry.";
        using sql $$SELECT st_buffer("a0", "a1")$$;
    };

    create function ext::postgis::buffer(a0: ext::postgis::geography, a1: std::float64, a2: std::int64) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := "args: g1, radius_of_buffer, buffer_style_parameters = ' - Computes a geometry covering all points within a given distance from a geometry.";
        using sql $$SELECT st_buffer("a0", "a1", "a2"::int4)$$;
    };

    create function ext::postgis::buffer(a0: ext::postgis::geography, a1: std::float64, a2: std::str) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := "args: g1, radius_of_buffer, buffer_style_parameters = ' - Computes a geometry covering all points within a given distance from a geometry.";
        using sql $$SELECT st_buffer("a0", "a1", "a2")$$;
    };

    create function ext::postgis::buffer(a0: std::str, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := "args: g1, radius_of_buffer, buffer_style_parameters = ' - Computes a geometry covering all points within a given distance from a geometry.";
        using sql $$SELECT st_buffer("a0", "a1")$$;
    };

    create function ext::postgis::buffer(a0: std::str, a1: std::float64, a2: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := "args: g1, radius_of_buffer, buffer_style_parameters = ' - Computes a geometry covering all points within a given distance from a geometry.";
        using sql $$SELECT st_buffer("a0", "a1", "a2"::int4)$$;
    };

    create function ext::postgis::buffer(a0: std::str, a1: std::float64, a2: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := "args: g1, radius_of_buffer, buffer_style_parameters = ' - Computes a geometry covering all points within a given distance from a geometry.";
        using sql $$SELECT st_buffer("a0", "a1", "a2")$$;
    };

    create function ext::postgis::minimumboundingcircle(inputgeom: ext::postgis::geometry, segs_per_quarter: std::int64 = 48) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, num_segs_per_qt_circ=48 - Returns the smallest circle polygon that contains a geometry.';
        using sql $$SELECT st_minimumboundingcircle("inputgeom", "segs_per_quarter"::int4)$$;
    };

    create function ext::postgis::orientedenvelope(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Returns a minimum-area rectangle containing a geometry.';
        using sql function 'st_orientedenvelope';
    };

    create function ext::postgis::offsetcurve(line: ext::postgis::geometry, distance: std::float64, params: std::str = '') ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := "args: line, signed_distance, style_parameters=' - Returns an offset line at a given distance and side from an input line.";
        using sql function 'st_offsetcurve';
    };

    create function ext::postgis::generatepoints(area: ext::postgis::geometry, npoints: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Volatile';
        set force_return_cast := true;
        create annotation description := 'args: g, npoints - Generates random points contained in a Polygon or MultiPolygon.';
        using sql $$SELECT st_generatepoints("area", "npoints"::int4)$$;
    };

    create function ext::postgis::generatepoints(area: ext::postgis::geometry, npoints: std::int64, seed: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g, npoints - Generates random points contained in a Polygon or MultiPolygon.';
        using sql $$SELECT st_generatepoints("area", "npoints"::int4, "seed"::int4)$$;
    };

    create function ext::postgis::convexhull(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Computes the convex hull of a geometry.';
        using sql function 'st_convexhull';
    };

    create function ext::postgis::simplifypreservetopology(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, tolerance - Returns a simplified and valid version of a geometry, using the Douglas-Peucker algorithm.';
        using sql function 'st_simplifypreservetopology';
    };

    create function ext::postgis::isvalidreason(a0: ext::postgis::geometry) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns text stating if a geometry is valid, or a reason for invalidity.';
        using sql $$SELECT st_isvalidreason("a0")$$;
    };

    create function ext::postgis::isvalidreason(a0: ext::postgis::geometry, a1: std::int64) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns text stating if a geometry is valid, or a reason for invalidity.';
        using sql $$SELECT st_isvalidreason("a0", "a1"::int4)$$;
    };

    create function ext::postgis::isvalid(a0: ext::postgis::geometry, a1: std::int64) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Tests if a geometry is well-formed in 2D.';
        using sql $$SELECT st_isvalid("a0", "a1"::int4)$$;
    };

    create function ext::postgis::isvalid(a0: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Tests if a geometry is well-formed in 2D.';
        using sql $$SELECT st_isvalid("a0")$$;
    };

    create function ext::postgis::hausdorffdistance(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the Hausdorff distance between two geometries.';
        using sql function 'st_hausdorffdistance';
    };

    create function ext::postgis::hausdorffdistance(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, a2: std::float64) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the Hausdorff distance between two geometries.';
        using sql function 'st_hausdorffdistance';
    };

    create function ext::postgis::frechetdistance(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, a2: std::float64 = -1) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2, densifyFrac = -1 - Returns the Fréchet distance between two geometries.';
        using sql function 'st_frechetdistance';
    };

    create function ext::postgis::difference(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, gridsize: std::float64 = -1.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, geomB, gridSize = -1 - Computes a geometry representing the part of geometry A that does not intersect geometry B.';
        using sql function 'st_difference';
    };

    create function ext::postgis::boundary(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns the boundary of a geometry.';
        using sql function 'st_boundary';
    };

    create function ext::postgis::points(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Returns a MultiPoint containing the coordinates of a geometry.';
        using sql function 'st_points';
    };

    create function ext::postgis::symdifference(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, gridsize: std::float64 = -1.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, geomB, gridSize = -1 - Computes a geometry representing the portions of geometries A and B that do not intersect.';
        using sql function 'st_symdifference';
    };

    create function ext::postgis::symmetricdifference(geom1: optional ext::postgis::geometry, geom2: optional ext::postgis::geometry) -> optional ext::postgis::geometry {
        set volatility := 'Volatile';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'st_symmetricdifference';
    };

    create function ext::postgis::union(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Computes a geometry representing the point-set union of the input geometries.';
        using sql $$SELECT st_union("geom1", "geom2")$$;
    };

    create function ext::postgis::union(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, gridsize: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Computes a geometry representing the point-set union of the input geometries.';
        using sql $$SELECT st_union("geom1", "geom2", "gridsize")$$;
    };

    # FIXME: array<geometry> is causing an issue
    # create function ext::postgis::union(a0: array<ext::postgis::geometry>) ->  ext::postgis::geometry {
    #     set volatility := 'Immutable';
    #     set force_return_cast := true;
    #     create annotation description := 'args: g1, g2 - Computes a geometry representing the point-set union of the input geometries.';
    #     using sql $$SELECT st_union("a0"::geometry[])$$;
    # };

    create function ext::postgis::unaryunion(a0: ext::postgis::geometry, gridsize: std::float64 = -1.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, gridSize = -1 - Computes the union of the components of a single geometry.';
        using sql function 'st_unaryunion';
    };

    create function ext::postgis::removerepeatedpoints(geom: ext::postgis::geometry, tolerance: std::float64 = 0.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, tolerance - Returns a version of a geometry with duplicate points removed.';
        using sql function 'st_removerepeatedpoints';
    };

    create function ext::postgis::clipbybox2d(geom: ext::postgis::geometry, box: ext::postgis::box2d) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, box - Computes the portion of a geometry falling within a rectangle.';
        using sql function 'st_clipbybox2d';
    };

    create function ext::postgis::subdivide(geom: ext::postgis::geometry, maxvertices: std::int64 = 256, gridsize: std::float64 = -1.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, max_vertices=256, gridSize = -1 - Computes a rectilinear subdivision of a geometry.';
        using sql $$SELECT st_subdivide("geom", "maxvertices"::int4, "gridsize")$$;
    };

    create function ext::postgis::reduceprecision(geom: ext::postgis::geometry, gridsize: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g, gridsize - Returns a valid geometry with points rounded to a grid tolerance.';
        using sql function 'st_reduceprecision';
    };

    create function ext::postgis::makevalid(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: input - Attempts to make an invalid geometry valid without losing vertices.';
        using sql function 'st_makevalid';
    };

    create function ext::postgis::makevalid(geom: ext::postgis::geometry, params: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: input - Attempts to make an invalid geometry valid without losing vertices.';
        using sql function 'st_makevalid';
    };

    create function ext::postgis::cleangeometry(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_cleangeometry';
    };

    create function ext::postgis::split(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: input, blade - Returns a collection of geometries created by splitting a geometry by another geometry.';
        using sql function 'st_split';
    };

    create function ext::postgis::sharedpaths(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: lineal1, lineal2 - Returns a collection containing paths shared by the two input linestrings/multilinestrings.';
        using sql function 'st_sharedpaths';
    };

    create function ext::postgis::snap(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, a2: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: input, reference, tolerance - Snap segments and vertices of input geometry to vertices of a reference geometry.';
        using sql function 'st_snap';
    };

    create function ext::postgis::relatematch(a0: std::str, a1: std::str) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_relatematch';
    };

    create function ext::postgis::node(g: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Nodes a collection of lines.';
        using sql function 'st_node';
    };

    create function ext::postgis::delaunaytriangles(g1: ext::postgis::geometry, tolerance: std::float64 = 0.0, flags: std::int64 = 0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, tolerance = 0.0, flags = 0 - Returns the Delaunay triangulation of the vertices of a geometry.';
        using sql $$SELECT st_delaunaytriangles("g1", "tolerance", "flags"::int4)$$;
    };

    create function ext::postgis::triangulatepolygon(g1: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Computes the constrained Delaunay triangulation of polygons';
        using sql function 'st_triangulatepolygon';
    };

    create function ext::postgis::voronoipolygons(g1: optional ext::postgis::geometry, tolerance: optional std::float64 = 0.0, extend_to: optional ext::postgis::geometry = {}) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, tolerance = 0.0, extend_to = NULL - Returns the cells of the Voronoi diagram of the vertices of a geometry.';
        set impl_is_strict := false;
        using sql function 'st_voronoipolygons';
    };

    create function ext::postgis::voronoilines(g1: optional ext::postgis::geometry, tolerance: optional std::float64 = 0.0, extend_to: optional ext::postgis::geometry = {}) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, tolerance = 0.0, extend_to = NULL - Returns the boundaries of the Voronoi diagram of the vertices of a geometry.';
        set impl_is_strict := false;
        using sql function 'st_voronoilines';
    };

    create function ext::postgis::combinebbox(a0: optional ext::postgis::box3d, a1: optional ext::postgis::geometry) -> optional ext::postgis::box3d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'st_combinebbox';
    };

    create function ext::postgis::combinebbox(a0: optional ext::postgis::box3d, a1: optional ext::postgis::box3d) -> optional ext::postgis::box3d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'st_combinebbox';
    };

    create function ext::postgis::combinebbox(a0: optional ext::postgis::box2d, a1: optional ext::postgis::geometry) -> optional ext::postgis::box2d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'st_combinebbox';
    };

    create function ext::postgis::collect(geom1: optional ext::postgis::geometry, geom2: optional ext::postgis::geometry) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Creates a GeometryCollection or Multi* geometry from a set of geometries.';
        set impl_is_strict := false;
        using sql $$SELECT st_collect("geom1", "geom2")$$;
    };

    # FIXME: array<geometry> is causing an issue
    # create function ext::postgis::collect(a0: array<ext::postgis::geometry>) ->  ext::postgis::geometry {
    #     set volatility := 'Immutable';
    #     set force_return_cast := true;
    #     create annotation description := 'args: g1, g2 - Creates a GeometryCollection or Multi* geometry from a set of geometries.';
    #     using sql $$SELECT st_collect("a0"::geometry[])$$;
    # };

    # FIXME: array<geometry> is causing an issue
    # create function ext::postgis::coverageunion(a0: array<ext::postgis::geometry>) ->  ext::postgis::geometry {
    #     set volatility := 'Immutable';
    #     set force_return_cast := true;
    #     using sql $$SELECT st_coverageunion("a0"::geometry[])$$;
    # };

    create function ext::postgis::relate(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_relate("geom1", "geom2")$$;
    };

    create function ext::postgis::relate(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, a2: std::int64) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_relate("geom1", "geom2", "a2"::int4)$$;
    };

    create function ext::postgis::relate(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, a2: std::str) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_relate("geom1", "geom2", "a2")$$;
    };

    create function ext::postgis::disjoint(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_disjoint';
    };

    create function ext::postgis::linecrossingdirection(line1: ext::postgis::geometry, line2: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_linecrossingdirection';
    };

    create function ext::postgis::dwithin(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, a2: std::float64) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_dwithin';
    };

    create function ext::postgis::dwithin(geog1: ext::postgis::geography, geog2: ext::postgis::geography, tolerance: std::float64, use_spheroid: std::bool = true) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_dwithin';
    };

    create function ext::postgis::dwithin(a0: optional std::str, a1: optional std::str, a2: optional std::float64) -> optional std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'st_dwithin';
    };

    create function ext::postgis::touches(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_touches';
    };

    create function ext::postgis::intersects(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_intersects';
    };

    create function ext::postgis::intersects(geog1: ext::postgis::geography, geog2: ext::postgis::geography) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_intersects';
    };

    create function ext::postgis::intersects(a0: optional std::str, a1: optional std::str) -> optional std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'st_intersects';
    };

    create function ext::postgis::crosses(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_crosses';
    };

    create function ext::postgis::contains(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_contains';
    };

    create function ext::postgis::containsproperly(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_containsproperly';
    };

    create function ext::postgis::within(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_within';
    };

    create function ext::postgis::covers(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_covers';
    };

    create function ext::postgis::covers(geog1: ext::postgis::geography, geog2: ext::postgis::geography) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_covers';
    };

    create function ext::postgis::covers(a0: optional std::str, a1: optional std::str) -> optional std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'st_covers';
    };

    create function ext::postgis::coveredby(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_coveredby';
    };

    create function ext::postgis::coveredby(geog1: ext::postgis::geography, geog2: ext::postgis::geography) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_coveredby';
    };

    create function ext::postgis::coveredby(a0: optional std::str, a1: optional std::str) -> optional std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'st_coveredby';
    };

    create function ext::postgis::overlaps(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_overlaps';
    };

    create function ext::postgis::dfullywithin(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, a2: std::float64) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_dfullywithin';
    };

    create function ext::postgis::dwithin3d(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, a2: std::float64) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_3ddwithin';
    };

    create function ext::postgis::dfullywithin3d(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, a2: std::float64) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_3ddfullywithin';
    };

    create function ext::postgis::intersects3d(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_3dintersects';
    };

    create function ext::postgis::orderingequals(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_orderingequals';
    };

    create function ext::postgis::equals(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_equals';
    };

    create function ext::postgis::minimumclearance(a0: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Returns the minimum clearance of a geometry, a measure of a geometrys robustness.';
        using sql function 'st_minimumclearance';
    };

    create function ext::postgis::minimumclearanceline(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Returns the two-point LineString spanning a geometrys minimum clearance.';
        using sql function 'st_minimumclearanceline';
    };

    create function ext::postgis::centroid(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the geometric center of a geometry.';
        using sql function 'st_centroid';
    };

    create function ext::postgis::centroid(a0: ext::postgis::geography, use_spheroid: std::bool = true) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the geometric center of a geometry.';
        using sql function 'st_centroid';
    };

    create function ext::postgis::centroid(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the geometric center of a geometry.';
        using sql function 'st_centroid';
    };

    create function ext::postgis::geometricmedian(g: optional ext::postgis::geometry, tolerance: optional std::float64 = {}, max_iter: optional std::int64 = 10000, fail_if_not_converged: optional std::bool = false) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, tolerance = NULL, max_iter = 10000, fail_if_not_converged = false - Returns the geometric median of a MultiPoint.';
        set impl_is_strict := false;
        using sql $$SELECT st_geometricmedian("g", "tolerance", "max_iter"::int4, "fail_if_not_converged")$$;
    };

    create function ext::postgis::isring(a0: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Tests if a LineString is closed and simple.';
        using sql function 'st_isring';
    };

    create function ext::postgis::pointonsurface(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Computes a point guaranteed to lie in a polygon, or on a geometry.';
        using sql function 'st_pointonsurface';
    };

    create function ext::postgis::issimple(a0: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Tests if a geometry has no points of self-intersection or self-tangency.';
        using sql function 'st_issimple';
    };

    create function ext::postgis::iscollection(a0: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Tests if a geometry is a geometry collection type.';
        using sql function 'st_iscollection';
    };

    create function ext::postgis::geomfromgml(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geomfromgml("a0", "a1"::int4)$$;
    };

    create function ext::postgis::geomfromgml(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geomfromgml("a0")$$;
    };

    create function ext::postgis::gmltosql(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_gmltosql("a0")$$;
    };

    create function ext::postgis::gmltosql(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_gmltosql("a0", "a1"::int4)$$;
    };

    create function ext::postgis::geomfromkml(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_geomfromkml';
    };

    create function ext::postgis::geomfrommarc21(marc21xml: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_geomfrommarc21';
    };

    create function ext::postgis::asmarc21(geom: ext::postgis::geometry, format: std::str = 'hdddmmss') ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_asmarc21';
    };

    create function ext::postgis::geomfromgeojson(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_geomfromgeojson';
    };

    create function ext::postgis::geomfromgeojson(a0: std::json) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_geomfromgeojson';
    };

    create function ext::postgis::postgis_libjson_version() ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'postgis_libjson_version';
    };

    create function ext::postgis::linefromencodedpolyline(txtin: std::str, nprecision: std::int64 = 5) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_linefromencodedpolyline("txtin", "nprecision"::int4)$$;
    };

    create function ext::postgis::asencodedpolyline(geom: ext::postgis::geometry, nprecision: std::int64 = 5) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asencodedpolyline("geom", "nprecision"::int4)$$;
    };

    create function ext::postgis::assvg(geom: ext::postgis::geometry, rel: std::int64 = 0, maxdecimaldigits: std::int64 = 15) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_assvg("geom", "rel"::int4, "maxdecimaldigits"::int4)$$;
    };

    create function ext::postgis::assvg(geog: ext::postgis::geography, rel: std::int64 = 0, maxdecimaldigits: std::int64 = 15) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_assvg("geog", "rel"::int4, "maxdecimaldigits"::int4)$$;
    };

    create function ext::postgis::assvg(a0: std::str) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_assvg("a0")$$;
    };

    create function ext::postgis::asgml(geom: optional ext::postgis::geometry, maxdecimaldigits: optional std::int64 = 15, options: optional std::int64 = 0) -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql $$SELECT st_asgml("geom", "maxdecimaldigits"::int4, "options"::int4)$$;
    };

    create function ext::postgis::asgml(version: optional std::int64, geom: optional ext::postgis::geometry, maxdecimaldigits: optional std::int64 = 15, options: optional std::int64 = 0, nprefix: optional std::str = {}, id: optional std::str = {}) -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql $$SELECT st_asgml("version"::int4, "geom", "maxdecimaldigits"::int4, "options"::int4, "nprefix", "id")$$;
    };

    create function ext::postgis::asgml(version: std::int64, geog: ext::postgis::geography, maxdecimaldigits: std::int64 = 15, options: std::int64 = 0, nprefix: std::str = 'gml', id: std::str = '') ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asgml("version"::int4, "geog", "maxdecimaldigits"::int4, "options"::int4, "nprefix", "id")$$;
    };

    create function ext::postgis::asgml(geog: ext::postgis::geography, maxdecimaldigits: std::int64 = 15, options: std::int64 = 0, nprefix: std::str = 'gml', id: std::str = '') ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asgml("geog", "maxdecimaldigits"::int4, "options"::int4, "nprefix", "id")$$;
    };

    create function ext::postgis::asgml(a0: std::str) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asgml("a0")$$;
    };

    create function ext::postgis::askml(geom: ext::postgis::geometry, maxdecimaldigits: std::int64 = 15, nprefix: std::str = '') ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_askml("geom", "maxdecimaldigits"::int4, "nprefix")$$;
    };

    create function ext::postgis::askml(geog: ext::postgis::geography, maxdecimaldigits: std::int64 = 15, nprefix: std::str = '') ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_askml("geog", "maxdecimaldigits"::int4, "nprefix")$$;
    };

    create function ext::postgis::askml(a0: std::str) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_askml("a0")$$;
    };

    create function ext::postgis::asgeojson(geom: ext::postgis::geometry, maxdecimaldigits: std::int64 = 9, options: std::int64 = 8) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asgeojson("geom", "maxdecimaldigits"::int4, "options"::int4)$$;
    };

    create function ext::postgis::asgeojson(geog: ext::postgis::geography, maxdecimaldigits: std::int64 = 9, options: std::int64 = 0) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asgeojson("geog", "maxdecimaldigits"::int4, "options"::int4)$$;
    };

    create function ext::postgis::asgeojson(a0: std::str) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_asgeojson("a0")$$;
    };

    create function ext::postgis::json(a0: ext::postgis::geometry) ->  std::json {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'json';
    };

    create function ext::postgis::asmvtgeom(geom: optional ext::postgis::geometry, bounds: optional ext::postgis::box2d, extent: optional std::int64 = 4096, buffer: optional std::int64 = 256, clip_geom: optional std::bool = true) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql $$SELECT st_asmvtgeom("geom", "bounds", "extent"::int4, "buffer"::int4, "clip_geom")$$;
    };

    create function ext::postgis::postgis_libprotobuf_version() ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'postgis_libprotobuf_version';
    };

    create function ext::postgis::geohash(geom: ext::postgis::geometry, maxchars: std::int64 = 0) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geohash("geom", "maxchars"::int4)$$;
    };

    create function ext::postgis::geohash(geog: ext::postgis::geography, maxchars: std::int64 = 0) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geohash("geog", "maxchars"::int4)$$;
    };

    create function ext::postgis::box2dfromgeohash(a0: optional std::str, a1: optional std::int64 = {}) -> optional ext::postgis::box2d {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql $$SELECT st_box2dfromgeohash("a0", "a1"::int4)$$;
    };

    create function ext::postgis::pointfromgeohash(a0: optional std::str, a1: optional std::int64 = {}) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql $$SELECT st_pointfromgeohash("a0", "a1"::int4)$$;
    };

    create function ext::postgis::geomfromgeohash(a0: optional std::str, a1: optional std::int64 = {}) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql $$SELECT st_geomfromgeohash("a0", "a1"::int4)$$;
    };

    create function ext::postgis::numpoints(a0: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Returns the number of points in a LineString or CircularString.';
        using sql function 'st_numpoints';
    };

    create function ext::postgis::numgeometries(a0: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Returns the number of elements in a geometry collection.';
        using sql function 'st_numgeometries';
    };

    create function ext::postgis::geometryn(a0: ext::postgis::geometry, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, n - Return an element of a geometry collection.';
        using sql $$SELECT st_geometryn("a0", "a1"::int4)$$;
    };

    create function ext::postgis::dimension(a0: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Returns the topological dimension of a geometry.';
        using sql function 'st_dimension';
    };

    create function ext::postgis::exteriorring(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_polygon - Returns a LineString representing the exterior ring of a Polygon.';
        using sql function 'st_exteriorring';
    };

    create function ext::postgis::numinteriorrings(a0: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_polygon - Returns the number of interior rings (holes) of a Polygon.';
        using sql function 'st_numinteriorrings';
    };

    create function ext::postgis::numinteriorring(a0: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_polygon - Returns the number of interior rings (holes) of a Polygon. Aias for ST_NumInteriorRings';
        using sql function 'st_numinteriorring';
    };

    create function ext::postgis::interiorringn(a0: ext::postgis::geometry, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_polygon, n - Returns the Nth interior ring (hole) of a Polygon.';
        using sql $$SELECT st_interiorringn("a0", "a1"::int4)$$;
    };

    create function ext::postgis::geometrytype(a0: ext::postgis::geometry) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns the type of a geometry as text.';
        using sql function 'geometrytype';
    };

    create function ext::postgis::geometrytype(a0: ext::postgis::geography) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns the type of a geometry as text.';
        using sql function 'geometrytype';
    };

    create function ext::postgis::pointn(a0: ext::postgis::geometry, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, n - Returns the Nth point in the first LineString or circular LineString in a geometry.';
        using sql $$SELECT st_pointn("a0", "a1"::int4)$$;
    };

    create function ext::postgis::numpatches(a0: ext::postgis::geometry) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1 - Return the number of faces on a Polyhedral Surface. Will return null for non-polyhedral geometries.';
        using sql function 'st_numpatches';
    };

    create function ext::postgis::patchn(a0: ext::postgis::geometry, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA, n - Returns the Nth geometry (face) of a PolyhedralSurface.';
        using sql $$SELECT st_patchn("a0", "a1"::int4)$$;
    };

    create function ext::postgis::startpoint(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Returns the first point of a LineString.';
        using sql function 'st_startpoint';
    };

    create function ext::postgis::endpoint(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Returns the last point of a LineString or CircularLineString.';
        using sql function 'st_endpoint';
    };

    create function ext::postgis::isclosed(a0: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g - Tests if a LineStringss start and end points are coincident. For a PolyhedralSurface tests if it is closed (volumetric).';
        using sql function 'st_isclosed';
    };

    create function ext::postgis::isempty(a0: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Tests if a geometry is empty.';
        using sql function 'st_isempty';
    };

    create function ext::postgis::asbinary(a0: ext::postgis::geometry, a1: std::str) ->  std::bytes {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_asbinary';
    };

    create function ext::postgis::asbinary(a0: ext::postgis::geometry) ->  std::bytes {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_asbinary';
    };

    create function ext::postgis::asbinary(a0: ext::postgis::geography) ->  std::bytes {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_asbinary';
    };

    create function ext::postgis::asbinary(a0: optional ext::postgis::geography, a1: optional std::str) -> optional std::bytes {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'st_asbinary';
    };

    create function ext::postgis::astext(a0: ext::postgis::geometry) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_astext("a0")$$;
    };

    create function ext::postgis::astext(a0: ext::postgis::geometry, a1: std::int64) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_astext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::astext(a0: ext::postgis::geography) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_astext("a0")$$;
    };

    create function ext::postgis::astext(a0: ext::postgis::geography, a1: std::int64) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_astext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::astext(a0: std::str) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_astext("a0")$$;
    };

    create function ext::postgis::geometryfromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geometryfromtext("a0")$$;
    };

    create function ext::postgis::geometryfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geometryfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::geomfromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geomfromtext("a0")$$;
    };

    create function ext::postgis::geomfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geomfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::wkttosql(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_wkttosql';
    };

    create function ext::postgis::pointfromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_pointfromtext("a0")$$;
    };

    create function ext::postgis::pointfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_pointfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::linefromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_linefromtext("a0")$$;
    };

    create function ext::postgis::linefromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_linefromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::polyfromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_polyfromtext("a0")$$;
    };

    create function ext::postgis::polyfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_polyfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::polygonfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_polygonfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::polygonfromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_polygonfromtext("a0")$$;
    };

    create function ext::postgis::mlinefromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mlinefromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::mlinefromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mlinefromtext("a0")$$;
    };

    create function ext::postgis::multilinestringfromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_multilinestringfromtext("a0")$$;
    };

    create function ext::postgis::multilinestringfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_multilinestringfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::mpointfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mpointfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::mpointfromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mpointfromtext("a0")$$;
    };

    create function ext::postgis::multipointfromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_multipointfromtext';
    };

    create function ext::postgis::mpolyfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mpolyfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::mpolyfromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mpolyfromtext("a0")$$;
    };

    create function ext::postgis::multipolygonfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_multipolygonfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::multipolygonfromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_multipolygonfromtext("a0")$$;
    };

    create function ext::postgis::geomcollfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geomcollfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::geomcollfromtext(a0: std::str) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geomcollfromtext("a0")$$;
    };

    create function ext::postgis::geomfromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geomfromwkb("a0")$$;
    };

    create function ext::postgis::geomfromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geomfromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::pointfromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_pointfromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::pointfromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_pointfromwkb("a0")$$;
    };

    create function ext::postgis::linefromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_linefromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::linefromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_linefromwkb("a0")$$;
    };

    create function ext::postgis::linestringfromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_linestringfromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::linestringfromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_linestringfromwkb("a0")$$;
    };

    create function ext::postgis::polyfromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_polyfromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::polyfromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_polyfromwkb("a0")$$;
    };

    create function ext::postgis::polygonfromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_polygonfromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::polygonfromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_polygonfromwkb("a0")$$;
    };

    create function ext::postgis::mpointfromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mpointfromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::mpointfromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mpointfromwkb("a0")$$;
    };

    create function ext::postgis::multipointfromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_multipointfromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::multipointfromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_multipointfromwkb("a0")$$;
    };

    create function ext::postgis::multilinefromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_multilinefromwkb';
    };

    create function ext::postgis::mlinefromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mlinefromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::mlinefromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mlinefromwkb("a0")$$;
    };

    create function ext::postgis::mpolyfromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mpolyfromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::mpolyfromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_mpolyfromwkb("a0")$$;
    };

    create function ext::postgis::multipolyfromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_multipolyfromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::multipolyfromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_multipolyfromwkb("a0")$$;
    };

    create function ext::postgis::geomcollfromwkb(a0: std::bytes, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geomcollfromwkb("a0", "a1"::int4)$$;
    };

    create function ext::postgis::geomcollfromwkb(a0: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_geomcollfromwkb("a0")$$;
    };

    create function ext::postgis::maxdistance(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the 2D largest distance between two geometries in projected units.';
        using sql function 'st_maxdistance';
    };

    create function ext::postgis::closestpoint(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom1, geom2 - Returns the 2D point on g1 that is closest to g2. This is the first point of the shortest line from one geometry to the other.';
        using sql function 'st_closestpoint';
    };

    create function ext::postgis::closestpoint(a0: ext::postgis::geography, a1: ext::postgis::geography, use_spheroid: std::bool = true) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom1, geom2 - Returns the 2D point on g1 that is closest to g2. This is the first point of the shortest line from one geometry to the other.';
        using sql function 'st_closestpoint';
    };

    create function ext::postgis::closestpoint(a0: optional std::str, a1: optional std::str) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom1, geom2 - Returns the 2D point on g1 that is closest to g2. This is the first point of the shortest line from one geometry to the other.';
        set impl_is_strict := false;
        using sql function 'st_closestpoint';
    };

    create function ext::postgis::shortestline(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom1, geom2 - Returns the 2D shortest line between two geometries';
        using sql function 'st_shortestline';
    };

    create function ext::postgis::shortestline(a0: ext::postgis::geography, a1: ext::postgis::geography, use_spheroid: std::bool = true) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom1, geom2 - Returns the 2D shortest line between two geometries';
        using sql function 'st_shortestline';
    };

    create function ext::postgis::shortestline(a0: optional std::str, a1: optional std::str) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom1, geom2 - Returns the 2D shortest line between two geometries';
        set impl_is_strict := false;
        using sql function 'st_shortestline';
    };

    create function ext::postgis::longestline(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the 2D longest line between two geometries.';
        using sql function 'st_longestline';
    };

    create function ext::postgis::flipcoordinates(a0: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom - Returns a version of a geometry with X and Y axis flipped.';
        using sql function 'st_flipcoordinates';
    };

    create function ext::postgis::bdpolyfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_bdpolyfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::bdmpolyfromtext(a0: std::str, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT st_bdmpolyfromtext("a0", "a1"::int4)$$;
    };

    create function ext::postgis::unlockrows(a0: std::str) ->  std::int64 {
        set volatility := 'Volatile';
        set force_return_cast := true;
        create annotation description := 'args: auth_token - Removes all locks held by an authorization token.';
        using sql function 'unlockrows';
    };

    create function ext::postgis::lockrow(a0: std::str, a1: std::str, a2: std::str, a3: std::str) ->  std::int64 {
        set volatility := 'Volatile';
        set force_return_cast := true;
        create annotation description := 'args: a_schema_name, a_table_name, a_row_key, an_auth_token, expire_dt - Sets lock/authorization for a row in a table.';
        using sql function 'lockrow';
    };

    create function ext::postgis::lockrow(a0: std::str, a1: std::str, a2: std::str) ->  std::int64 {
        set volatility := 'Volatile';
        set force_return_cast := true;
        create annotation description := 'args: a_schema_name, a_table_name, a_row_key, an_auth_token, expire_dt - Sets lock/authorization for a row in a table.';
        using sql function 'lockrow';
    };

    create function ext::postgis::addauth(a0: optional std::str) -> optional std::bool {
        set volatility := 'Volatile';
        set force_return_cast := true;
        create annotation description := 'args: auth_token - Adds an authorization token to be used in the current transaction.';
        set impl_is_strict := false;
        using sql function 'addauth';
    };

    create function ext::postgis::checkauth(a0: optional std::str, a1: optional std::str, a2: optional std::str) -> optional std::int64 {
        set volatility := 'Volatile';
        set force_return_cast := true;
        create annotation description := 'args: a_schema_name, a_table_name, a_key_column_name - Creates a trigger on a table to prevent/allow updates and deletes of rows based on authorization token.';
        set impl_is_strict := false;
        using sql function 'checkauth';
    };

    create function ext::postgis::checkauth(a0: optional std::str, a1: optional std::str) -> optional std::int64 {
        set volatility := 'Volatile';
        set force_return_cast := true;
        create annotation description := 'args: a_schema_name, a_table_name, a_key_column_name - Creates a trigger on a table to prevent/allow updates and deletes of rows based on authorization token.';
        set impl_is_strict := false;
        using sql function 'checkauth';
    };

    create function ext::postgis::enablelongtransactions() -> optional std::str {
        set volatility := 'Volatile';
        set force_return_cast := true;
        create annotation description := 'Enables long transaction support.';
        set impl_is_strict := false;
        using sql function 'enablelongtransactions';
    };

    create function ext::postgis::longtransactionsenabled() -> optional std::bool {
        set volatility := 'Volatile';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql function 'longtransactionsenabled';
    };

    create function ext::postgis::disablelongtransactions() -> optional std::str {
        set volatility := 'Volatile';
        set force_return_cast := true;
        create annotation description := 'Disables long transaction support.';
        set impl_is_strict := false;
        using sql function 'disablelongtransactions';
    };

    create function ext::postgis::to_geography(a0: ext::postgis::geography, a1: std::int64, a2: std::bool) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT geography("a0", "a1"::int4, "a2")$$;
    };

    create function ext::postgis::to_geography(a0: std::bytes) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT geography("a0")$$;
    };

    create function ext::postgis::to_geography(a0: ext::postgis::geometry) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT geography("a0")$$;
    };

    create function ext::postgis::geographyfromtext(a0: std::str) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_geographyfromtext';
    };

    create function ext::postgis::geogfromtext(a0: std::str) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_geogfromtext';
    };

    create function ext::postgis::geogfromwkb(a0: std::bytes) ->  ext::postgis::geography {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_geogfromwkb';
    };

    create function ext::postgis::postgis_typmod_dims(a0: std::int64) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT postgis_typmod_dims("a0"::int4)$$;
    };

    create function ext::postgis::postgis_typmod_srid(a0: std::int64) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT postgis_typmod_srid("a0"::int4)$$;
    };

    create function ext::postgis::postgis_typmod_type(a0: std::int64) ->  std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql $$SELECT postgis_typmod_type("a0"::int4)$$;
    };

    create function ext::postgis::geography_cmp(a0: ext::postgis::geography, a1: ext::postgis::geography) ->  std::int64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'geography_cmp';
    };

    create function ext::postgis::distancesphere(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomlonlatA, geomlonlatB, radius=6371008 - Returns minimum distance in meters between two lon/lat geometries using a spherical earth model.';
        using sql function 'st_distancesphere';
    };

    create function ext::postgis::distancesphere(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry, radius: std::float64) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomlonlatA, geomlonlatB, radius=6371008 - Returns minimum distance in meters between two lon/lat geometries using a spherical earth model.';
        using sql function 'st_distancesphere';
    };

    create function ext::postgis::postgis_constraint_srid(geomschema: std::str, geomtable: std::str, geomcolumn: std::str) ->  std::int64 {
        set volatility := 'Stable';
        set force_return_cast := true;
        using sql function 'postgis_constraint_srid';
    };

    create function ext::postgis::postgis_constraint_dims(geomschema: std::str, geomtable: std::str, geomcolumn: std::str) ->  std::int64 {
        set volatility := 'Stable';
        set force_return_cast := true;
        using sql function 'postgis_constraint_dims';
    };

    create function ext::postgis::distance3d(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the 3D cartesian minimum distance (based on spatial ref) between two geometries in projected units.';
        using sql function 'st_3ddistance';
    };

    create function ext::postgis::maxdistance3d(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the 3D cartesian maximum distance (based on spatial ref) between two geometries in projected units.';
        using sql function 'st_3dmaxdistance';
    };

    create function ext::postgis::closestpoint3d(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the 3D point on g1 that is closest to g2. This is the first point of the 3D shortest line.';
        using sql function 'st_3dclosestpoint';
    };

    create function ext::postgis::shortestline3d(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the 3D shortest line between two geometries';
        using sql function 'st_3dshortestline';
    };

    create function ext::postgis::longestline3d(geom1: ext::postgis::geometry, geom2: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: g1, g2 - Returns the 3D longest line between two geometries';
        using sql function 'st_3dlongestline';
    };

    create function ext::postgis::coorddim(geometry: ext::postgis::geometry) ->  std::int16 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Return the coordinate dimension of a geometry.';
        using sql function 'st_coorddim';
    };

    create function ext::postgis::curvetoline(geom: ext::postgis::geometry, tol: std::float64 = 32, toltype: std::int64 = 0, flags: std::int64 = 0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: curveGeom, tolerance, tolerance_type, flags - Converts a geometry containing curves to a linear geometry.';
        using sql $$SELECT st_curvetoline("geom", "tol", "toltype"::int4, "flags"::int4)$$;
    };

    create function ext::postgis::hasarc(geometry: ext::postgis::geometry) ->  std::bool {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomA - Tests if a geometry contains a circular arc';
        using sql function 'st_hasarc';
    };

    create function ext::postgis::linetocurve(geometry: ext::postgis::geometry) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geomANoncircular - Converts a linear geometry to a curved geometry.';
        using sql function 'st_linetocurve';
    };

    create function ext::postgis::point(a0: std::float64, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: x, y - Creates a Point with X, Y and SRID values.';
        using sql $$SELECT st_point("a0", "a1")$$;
    };

    create function ext::postgis::point(a0: std::float64, a1: std::float64, srid: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: x, y - Creates a Point with X, Y and SRID values.';
        using sql $$SELECT st_point("a0", "a1", "srid"::int4)$$;
    };

    create function ext::postgis::pointz(xcoordinate: std::float64, ycoordinate: std::float64, zcoordinate: std::float64, srid: std::int64 = 0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: x, y, z, srid=unknown - Creates a Point with X, Y, Z and SRID values.';
        using sql $$SELECT st_pointz("xcoordinate", "ycoordinate", "zcoordinate", "srid"::int4)$$;
    };

    create function ext::postgis::pointm(xcoordinate: std::float64, ycoordinate: std::float64, mcoordinate: std::float64, srid: std::int64 = 0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: x, y, m, srid=unknown - Creates a Point with X, Y, M and SRID values.';
        using sql $$SELECT st_pointm("xcoordinate", "ycoordinate", "mcoordinate", "srid"::int4)$$;
    };

    create function ext::postgis::pointzm(xcoordinate: std::float64, ycoordinate: std::float64, zcoordinate: std::float64, mcoordinate: std::float64, srid: std::int64 = 0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: x, y, z, m, srid=unknown - Creates a Point with X, Y, Z, M and SRID values.';
        using sql $$SELECT st_pointzm("xcoordinate", "ycoordinate", "zcoordinate", "mcoordinate", "srid"::int4)$$;
    };

    create function ext::postgis::polygon(a0: ext::postgis::geometry, a1: std::int64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: lineString, srid - Creates a Polygon from a LineString with a specified SRID.';
        using sql $$SELECT st_polygon("a0", "a1"::int4)$$;
    };

    create function ext::postgis::wkbtosql(wkb: std::bytes) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        using sql function 'st_wkbtosql';
    };

    create function ext::postgis::locatebetween(geometry: ext::postgis::geometry, frommeasure: std::float64, tomeasure: std::float64, leftrightoffset: std::float64 = 0.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, measure_start, measure_end, offset = 0 - Returns the portions of a geometry that match a measure range.';
        using sql function 'st_locatebetween';
    };

    create function ext::postgis::locatealong(geometry: ext::postgis::geometry, measure: std::float64, leftrightoffset: std::float64 = 0.0) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom_with_measure, measure, offset = 0 - Returns the point(s) on a geometry that match a measure value.';
        using sql function 'st_locatealong';
    };

    create function ext::postgis::locatebetweenelevations(geometry: ext::postgis::geometry, fromelevation: std::float64, toelevation: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: geom, elevation_start, elevation_end - Returns the portions of a geometry that lie in an elevation (Z) range.';
        using sql function 'st_locatebetweenelevations';
    };

    create function ext::postgis::interpolatepoint(line: ext::postgis::geometry, point: ext::postgis::geometry) ->  std::float64 {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: linear_geom_with_measure, point - Returns the interpolated measure of a geometry closest to a point.';
        using sql function 'st_interpolatepoint';
    };

    create function ext::postgis::hexagon(size: std::float64, cell_i: std::int64, cell_j: std::int64, origin: ext::postgis::geometry = 'POINT(0 0)') ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: size, cell_i, cell_j, origin - Returns a single hexagon, using the provided edge size and cell coordinate within the hexagon grid space.';
        using sql $$SELECT st_hexagon("size", "cell_i"::int4, "cell_j"::int4, "origin")$$;
    };

    create function ext::postgis::square(size: std::float64, cell_i: std::int64, cell_j: std::int64, origin: ext::postgis::geometry = 'POINT(0 0)') ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: size, cell_i, cell_j, origin - Returns a single square, using the provided edge size and cell coordinate within the square grid space.';
        using sql $$SELECT st_square("size", "cell_i"::int4, "cell_j"::int4, "origin")$$;
    };

    create function ext::postgis::simplifypolygonhull(geom: ext::postgis::geometry, vertex_fraction: std::float64, is_outer: std::bool = true) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: param_geom, vertex_fraction, is_outer = true - Computes a simplifed topology-preserving outer or inner hull of a polygonal geometry.';
        using sql function 'st_simplifypolygonhull';
    };

    create function ext::postgis::concavehull(param_geom: ext::postgis::geometry, param_pctconvex: std::float64, param_allow_holes: std::bool = false) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: param_geom, param_pctconvex, param_allow_holes = false - Computes a possibly concave geometry that contains all input geometry vertices';
        using sql function 'st_concavehull';
    };

    create function ext::postgis::asx3d(geom: optional ext::postgis::geometry, maxdecimaldigits: optional std::int64 = 15, options: optional std::int64 = 0) -> optional std::str {
        set volatility := 'Immutable';
        set force_return_cast := true;
        set impl_is_strict := false;
        using sql $$SELECT st_asx3d("geom", "maxdecimaldigits"::int4, "options"::int4)$$;
    };

    create function ext::postgis::lineinterpolatepoint3d(a0: ext::postgis::geometry, a1: std::float64) ->  ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args: a_linestring, a_fraction - Returns a point interpolated along a 3D line at a fractional location.';
        using sql function 'st_3dlineinterpolatepoint';
    };

    create function ext::postgis::letters(letters: optional std::str, font: optional std::json = {}) -> optional ext::postgis::geometry {
        set volatility := 'Immutable';
        set force_return_cast := true;
        create annotation description := 'args:  letters,  font - Returns the input letters rendered as geometry with a default start position at the origin and default text height of 100.';
        set impl_is_strict := false;
        using sql function 'st_letters';
    };
};