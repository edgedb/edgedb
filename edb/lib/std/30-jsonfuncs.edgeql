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


## JSON functions and operators.


CREATE FUNCTION
std::json_typeof(json: std::json) -> std::str
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'jsonb_typeof';
};


CREATE FUNCTION
std::json_array_unpack(array: std::json) -> SET OF std::json
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'jsonb_array_elements';
};


CREATE FUNCTION
std::json_object_unpack(obj: std::json) -> SET OF tuple<std::str, std::json>
{
    SET volatility := 'IMMUTABLE';
    USING SQL FUNCTION 'jsonb_each';
    # jsonb_each is defined as (jsonb, OUT key text, OUT value jsonb),
    # and, quite perprexingly, would reject a column definition list
    # with `a column definition list is only allowed for functions
    # returning "record"`, even though it _is_ returning "record".
    # Hence, we need this flag to tell the compiler to avoid generating
    # a coldeflist for this function.
    SET sql_func_has_out_params := True;
};


CREATE FUNCTION
std::json_get(
    json: std::json,
    VARIADIC path: std::str,
    NAMED ONLY default: OPTIONAL std::json={}) -> OPTIONAL std::json
{
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT COALESCE(
        jsonb_extract_path("json", VARIADIC "path"),
        "default"
    )
    $$;
};


CREATE INFIX OPERATOR
std::`=` (l: std::json, r: std::json) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::json, r: OPTIONAL std::json) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::json, r: std::json) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::json, r: OPTIONAL std::json) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>=` (l: std::json, r: std::json) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: std::json, r: std::json) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::json, r: std::json) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::json, r: std::json) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '<';
};


## CASTS

# This is only a container cast, and subject to element type cast
# availability.
CREATE CAST FROM array<anytype> TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


# This is only a container cast, and subject to element type cast
# availability.
CREATE CAST FROM anytuple TO std::json {
    SET volatility := 'STABLE';
    USING SQL EXPRESSION;
};


CREATE CAST FROM std::json TO array<json> {
    SET volatility := 'IMMUTABLE';
    USING SQL $$
        SELECT array_agg(j)
        FROM jsonb_array_elements(val) AS j
    $$;
};


# The function to_jsonb is STABLE in PostgreSQL, but this function is
# generic and STABLE volatility may be an overestimation in many cases.
CREATE CAST FROM std::bool TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::uuid TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::str TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::datetime TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::duration TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::int16 TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::int32 TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::int64 TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::float32 TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::float64 TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::decimal TO std::json {
    SET volatility := 'STABLE';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::json TO std::bool  {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'boolean')::bool;
    $$;
};


CREATE CAST FROM std::json TO std::uuid {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'string')::uuid;
    $$;
};


CREATE CAST FROM std::json TO std::str {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'string');
    $$;
};


CREATE CAST FROM std::json TO std::datetime {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.datetime_in(edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO std::duration {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.duration_in(edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO std::int16 {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'number')::int2;
    $$;
};


CREATE CAST FROM std::json TO std::int32 {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'number')::int4;
    $$;
};


CREATE CAST FROM std::json TO std::int64 {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'number')::int8;
    $$;
};


CREATE CAST FROM std::json TO std::float32 {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'number')::float4;
    $$;
};


CREATE CAST FROM std::json TO std::float64 {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'number')::float8;
    $$;
};


CREATE CAST FROM std::json TO std::decimal {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.str_to_decimal(
        edgedb.jsonb_extract_scalar(val, 'number')
    );
    $$;
};


CREATE CAST FROM std::json TO std::bigint {
    SET volatility := 'STABLE';
    USING SQL $$
    SELECT edgedb.str_to_bigint(
        edgedb.jsonb_extract_scalar(val, 'number')
    );
    $$;
};
