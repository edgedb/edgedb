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
    CREATE ANNOTATION std::description :=
        'Return the type of the outermost JSON value as a string.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'jsonb_typeof';
};


CREATE FUNCTION
std::json_array_unpack(array: std::json) -> SET OF std::json
{
    CREATE ANNOTATION std::description :=
        'Return elements of JSON array as a set of `json`.';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'jsonb_array_elements';
};


CREATE FUNCTION
std::json_object_unpack(obj: std::json) -> SET OF tuple<std::str, std::json>
{
    CREATE ANNOTATION std::description :=
        'Return set of key/value tuples that make up the JSON object.';
    SET volatility := 'Immutable';
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
    CREATE ANNOTATION std::description :=
        'Return the JSON value at the end of the specified path or an empty set.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT COALESCE(
        jsonb_extract_path("json", VARIADIC "path"),
        "default"
    )
    $$;
};


CREATE INFIX OPERATOR
std::`=` (l: std::json, r: std::json) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::json, r: OPTIONAL std::json) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::json, r: std::json) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::json, r: OPTIONAL std::json) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`>=` (l: std::json, r: std::json) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: std::json, r: std::json) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::json, r: std::json) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::json, r: std::json) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<';
};

CREATE INFIX OPERATOR
std::`[]` (l: std::json, r: std::int64) -> std::json {
    CREATE ANNOTATION std::identifier := 'index';
    CREATE ANNOTATION std::description := 'JSON array/string indexing.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};

CREATE INFIX OPERATOR
std::`[]` (l: std::json, r: tuple<std::int64, std::int64>) -> std::json {
    CREATE ANNOTATION std::identifier := 'slice';
    CREATE ANNOTATION std::description := 'JSON array/string slicing.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};

CREATE INFIX OPERATOR
std::`[]` (l: std::json, r: std::str) -> std::json {
    CREATE ANNOTATION std::identifier := 'destructure';
    CREATE ANNOTATION std::description := 'JSON object property access.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};

CREATE INFIX OPERATOR
std::`++` (l: std::json, r: std::json) -> std::json {
    CREATE ANNOTATION std::identifier := 'concatenate';
    CREATE ANNOTATION std::description := 'Concatenate two JSON values into a new JSON value.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR '||';
};

## CASTS

# This is only a container cast, and subject to element type cast
# availability.
CREATE CAST FROM array<anytype> TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


# This is only a container cast, and subject to element type cast
# availability.
CREATE CAST FROM anytuple TO std::json {
    SET volatility := 'Stable';
    USING SQL EXPRESSION;
};


CREATE CAST FROM std::json TO anytuple {
    SET volatility := 'Stable';
    USING SQL EXPRESSION;
};


CREATE CAST FROM std::json TO array<json> {
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT array_agg(j)
        FROM jsonb_array_elements(val) AS j
    $$;
};


CREATE CAST FROM std::json TO array<anytype> {
    SET volatility := 'Stable';
    USING SQL EXPRESSION;
};


# The function to_jsonb is STABLE in PostgreSQL, but this function is
# generic and STABLE volatility may be an overestimation in many cases.
CREATE CAST FROM std::bool TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::uuid TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::str TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::datetime TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::duration TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::int16 TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::int32 TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::int64 TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::float32 TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::float64 TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::decimal TO std::json {
    SET volatility := 'Stable';
    USING SQL FUNCTION 'to_jsonb';
};


CREATE CAST FROM std::json TO std::bool  {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'boolean')::bool;
    $$;
};


CREATE CAST FROM std::json TO std::uuid {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'string')::uuid;
    $$;
};


CREATE CAST FROM std::json TO std::str {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'string');
    $$;
};


CREATE CAST FROM std::json TO std::datetime {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.datetime_in(edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO std::duration {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.duration_in(edgedb.jsonb_extract_scalar(val, 'string'));
    $$;
};


CREATE CAST FROM std::json TO std::int16 {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'number')::int2;
    $$;
};


CREATE CAST FROM std::json TO std::int32 {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'number')::int4;
    $$;
};


CREATE CAST FROM std::json TO std::int64 {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'number')::int8;
    $$;
};


CREATE CAST FROM std::json TO std::float32 {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'number')::float4;
    $$;
};


CREATE CAST FROM std::json TO std::float64 {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.jsonb_extract_scalar(val, 'number')::float8;
    $$;
};


CREATE CAST FROM std::json TO std::decimal {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.str_to_decimal(
        edgedb.jsonb_extract_scalar(val, 'number')
    );
    $$;
};


CREATE CAST FROM std::json TO std::bigint {
    SET volatility := 'Stable';
    USING SQL $$
    SELECT edgedb.str_to_bigint(
        edgedb.jsonb_extract_scalar(val, 'number')
    );
    $$;
};
