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


## Byte string functions
## ---------------------

CREATE FUNCTION
std::bytes_get_bit(bytes: std::bytes, num: int64) -> std::int64
{
    CREATE ANNOTATION std::description :=
        'Get the *nth* bit of the *bytes* value.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT get_bit("bytes", "num"::int)::bigint
    $$;
};


CREATE FUNCTION
std::bytes_convert_from(bytes: std::bytes, src_encoding: std::str) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Convert *bytes* to a string using *src_encoding*';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'convert_from';
};


CREATE FUNCTION
std::bytes_convert_to(str: std::str, dest_encoding: std::str) -> std::bytes
{
    CREATE ANNOTATION std::description :=
        'Convert *str* to bytes using *dest_encoding*';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'convert_to';
};


CREATE FUNCTION
std::bytes_encode(bytes: std::bytes, format: std::str) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Encode *bytes* into a textual representation';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'encode';
};


CREATE FUNCTION
std::bytes_decode(str: std::str, format: std::str) -> std::bytes
{
    CREATE ANNOTATION std::description :=
        'Decode a textual representation of a binary string';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'decode';
};


CREATE FUNCTION
std::sha224(bytes: std::bytes) -> std::bytes
{
    CREATE ANNOTATION std::description :=
        'Compute the SHA-224 hash of *bytes*';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'sha224';
};


CREATE FUNCTION
std::sha256(bytes: std::bytes) -> std::bytes
{
    CREATE ANNOTATION std::description :=
        'Compute the SHA-256 hash of *bytes*';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'sha256';
};


CREATE FUNCTION
std::sha384(bytes: std::bytes) -> std::bytes
{
    CREATE ANNOTATION std::description :=
        'Compute the SHA-384 hash of *bytes*';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'sha384';
};



CREATE FUNCTION
std::sha512(bytes: std::bytes) -> std::bytes
{
    CREATE ANNOTATION std::description :=
        'Compute the SHA-512 hash of *bytes*';
    SET volatility := 'Immutable';
    USING SQL FUNCTION 'sha512';
};


## Byte string operators
## ---------------------

CREATE INFIX OPERATOR
std::`=` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'eq';
    CREATE ANNOTATION std::description := 'Compare two values for equality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::=';
    SET negator := 'std::!=';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::bytes, r: OPTIONAL std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_eq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for equality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'neq';
    CREATE ANNOTATION std::description := 'Compare two values for inequality.';
    SET volatility := 'Immutable';
    SET commutator := 'std::!=';
    SET negator := 'std::=';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::bytes, r: OPTIONAL std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'coal_neq';
    CREATE ANNOTATION std::description :=
        'Compare two (potentially empty) values for inequality.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`++` (l: std::bytes, r: std::bytes) -> std::bytes {
    CREATE ANNOTATION std::identifier := 'concat';
    CREATE ANNOTATION std::description := 'Bytes concatenation.';
    SET volatility := 'Immutable';
    USING SQL OPERATOR r'||';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gte';
    CREATE ANNOTATION std::description := 'Greater than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<=';
    SET negator := 'std::<';
    USING SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'gt';
    CREATE ANNOTATION std::description := 'Greater than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::<';
    SET negator := 'std::<=';
    USING SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lte';
    CREATE ANNOTATION std::description := 'Less than or equal.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>=';
    SET negator := 'std::>';
    USING SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::bytes, r: std::bytes) -> std::bool {
    CREATE ANNOTATION std::identifier := 'lt';
    CREATE ANNOTATION std::description := 'Less than.';
    SET volatility := 'Immutable';
    SET commutator := 'std::>';
    SET negator := 'std::>=';
    USING SQL OPERATOR '<';
};

CREATE INFIX OPERATOR
std::`[]` (l: std::bytes, r: std::int64) -> std::bytes {
    CREATE ANNOTATION std::identifier := 'index';
    CREATE ANNOTATION std::description := 'Bytes indexing.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};

CREATE INFIX OPERATOR
std::`[]` (l: std::bytes, r: tuple<std::int64, std::int64>) -> std::bytes {
    CREATE ANNOTATION std::identifier := 'slice';
    CREATE ANNOTATION std::description := 'Bytes slicing.';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};
