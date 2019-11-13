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
    SET volatility := 'IMMUTABLE';
    USING SQL $$
    SELECT get_bit("bytes", "num"::int)::bigint
    $$;
};



## Byte string operators
## ---------------------

CREATE INFIX OPERATOR
std::`=` (l: std::bytes, r: std::bytes) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'=';
};


CREATE INFIX OPERATOR
std::`?=` (l: OPTIONAL std::bytes, r: OPTIONAL std::bytes) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`!=` (l: std::bytes, r: std::bytes) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'<>';
};


CREATE INFIX OPERATOR
std::`?!=` (l: OPTIONAL std::bytes, r: OPTIONAL std::bytes) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL EXPRESSION;
};


CREATE INFIX OPERATOR
std::`++` (l: std::bytes, r: std::bytes) -> std::bytes {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR r'||';
};


CREATE INFIX OPERATOR
std::`>=` (l: std::bytes, r: std::bytes) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '>=';
};


CREATE INFIX OPERATOR
std::`>` (l: std::bytes, r: std::bytes) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '>';
};


CREATE INFIX OPERATOR
std::`<=` (l: std::bytes, r: std::bytes) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '<=';
};


CREATE INFIX OPERATOR
std::`<` (l: std::bytes, r: std::bytes) -> std::bool {
    SET volatility := 'IMMUTABLE';
    USING SQL OPERATOR '<';
};
