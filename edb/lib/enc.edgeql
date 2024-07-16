#
# This source file is part of the EdgeDB open source project.
#
# Copyright EdgeDB Inc. and the EdgeDB authors.
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


CREATE MODULE std::enc;


CREATE SCALAR TYPE
std::enc::Base64Alphabet EXTENDING enum<standard, urlsafe>;


CREATE FUNCTION
std::enc::base64_encode(
    data: std::bytes,
    NAMED ONLY alphabet: std::enc::Base64Alphabet =
        std::enc::Base64Alphabet.standard,
    NAMED ONLY padding: std::bool = true,
) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Encode given data as a base64 string';
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT
            CASE
            WHEN "alphabet" = 'standard' AND "padding" THEN
                pg_catalog.translate(
                    pg_catalog.encode("data", 'base64'),
                    E'\n',
                    ''
                )
            WHEN "alphabet" = 'standard' AND NOT "padding" THEN
                pg_catalog.translate(
                    pg_catalog.rtrim(
                        pg_catalog.encode("data", 'base64'),
                        '='
                    ),
                    E'\n',
                    ''
                )
            WHEN "alphabet" = 'urlsafe' AND "padding" THEN
                pg_catalog.translate(
                    pg_catalog.encode("data", 'base64'),
                    E'+/\n',
                    '-_'
                )
            WHEN "alphabet" = 'urlsafe' AND NOT "padding" THEN
                pg_catalog.translate(
                    pg_catalog.rtrim(
                        pg_catalog.encode("data", 'base64'),
                        '='
                    ),
                    E'+/\n',
                    '-_'
                )
            ELSE
                edgedb_VER.raise(
                    NULL::text,
                    'invalid_parameter_value',
                    msg => (
                        'invalid alphabet for std::enc::base64_encode: '
                        || pg_catalog.quote_literal("alphabet")
                    ),
                    detail => (
                        '{"hint":"Supported alphabets: standard, urlsafe."}'
                    )
                )
            END
    $$;
};


CREATE FUNCTION
std::enc::base64_decode(
    data: std::str,
    NAMED ONLY alphabet: std::enc::Base64Alphabet =
        std::enc::Base64Alphabet.standard,
    NAMED ONLY padding: std::bool = true,
) -> std::bytes
{
    CREATE ANNOTATION std::description :=
        'Decode the byte64-encoded byte string and return decoded bytes.';
    SET volatility := 'Immutable';
    USING SQL $$
        SELECT
            CASE
            WHEN "alphabet" = 'standard' AND "padding" THEN
                pg_catalog.decode("data", 'base64')
            WHEN "alphabet" = 'standard' AND NOT "padding" THEN
                pg_catalog.decode(
                    edgedb_VER.pad_base64_string("data"),
                    'base64'
                )
            WHEN "alphabet" = 'urlsafe' AND "padding" THEN
                pg_catalog.decode(
                    pg_catalog.translate("data", '-_', '+/'),
                    'base64'
                )
            WHEN "alphabet" = 'urlsafe' AND NOT "padding" THEN
                pg_catalog.decode(
                    edgedb_VER.pad_base64_string(
                        pg_catalog.translate("data", '-_', '+/')
                    ),
                    'base64'
                )
            ELSE
                edgedb_VER.raise(
                    NULL::bytea,
                    'invalid_parameter_value',
                    msg => (
                        'invalid alphabet for std::enc::base64_decode: '
                        || pg_catalog.quote_literal("alphabet")
                    ),
                    detail => (
                        '{"hint":"Supported alphabets: standard, urlsafe."}'
                    )
                )
            END
    $$;
};
