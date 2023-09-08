#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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


CREATE MODULE fts;

CREATE ABSTRACT INDEX fts::textsearch(named only language: std::str) {
    CREATE ANNOTATION std::description :=
        "Full-text search index based on the Postgres's GIN index.";
    SET code := 'gin (to_tsvector(__kw_language__, __col__))';
};

CREATE SCALAR TYPE fts::searchable_str {
    SET transient := true;
};

## Functions
## ---------

CREATE FUNCTION
fts::test(
    query: std::str,
    variadic doc: optional std::str,
    named only language: std::str,
) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'Return true if the document matches the FTS query.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT
        to_tsvector("language"::regconfig, array_to_string("doc", ' ')) @@
        edgedb.fts_parse_query("query", "language"::regconfig)
    $$;
};


CREATE FUNCTION
fts::match_rank(
    query: std::str,
    variadic doc: optional std::str,
    named only language: std::str,
) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return just the rank of the document given the FTS query.';
    SET volatility := 'Stable';
    USING SQL $$
    SELECT ts_rank(
        to_tsvector("language"::regconfig, array_to_string("doc", ' ')),
        edgedb.fts_parse_query("query")
    )
    $$;
};


CREATE FUNCTION
fts::highlight_match(
    query: std::str,
    variadic doc: optional std::str,
    named only language: std::str,
) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return just the part of the document matching the FTS query.';
    SET volatility := 'Stable';
    USING SQL $$
    SELECT ts_headline(
        "language"::regconfig,
        array_to_string("doc", ' '),
        edgedb.fts_parse_query("query")
    )
    $$;
};


CREATE FUNCTION
fts::match(
    query: std::str,
    variadic doc: optional std::str,
    named only language: std::str,
    named only rank_opts: optional std::str = 'default',
    named only weights: optional array<std::float64> = {},
    named only highlight_opts: optional std::str = {},
) -> tuple<rank: std::float64, highlights: array<std::str>>
{
    CREATE ANNOTATION std::description :=
        'Return the parts of the document given the FTS query and their \
        ranks.';
    SET volatility := 'Stable';
    USING SQL $$
    SELECT
        (ts.rank)::float8,
        (
            CASE WHEN highlight_opts IS NULL THEN ARRAY[]::text[]
            ELSE ARRAY[ts.hl] END
        )
    FROM
        (
            SELECT
                CASE WHEN rank_opts = 'default' THEN
                    ts_rank(
                        edgedb.fts_normalize_weights(weights),
                        edgedb.fts_normalize_doc(doc, weights, data.lang),
                        data.q
                    )
                ELSE
                    0
                END
                AS rank,

                CASE WHEN highlight_opts = 'default' THEN
                    ts_headline(
                        data.lang, data.d, data.q
                    )
                ELSE
                    ts_headline(
                        data.lang, data.d, data.q, highlight_opts
                    )
                END
                AS hl
            FROM
                (
                    SELECT
                        "language"::regconfig as lang,
                        array_to_string("doc", ' ') as d,
                        edgedb.fts_parse_query("query") as q
                ) AS data
        ) AS ts
    $$;
};
