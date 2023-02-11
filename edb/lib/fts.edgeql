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

CREATE ABSTRACT INDEX fts::textsearch(named only language: str) {
    CREATE ANNOTATION std::description :=
        "Full-text search index based on the Postgres's GIN index.";
};

## Functions
## ---------

CREATE FUNCTION
fts::test(doc: std::str, query: std::str) -> std::bool
{
    CREATE ANNOTATION std::description :=
        'Return true if the document matches the FTS query.';
    SET volatility := 'Immutable';
    USING SQL $$
    SELECT "doc" @@ edgedb.fts_parse_query("query")
    $$;
};


CREATE FUNCTION
fts::match_rank(
    doc: std::str,
    query: std::str,
    language: str,
) -> std::float64
{
    CREATE ANNOTATION std::description :=
        'Return just the rank of the document given the FTS query.';
    SET volatility := 'Stable';
    USING SQL $$
    SELECT ts_rank(
        to_tsvector("language"::regconfig, "doc"),
        edgedb.fts_parse_query("query")
    )
    $$;
};


CREATE FUNCTION
fts::highlight_match(
    doc: std::str,
    query: std::str,
    language: str,
) -> std::str
{
    CREATE ANNOTATION std::description :=
        'Return just the part of the document matching the FTS query.';
    SET volatility := 'Stable';
    USING SQL $$
    SELECT ts_headline(
        "language"::regconfig,
        "doc",
        edgedb.fts_parse_query("query")
    )
    $$;
};
