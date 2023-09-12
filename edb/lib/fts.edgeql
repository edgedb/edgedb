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

CREATE ABSTRACT INDEX fts::textsearch {
    CREATE ANNOTATION std::description :=
        "Full-text search index based on the Postgres's GIN index.";
    SET code := ''; # overridden by special case
};

CREATE SCALAR TYPE fts::searchable_str {
    SET transient := true;
};

CREATE FUNCTION fts::with_language(
    text: std::str,
    language: anyenum,
) -> fts::searchable_str {
    CREATE ANNOTATION std::description :=
        'Adds language information to a string';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};

CREATE FUNCTION fts::search(
    object: anyobject,
    query: std::str,
    named only language: std::str,
) -> optional tuple<
    object: anyobject,
    score: float32,
>
{
    CREATE ANNOTATION std::description := '
        Search an object using its fts::textsearch index.
        Returns objects that match the specified query and the matching score
    ';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};

CREATE SCALAR TYPE fts::PGLanguage
    EXTENDING enum<
        Simple,
        Arabic,
        Armenian,
        Basque,
        Catalan,
        Danish,
        Dutch,
        English,
        Finnish,
        French,
        German,
        Greek,
        Hindi,
        Hungarian,
        Indonesian,
        Irish,
        Italian,
        Lithuanian,
        Nepali,
        Norwegian,
        Portuguese,
        Romanian,
        Russian,
        Serbian,
        Spanish,
        Swedish,
        Tamil,
        Turkish,
        Yiddish,
    > {
    CREATE ANNOTATION std::description :=
        'Languages supported by PostgreSQL FTS';
};

CREATE SCALAR TYPE fts::ElasticLanguage
    EXTENDING enum<
        Arabic,
        Armenian,
        Basque,
        Brazilian,
        Bulgarian,
        Catalan,
        Chinese,
        Cjk,
        Czech,
        Danish,
        Dutch,
        English,
        Finnish,
        French,
        Galician,
        German,
        Greek,
        Hindi,
        Hungarian,
        Indonesian,
        Irish,
        Italian,
        Latvian,
        Norwegian,
        Persian,
        Portuguese,
        Romanian,
        Russian,
        Sorani,
        Spanish,
        Swedish,
        Turkish,
        Thai,
    > {
    CREATE ANNOTATION std::description :=
        'Languages supported by ElasticSearch';
};

CREATE SCALAR TYPE fts::Language
    EXTENDING enum<
        Arabic,
        Armenian,
        Basque,
        Catalan,
        Danish,
        Dutch,
        English,
        Finnish,
        French,
        German,
        Greek,
        Hindi,
        Hungarian,
        Indonesian,
        Irish,
        Italian,
        Norwegian,
        Portuguese,
        Romanian,
        Russian,
        Spanish,
        Swedish,
        Turkish,
    > {
    CREATE ANNOTATION std::description :=
        'Languages supported by both PostgreSQL FTS and ElasticSearch';
};
