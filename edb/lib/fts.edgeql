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


CREATE MODULE std::fts;

CREATE SCALAR TYPE std::fts::Language
    EXTENDING enum<
        ara,
        hye,
        eus,
        cat,
        dan,
        nld,
        eng,
        fin,
        fra,
        deu,
        ell,
        hin,
        hun,
        ind,
        gle,
        ita,
        nor,
        por,
        ron,
        rus,
        spa,
        swe,
        tur,
    > {
    CREATE ANNOTATION std::description := '
        Languages supported by PostgreSQL FTS, ElasticSearch and Apache Lucene.
        Names are ISO 639-3 language identifiers.
    ';
};

CREATE SCALAR TYPE std::fts::Weight EXTENDING enum<A, B, C, D> {
    CREATE ANNOTATION std::description := "
        Weight category.
        Weight values for each category can be provided in std::fts::search.
    ";
};

CREATE ABSTRACT INDEX std::fts::index {
    CREATE ANNOTATION std::description :=
        "Full-text search index based on the Postgres's GIN index.";
    SET code := ''; # overridden by a special case
};

CREATE SCALAR TYPE std::fts::document {
    SET transient := true;
};

create index match for std::fts::document using std::fts::index;

CREATE FUNCTION std::fts::with_options(
    text: std::str,
    NAMED ONLY language: anyenum,
    NAMED ONLY weight_category: optional std::fts::Weight = std::fts::Weight.A,
) -> std::fts::document {
    CREATE ANNOTATION std::description := '
        Adds language and weight category information to a string,
        so it be indexed with std::fts::index.
    ';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};

CREATE FUNCTION std::fts::search(
    object: anyobject,
    query: std::str,
    named only language: std::str = <std::str>std::fts::Language.eng,
    named only weights: optional array<float64> = {},
) -> optional tuple<object: anyobject, score: float32>
{
    CREATE ANNOTATION std::description := '
        Search an object using its std::fts::index index.
        Returns objects that match the specified query and the matching score.
    ';
    SET volatility := 'Stable';
    USING SQL EXPRESSION;
};

CREATE SCALAR TYPE std::fts::PGLanguage
    EXTENDING enum<
        xxx_simple,
        ara,
        hye,
        eus,
        cat,
        dan,
        nld,
        eng,
        fin,
        fra,
        deu,
        ell,
        hin,
        hun,
        ind,
        gle,
        ita,
        lit,
        npi,
        nor,
        por,
        ron,
        rus,
        srp,
        spa,
        swe,
        tam,
        tur,
        yid,
    > {
    CREATE ANNOTATION std::description :='
        Languages supported by PostgreSQL FTS.
        Names are ISO 639-3 language identifiers or Postgres regconfig names
        prefixed with `xxx_`.
    ';
};

CREATE SCALAR TYPE std::fts::ElasticLanguage
    EXTENDING enum<
        ara,
        bul,
        cat,
        ces,
        ckb,
        dan,
        deu,
        ell,
        eng,
        eus,
        fas,
        fin,
        fra,
        gle,
        glg,
        hin,
        hun,
        hye,
        ind,
        ita,
        lav,
        nld,
        nor,
        por,
        ron,
        rus,
        spa,
        swe,
        tha,
        tur,
        zho,
        edb_Brazilian,
        edb_ChineseJapaneseKorean,
    > {
    CREATE ANNOTATION std::description := '
        Languages supported by ElasticSearch.
        Names are ISO 639-3 language identifiers or EdgeDB language identifers.
    ';
};

CREATE SCALAR TYPE std::fts::LuceneLanguage
    EXTENDING enum<
        ara,
        ben,
        bul,
        cat,
        ces,
        ckb,
        dan,
        deu,
        ell,
        eng,
        est,
        eus,
        fas,
        fin,
        fra,
        gle,
        glg,
        hin,
        hun,
        hye,
        ind,
        ita,
        lav,
        lit,
        nld,
        nor,
        por,
        ron,
        rus,
        spa,
        srp,
        swe,
        tha,
        tur,
        edb_Brazilian,
        edb_ChineseJapaneseKorean,
        edb_Indian,
    > {
    CREATE ANNOTATION std::description := '
        Languages supported by Apache Lucene.
        Names are ISO 639-3 language identifiers or EdgeDB language identifers.
    ';
};
