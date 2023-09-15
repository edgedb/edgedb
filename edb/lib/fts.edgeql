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

CREATE SCALAR TYPE fts::Analyzer
    EXTENDING enum<
        ISO_ara,
        ISO_hye,
        ISO_eus,
        ISO_cat,
        ISO_dan,
        ISO_nld,
        ISO_eng,
        ISO_fin,
        ISO_fra,
        ISO_deu,
        ISO_ell,
        ISO_hin,
        ISO_hun,
        ISO_ind,
        ISO_gle,
        ISO_ita,
        ISO_nor,
        ISO_por,
        ISO_ron,
        ISO_rus,
        ISO_spa,
        ISO_swe,
        ISO_tur,
    > {
    CREATE ANNOTATION std::description := '
        Analyzers supported by PostgreSQL FTS, ElasticSearch and Apache Lucene.
        Names prefixed with ISO are ISO 639-3 language identifiers.
    ';
};

CREATE ABSTRACT INDEX fts::index {
    CREATE ANNOTATION std::description :=
        "Full-text search index based on the Postgres's GIN index.";
    SET code := ''; # overridden by special case
};

CREATE SCALAR TYPE fts::document {
    SET transient := true;
};

CREATE FUNCTION fts::with_options(
    text: std::str,
    analyzer: anyenum,
    weight_category: optional std::str = <std::str>{},
) -> fts::document {
    CREATE ANNOTATION std::description := '
        Adds analyzer (i.e. language) and weight information to a string,
        so it be indexed with fts::index.
    ';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};

CREATE FUNCTION fts::search(
    object: anyobject,
    query: std::str,
    named only analyzer: std::str = <std::str>fts::Analyzer.ISO_eng,
    named only weights: optional array<float64> = {},
) -> optional tuple<object: anyobject, score: float32>
{
    CREATE ANNOTATION std::description := '
        Search an object using its fts::index index.
        Returns objects that match the specified query and the matching score.
    ';
    SET volatility := 'Immutable';
    USING SQL EXPRESSION;
};

CREATE SCALAR TYPE fts::PGAnalyzer
    EXTENDING enum<
        Simple,
        ISO_ara,
        ISO_hye,
        ISO_eus,
        ISO_cat,
        ISO_dan,
        ISO_nld,
        ISO_eng,
        ISO_fin,
        ISO_fra,
        ISO_deu,
        ISO_ell,
        ISO_hin,
        ISO_hun,
        ISO_ind,
        ISO_gle,
        ISO_ita,
        ISO_lit,
        ISO_npi,
        ISO_nor,
        ISO_por,
        ISO_ron,
        ISO_rus,
        ISO_srp,
        ISO_spa,
        ISO_swe,
        ISO_tam,
        ISO_tur,
        ISO_yid,
    > {
    CREATE ANNOTATION std::description :='
        Analyzers supported by PostgreSQL FTS.
        Names prefixed with ISO are ISO 639-3 language identifiers.
    ';
};

CREATE SCALAR TYPE fts::ElasticAnalyzer
    EXTENDING enum<
        ISO_ara,
        ISO_hye,
        ISO_eus,
        ISO_bul,
        ISO_cat,
        ISO_zho,
        ISO_ces,
        ISO_dan,
        ISO_nld,
        ISO_eng,
        ISO_fin,
        ISO_fra,
        ISO_glg,
        ISO_deu,
        ISO_ell,
        ISO_hin,
        ISO_hun,
        ISO_ind,
        ISO_gle,
        ISO_ita,
        ISO_lav,
        ISO_nor,
        ISO_fas,
        ISO_por,
        ISO_ron,
        ISO_rus,
        ISO_ckb,
        ISO_spa,
        ISO_swe,
        ISO_tur,
        ISO_tha,
        Brazilian,
        ChineseJapaneseKorean,
    > {
    CREATE ANNOTATION std::description := '
        Analyzers supported by ElasticSearch.
        Names prefixed with ISO are ISO 639-3 language identifiers.
    ';
};

CREATE SCALAR TYPE fts::LuceneAnalyzer
    EXTENDING enum<
        ISO_ara,
        ISO_bul,
        ISO_ben,
        ISO_cat,
        ISO_ckb,
        ISO_ces,
        ISO_dan,
        ISO_deu,
        ISO_ell,
        ISO_eng,
        ISO_spa,
        ISO_est,
        ISO_eus,
        ISO_fas,
        ISO_fin,
        ISO_fra,
        ISO_gle,
        ISO_glg,
        ISO_hin,
        ISO_hun,
        ISO_hye,
        ISO_ind,
        ISO_ita,
        ISO_lit,
        ISO_lav,
        ISO_nld,
        ISO_nor,
        ISO_por,
        ISO_ron,
        ISO_rus,
        ISO_srp,
        ISO_swe,
        ISO_tha,
        ISO_tur,
        BrazilianPortuguese,
        ChineseJapaneseKorean,
        Indian,
    > {
    CREATE ANNOTATION std::description := '
        Analyzers supported by Apache Lucene.
        Names prefixed with ISO are ISO 639-3 language identifiers.
    ';
};
