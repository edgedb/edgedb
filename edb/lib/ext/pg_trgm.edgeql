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


create extension package pg_trgm version '1.6' {
    set ext_module := "ext::pg_trgm";
    set sql_extensions := ["pg_trgm >=1.6"];

    create module ext::pg_trgm;

    create type ext::pg_trgm::Config extending cfg::ExtensionConfig {
        create required property similarity_threshold: std::float32 {
            create annotation cfg::backend_setting :=
                '"pg_trgm.similarity_threshold"';
            create annotation std::description :=
                "The current similarity threshold that is used by the "
                ++ "pg_trgm::similar() function, the pg_trgm::gin and "
                ++ "the pg_trgm::gist indexes.  The threshold must be "
                ++ "between 0 and 1 (default is 0.3).";
            set default := 0.3;
            create constraint std::min_value(0.0);
            create constraint std::max_value(1.0);
        };
        create required property word_similarity_threshold: std::float32 {
            create annotation cfg::backend_setting :=
                '"pg_trgm.word_similarity_threshold"';
            create annotation std::description :=
                "The current word similarity threshold that is used by the "
                ++ "pg_trgrm::word_similar() function. The threshold must be "
                ++ "between 0 and 1 (default is 0.6).";
            set default := 0.6;
            create constraint std::min_value(0.0);
            create constraint std::max_value(1.0);
        };
        create required property strict_word_similarity_threshold: std::float32
        {
            create annotation cfg::backend_setting :=
                '"pg_trgm.strict_word_similarity_threshold"';
            create annotation std::description :=
                "The current strict word similarity threshold that is used by "
                ++ "the pg_trgrm::strict_word_similar() function. The "
                ++ "threshold must be between 0 and 1 (default is 0.5).";
            set default := 0.5;
            create constraint std::min_value(0.0);
            create constraint std::max_value(1.0);
        };
    };

    create function ext::pg_trgm::similarity(
        a: std::str,
        b: std::str,
    ) -> std::float32 {
        set volatility := 'Immutable';
        using sql 'select 1.0::real - (a <-> b)';
    };

    create function ext::pg_trgm::similar(
        a: std::str,
        b: std::str,
    ) -> std::bool {
        set volatility := 'Stable';  # Depends on config.
        using sql 'select a % b';
    };

    create function ext::pg_trgm::similarity_dist(
        a: std::str,
        b: std::str,
    ) -> std::float32 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'select a <-> b';
    };

    create function ext::pg_trgm::word_similarity(
        a: std::str,
        b: std::str,
    ) -> std::float32 {
        set volatility := 'Immutable';
        using sql 'select 1.0::real - (a <<-> b)';
    };

    create function ext::pg_trgm::word_similar(
        a: std::str,
        b: std::str,
    ) -> std::bool {
        set volatility := 'Stable';  # Depends on config.
        using sql 'select a <% b';
    };

    create function ext::pg_trgm::word_similarity_dist(
        a: std::str,
        b: std::str,
    ) -> std::float32 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'select a <<-> b';
    };

    create function ext::pg_trgm::strict_word_similarity(
        a: std::str,
        b: std::str,
    ) -> std::float32 {
        set volatility := 'Immutable';
        using sql 'select 1.0::real - (a <<<-> b)';
    };

    create function ext::pg_trgm::strict_word_similar(
        a: std::str,
        b: std::str,
    ) -> std::bool {
        set volatility := 'Stable';  # Depends on config.
        using sql 'select a <<% b';
    };

    create function ext::pg_trgm::strict_word_similarity_dist(
        a: std::str,
        b: std::str,
    ) -> std::float32 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'select a <<<-> b';
    };

    create abstract index ext::pg_trgm::gin() {
        create annotation std::description :=
            'pg_trgm GIN index.';
        set code :=
            'GIN (__col__ gin_trgm_ops)';
    };

    create abstract index ext::pg_trgm::gist(
        named only siglen: int64 = 12
    ) {
        create annotation std::description :=
            'pg_trgm GIST index.';
        set code :=
            'GIST (__col__ gist_trgm_ops(siglen = __kw_siglen__))';
    };

    create index match for std::str using ext::pg_trgm::gin;
    create index match for std::str using ext::pg_trgm::gist;
};
