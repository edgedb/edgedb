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


create extension package pgvector version '0.7.4' {
    set ext_module := "ext::pgvector";
    set sql_extensions := ["vector >=0.7.4,<0.8.0"];

    set sql_setup_script := $script$
        -- Rename the vector_norm to be consistent with l2_norm
        ALTER FUNCTION edgedb.vector_norm(edgedb.vector) RENAME TO l2_norm;

        -- Add some helpers

        -- about 5-6 times slower than the C cast, but retains 0-based index
        CREATE FUNCTION sparsevec_to_text(val sparsevec) RETURNS text AS $$
        DECLARE
            vectxt text := val::text;
            mid text[];
            kv text[];
            i int8;
            res text := '{';
        BEGIN
            mid := string_to_array(substr(split_part(vectxt, '}', 1), 2), ',');
            FOR i IN 1..cardinality(mid)
            LOOP
                kv := string_to_array(mid[i], ':');
                kv[1] := (kv[1]::int8 - 1)::text;
                res := res || kv[1] || ':' || kv[2] || ',';
            END LOOP;

            RETURN left(res, -1) || '}' || split_part(vectxt, '}', 2);
        END;
        $$ LANGUAGE plpgsql
        IMMUTABLE STRICT;

        -- about 10 times slower than a cast
        CREATE FUNCTION text_to_sparsevec(val text) RETURNS sparsevec AS $$
        DECLARE
            mid text[];
            kv text[];
            i int8;
            res text := '{';
        BEGIN
            IF val ~ '^\s*{\s*(\d+\s*:.+?,\s*)*\d+\s*:.+}\s*/\s*\d+\s*$'
            THEN
                mid := string_to_array(split_part(split_part(val, '}', 1), '{', 2), ',');
                FOR i IN 1..cardinality(mid)
                LOOP
                    kv := string_to_array(mid[i], ':');
                    kv[1] := (trim(kv[1])::int8 + 1)::text;
                    res := res || kv[1] || ':' || kv[2] || ',';
                END LOOP;
                RETURN (left(res, -1) || '}' || split_part(val, '}', 2))::sparsevec;
            ELSE
                RETURN val::sparsevec;
            END IF;
        END;
        $$ LANGUAGE plpgsql
        IMMUTABLE STRICT;

        CREATE FUNCTION sparsevec_to_jsonb(val sparsevec) RETURNS jsonb AS $$
        DECLARE
            vectxt text := val::text;
            mid text[];
            kv text[];
            i int8;
            dim text := split_part(vectxt, '/', 2);
            res text := '{';
        BEGIN
            mid := string_to_array(substr(split_part(vectxt, '}', 1), 2), ',');
            FOR i IN 1..cardinality(mid)
            LOOP
                kv := string_to_array(mid[i], ':');
                kv[1] := (kv[1]::int8 - 1)::text;
                res := res || '"' || kv[1] || '":' || kv[2] || ',';
            END LOOP;

            RETURN (res || '"dim":' || dim || '}')::jsonb;
        END;
        $$ LANGUAGE plpgsql
        IMMUTABLE STRICT;

        CREATE FUNCTION jsonb_to_sparsevec(val jsonb) RETURNS sparsevec AS $$
        DECLARE
            mid text[];
            kv text[];
            r record;
            i int8;
            dim text := NULL;
            res text := '{';
            msg text;
        BEGIN
            IF jsonb_typeof(val) = 'object'
            THEN
                msg := 'missing "dim"';
                FOR r IN SELECT * FROM jsonb_each(val)
                LOOP
                    CASE
                        WHEN r.key = 'dim' THEN
                            dim := r.value::text;
                        WHEN r.key ~ $r$\d+$r$ THEN
                            res := res || (r.key::int8 + 1)::text || ':'
                                       || r.value::text || ',';
                        ELSE
                            msg := 'unexpected key in JSON object: ' || r.key;
                            EXIT;
                    END CASE;
                END LOOP;

                IF dim IS NOT NULL
                THEN
                    RETURN (left(res, -1) || '}/' || dim)::sparsevec;
                END IF;
            ELSE
                msg := 'JSON object expected, got ' || jsonb_typeof(val) || ' instead';
            END IF;

            RAISE EXCEPTION USING
                ERRCODE = 22000,
                MESSAGE = msg;
        END;
        $$ LANGUAGE plpgsql
        IMMUTABLE STRICT;
    $script$;
    set sql_teardown_script := $$
        ALTER FUNCTION edgedb.l2_norm(edgedb.vector) RENAME TO vector_norm;
        -- remove helpers
        DROP FUNCTION edgedb.sparsevec_to_jsonb;
        DROP FUNCTION edgedb.jsonb_to_sparsevec;
        DROP FUNCTION edgedb.sparsevec_to_text;
        DROP FUNCTION edgedb.text_to_sparsevec;
    $$;

    create module ext::pgvector;

    create type ext::pgvector::Config extending cfg::ExtensionConfig {
        create required property probes: std::int64 {
            create annotation cfg::backend_setting :=
                '"ivfflat.probes"';
            create annotation std::description :=
                "The number of probes (1 by default) used by IVFFlat "
                ++ "index. A higher value provides better recall at the "
                ++ "cost of speed, and it can be set to the number of "
                ++ "lists for exact nearest neighbor search (at which point "
                ++ "the planner won’t use the index)";
            set default := 1;
            create constraint std::min_value(1);
        };
        create required property ef_search: std::int64 {
            create annotation cfg::backend_setting :=
                '"hnsw.ef_search"';
            create annotation std::description :=
                "The size of the dynamic candidate list for search (40 "
                ++ "by default) used by HNSW index. A higher value "
                ++ "provides better recall at the  cost of speed.";
            set default := 40;
            create constraint std::min_value(1);
        };
    };

    create scalar type ext::pgvector::vector extending std::anyscalar {
        set id := <uuid>"9565dd88-04f5-11ee-a691-0b6ebe179825";
        set sql_type := "vector";
        set sql_type_scheme := "vector({__arg_0__})";
        set num_params := 1;
    };

    create scalar type ext::pgvector::halfvec extending std::anyscalar {
        set id := <uuid>"4ba84534-188e-43b4-a7ce-cea2af0f405b";
        set sql_type := "halfvec";
        set sql_type_scheme := "halfvec({__arg_0__})";
        set num_params := 1;
    };

    create scalar type ext::pgvector::sparsevec extending std::anyscalar {
        set id := <uuid>"003e434d-cac2-430a-b238-fb39d73447d2";
        set sql_type := "sparsevec";
        set sql_type_scheme := "sparsevec({__arg_0__})";
        set num_params := 1;
    };

    create cast from ext::pgvector::vector to std::json {
        set volatility := 'Immutable';
        using sql 'SELECT val::text::jsonb';
    };

    create cast from std::json to ext::pgvector::vector {
        set volatility := 'Immutable';
        using sql $$
        SELECT (
            nullif(val, 'null'::jsonb)::text::vector
        )
        $$;
    };

    create cast from ext::pgvector::vector to std::str {
        set volatility := 'Immutable';
        using sql cast;
    };

    create cast from std::str to ext::pgvector::vector {
        set volatility := 'Immutable';
        using sql cast;
    };

    create cast from ext::pgvector::vector to std::bytes {
        set volatility := 'Immutable';
        using sql 'SELECT vector_send(val)';
    };

    create cast from ext::pgvector::halfvec to std::json {
        set volatility := 'Immutable';
        using sql 'SELECT val::text::jsonb';
    };

    create cast from std::json to ext::pgvector::halfvec {
        set volatility := 'Immutable';
        using sql $$
        SELECT (
            nullif(val, 'null'::jsonb)::text::halfvec
        )
        $$;
    };

    create cast from ext::pgvector::halfvec to std::str {
        set volatility := 'Immutable';
        using sql cast;
    };

    create cast from std::str to ext::pgvector::halfvec {
        set volatility := 'Immutable';
        using sql cast;
    };

    create cast from ext::pgvector::halfvec to std::bytes {
        set volatility := 'Immutable';
        using sql 'SELECT halfvec_send(val)';
    };

    create cast from ext::pgvector::sparsevec to std::str {
        set volatility := 'Immutable';
        using sql 'SELECT sparsevec_to_text(val)';
    };

    create cast from std::str to ext::pgvector::sparsevec {
        set volatility := 'Immutable';
        using sql 'SELECT text_to_sparsevec(val)';
    };

    create cast from ext::pgvector::sparsevec to std::bytes {
        set volatility := 'Immutable';
        using sql 'SELECT sparsevec_send(val)';
    };

    create cast from ext::pgvector::sparsevec to std::json {
        set volatility := 'Immutable';
        using sql 'SELECT sparsevec_to_jsonb(val)';
    };

    create cast from std::json to ext::pgvector::sparsevec {
        set volatility := 'Immutable';
        using sql 'SELECT jsonb_to_sparsevec(val)';
    };

    # All casts from numerical arrays should allow assignment casts.
    create cast from array<std::float32> to ext::pgvector::vector {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from array<std::float32> to ext::pgvector::halfvec {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from array<std::float64> to ext::pgvector::vector {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from array<std::float64> to ext::pgvector::halfvec {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from array<std::int16> to ext::pgvector::vector {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::float4[]::vector
        $$;
        allow assignment;
    };

    create cast from array<std::int16> to ext::pgvector::halfvec {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::float4[]::halfvec
        $$;
        allow assignment;
    };

    create cast from array<std::int32> to ext::pgvector::vector {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from array<std::int32> to ext::pgvector::halfvec {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from array<std::int64> to ext::pgvector::vector {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::float4[]::vector
        $$;
        allow assignment;
    };

    create cast from array<std::int64> to ext::pgvector::halfvec {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::float4[]::halfvec
        $$;
        allow assignment;
    };

    create cast from ext::pgvector::vector to array<std::float32> {
        set volatility := 'Immutable';
        using sql cast;
    };

    create cast from ext::pgvector::halfvec to array<std::float32> {
        set volatility := 'Immutable';
        using sql cast;
    };

    create cast from ext::pgvector::vector to ext::pgvector::halfvec {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from ext::pgvector::vector to ext::pgvector::sparsevec {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from ext::pgvector::halfvec to ext::pgvector::vector {
        set volatility := 'Immutable';
        using sql cast;
        allow implicit;
    };

    create cast from ext::pgvector::halfvec to ext::pgvector::sparsevec {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from ext::pgvector::sparsevec to ext::pgvector::vector {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from ext::pgvector::sparsevec to ext::pgvector::halfvec {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create function ext::pgvector::euclidean_distance(
        a: ext::pgvector::vector,
        b: ext::pgvector::vector,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <-> b';
    };

    create function ext::pgvector::euclidean_distance(
        a: ext::pgvector::halfvec,
        b: ext::pgvector::halfvec,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <-> b';
    };

    create function ext::pgvector::euclidean_distance(
        a: ext::pgvector::sparsevec,
        b: ext::pgvector::sparsevec,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <-> b';
    };

    create function ext::pgvector::neg_inner_product(
        a: ext::pgvector::vector,
        b: ext::pgvector::vector,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT (a <#> b)';
    };

    create function ext::pgvector::neg_inner_product(
        a: ext::pgvector::halfvec,
        b: ext::pgvector::halfvec,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT (a <#> b)';
    };

    create function ext::pgvector::neg_inner_product(
        a: ext::pgvector::sparsevec,
        b: ext::pgvector::sparsevec,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT (a <#> b)';
    };

    create function ext::pgvector::cosine_distance(
        a: ext::pgvector::vector,
        b: ext::pgvector::vector,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <=> b';
    };

    create function ext::pgvector::cosine_distance(
        a: ext::pgvector::halfvec,
        b: ext::pgvector::halfvec,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <=> b';
    };

    create function ext::pgvector::cosine_distance(
        a: ext::pgvector::sparsevec,
        b: ext::pgvector::sparsevec,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <=> b';
    };

    create function ext::pgvector::taxicab_distance(
        a: ext::pgvector::vector,
        b: ext::pgvector::vector,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <+> b';
    };

    create function ext::pgvector::taxicab_distance(
        a: ext::pgvector::halfvec,
        b: ext::pgvector::halfvec,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <+> b';
    };

    create function ext::pgvector::taxicab_distance(
        a: ext::pgvector::sparsevec,
        b: ext::pgvector::sparsevec,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <+> b';
    };

    create function ext::pgvector::euclidean_norm(
        a: ext::pgvector::vector
    ) -> std::float64 {
        using sql function 'l2_norm';
        set volatility := 'Immutable';
        set force_return_cast := true;
    };

    create function ext::pgvector::euclidean_norm(
        a: ext::pgvector::halfvec
    ) -> std::float64 {
        using sql function 'l2_norm';
        set volatility := 'Immutable';
        set force_return_cast := true;
    };

    create function ext::pgvector::euclidean_norm(
        a: ext::pgvector::sparsevec
    ) -> std::float64 {
        using sql function 'l2_norm';
        set volatility := 'Immutable';
        set force_return_cast := true;
    };

    create function ext::pgvector::l2_normalize(
        a: ext::pgvector::vector
    ) -> ext::pgvector::vector {
        using sql function 'l2_normalize';
        set volatility := 'Immutable';
        set force_return_cast := true;
    };

    create function ext::pgvector::l2_normalize(
        a: ext::pgvector::halfvec
    ) -> ext::pgvector::halfvec {
        using sql function 'l2_normalize';
        set volatility := 'Immutable';
        set force_return_cast := true;
    };

    create function ext::pgvector::l2_normalize(
        a: ext::pgvector::sparsevec
    ) -> ext::pgvector::sparsevec {
        using sql function 'l2_normalize';
        set volatility := 'Immutable';
        set force_return_cast := true;
    };

    create function ext::pgvector::subvector(
        a: ext::pgvector::vector,
        i: std::int64,
        len: std::int64,
    ) -> ext::pgvector::vector {
        set volatility := 'Immutable';
        using sql 'SELECT subvector(a, (i+1)::int, len::int)';
    };

    create function ext::pgvector::subvector(
        a: ext::pgvector::halfvec,
        i: std::int64,
        len: std::int64,
    ) -> ext::pgvector::halfvec {
        set volatility := 'Immutable';
        using sql 'SELECT subvector(a, (i+1)::int, len::int)';
    };

    create function ext::pgvector::set_probes(num: std::int64) -> std::int64 {
        using sql $$
            select num from (
                select set_config('ivfflat.probes', num::text, true)
            ) as dummy;
        $$;
        CREATE ANNOTATION std::deprecated :=
            'This function is deprecated. ' ++
            'Configure ext::pgvector::Config::probes instead';
    };

    create abstract index ext::pgvector::ivfflat_euclidean(
        named only lists: int64
    ) {
        create annotation std::description :=
            'IVFFlat index for euclidean distance.';
        set code :=
            'ivfflat (__col__ vector_l2_ops) WITH (lists = __kw_lists__)';
    };

    create abstract index ext::pgvector::ivfflat_ip(
        named only lists: int64
    ) {
        create annotation std::description :=
            'IVFFlat index for inner product.';
        set code :=
            'ivfflat (__col__ vector_ip_ops) WITH (lists = __kw_lists__)';
    };

    create abstract index ext::pgvector::ivfflat_cosine(
        named only lists: int64
    ) {
        create annotation std::description :=
            'IVFFlat index for cosine distance.';
        set code :=
            'ivfflat (__col__ vector_cosine_ops) WITH (lists = __kw_lists__)';
    };

    create abstract index ext::pgvector::hnsw_euclidean(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for euclidean distance.';
        set code := $$
            hnsw (__col__ vector_l2_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create abstract index ext::pgvector::hnsw_ip(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for inner product.';
        set code := $$
            hnsw (__col__ vector_ip_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create abstract index ext::pgvector::hnsw_cosine(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for cosine distance.';
        set code := $$
            hnsw (__col__ vector_cosine_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create abstract index ext::pgvector::hnsw_taxicab(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for taxicab (L1) distance.';
        set code := $$
            hnsw (__col__ vector_l1_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create index match for ext::pgvector::vector using ext::pgvector::ivfflat_euclidean;
    create index match for ext::pgvector::vector using ext::pgvector::ivfflat_ip;
    create index match for ext::pgvector::vector using ext::pgvector::ivfflat_cosine;
    create index match for ext::pgvector::vector using ext::pgvector::hnsw_euclidean;
    create index match for ext::pgvector::vector using ext::pgvector::hnsw_ip;
    create index match for ext::pgvector::vector using ext::pgvector::hnsw_cosine;
    create index match for ext::pgvector::vector using ext::pgvector::hnsw_taxicab;

    create abstract index ext::pgvector::ivfflat_hv_euclidean(
        named only lists: int64
    ) {
        create annotation std::description :=
            'IVFFlat index for euclidean distance.';
        set code :=
            'ivfflat (__col__ halfvec_l2_ops) WITH (lists = __kw_lists__)';
    };

    create abstract index ext::pgvector::ivfflat_hv_ip(
        named only lists: int64
    ) {
        create annotation std::description :=
            'IVFFlat index for inner product.';
        set code :=
            'ivfflat (__col__ halfvec_ip_ops) WITH (lists = __kw_lists__)';
    };

    create abstract index ext::pgvector::ivfflat_hv_cosine(
        named only lists: int64
    ) {
        create annotation std::description :=
            'IVFFlat index for cosine distance.';
        set code :=
            'ivfflat (__col__ halfvec_cosine_ops) WITH (lists = __kw_lists__)';
    };

    create abstract index ext::pgvector::hnsw_hv_euclidean(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for euclidean distance.';
        set code := $$
            hnsw (__col__ halfvec_l2_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create abstract index ext::pgvector::hnsw_hv_ip(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for inner product.';
        set code := $$
            hnsw (__col__ halfvec_ip_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create abstract index ext::pgvector::hnsw_hv_cosine(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for cosine distance.';
        set code := $$
            hnsw (__col__ halfvec_cosine_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create abstract index ext::pgvector::hnsw_hv_taxicab(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for taxicab (L1) distance.';
        set code := $$
            hnsw (__col__ halfvec_l1_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create index match for ext::pgvector::halfvec using ext::pgvector::ivfflat_hv_euclidean;
    create index match for ext::pgvector::halfvec using ext::pgvector::ivfflat_hv_ip;
    create index match for ext::pgvector::halfvec using ext::pgvector::ivfflat_hv_cosine;
    create index match for ext::pgvector::halfvec using ext::pgvector::hnsw_hv_euclidean;
    create index match for ext::pgvector::halfvec using ext::pgvector::hnsw_hv_ip;
    create index match for ext::pgvector::halfvec using ext::pgvector::hnsw_hv_cosine;
    create index match for ext::pgvector::halfvec using ext::pgvector::hnsw_hv_taxicab;

    create abstract index ext::pgvector::hnsw_sv_euclidean(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for euclidean distance.';
        set code := $$
            hnsw (__col__ sparsevec_l2_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create abstract index ext::pgvector::hnsw_sv_ip(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for inner product.';
        set code := $$
            hnsw (__col__ sparsevec_ip_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create abstract index ext::pgvector::hnsw_sv_cosine(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for cosine distance.';
        set code := $$
            hnsw (__col__ sparsevec_cosine_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create abstract index ext::pgvector::hnsw_sv_taxicab(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for taxicab (L1) distance.';
        set code := $$
            hnsw (__col__ sparsevec_l1_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create index match for ext::pgvector::sparsevec using ext::pgvector::hnsw_sv_euclidean;
    create index match for ext::pgvector::sparsevec using ext::pgvector::hnsw_sv_ip;
    create index match for ext::pgvector::sparsevec using ext::pgvector::hnsw_sv_cosine;
    create index match for ext::pgvector::sparsevec using ext::pgvector::hnsw_sv_taxicab;
};
