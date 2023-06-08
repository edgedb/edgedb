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


create extension package vector version '1.0' {
    set ext_module := "vector";
    set sql_extensions := ["vector"];

    create module vector;

    create scalar type vector::vector extending std::anyscalar {
        set id := <uuid>"9565dd88-04f5-11ee-a691-0b6ebe179825";
        set sql_type := "vector";
        set sql_type_scheme := "vector({__arg_0__})";
        set num_params := 1;
    };

    create cast from vector::vector to std::str {
        set volatility := 'Immutable';
        using sql cast;
    };

    create cast from std::str to vector::vector {
        set volatility := 'Immutable';
        using sql cast;
    };

    create cast from vector::vector to std::json {
        set volatility := 'Immutable';
        using sql 'SELECT val::text::jsonb';
    };

    create cast from std::json to vector::vector {
        set volatility := 'Immutable';
        using sql $$
        SELECT (
            CASE WHEN nullif(val, 'null'::jsonb) IS NULL THEN NULL
            ELSE
                (SELECT COALESCE(array_agg(j), ARRAY[]::jsonb[])
                FROM jsonb_array_elements(val) as j)
            END
        )::float[]::vector
        $$;
    };

    create function vector::euclidean_distance(
        a: vector::vector,
        b: vector::vector,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <-> b';
    };

    create function vector::inner_product(
        a: vector::vector,
        b: vector::vector,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT -(a <#> b)';
    };

    create function vector::cosine_distance(
        a: vector::vector,
        b: vector::vector,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <=> b';
    };

    create function vector::len(a: vector::vector) -> std::int64 {
        using sql function 'vector_dims';
        set volatility := 'Immutable';
        set force_return_cast := true;
    };

    create function vector::euclidean_norm(
        a: vector::vector
    ) -> std::float64 {
        using sql function 'vector_norm';
        set volatility := 'Immutable';
        set force_return_cast := true;
    };

    create function vector::mean(
        a: set of vector::vector
    ) -> vector::vector {
        using sql function 'avg';
        set volatility := 'Immutable';
        set force_return_cast := true;
    };

    create function vector::set_probes(num: std::int64) -> std::int64 {
	using sql $$
            select num from (
	        select set_config('ivfflat.probes', num::text, true)
            ) as dummy;
	$$;
    };

    create function vector::_get_probes() -> optional std::int64 {
        using sql $$
          select nullif(current_setting('ivfflat.probes'), '')::int8
        $$;
    };

    create abstract index vector::ivfflat_euclidean(
        named only lists: int64
    ) {
        create annotation std::description :=
            'IVFFlat index for euclidean distance.';
        set code :=
            'ivfflat (__col__ vector_l2_ops) WITH (lists = __kw_lists__)';
    };

    create abstract index vector::ivfflat_ip(
        named only lists: int64
    ) {
        create annotation std::description :=
            'IVFFlat index for inner product.';
        set code :=
            'ivfflat (__col__ vector_ip_ops) WITH (lists = __kw_lists__)';
    };

    create abstract index vector::ivfflat_cosine(
        named only lists: int64
    ) {
        create annotation std::description :=
            'IVFFlat index for cosine distance.';
        set code :=
            'ivfflat (__col__ vector_cosine_ops) WITH (lists = __kw_lists__)';
    };
};
