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


create extension package pgvector version '0.4.0' {
    set ext_module := "ext::pgvector";
    set sql_extensions := ["vector >=0.4.0,<0.5.0"];

    create module ext::pgvector;

    create scalar type ext::pgvector::vector extending std::anyscalar {
        set id := <uuid>"9565dd88-04f5-11ee-a691-0b6ebe179825";
        set sql_type := "vector";
        set sql_type_scheme := "vector({__arg_0__})";
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

    # All casts from numerical arrays should allow assignment casts.
    create cast from array<std::float32> to ext::pgvector::vector {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from array<std::float64> to ext::pgvector::vector {
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

    create cast from array<std::int32> to ext::pgvector::vector {
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

    create cast from ext::pgvector::vector to array<std::float32> {
        set volatility := 'Immutable';
        using sql cast;
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

    create function ext::pgvector::neg_inner_product(
        a: ext::pgvector::vector,
        b: ext::pgvector::vector,
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

    create function ext::pgvector::euclidean_norm(
        a: ext::pgvector::vector
    ) -> std::float64 {
        using sql function 'vector_norm';
        set volatility := 'Immutable';
        set force_return_cast := true;
    };

    create function ext::pgvector::set_probes(num: std::int64) -> std::int64 {
	using sql $$
            select num from (
	        select set_config('ivfflat.probes', num::text, true)
            ) as dummy;
	$$;
    };

    create function ext::pgvector::_get_probes() -> optional std::int64 {
        using sql $$
          select nullif(current_setting('ivfflat.probes'), '')::int8
        $$;
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
};
