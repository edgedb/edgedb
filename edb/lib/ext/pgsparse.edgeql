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


create extension package pgsparse version '0.5.4' {
    set ext_module := "ext::pgsparse";
    set sql_extensions := ["svector >=0.5.0,<0.6.0"];

    create module ext::pgsparse;

    create scalar type ext::pgsparse::vector extending std::anyscalar {
        set id := <uuid>"b646ace0-266d-47ce-8263-1224c38a4a12";
        set sql_type := "svector";
        set sql_type_scheme := "svector({__arg_0__})";
        set num_params := 1;
    };

    create cast from ext::pgsparse::vector to std::json {
        set volatility := 'Immutable';
        using sql 'SELECT val::text::jsonb';
    };

    create cast from std::json to ext::pgsparse::vector {
        set volatility := 'Immutable';
        using sql $$
        SELECT (
            nullif(val, 'null'::jsonb)::text::svector
        )
        $$;
    };

    # All casts from numerical arrays should allow assignment casts.
    create cast from array<std::float32> to ext::pgsparse::vector {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from array<std::float64> to ext::pgsparse::vector {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from array<std::int16> to ext::pgsparse::vector {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::float4[]::svector
        $$;
        allow assignment;
    };

    create cast from array<std::int32> to ext::pgsparse::vector {
        set volatility := 'Immutable';
        using sql cast;
        allow assignment;
    };

    create cast from array<std::int64> to ext::pgsparse::vector {
        set volatility := 'Immutable';
        using sql $$
        SELECT val::float4[]::svector
        $$;
        allow assignment;
    };

    create cast from ext::pgsparse::vector to array<std::float32> {
        set volatility := 'Immutable';
        using sql cast;
    };

    create function ext::pgsparse::euclidean_distance(
        a: ext::pgsparse::vector,
        b: ext::pgsparse::vector,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <-> b';
    };

    create function ext::pgsparse::neg_inner_product(
        a: ext::pgsparse::vector,
        b: ext::pgsparse::vector,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT (a <#> b)';
    };

    create function ext::pgsparse::cosine_distance(
        a: ext::pgsparse::vector,
        b: ext::pgsparse::vector,
    ) -> std::float64 {
        set volatility := 'Immutable';
        # Needed to pick up the indexes when used in ORDER BY.
        set prefer_subquery_args := true;
        using sql 'SELECT a <=> b';
    };

    create function ext::pgsparse::euclidean_norm(
        a: ext::pgsparse::vector
    ) -> std::float64 {
        using sql function 'svector_norm';
        set volatility := 'Immutable';
        set force_return_cast := true;
    };

    create function ext::pgsparse::set_ef_search(num: std::int64) -> std::int64 {
    using sql $$
        select num from (
            select set_config('shnsw.ef_search', num::text, true)
        ) as dummy;
    $$;
    };

    create function ext::pgsparse::_get_ef_search() -> optional std::int64 {
        using sql $$
          select nullif(current_setting('shnsw.ef_search'), '')::int8
        $$;
    };

    create abstract index ext::pgsparse::hnsw_euclidean(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for euclidean distance.';
        set code := $$
            shnsw (__col__ svector_l2_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create abstract index ext::pgsparse::hnsw_ip(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for inner product.';
        set code := $$
            shnsw (__col__ svector_ip_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };

    create abstract index ext::pgsparse::hnsw_cosine(
        named only m: int64 = 16,
        named only ef_construction: int64 = 64,
    ) {
        create annotation std::description :=
            'HNSW index for cosine distance.';
        set code := $$
            shnsw (__col__ svector_cosine_ops)
            WITH (m = __kw_m__, ef_construction = __kw_ef_construction__)
        $$;
    };
};
