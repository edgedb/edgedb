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


scalar type v3 extending ext::pgvector::vector<3>;
scalar type hv3 extending ext::pgvector::halfvec<3>;
scalar type sv3 extending ext::pgvector::sparsevec<3>;

scalar type myf64 extending float64 {
    constraint max_value(100);
}
scalar type deepf64 extending myf64;


type Basic {
    required p_str: str;
    p_json: json {rewrite insert using (to_json(__subject__.p_str))};
}

type IVFFlat_vec_L2 {
    required vec: v3;
    index ext::pgvector::ivfflat_euclidean(lists := 100) on (.vec);
}

type IVFFlat_vec_IP {
    required vec: v3;
    index ext::pgvector::ivfflat_ip(lists := 100) on (.vec);
}

type IVFFlat_vec_Cosine {
    required vec: v3;
    index ext::pgvector::ivfflat_cosine(lists := 100) on (.vec);
}

type HNSW_vec_L2 {
    required vec: v3;
    index ext::pgvector::hnsw_euclidean() on (.vec);
}

type HNSW_vec_IP {
    required vec: v3;
    index ext::pgvector::hnsw_ip(m := 4) on (.vec);
}

type HNSW_vec_Cosine {
    required vec: v3;
    index ext::pgvector::hnsw_cosine(m := 2, ef_construction := 4) on (.vec);
}

type HNSW_vec_L1 {
    required vec: v3;
    index ext::pgvector::hnsw_taxicab() on (.vec);
}

type IVFFlat_hv_L2 {
    required vec: hv3;
    index ext::pgvector::ivfflat_hv_euclidean(lists := 100) on (.vec);
}

type IVFFlat_hv_IP {
    required vec: hv3;
    index ext::pgvector::ivfflat_hv_ip(lists := 100) on (.vec);
}

type IVFFlat_hv_Cosine {
    required vec: hv3;
    index ext::pgvector::ivfflat_hv_cosine(lists := 100) on (.vec);
}

type HNSW_hv_L2 {
    required vec: hv3;
    index ext::pgvector::hnsw_hv_euclidean() on (.vec);
}

type HNSW_hv_IP {
    required vec: hv3;
    index ext::pgvector::hnsw_hv_ip(m := 4) on (.vec);
}

type HNSW_hv_Cosine {
    required vec: hv3;
    index ext::pgvector::hnsw_hv_cosine(m := 2, ef_construction := 4) on (.vec);
}

type HNSW_hv_L1 {
    required vec: hv3;
    index ext::pgvector::hnsw_hv_taxicab() on (.vec);
}

type HNSW_sv_L2 {
    required vec: sv3;
    index ext::pgvector::hnsw_sv_euclidean() on (.vec);
}

type HNSW_sv_IP {
    required vec: sv3;
    index ext::pgvector::hnsw_sv_ip(m := 4) on (.vec);
}

type HNSW_sv_Cosine {
    required vec: sv3;
    index ext::pgvector::hnsw_sv_cosine(m := 2, ef_construction := 4) on (.vec);
}

type HNSW_sv_L1 {
    required vec: sv3;
    index ext::pgvector::hnsw_sv_taxicab() on (.vec);
}

type Con {
    required vec: v3 {
        constraint expression on (
            ext::pgvector::cosine_distance(
                __subject__, <ext::pgvector::vector>[1, 1, 1]
            ) < 0.2
        )
    }
}


type Raw {
    required val: float64;

    p_int16: int16 {rewrite insert using (<int16>__subject__.val)};
    p_int32: int32 {rewrite insert using (<int32>__subject__.val)};
    p_int64: int64 {rewrite insert using (<int64>__subject__.val)};
    p_bigint: bigint {rewrite insert using (<bigint>__subject__.val)};
    p_float32: float32 {rewrite insert using (<float32>__subject__.val)};
    p_decimal: decimal {rewrite insert using (<decimal>__subject__.val)};

    p_myf64: myf64 {rewrite insert using (<myf64>__subject__.val)};
    p_deepf64: deepf64 {rewrite insert using (<deepf64>__subject__.val)};
}
