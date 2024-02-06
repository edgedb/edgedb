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


for x in {0, 3, 4.25, 6.75}
union (
    insert Raw {val := x}
);


for x in {[0, 1, 2.3], [1, 1, 10.11], [4.5, 6.7, 8.9]}
union (
    (insert Basic {p_str := to_str(<json>x)}),
    (insert IVFFlat_L2 {vec := <v3>x}),
    (insert IVFFlat_IP {vec := <v3>x}),
    (insert IVFFlat_Cosine {vec := <v3>x}),
    (insert HNSW_L2 {vec := <v3>x}),
    (insert HNSW_IP {vec := <v3>x}),
    (insert HNSW_Cosine {vec := <v3>x}),
);
