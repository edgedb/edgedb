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

type L2 {
    required vec: v3;
    index ext::pgvector::ivfflat_euclidean(lists := 100) on (.vec);
}

type L3 {
    property x: str;

    index fts::index on (fts::with_options(.x, language := fts::Language.eng));
}
