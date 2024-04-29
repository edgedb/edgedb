#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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


set module default;

for x in range_unpack(range(1, 1000))
# Large, varied, but deterministic dataset.
insert L2 {vec := [x % 10, math::ln(x), x / 7 % 13]};

# set the ef_search extension config value
configure current database set
ext::pgvector::Config::ef_search := 5;

insert Astronomy {
    content := 'Skies on Mars are red'
};
insert Astronomy {
    content := 'Skies on Earth are blue'
};
