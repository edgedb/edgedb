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


type GeoTest {
    required name: str;
    geometry: ext::postgis::geometry;
    geography: ext::postgis::geography;

    index pg::gist on (.geometry);
    index pg::gist on (.geography);
}


# Add a function that disables sequential scan.
function _set_seqscan(val: std::str) -> std::str {
    using sql $$
      select set_config('enable_seqscan', val, true)
    $$;
};
