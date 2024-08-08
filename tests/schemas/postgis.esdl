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


type GeoTest0 {
    required name: str;
    geometry: ext::postgis::geometry;
    geography: ext::postgis::geography;
    b2: ext::postgis::box2d;
    b3: ext::postgis::box3d;
    tup_b2: tuple<ext::postgis::box2d, str>;

    index pg::gist on (.geometry);
    index pg::gist on (.geography);
}


type GeoTest1 {
    required name: str;
    geometry: ext::postgis::geometry;
    geography: ext::postgis::geography;

    index pg::brin on (.geometry);
    index pg::brin on (.geography);
}


type GeoTest2 {
    required name: str;
    geometry: ext::postgis::geometry;
    geography: ext::postgis::geography;

    index pg::spgist on (.geometry);
    index pg::spgist on (.geography);
}


# Add a function that disables sequential scan.
function _set_seqscan(val: std::str) -> std::str {
    using sql $$
      select set_config('enable_seqscan', val, true)
    $$;
};
