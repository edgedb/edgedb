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


with gis as module ext::postgis
insert GeoTest0 {
    name := '1st',
    geometry := <gis::geometry>'POINT(0 1)',
    geography := <gis::geography>'POINT(2 3)',
    b2 := <gis::box2d>'BOX(0 1, 2 3)',
    b3 := <gis::box3d>'BOX3D(0 1 5, 2 3 9)',
    tup_b2 := (<gis::box2d>'BOX(0 1, 2 3)', 'ok'),
};

with gis as module ext::postgis
insert GeoTest0 {
    name := '2nd',
    geometry := <gis::geometry><gis::box2d>'box(0 0, 2 3)',
    geography := <gis::geography>'POINT(4 5)',
};

with gis as module ext::postgis
for x in range_unpack(range(0, 100_000)) union (
    insert GeoTest0 {
        name := 'gen' ++ to_str(x),
        geometry := <gis::geometry>(
            'POLYGON((' ++
            to_str(x) ++ ' 0,' ++
            to_str(x + 1) ++ ' 0,' ++
            to_str(x + 1) ++ ' 2,' ++
            to_str(x) ++ ' 2,' ++
            to_str(x) ++ ' 0))'
        ),
        geography := <gis::geography>(
            'POLYGON((' ++
            to_str(x/1000 - 10) ++ ' 0,' ++
            to_str(x/1000 - 9) ++ ' 0,' ++
            to_str(x/1000 - 9) ++ ' 2,' ++
            to_str(x/1000 - 10) ++ ' 2,' ++
            to_str(x/1000 - 10) ++ ' 0))'
        ),
    }
);

with gis as module ext::postgis
for x in range_unpack(range(0, 100_000)) union (
    insert GeoTest1 {
        name := 'gen' ++ to_str(x),
        geometry := <gis::geometry>(
            'POLYGON((' ++
            to_str(x) ++ ' 0,' ++
            to_str(x + 1) ++ ' 0,' ++
            to_str(x + 1) ++ ' 2,' ++
            to_str(x) ++ ' 2,' ++
            to_str(x) ++ ' 0))'
        ),
        geography := <gis::geography>(
            'POLYGON((' ++
            to_str(x/1000 - 10) ++ ' 0,' ++
            to_str(x/1000 - 9) ++ ' 0,' ++
            to_str(x/1000 - 9) ++ ' 2,' ++
            to_str(x/1000 - 10) ++ ' 2,' ++
            to_str(x/1000 - 10) ++ ' 0))'
        ),
    }
);

with gis as module ext::postgis
for x in range_unpack(range(0, 100_000)) union (
    insert GeoTest2 {
        name := 'gen' ++ to_str(x),
        geometry := <gis::geometry>(
            'POLYGON((' ++
            to_str(x) ++ ' 0,' ++
            to_str(x + 1) ++ ' 0,' ++
            to_str(x + 1) ++ ' 2,' ++
            to_str(x) ++ ' 2,' ++
            to_str(x) ++ ' 0))'
        ),
        geography := <gis::geography>(
            'POLYGON((' ++
            to_str(x/1000 - 10) ++ ' 0,' ++
            to_str(x/1000 - 9) ++ ' 0,' ++
            to_str(x/1000 - 9) ++ ' 2,' ++
            to_str(x/1000 - 10) ++ ' 2,' ++
            to_str(x/1000 - 10) ++ ' 0))'
        ),
    }
);