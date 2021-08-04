#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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



INSERT JSONTest {
    number := 0,
    j_string := <json>'the',
    j_number := <json>2,
    j_boolean := <json>true,
    j_array := to_json('[1, 1, 1]'),
    j_object := to_json('{
        "a": 1,
        "b": 2
    }'),
    data := to_json('null'),
    edb_string := 'jumps'
};

INSERT JSONTest {
    number := 1,
    j_string := <json>'quick',
    j_number := <json>2.7,
    j_boolean := <json>false,
    j_array := to_json('[]'),
    j_object := to_json('{
        "b": 1,
        "c": 2
    }'),
    data := to_json('{}'),
    edb_string := 'over'
};

INSERT JSONTest {
    number := 2,
    j_string := <json>'brown',
    j_number := <json>2.71,
    j_boolean := <json>true,
    j_array := to_json('[2, "q", [3], {}, null]'),
};

INSERT JSONTest {
    number := 3,
    data := to_json('[
        1.61,
        null,
        "Fraka",
        8033,
        {
            "a": "apple",
            "b": {
                "foo": 988,
                "bar": [null, null, {"bingo": "42!"}]
            },
            "c": "corn"
        },
        75,
        true
    ]'),
};
