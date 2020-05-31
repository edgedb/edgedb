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


type JSONTest {
    required property number -> int64 {
        constraint exclusive;
    }

    # these properties are intended to have specific JSON types in them
    optional property j_string -> json;
    optional property j_number -> json;
    optional property j_boolean -> json;
    optional property j_array -> json;
    optional property j_object -> json;

    # this property is for more generic JSON handling
    optional property data -> json;

    # these properties are used for testing casting
    optional property edb_string -> str;
}
