#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


type Person {
    required property first_name -> str;
    property last_name -> str;
}

type Genre {
    required property name -> str;
}

type Content {
    required property title -> str;
    link genre -> Genre;
}

type Movie extending Content {
    property release_year -> int64;
    multi link actors -> Person;
    link director -> Person;
}

type Book extending Content {
    required property pages -> int16;
}