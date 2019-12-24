#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


type Status {
    required property name -> str {
        constraint exclusive;
    }
}

type Tag {
    required property name -> str {
        constraint exclusive;
    }
}

type UpdateTest {
    required property name -> str;
    property comment -> str;

    # for testing singleton links
    link status -> Status;
    link annotated_status -> Status {
        property note -> str;
    }

    # for testing links to sets
    multi link tags -> Tag;
    multi link weighted_tags -> Tag {
        property weight -> int64;
        property readonly_note -> str {
            readonly := true;
        }
    }

    # for testing links to sets of the same type as originator
    multi link related -> UpdateTest;
    multi link annotated_tests -> UpdateTest {
        property note -> str;
    }

    link readonly_tag -> Tag {
        readonly := true;
    }

    property readonly_note -> str {
        readonly := true;
    }
}

type CollectionTest {
    required property name -> str;
    property some_tuple -> tuple<str, int64>;
    property str_array -> array<str>;
}
