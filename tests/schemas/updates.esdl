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
type MajorLifeEvent extending Status;

type Tag {
    required property name -> str {
        constraint exclusive;
    }
    required property flag -> int64 {
        default := 0;
    }
}

type UpdateTest {
    required property name -> str {
        constraint exclusive;
    }
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
        property note -> str;
        property readonly_note -> str {
            readonly := true;
        }
    }
    multi link statuses -> Status;

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

    multi property str_tags -> str;
}

type UpdateTestSubType extending UpdateTest;

type UpdateTestSubSubType extending UpdateTestSubType {
    overloaded link status -> MajorLifeEvent;
    overloaded multi link statuses -> MajorLifeEvent;
};

type CollectionTest {
    required property name -> str;
    property some_tuple -> tuple<str, int64>;
    property str_array -> array<str>;
}

type MultiRequiredTest {
    required property name -> str {
        constraint exclusive;
    };
    required multi property prop -> str;
    required multi link tags -> Tag;
}

type DunderDefaultTest01 {
    required a: int64;
    required b: int64 {
        default := __source__.a+1
    };
    required c: int64 {
        default := 1
    }
}

type DunderDefaultTest02_A {
    required a: int64 {
        default := 1
    };
}

type DunderDefaultTest02_B {
    multi default_with_insert: DunderDefaultTest02_A {
        default := (
            insert DunderDefaultTest02_A {
                a := 1
            }
        )
    };
    multi default_with_update: DunderDefaultTest02_A {
        default := (
            update DunderDefaultTest02_A
            filter DunderDefaultTest02_A.a = 2
            set {
                a := 22
            }
        )
    };
    multi default_with_delete: DunderDefaultTest02_A {
        default := (
            delete DunderDefaultTest02_A
            filter DunderDefaultTest02_A.a = 3
        )
    };
    multi default_with_select: DunderDefaultTest02_A {
        default := (
            select DunderDefaultTest02_A
            filter DunderDefaultTest02_A.a = 4
        )
    };
}
