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


type Subordinate {
    required property name -> str;
    property val -> int64;
}

type InsertTest {
    property name -> str;
    property l1 -> int64;
    required property l2 -> int64;
    property l3 -> str {
        default := "test";
    }
    multi link subordinates -> Subordinate {
        property comment -> str;
    }
    link sub -> Subordinate {
        property note -> str;
    }
    link sub_ex -> Subordinate {
        constraint exclusive;
    }
}

type DerivedTest extending InsertTest;

type Note {
    required property name -> str;
    property note -> str;
    link subject -> Object;
}
type DerivedNote extending Note;

type Person {
    required single property name -> std::str {
        constraint std::exclusive;
        default := "Nemo";
    };
    optional single property tag -> std::str;
    required single property tag2 -> std::str {
        default := "<n/a>";
    };
    optional multi link notes -> Note;
    optional multi property multi_prop -> std::str {
        constraint std::exclusive;
    };
    optional single link note -> Note;
    property case_name -> str {
        constraint exclusive on (str_lower(__subject__));
    }
}
type DerivedPerson extending Person {
    property sub_key -> str {
        constraint exclusive;
    }
};

type Person2 {
    required single property first -> std::str;
    optional single link note -> Note;
    optional multi link notes -> Note;
}

type Person2a extending Person2 {
    required single property last -> std::str;
    constraint exclusive on ((__subject__.first, __subject__.last));
    single link bff -> Person;
    constraint exclusive on ((.first, .bff));
}
type DerivedPerson2a extending Person2a;

type Person2b extending Person2 {
    optional single property last -> std::str;
    property namespace := .first ++ " "; # har, har
    property name {
        using (.namespace ++ .last);
        constraint exclusive;
    }
}
type DerivedPerson2b extending Person2b;

type PersonWrapper {
    required single link person -> Person;
}

type DefaultTest1 {
    property num -> int64 {
        default := 42;
    }
    property foo -> str;
}

type DefaultTest2 {
    property foo -> str;
    required property num -> int64 {
        # XXX: circumventing sequence deficiency
        default := (
            SELECT DefaultTest1.num + 1
            ORDER BY DefaultTest1.num DESC
            LIMIT 1
        );
    }
}

type DefaultTest3 {
    required property foo -> float64 {
        # non-deterministic dynamic value
        default := random();
    }
}

type DefaultTest4 {
    required property bar -> int64 {
        # deterministic dynamic value
        default := (SELECT count(DefaultTest4));
    }
}

type DefaultTest5 {
    required property name -> str;
    link other -> Subordinate {
        # statically defined value
        default := (
            SELECT Subordinate
            FILTER .name = 'DefaultTest5/Sub'
            LIMIT 1
        );
    }
}

type DefaultTest6 {
    required property name -> str;
    link other -> DefaultTest5 {
        # staticly defined insert
        default := (
            INSERT DefaultTest5 {
                name := 'DefaultTest6/5'
            }
        );
    }
}

type DefaultTest7 {
    required property name -> str;
    link other -> DefaultTest6 {
        # staticly defined insert that creates an implicit insert chain
        default := (
            INSERT DefaultTest6 {
                name := 'DefaultTest7/6'
            }
        );
    }
}

# self incrementing integer sequence
scalar type int_seq8_t extending sequence;

type DefaultTest8 {
    required property number -> int_seq8_t {
        # The number values are automatically
        # generated, and are not supposed to be
        # directly writable.
        readonly := true;
    }
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

# types to test some inheritance issues
type InputValue {
    property val -> str;
}

abstract type Callable {
    multi link args -> InputValue;
}

type Field extending Callable {
    # This link 'args' appears to be overriding the overloaded 'args'
    # from Callable.
    overloaded multi link args -> InputValue;
}

type Directive extending Callable;


type SelfRef {
    required property name -> str;
    multi link ref -> SelfRef;
}

type CollectionTest {
    property some_tuple -> tuple<str, int64>;
    multi property some_multi_tuple -> tuple<str, int64>;
    property str_array -> array<str>;
    property float_array -> array<float32>;
}

type ExceptTest {
    required property name -> str;
    property deleted -> bool;
    constraint exclusive on (.name) except (.deleted);
};
type ExceptTestSub extending ExceptTest;
