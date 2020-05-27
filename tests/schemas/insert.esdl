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
}

type InsertTest {
    optional property name -> str;
    optional property l1 -> int64;
    required property l2 -> int64;
    optional property l3 -> str {
        default := "test";
    }
    optional multi link subordinates -> Subordinate {
        optional property comment -> str;
    }
    optional link sub -> Subordinate {
        optional property note -> str;
    }
}

type DerivedTest extending InsertTest;

type Note {
    required property name -> str;
    optional property note -> str;
    optional link subject -> Object;
}

type DefaultTest1 {
    optional property num -> int64 {
        default := 42;
    }
    optional property foo -> str;
}

type DefaultTest2 {
    optional property foo -> str;
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
    optional link other -> Subordinate {
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
    optional link other -> DefaultTest5 {
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
    optional link other -> DefaultTest6 {
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

# types to test some inheritance issues
type InputValue {
    optional property val -> str;
}

abstract type Callable {
    optional multi link args -> InputValue;
}

type Field extending Callable {
    # This link 'args' appears to be overriding the overloaded 'args'
    # from Callable.
    overloaded optional multi link args -> InputValue;
}

type Directive extending Callable;


type SelfRef {
    required property name -> str;
    optional multi link ref -> SelfRef;
}

type CollectionTest {
    optional property some_tuple -> tuple<str, int64>;
    optional property str_array -> array<str>;
    optional property float_array -> array<float32>;
}
