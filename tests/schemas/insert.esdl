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
    property name -> str;
    property l1 -> int64;
    required property l2 -> int64;
    property l3 -> str {
        default := "test";
    }
    multi link subordinates -> Subordinate {
        property comment -> str;
    }
}

type Annotation {
    required property name -> str;
    property note -> str;
    link subject -> Object;
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

# types to test some inheritance issues
type InputValue {
    property val -> str;
}

abstract type Callable {
    multi link args -> InputValue;
}

type Field extending Callable {
    # This link 'args' appears to be overriding the inherited 'args'
    # from Callable.
    inherited multi link args -> InputValue;
}

type Directive extending Callable;


type SelfRef {
    required property name -> str;
    multi link ref -> SelfRef;
}
