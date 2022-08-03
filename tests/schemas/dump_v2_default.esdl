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


type Test1 {
    property t1 -> array<tuple<name: str, severity: int16>> {
        # https://github.com/edgedb/edgedb/issues/2606
        default := <array<tuple<name: str, severity: int16>>>[]
    };
};

type Test2 {
    property range_of_int -> range<int64>;
    property range_of_date -> range<datetime>;
    property date_duration -> cal::date_duration;

    access policy test allow all using (true);
};

global foo -> str;
required global bar -> int64 {
    default := -1;
};
global baz := (select TargetA filter .name = global foo);

type TargetA {
    required property name -> str {
        constraint exclusive;
    }
}

type SourceA {
    required property name -> str {
        constraint exclusive;
    }

    link link1 -> TargetA {
        on source delete delete target;
    };
    link link2 -> TargetA {
        on source delete delete target if orphan;
    };
};
