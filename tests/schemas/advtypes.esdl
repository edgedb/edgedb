#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

abstract type R {
    required property name -> str {
        delegated constraint exclusive;
    }
}

type A extending R;

type S extending R {
    required property s -> str;
    multi link l_a -> A;
}

type T extending R {
    required property t -> str;
    multi link l_a -> A;
}

abstract type U {
    required property u -> str;
}

type V extending U, S, T;

type W {
    required property name -> str {
        constraint exclusive;
    }
    link w -> W;
}

type X extending W, U;

type Z {
    required property name -> str {
        constraint exclusive;
    };

    # have 'name' in common
    multi link stw0 -> S | T | W;
}

# 3 abstract base types and their concrete permutations
abstract type Ba {
    required property ba -> str;
}

abstract type Bb {
    required property bb -> int64;
}

abstract type Bc {
    required property bc -> float64;
}

type CBa extending Ba;

type CBb extending Bb;

type CBc extending Bc;

type CBaBb extending Ba, Bb;

type CBaBc extending Ba, Bc;

type CBbBc extending Bb, Bc;

type CBaBbBc extending Ba, Bb, Bc;
