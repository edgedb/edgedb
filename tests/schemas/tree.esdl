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


# A tree setup in a simple way with a parent link and computable children.
type Tree {
    required property val -> str {
        constraint exclusive;
    }

    link parent -> Tree;
    multi link children := .<parent[IS Tree];
}


# A tree setup in a reverse way compared to Tree: children links are
# real and parent is computable.
type Eert {
    required property val -> str {
        constraint exclusive;
    }

    link parent := .<children[IS Eert];
    multi link children -> Eert {
        constraint exclusive;
    }
}
