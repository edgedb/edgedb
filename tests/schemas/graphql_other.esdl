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


scalar type ColorEnum extending enum<'RED', 'GREEN', 'BLUE'> {
    annotation description := 'RGB color enum';
}

type Foo {
    annotation description := 'Test type "Foo"';

    property `select` -> str;
    property after -> str;
    required property color -> ColorEnum;
    multi property multi_color -> ColorEnum;
    property color_array -> array<ColorEnum>;

    # Testing linking to the same type
    multi link foos -> Foo {
        on target delete deferred restrict;
    }
}
