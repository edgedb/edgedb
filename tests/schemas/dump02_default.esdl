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

abstract annotation `🍿`;

function `💯`(NAMED ONLY `🙀`: int64) -> int64 {
    using (
        SELECT 100 - `🙀`
    );

    annotation `🍿` := 'fun!';
    volatility := 'IMMUTABLE';
}

type `S p a M` {
    required property `🚀` -> int32;
    property c100 := (SELECT `💯`(`🙀` := .`🚀`));
}

type A {
    required link `s p A m 🤞` -> `S p a M`;
}
