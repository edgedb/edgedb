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


function user_func_3(x: int64) -> str {
    using (
        SELECT 'test' ++ <str>x
    );
    volatility := 'IMMUTABLE';
};

# cross-module references
type TestA {
    required property a -> str;
}


type TestB {
    required property b -> str;
    link blink -> TestA;
}


type TestC {
    required property c -> str;
    link clink -> default::DefC;
}
