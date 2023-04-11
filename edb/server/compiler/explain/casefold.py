#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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

import re

# This matches spaces, minus or an empty string that comes before capital
# letter (and not at the start of the string).
# And is used to replace that word boundary for the underscore.
# It handles cases like this:
# * `Foo Bar` -- title case -- matches space
# * `FooBar` -- CamelCase -- matches empty string before `Bar`
# * `Some-word` -- words with dash -- matches dash
word_boundary_re = re.compile(r'(?<!^)(?<!\s|-)[\s-]*(?=[A-Z])')


def to_snake_case(name: str) -> str:
    # note this only covers cases we have not all possible cases of
    # case conversion
    return word_boundary_re.sub('_', name).lower()


def to_camel_case(name: str) -> str:
    # note this only covers cases we have not all possible cases of
    # case conversion
    return word_boundary_re.sub('', name)
