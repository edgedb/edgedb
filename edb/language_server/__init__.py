#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

import dataclasses
from typing import Generic, Optional, TypeVar


T = TypeVar('T', covariant=True)
E = TypeVar('E', covariant=True)


@dataclasses.dataclass(kw_only=True, slots=True, frozen=True)
class Result(Generic[T, E]):
    ok: Optional[T] = None
    err: Optional[E] = None


def is_schema_file(path: str) -> bool:
    return path.endswith(('.esdl', '.gel'))
