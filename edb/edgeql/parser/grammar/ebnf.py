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


from dataclasses import dataclass
import typing


class Item:
    pass


@dataclass(eq=False, match_args=False)
class Literal(Item):
    token: str


@dataclass(eq=False, match_args=False)
class Reference(Item):
    name: str


@dataclass(eq=False, match_args=False)
class Single(Item):
    inner: Item


@dataclass(eq=False, match_args=False)
class Optional(Single):
    pass


@dataclass(eq=False, match_args=False)
class Multiple(Item):
    inner: typing.Sequence[Item]


@dataclass(eq=False, match_args=False)
class Sequence(Multiple):
    pass


@dataclass(eq=False, match_args=False)
class Choice(Multiple):
    pass


@dataclass(eq=False, match_args=False)
class Production:
    name: str
    item: Item
