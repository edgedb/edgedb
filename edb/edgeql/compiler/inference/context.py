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


from __future__ import annotations
from typing import Optional, Tuple, Union, Dict, FrozenSet, NamedTuple

import dataclasses

from edb.ir import ast as irast
from edb.edgeql import qltypes

from .. import context


@dataclasses.dataclass(frozen=True, eq=False)
class MultiplicityInfo:
    """Extended multiplicity descriptor"""

    #: Actual multiplicity number
    own: qltypes.Multiplicity
    #: Whether this multiplicity descriptor describes
    #: part of a disjoint set.
    disjoint_union: bool = False
    #: Whether this multiplicity descriptor represents
    #: a freshly created free object.
    fresh_free_object: bool = False

    def is_empty(self) -> bool:
        return self.own.is_empty()

    def is_unique(self) -> bool:
        return self.own.is_unique()

    def is_duplicate(self) -> bool:
        return self.own.is_duplicate()


class InfCtx(NamedTuple):
    env: context.Environment
    inferred_cardinality: Dict[
        Union[
            Tuple[irast.Base, irast.ScopeTreeNode, FrozenSet[irast.PathId]],
            irast.Base,
        ],
        qltypes.Cardinality,
    ]
    inferred_multiplicity: Dict[
        Tuple[irast.Base, irast.ScopeTreeNode, Optional[irast.PathId]],
        MultiplicityInfo,
    ]
    singletons: FrozenSet[irast.PathId]
    distinct_iterator: Optional[irast.PathId]
    ignore_computed_cards: bool
    # Whether to make updates to the cardinality fields in the IR/schema.
    # This is used in cases where we need to do a "hypothetical"
    # inference, but don't want to affect real state.
    make_updates: bool


def make_ctx(env: context.Environment) -> InfCtx:
    return InfCtx(
        env=env,
        inferred_cardinality={},
        inferred_multiplicity={},
        singletons=frozenset(env.singletons),
        distinct_iterator=None,
        ignore_computed_cards=False,
        make_updates=True,
    )
