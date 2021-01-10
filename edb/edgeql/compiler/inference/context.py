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
from typing import *

from edb.ir import ast as irast
from edb.edgeql import qltypes

from .. import context


class InfCtx(NamedTuple):
    env: context.Environment
    inferred_cardinality: Dict[
        Tuple[irast.Base, irast.ScopeTreeNode],
        qltypes.Cardinality]
    inferred_multiplicity: Dict[
        Tuple[irast.Base, irast.ScopeTreeNode],
        qltypes.Multiplicity]
    singletons: Collection[irast.PathId]
    bindings: Dict[irast.PathId, irast.ScopeTreeNode]
    volatile_uses: Dict[irast.PathId, irast.ScopeTreeNode]
    in_for_body: bool


def make_ctx(env: context.Environment) -> InfCtx:
    return InfCtx(
        env=env,
        inferred_cardinality={},
        inferred_multiplicity={},
        singletons=frozenset(env.singletons),
        bindings={},
        volatile_uses={},
        in_for_body=False,
    )
