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


from __future__ import annotations
from typing import *

from edb.common import checked

from edb.edgeql import qltypes

from . import annos as s_anno
from . import expr as s_expr
from . import objects as so
from . import referencing


class AccessPolicy(
    referencing.ReferencedInheritingObject,
    s_anno.AnnotationSubject,
):

    condition = so.SchemaField(
        s_expr.Expression,
        default=None,
        coerce=True,
        compcoef=0.1,
        special_ddl_syntax=True,
    )

    expr = so.SchemaField(
        s_expr.Expression,
        compcoef=0.1,
        special_ddl_syntax=True,
    )

    action = so.SchemaField(
        qltypes.AccessPolicyAction,
        coerce=True,
        compcoef=0.1,
        special_ddl_syntax=True,
    )

    access_kind = so.SchemaField(
        checked.FrozenCheckedList[qltypes.AccessKind],
        coerce=True,
        compcoef=0.1,
        special_ddl_syntax=True,
    )

    subject = so.SchemaField(
        so.InheritingObject,
        compcoef=None,
        inheritable=False)
