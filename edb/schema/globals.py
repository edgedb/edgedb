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

from edb.edgeql import qltypes

from . import annos as s_anno
from . import expr as s_expr
from . import objects as so
from . import types as s_types


class Global(
    so.QualifiedObject,
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.GLOBAL,
    data_safe=True,
):

    target = so.SchemaField(
        s_types.Type,
        default=None,
        compcoef=0.85,
        special_ddl_syntax=True,
    )

    required = so.SchemaField(
        bool,
        default=False,
        compcoef=0.909,
        special_ddl_syntax=True,
        describe_visibility=(
            so.DescribeVisibilityPolicy.SHOW_IF_EXPLICIT_OR_DERIVED
        ),
    )

    cardinality = so.SchemaField(
        qltypes.SchemaCardinality,
        default=qltypes.SchemaCardinality.One,
        compcoef=0.833,
        coerce=True,
        special_ddl_syntax=True,
        describe_visibility=(
            so.DescribeVisibilityPolicy.SHOW_IF_EXPLICIT_OR_DERIVED
        ),
    )

    # Computable globals have this set to an expression
    # defining them.
    expr = so.SchemaField(
        s_expr.Expression,
        default=None,
        coerce=True,
        compcoef=0.909,
        special_ddl_syntax=True,
    )

    default = so.SchemaField(
        s_expr.Expression,
        allow_ddl_set=True,
        default=None,
        coerce=True,
        compcoef=0.909,
    )
