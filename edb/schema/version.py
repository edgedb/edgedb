# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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

import uuid

from edb.edgeql import qltypes

from . import delta as sd
from . import objects as so


class SchemaVersion(
    so.InternalObject,
    qlkind=qltypes.SchemaObjectClass.SCHEMA_VERSION,
):

    version = so.SchemaField(uuid.UUID)


class SchemaVersionCommandContext(sd.ObjectCommandContext[SchemaVersion]):
    pass


class SchemaVersionCommand(
    sd.ObjectCommand[SchemaVersion],
    context_class=SchemaVersionCommandContext,
):
    pass


class CreateSchemaVersion(
    SchemaVersionCommand,
    sd.CreateObject[SchemaVersion],
):
    pass


class AlterSchemaVersion(
    SchemaVersionCommand,
    sd.AlterObject[SchemaVersion],
):
    pass
