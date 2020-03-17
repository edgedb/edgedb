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

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import annos as s_anno
from . import delta as sd
from . import objects as so


class Module(s_anno.AnnotationSubject,
             qlkind=qltypes.SchemaObjectClass.MODULE):

    builtin = so.SchemaField(
        bool, default=False, compcoef=0.4, introspectable=True)


class ModuleCommandContext(sd.ObjectCommandContext[Module]):
    pass


class ModuleCommand(sd.ObjectCommand[Module], schema_metaclass=Module,
                    context_class=ModuleCommandContext):
    pass


class CreateModule(ModuleCommand, sd.CreateObject[Module]):
    astnode = qlast.CreateModule


class AlterModule(ModuleCommand, sd.AlterObject[Module]):
    astnode = qlast.AlterModule


class DeleteModule(ModuleCommand, sd.DeleteObject[Module]):
    astnode = qlast.DropModule
