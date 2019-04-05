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


from edb.edgeql import ast as qlast

from . import abc as s_abc
from . import annotations
from . import delta as sd
from . import objects as so


class Database(so.GlobalObject, annotations.AnnotationSubject, s_abc.Database):
    pass


class DatabaseCommandContext(sd.CommandContextToken):
    pass


class DatabaseCommand(sd.GlobalObjectCommand, schema_metaclass=Database,
                      context_class=DatabaseCommandContext):
    pass


class CreateDatabase(DatabaseCommand):
    astnode = qlast.CreateDatabase


class AlterDatabase(DatabaseCommand):
    astnode = qlast.AlterDatabase


class DropDatabase(DatabaseCommand):
    astnode = qlast.DropDatabase
