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
from typing import *  # NoQA

from . import indexes
from . import name as sn
from . import objects as so
from . import pointers

if TYPE_CHECKING:
    from . import schema as s_schema


class SourceCommandContext(indexes.IndexSourceCommandContext):
    # context mixin
    pass


class SourceCommand(indexes.IndexSourceCommand):
    pass


class Source(indexes.IndexableSubject):
    pointers_refs = so.RefDict(
        attr='pointers',
        requires_explicit_overloaded=True,
        backref_attr='source',
        ref_cls=pointers.Pointer)

    pointers = so.SchemaField(
        so.ObjectIndexByUnqualifiedName,
        inheritable=False, ephemeral=True, coerce=True, compcoef=0.857,
        default=so.ObjectIndexByUnqualifiedName)

    def getptr(
        self,
        schema: s_schema.Schema,
        name: str,
    ) -> Optional[pointers.Pointer]:
        if sn.Name.is_qualified(name):
            raise ValueError(
                'references to concrete pointers must not be qualified')
        return self.get_pointers(schema).get(schema, name, None)

    def getrptrs(self, schema, name, *, sources=()):
        return set()

    def add_pointer(self, schema, pointer, *, replace=False):
        schema = self.add_classref(
            schema, 'pointers', pointer, replace=replace)
        return schema
