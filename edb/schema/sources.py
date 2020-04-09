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


class Source(so.QualifiedObject, indexes.IndexableSubject):
    pointers_refs = so.RefDict(
        attr='pointers',
        requires_explicit_overloaded=True,
        backref_attr='source',
        ref_cls=pointers.Pointer)

    pointers = so.SchemaField(
        so.ObjectIndexByUnqualifiedName[pointers.Pointer],
        inheritable=False, ephemeral=True, coerce=True, compcoef=0.857,
        default=so.DEFAULT_CONSTRUCTOR)

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


def populate_pointer_set_for_source_union(
    schema: s_schema.Schema,
    components: Iterable[Source],
    union: Source,
    *,
    modname: Optional[str] = None,
) -> s_schema.Schema:
    if modname is None:
        modname = '__derived__'

    union_pointers = {}

    for pn, ptr in components[0].get_pointers(schema).items(schema):
        ptrs = [ptr]
        for component in components[1:]:
            other_ptr = component.get_pointers(schema).get(
                schema, pn, None)
            if other_ptr is None:
                break
            ptrs.append(other_ptr)

        if len(ptrs) == len(components):
            # The pointer is present in all components.
            if len(ptrs) == 1:
                ptr = ptrs[0]
            else:
                ptrs = set(ptrs)
                schema, ptr = pointers.get_or_create_union_pointer(
                    schema,
                    ptrname=pn,
                    source=union,
                    direction=pointers.PointerDirection.Outbound,
                    components=ptrs,
                    modname=modname,
                )

            union_pointers[pn] = ptr

    if union_pointers:
        for pn, ptr in union_pointers.items():
            if union.getptr(schema, pn) is None:
                schema = union.add_pointer(schema, ptr)

    return schema
