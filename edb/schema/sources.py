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
from typing import (
    Optional,
    Tuple,
    Type,
    TypeVar,
    Iterable,
    Sequence,
    List,
    Set,
    overload,
    TYPE_CHECKING,
)


from edb import errors

from . import delta as sd
from . import indexes
from . import name as sn
from . import objects as so
from . import pointers as s_pointers

if TYPE_CHECKING:
    from . import links
    from . import schema as s_schema


Source_T = TypeVar('Source_T', bound='Source')


class SourceCommandContext(
    sd.ObjectCommandContext[Source_T],
    indexes.IndexSourceCommandContext,
):
    # context mixin
    pass


class SourceCommand(indexes.IndexSourceCommand[Source_T]):
    pass


class Source(
    so.QualifiedObject,
    indexes.IndexableSubject,
    so.Object,  # Help reflection figure out the right db MRO
):
    pointers_refs = so.RefDict(
        attr='pointers',
        requires_explicit_overloaded=True,
        backref_attr='source',
        ref_cls=s_pointers.Pointer)

    pointers = so.SchemaField(
        so.ObjectIndexByUnqualifiedName[s_pointers.Pointer],
        inheritable=False, ephemeral=True, coerce=True, compcoef=0.857,
        default=so.DEFAULT_CONSTRUCTOR)

    @overload
    def maybe_get_ptr(
        self,
        schema: s_schema.Schema,
        name: sn.UnqualName,
        *,
        type: Type[s_pointers.Pointer_T],
    ) -> Optional[s_pointers.Pointer_T]:
        ...

    @overload
    def maybe_get_ptr(
        self,
        schema: s_schema.Schema,
        name: sn.UnqualName,
        *,
        type: Optional[Type[s_pointers.Pointer_T]] = None,
    ) -> Optional[s_pointers.Pointer]:
        ...

    def maybe_get_ptr(
        self,
        schema: s_schema.Schema,
        name: sn.UnqualName,
        *,
        type: Optional[Type[s_pointers.Pointer_T]] = None,
    ) -> Optional[s_pointers.Pointer]:
        ptr = self.get_pointers(schema).get(schema, name, None)
        if ptr is not None and type is not None and not isinstance(ptr, type):
            raise AssertionError(
                f'{self.get_verbosename(schema)} has a the '
                f' {str(name)!r} pointer, but it is not a'
                f' {type.get_schema_class_displayname()}'
            )
        return ptr

    @overload
    def getptr(
        self,
        schema: s_schema.Schema,
        name: sn.UnqualName,
        *,
        type: Type[s_pointers.Pointer_T],
    ) -> s_pointers.Pointer_T:
        ...

    @overload
    def getptr(
        self,
        schema: s_schema.Schema,
        name: sn.UnqualName,
        *,
        type: Optional[Type[s_pointers.Pointer_T]] = None,
    ) -> s_pointers.Pointer:
        ...

    def getptr(
        self,
        schema: s_schema.Schema,
        name: sn.UnqualName,
        *,
        type: Optional[Type[s_pointers.Pointer_T]] = None,
    ) -> s_pointers.Pointer:
        ptr = self.maybe_get_ptr(schema, name, type=type)
        if ptr is None:
            raise AssertionError(
                f'{self.get_verbosename(schema)} has no'
                f' link or property {str(name)!r}'
            )
        return ptr

    def getrptrs(
        self,
        schema: s_schema.Schema,
        name: str,
        *,
        sources: Iterable[so.Object] = ()
    ) -> Set[links.Link]:
        return set()

    def add_pointer(
        self,
        schema: s_schema.Schema,
        pointer: s_pointers.Pointer,
        *,
        replace: bool = False
    ) -> s_schema.Schema:
        schema = self.add_classref(
            schema, 'pointers', pointer, replace=replace)
        return schema

    def get_addon_columns(
        self, schema: s_schema.Schema
    ) -> Sequence[Tuple[str, str, Tuple[str, str]]]:
        """
        Returns a list of columns that are present in the backing table of
        this source, apart from the columns for pointers.
        """
        res = []
        from edb.common import debug

        if not debug.flags.zombodb:
            fts_index, _ = indexes.get_effective_object_index(
                schema, self, sn.QualName("std::fts", "index")
            )

            if fts_index:
                res.append(
                    (
                        '__fts_document__',
                        '__fts_document__',
                        (
                            'pg_catalog',
                            'tsvector',
                        ),
                    )
                )

        ext_ai_index, _ = indexes.get_effective_object_index(
            schema, self, sn.QualName("ext::ai", "index")
        )
        if ext_ai_index:
            idx_id = indexes.get_ai_index_id(schema, ext_ai_index)
            dimensions = ext_ai_index.must_get_json_annotation(
                schema,
                sn.QualName(
                    "ext::ai", "embedding_dimensions"),
                int,
            )
            res.append(
                (
                    f'__ext_ai_{idx_id}_embedding__',
                    f'__ext_ai_{idx_id}_embedding__',
                    (
                        'edgedb',
                        f'vector({dimensions})',
                    ),
                )
            )

        return res


def populate_pointer_set_for_source_union(
    schema: s_schema.Schema,
    components: List[Source],
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
                try:
                    schema, ptr = s_pointers.get_or_create_union_pointer(
                        schema,
                        ptrname=pn,
                        source=union,
                        direction=s_pointers.PointerDirection.Outbound,
                        components=set(ptrs),
                        modname=modname,
                    )
                except errors.SchemaError as e:
                    # ptrs may have different verbose names
                    # ensure the same one is always chosen
                    vn = sorted(p.get_verbosename(schema) for p in ptrs)[0]
                    e.args = (
                        (f'with {vn} {e.args[0]}',)
                        + e.args[1:]
                    )
                    raise e

            union_pointers[pn] = ptr

    if union_pointers:
        for pn, ptr in union_pointers.items():
            if union.maybe_get_ptr(schema, pn) is None:
                schema = union.add_pointer(schema, ptr)

    return schema
