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

import abc
import collections
import functools
import itertools

import immutables as immu

from edb import errors

from . import casts as s_casts
from . import expr as s_expr
from . import functions as s_func
from . import migrations as s_migrations
from . import modules as s_mod
from . import name as sn
from . import objects as so
from . import operators as s_oper
from . import pseudo as s_pseudo
from . import types as s_types

if TYPE_CHECKING:
    import uuid
    from edb.common import parsing

    Refs_T = immu.Map[
        uuid.UUID,
        immu.Map[
            Tuple[Type[so.Object], str],
            immu.Map[uuid.UUID, None],
        ],
    ]

STD_LIB = ('std', 'schema', 'math', 'sys', 'cfg', 'cal')
STD_MODULES = frozenset(STD_LIB + ('stdgraphql',))


Schema_T = TypeVar('Schema_T', bound='Schema')


class Schema(abc.ABC):

    @abc.abstractmethod
    def add(
        self: Schema_T,
        id: uuid.UUID,
        scls: so.Object,
        data: Mapping[str, Any],
    ) -> Schema_T:
        raise NotImplementedError

    @abc.abstractmethod
    def discard(self: Schema_T, obj: so.Object) -> Schema_T:
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self: Schema_T, obj: so.Object) -> Schema_T:
        raise NotImplementedError

    @abc.abstractmethod
    def update_obj(
        self: Schema_T,
        obj_id: uuid.UUID,
        updates: Mapping[str, Any],
    ) -> Schema_T:
        raise NotImplementedError

    @abc.abstractmethod
    def get_obj_field(
        self,
        obj_id: uuid.UUID,
        field: str,
    ) -> Any:
        raise NotImplementedError

    @abc.abstractmethod
    def set_obj_field(
        self: Schema_T,
        obj_id: uuid.UUID,
        field: str,
        value: Any,
    ) -> Schema_T:
        raise NotImplementedError

    @abc.abstractmethod
    def unset_obj_field(
        self: Schema_T,
        obj_id: uuid.UUID,
        field: str,
    ) -> Schema_T:
        raise NotImplementedError

    @abc.abstractmethod
    def get_functions(
        self,
        name: str,
        default: Union[
            Tuple[s_func.Function, ...], so.NoDefaultT
        ] = so.NoDefault,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
    ) -> Tuple[s_func.Function, ...]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_operators(
        self,
        name: str,
        default: Union[
            Tuple[s_oper.Operator, ...], so.NoDefaultT
        ] = so.NoDefault,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
    ) -> Tuple[s_oper.Operator, ...]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_casts_to_type(
        self,
        to_type: s_types.Type,
        *,
        implicit: bool = False,
        assignment: bool = False,
    ) -> FrozenSet[s_casts.Cast]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_casts_from_type(
        self,
        from_type: s_types.Type,
        *,
        implicit: bool = False,
        assignment: bool = False,
    ) -> FrozenSet[s_casts.Cast]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_referrers(
        self,
        scls: so.Object,
        *,
        scls_type: Optional[Type[so.Object]] = None,
        field_name: Optional[str] = None,
    ) -> FrozenSet[so.Object]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_referrers_ex(
        self,
        scls: so.Object,
    ) -> Dict[
        Tuple[Type[so.Object], str],
        FrozenSet[so.Object],
    ]:
        raise NotImplementedError

    @overload
    def get_by_id(
        self,
        obj_id: uuid.UUID,
        default: Union[so.Object, so.NoDefaultT] = so.NoDefault,
        *,
        type: None = None,
    ) -> so.Object:
        ...

    @overload
    def get_by_id(  # NoQA: F811
        self,
        obj_id: uuid.UUID,
        default: Union[so.Object_T, so.NoDefaultT] = so.NoDefault,
        *,
        type: Optional[Type[so.Object_T]] = None,
    ) -> so.Object_T:
        ...

    @overload
    def get_by_id(  # NoQA: F811
        self,
        obj_id: uuid.UUID,
        default: None = None,
        *,
        type: Optional[Type[so.Object_T]] = None,
    ) -> Optional[so.Object_T]:
        ...

    @abc.abstractmethod
    def get_by_id(  # NoQA: F811
        self,
        obj_id: uuid.UUID,
        default: Union[so.Object_T, so.NoDefaultT, None] = so.NoDefault,
        *,
        type: Optional[Type[so.Object_T]] = None,
    ) -> Optional[so.Object_T]:
        raise NotImplementedError

    @overload
    def get_global(
        self,
        objtype: Type[so.Object_T],
        name: str,
        default: Union[so.Object_T, so.NoDefaultT] = so.NoDefault,
    ) -> so.Object_T:
        ...

    @overload
    def get_global(  # NoQA: F811
        self,
        objtype: Type[so.Object_T],
        name: str,
        default: None = None,
    ) -> Optional[so.Object_T]:
        ...

    @abc.abstractmethod
    def get_global(  # NoQA: F811
        self,
        objtype: Type[so.Object_T],
        name: str,
        default: Union[so.Object_T, so.NoDefaultT, None] = so.NoDefault,
    ) -> Optional[so.Object_T]:
        raise NotImplementedError

    @overload
    def get(  # NoQA: F811
        self,
        name: str,
        default: Union[so.Object_T, so.NoDefaultT] = so.NoDefault,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
        condition: Optional[Callable[[so.Object], bool]] = None,
        label: Optional[str] = None,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> so.Object:
        ...

    @overload
    def get(  # NoQA: F811
        self,
        name: str,
        default: None,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
        condition: Optional[Callable[[so.Object], bool]] = None,
        label: Optional[str] = None,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> Optional[so.Object]:
        ...

    @overload
    def get(  # NoQA: F811
        self,
        name: str,
        default: Union[so.Object_T, so.NoDefaultT] = so.NoDefault,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
        type: Type[so.Object_T],
        condition: Optional[Callable[[so.Object], bool]] = None,
        label: Optional[str] = None,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> so.Object_T:
        ...

    @overload
    def get(  # NoQA: F811
        self,
        name: str,
        default: None,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
        type: Type[so.Object_T],
        condition: Optional[Callable[[so.Object], bool]] = None,
        label: Optional[str] = None,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> Optional[so.Object_T]:
        ...

    @overload
    def get(  # NoQA: F811
        self,
        name: str,
        default: Union[so.Object, so.NoDefaultT, None] = so.NoDefault,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
        type: Optional[Type[so.Object_T]] = None,
        condition: Optional[Callable[[so.Object], bool]] = None,
        label: Optional[str] = None,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> Optional[so.Object]:
        ...

    def get(  # NoQA: F811
        self,
        name: str,
        default: Union[so.Object, so.NoDefaultT, None] = so.NoDefault,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
        type: Optional[Type[so.Object_T]] = None,
        condition: Optional[Callable[[so.Object], bool]] = None,
        label: Optional[str] = None,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> Optional[so.Object]:
        return self.get_generic(
            name,
            default,
            module_aliases=module_aliases,
            type=type,
            condition=condition,
            label=label,
            sourcectx=sourcectx,
        )

    @abc.abstractmethod
    def get_generic(  # NoQA: F811
        self,
        name: str,
        default: Union[so.Object, so.NoDefaultT, None],
        *,
        module_aliases: Optional[Mapping[Optional[str], str]],
        type: Optional[Type[so.Object_T]],
        condition: Optional[Callable[[so.Object], bool]],
        label: Optional[str],
        sourcectx: Optional[parsing.ParserContext],
    ) -> Optional[so.Object]:
        raise NotImplementedError

    @abc.abstractmethod
    def has_object(self, object_id: uuid.UUID) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def has_module(self, module: str) -> bool:
        raise NotImplementedError

    def get_children(
        self,
        scls: so.Object_T,
    ) -> FrozenSet[so.Object_T]:
        # Ideally get_referrers needs to be made generic via
        # an overload on scls_type, but mypy crashes on that.
        return self.get_referrers(  # type: ignore
            scls,
            scls_type=type(scls),
            field_name='bases',
        )

    def get_descendants(
        self,
        scls: so.Object_T,
    ) -> FrozenSet[so.Object_T]:
        return self.get_referrers(  # type: ignore
            scls, scls_type=type(scls), field_name='ancestors')

    @abc.abstractmethod
    def get_objects(
        self,
        *,
        exclude_stdlib: bool = False,
        included_modules: Optional[Iterable[str]] = None,
        excluded_modules: Optional[Iterable[str]] = None,
        included_items: Optional[Iterable[str]] = None,
        excluded_items: Optional[Iterable[str]] = None,
        type: Optional[Type[so.Object_T]] = None,
        extra_filters: Iterable[Callable[[Schema, so.Object], bool]] = (),
    ) -> SchemaIterator[so.Object_T]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_modules(self) -> Tuple[s_mod.Module, ...]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_last_migration(self) -> Optional[s_migrations.Migration]:
        raise NotImplementedError


class FlatSchema(Schema):

    _id_to_data: immu.Map[uuid.UUID, immu.Map[str, Any]]
    _id_to_type: immu.Map[uuid.UUID, so.Object]
    _name_to_id: immu.Map[str, uuid.UUID]
    _shortname_to_id: immu.Map[
        Tuple[Type[so.Object], str],
        FrozenSet[uuid.UUID]
    ]
    _globalname_to_id: immu.Map[Tuple[Type[so.Object], str], uuid.UUID]
    _refs_to: Refs_T
    _generation: int

    def __init__(self) -> None:
        self._id_to_data = immu.Map()
        self._id_to_type = immu.Map()
        self._shortname_to_id = immu.Map()
        self._name_to_id = immu.Map()
        self._globalname_to_id = immu.Map()
        self._refs_to = immu.Map()
        self._generation = 0

    def _replace(
        self,
        *,
        id_to_data: Optional[immu.Map[uuid.UUID, immu.Map[str, Any]]] = None,
        id_to_type: Optional[immu.Map[uuid.UUID, so.Object]] = None,
        name_to_id: Optional[immu.Map[str, uuid.UUID]] = None,
        shortname_to_id: Optional[
            immu.Map[
                Tuple[Type[so.Object], str],
                FrozenSet[uuid.UUID]
            ]
        ],
        globalname_to_id: Optional[
            immu.Map[Tuple[Type[so.Object], str], uuid.UUID]
        ],
        refs_to: Optional[Refs_T] = None,
    ) -> FlatSchema:
        new = FlatSchema.__new__(FlatSchema)

        if id_to_data is None:
            new._id_to_data = self._id_to_data
        else:
            new._id_to_data = id_to_data

        if id_to_type is None:
            new._id_to_type = self._id_to_type
        else:
            new._id_to_type = id_to_type

        if name_to_id is None:
            new._name_to_id = self._name_to_id
        else:
            new._name_to_id = name_to_id

        if shortname_to_id is None:
            new._shortname_to_id = self._shortname_to_id
        else:
            new._shortname_to_id = shortname_to_id

        if globalname_to_id is None:
            new._globalname_to_id = self._globalname_to_id
        else:
            new._globalname_to_id = globalname_to_id

        if refs_to is None:
            new._refs_to = self._refs_to
        else:
            new._refs_to = refs_to

        new._generation = self._generation + 1

        return new  # type: ignore

    def _update_obj_name(
        self,
        obj_id: uuid.UUID,
        scls: so.Object,
        old_name: Optional[str],
        new_name: Optional[str],
    ) -> Tuple[
        immu.Map[str, uuid.UUID],
        immu.Map[Tuple[Type[so.Object], str], FrozenSet[uuid.UUID]],
        immu.Map[Tuple[Type[so.Object], str], uuid.UUID],
    ]:
        name_to_id = self._name_to_id
        shortname_to_id = self._shortname_to_id
        globalname_to_id = self._globalname_to_id
        stype = type(scls)
        is_global = not issubclass(stype, so.QualifiedObject)

        has_sn_cache = issubclass(stype, (s_func.Function, s_oper.Operator))

        if old_name is not None:
            if is_global:
                globalname_to_id = globalname_to_id.delete((stype, old_name))
            else:
                name_to_id = name_to_id.delete(old_name)
            if has_sn_cache:
                old_shortname = sn.shortname_from_fullname(old_name)
                sn_key = (stype, old_shortname)

                new_ids = shortname_to_id[sn_key] - {obj_id}
                if new_ids:
                    shortname_to_id = shortname_to_id.set(
                        sn_key, new_ids)
                else:
                    shortname_to_id = shortname_to_id.delete(sn_key)

        if new_name is not None:
            if is_global:
                key = (stype, new_name)
                if key in globalname_to_id:
                    raise errors.SchemaError(
                        f'{stype.__name__} {new_name!r} '
                        f'is already present in the schema')
                globalname_to_id = globalname_to_id.set(key, obj_id)
            else:
                assert isinstance(new_name, sn.Name)
                if (
                    not self.has_module(new_name.module)
                    and new_name.module != '__derived__'
                ):
                    raise errors.UnknownModuleError(
                        f'module {new_name.module!r} is not in this schema')

                if new_name in name_to_id:
                    raise errors.SchemaError(
                        f'name {new_name!r} is already in the schema')
                name_to_id = name_to_id.set(new_name, obj_id)

            if has_sn_cache:
                new_shortname = sn.shortname_from_fullname(new_name)
                sn_key = (stype, new_shortname)

                try:
                    ids = shortname_to_id[sn_key]
                except KeyError:
                    ids = frozenset()

                shortname_to_id = shortname_to_id.set(
                    sn_key, ids | {obj_id})

        return name_to_id, shortname_to_id, globalname_to_id

    def update_obj(
        self,
        obj_id: uuid.UUID,
        updates: Mapping[str, Any],
    ) -> FlatSchema:
        if not updates:
            return self

        try:
            data = self._id_to_data[obj_id]
        except KeyError:
            data = immu.Map()

        name_to_id = None
        shortname_to_id = None
        globalname_to_id = None
        with data.mutate() as mm:
            for field, value in updates.items():
                if field == 'name':
                    name_to_id, shortname_to_id, globalname_to_id = (
                        self._update_obj_name(
                            obj_id,
                            self._id_to_type[obj_id],
                            mm.get('name'),
                            value
                        )
                    )

                if value is None:
                    mm.pop(field, None)
                else:
                    mm[field] = value

            new_data = mm.finish()

        id_to_data = self._id_to_data.set(obj_id, new_data)
        scls = self._id_to_type[obj_id]
        refs_to = self._update_refs_to(obj_id, type(scls), data, new_data)
        return self._replace(name_to_id=name_to_id,
                             shortname_to_id=shortname_to_id,
                             globalname_to_id=globalname_to_id,
                             id_to_data=id_to_data,
                             refs_to=refs_to)

    def get_obj_field(
        self,
        obj_id: uuid.UUID,
        field: str,
    ) -> Any:
        try:
            d = self._id_to_data[obj_id]
        except KeyError:
            err = (f'cannot get {field!r} value: item {str(obj_id)!r} '
                   f'is not present in the schema {self!r}')
            raise errors.SchemaError(err) from None

        return d.get(field)

    def set_obj_field(
        self,
        obj_id: uuid.UUID,
        field: str,
        value: Any,
    ) -> FlatSchema:
        try:
            data = self._id_to_data[obj_id]
        except KeyError:
            err = (f'cannot set {field!r} value: item {str(obj_id)!r} '
                   f'is not present in the schema {self!r}')
            raise errors.SchemaError(err) from None

        name_to_id = None
        shortname_to_id = None
        globalname_to_id = None
        if field == 'name':
            old_name = data.get('name')
            name_to_id, shortname_to_id, globalname_to_id = (
                self._update_obj_name(
                    obj_id,
                    self._id_to_type[obj_id],
                    old_name,
                    value
                )
            )

        new_data = data.set(field, value)
        id_to_data = self._id_to_data.set(obj_id, new_data)
        scls = self._id_to_type[obj_id]

        if field in data:
            orig_field_data = {field: data[field]}
        else:
            orig_field_data = {}

        refs_to = self._update_refs_to(
            obj_id, type(scls), orig_field_data, {field: value})

        return self._replace(name_to_id=name_to_id,
                             shortname_to_id=shortname_to_id,
                             globalname_to_id=globalname_to_id,
                             id_to_data=id_to_data,
                             refs_to=refs_to)

    def unset_obj_field(
        self,
        obj_id: uuid.UUID,
        field: str,
    ) -> FlatSchema:
        try:
            data = self._id_to_data[obj_id]
        except KeyError:
            return self

        name_to_id = None
        shortname_to_id = None
        globalname_to_id = None
        name = data.get('name')
        if field == 'name' and name is not None:
            name_to_id, shortname_to_id, globalname_to_id = (
                self._update_obj_name(
                    obj_id,
                    self._id_to_type[obj_id],
                    name,
                    None
                )
            )
            new_data = data.delete(field)
        else:
            try:
                new_data = data.delete(field)
            except KeyError:
                return self

        id_to_data = self._id_to_data.set(obj_id, new_data)
        scls = self._id_to_type[obj_id]
        refs_to = self._update_refs_to(
            obj_id, type(scls), {field: data[field]}, None)

        return self._replace(name_to_id=name_to_id,
                             shortname_to_id=shortname_to_id,
                             globalname_to_id=globalname_to_id,
                             id_to_data=id_to_data,
                             refs_to=refs_to)

    def _update_refs_to(
        self,
        object_id: uuid.UUID,
        schemaclass: Type[so.Object],
        orig_data: Optional[Mapping[str, Any]],
        new_data: Optional[Mapping[str, Any]],
    ) -> Refs_T:
        objfields = schemaclass.get_object_fields()
        if not objfields:
            return self._refs_to

        with self._refs_to.mutate() as mm:
            for field in objfields:
                if not new_data:
                    ids = None
                else:
                    try:
                        ref = new_data[field.name]
                    except KeyError:
                        ids = None
                    else:
                        if isinstance(ref, so.ObjectCollection):
                            ids = frozenset(ref.ids(self))
                        elif isinstance(ref, s_expr.Expression):
                            if ref.refs:
                                ids = frozenset(ref.refs.ids(self))
                            else:
                                ids = frozenset()
                        else:
                            ids = frozenset((ref.id,))

                if not orig_data:
                    orig_ids = None
                else:
                    try:
                        ref = orig_data[field.name]
                    except KeyError:
                        orig_ids = None
                    else:
                        if isinstance(ref, so.ObjectCollection):
                            orig_ids = frozenset(ref.ids(self))
                        elif isinstance(ref, s_expr.Expression):
                            if ref.refs:
                                orig_ids = frozenset(ref.refs.ids(self))
                            else:
                                orig_ids = frozenset()
                        else:
                            orig_ids = frozenset((ref.id,))

                if not ids and not orig_ids:
                    continue

                old_ids: Optional[FrozenSet[uuid.UUID]]
                new_ids: Optional[FrozenSet[uuid.UUID]]

                key = (schemaclass, field.name)

                if ids and orig_ids:
                    new_ids = ids - orig_ids
                    old_ids = orig_ids - ids
                elif ids:
                    new_ids = ids
                    old_ids = None
                else:
                    new_ids = None
                    old_ids = orig_ids

                if new_ids:
                    for ref_id in new_ids:
                        try:
                            refs = mm[ref_id]
                        except KeyError:
                            mm[ref_id] = immu.Map((
                                (key, immu.Map(((object_id, None),))),
                            ))
                        else:
                            try:
                                field_refs = refs[key]
                            except KeyError:
                                field_refs = immu.Map(((object_id, None),))
                            else:
                                field_refs = field_refs.set(object_id, None)
                            mm[ref_id] = refs.set(key, field_refs)

                if old_ids:
                    for ref_id in old_ids:
                        refs = mm[ref_id]
                        field_refs = refs[key].delete(object_id)
                        if not field_refs:
                            mm[ref_id] = refs.delete(key)
                        else:
                            mm[ref_id] = refs.set(key, field_refs)

            return mm.finish()

    def add(
        self,
        id: uuid.UUID,
        scls: so.Object,
        data: Mapping[str, Any],
    ) -> FlatSchema:
        name = data['name']

        if name in self._name_to_id:
            raise errors.SchemaError(
                f'{type(scls).__name__} {name!r} is already present '
                f'in the schema {self!r}')

        data = immu.Map(data)

        name_to_id, shortname_to_id, globalname_to_id = self._update_obj_name(
            id, scls, None, name)

        updates = dict(
            id_to_data=self._id_to_data.set(id, data),
            id_to_type=self._id_to_type.set(id, scls),
            name_to_id=name_to_id,
            shortname_to_id=shortname_to_id,
            globalname_to_id=globalname_to_id,
            refs_to=self._update_refs_to(id, type(scls), None, data),
        )

        if (
            isinstance(scls, so.QualifiedObject)
            and not self.has_module(name.module)
            and name.module != '__derived__'
        ):
            raise errors.UnknownModuleError(
                f'module {name.module!r} is not in this schema')

        return self._replace(**updates)  # type: ignore

    def _delete(self, obj: so.Object) -> FlatSchema:
        data = self._id_to_data.get(obj.id)
        if data is None:
            raise errors.InvalidReferenceError(
                f'cannot delete {obj!r}: not in this schema')

        name = data['name']

        updates = {}

        name_to_id, shortname_to_id, globalname_to_id = self._update_obj_name(
            obj.id, self._id_to_type[obj.id], name, None)

        refs_to = self._update_refs_to(
            obj.id, type(obj), self._id_to_data[obj.id], None)

        updates.update(dict(
            name_to_id=name_to_id,
            shortname_to_id=shortname_to_id,
            globalname_to_id=globalname_to_id,
            id_to_data=self._id_to_data.delete(obj.id),
            id_to_type=self._id_to_type.delete(obj.id),
            refs_to=refs_to,
        ))

        return self._replace(**updates)  # type: ignore

    def discard(self, obj: so.Object) -> FlatSchema:
        if obj.id in self._id_to_data:
            return self._delete(obj)
        else:
            return self

    def delete(self, obj: so.Object) -> FlatSchema:
        return self._delete(obj)

    def _get(
        self,
        name: str,
        *,
        getter: Callable[[FlatSchema, str], Any],
        default: Any,
        module_aliases: Optional[Mapping[Optional[str], str]],
    ) -> Any:
        name, module, shortname = sn.split_name(name)
        implicit_builtins = module is None

        if module == '__std__':
            fqname = sn.SchemaName(shortname, 'std')
            result = getter(self, fqname)
            if result is not None:
                return result
            else:
                return default

        if module_aliases is not None:
            fq_module = module_aliases.get(module)
            if fq_module is not None:
                module = fq_module

        if module is not None:
            fqname = sn.SchemaName(shortname, module)
            result = getter(self, fqname)
            if result is not None:
                return result

        if implicit_builtins:
            fqname = sn.SchemaName(shortname, 'std')
            result = getter(self, fqname)
            if result is not None:
                return result

        return default

    def get_functions(
        self,
        name: str,
        default: Union[
            Tuple[s_func.Function, ...], so.NoDefaultT
        ] = so.NoDefault,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
    ) -> Tuple[s_func.Function, ...]:
        funcs = self._get(name,
                          getter=_get_functions,
                          module_aliases=module_aliases,
                          default=default)

        if funcs is not so.NoDefault:
            return cast(
                Tuple[s_func.Function, ...],
                funcs,
            )

        raise errors.InvalidReferenceError(
            f'function {name!r} does not exist')

    def get_operators(
        self,
        name: str,
        default: Union[
            Tuple[s_oper.Operator, ...], so.NoDefaultT
        ] = so.NoDefault,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
    ) -> Tuple[s_oper.Operator, ...]:
        funcs = self._get(name,
                          getter=_get_operators,
                          module_aliases=module_aliases,
                          default=default)

        if funcs is not so.NoDefault:
            return cast(
                Tuple[s_oper.Operator, ...],
                funcs,
            )

        raise errors.InvalidReferenceError(
            f'operator {name!r} does not exist')

    @functools.lru_cache()
    def _get_casts(
        self,
        stype: s_types.Type,
        *,
        disposition: str,
        implicit: bool = False,
        assignment: bool = False,
    ) -> FrozenSet[s_casts.Cast]:

        all_casts = cast(
            FrozenSet[s_casts.Cast],
            self.get_referrers(
                stype, scls_type=s_casts.Cast, field_name=disposition),
        )

        casts = []
        for castobj in all_casts:
            if implicit and not castobj.get_allow_implicit(self):
                continue
            if assignment and not castobj.get_allow_assignment(self):
                continue
            casts.append(castobj)

        return frozenset(casts)

    def get_casts_to_type(
        self,
        to_type: s_types.Type,
        *,
        implicit: bool = False,
        assignment: bool = False,
    ) -> FrozenSet[s_casts.Cast]:
        return self._get_casts(to_type, disposition='to_type',
                               implicit=implicit, assignment=assignment)

    def get_casts_from_type(
        self,
        from_type: s_types.Type,
        *,
        implicit: bool = False,
        assignment: bool = False,
    ) -> FrozenSet[s_casts.Cast]:
        return self._get_casts(from_type, disposition='from_type',
                               implicit=implicit, assignment=assignment)

    @functools.lru_cache()
    def get_referrers(
        self,
        scls: so.Object,
        *,
        scls_type: Optional[Type[so.Object]] = None,
        field_name: Optional[str] = None,
    ) -> FrozenSet[so.Object]:

        try:
            refs = self._refs_to[scls.id]
        except KeyError:
            return frozenset()
        else:
            referrers: Set[so.Object] = set()

            if scls_type is not None:
                if field_name is not None:
                    for (st, fn), ids in refs.items():
                        if st is scls_type and fn == field_name:
                            referrers.update(
                                self._id_to_type[objid] for objid in ids)
                else:
                    for (st, _), ids in refs.items():
                        if st is scls_type:
                            referrers.update(
                                self._id_to_type[objid] for objid in ids)
            elif field_name is not None:
                raise ValueError(
                    'get_referrers: field_name cannot be used '
                    'without scls_type')
            else:
                refids = itertools.chain.from_iterable(refs.values())
                referrers.update(self._id_to_type[objid] for objid in refids)

            return frozenset(referrers)

    @functools.lru_cache()
    def get_referrers_ex(
        self,
        scls: so.Object,
    ) -> Dict[
        Tuple[Type[so.Object], str],
        FrozenSet[so.Object],
    ]:
        try:
            refs = self._refs_to[scls.id]
        except KeyError:
            return {}
        else:
            result = {}

            for (st, fn), ids in refs.items():
                result[st, fn] = frozenset(
                    self._id_to_type[objid] for objid in ids)

            return result

    @overload
    def get_by_id(
        self,
        obj_id: uuid.UUID,
        default: Union[so.Object, so.NoDefaultT] = so.NoDefault,
        *,
        type: None = None,
    ) -> so.Object:
        ...

    @overload
    def get_by_id(  # NoQA: F811
        self,
        obj_id: uuid.UUID,
        default: Union[so.Object_T, so.NoDefaultT] = so.NoDefault,
        *,
        type: Optional[Type[so.Object_T]] = None,
    ) -> so.Object_T:
        ...

    @overload
    def get_by_id(  # NoQA: F811
        self,
        obj_id: uuid.UUID,
        default: None = None,
        *,
        type: Optional[Type[so.Object_T]] = None,
    ) -> Optional[so.Object_T]:
        ...

    def get_by_id(  # NoQA: F811
        self,
        obj_id: uuid.UUID,
        default: Union[so.Object_T, so.NoDefaultT, None] = so.NoDefault,
        *,
        type: Optional[Type[so.Object_T]] = None,
    ) -> Optional[so.Object_T]:
        try:
            obj = self._id_to_type[obj_id]
        except KeyError:
            if default is so.NoDefault:
                raise errors.InvalidReferenceError(
                    f'reference to a non-existent schema item {obj_id}'
                    f' in schema {self!r}'
                ) from None
            else:
                return default
        else:
            if type is not None and not isinstance(obj, type):
                raise errors.InvalidReferenceError(
                    f'schema object {obj_id!r} exists, but is not '
                    f'{type.get_schema_class_displayname()}'
                )

            return cast(so.Object_T, obj)

    def _get_by_name(
        self,
        name: str,
        *,
        type: Optional[Type[so.Object]],
    ) -> Optional[so.Object]:
        obj_id = self._name_to_id.get(name)
        if obj_id is None:
            return None

        obj = self._id_to_type[obj_id]
        if type is not None and not isinstance(obj, type):
            return None

        return obj

    @overload
    def get_global(
        self,
        objtype: Type[so.Object_T],
        name: str,
        default: Union[so.Object_T, so.NoDefaultT] = so.NoDefault,
    ) -> so.Object_T:
        ...

    @overload
    def get_global(  # NoQA: F811
        self,
        objtype: Type[so.Object_T],
        name: str,
        default: None = None,
    ) -> Optional[so.Object_T]:
        ...

    def get_global(  # NoQA: F811
        self,
        objtype: Type[so.Object_T],
        name: str,
        default: Union[so.Object_T, so.NoDefaultT, None] = so.NoDefault,
    ) -> Optional[so.Object_T]:
        obj_id = self._globalname_to_id.get((objtype, name))
        if obj_id is not None:
            return cast(so.Object_T, self._id_to_type[obj_id])
        elif default is not so.NoDefault:
            return default
        else:
            desc = objtype.get_schema_class_displayname()
            raise errors.InvalidReferenceError(
                f'{desc} {name!r} does not exist')

    def get_generic(
        self,
        name: str,
        default: Union[so.Object, so.NoDefaultT, None],
        *,
        module_aliases: Optional[Mapping[Optional[str], str]],
        type: Optional[Type[so.Object_T]],
        condition: Optional[Callable[[so.Object], bool]],
        label: Optional[str],
        sourcectx: Optional[parsing.ParserContext],
    ) -> Optional[so.Object]:
        def getter(schema: FlatSchema, name: str) -> Optional[so.Object]:
            obj = schema._get_by_name(name, type=type)
            if obj is not None and condition is not None:
                if not condition(obj):
                    obj = None
            return obj

        obj = self._get(name,
                        getter=getter,
                        module_aliases=module_aliases,
                        default=default)

        if obj is not so.NoDefault:
            return cast(so.Object_T, obj)

        if label is None:
            if type is not None:
                label = type.get_schema_class_displayname()
            else:
                label = 'schema item'

        refname = name

        if type is not None:
            if not sn.Name.is_qualified(refname):
                if module_aliases is not None:
                    default_module = module_aliases.get(None)
                    if default_module is not None:
                        refname = type.get_displayname_static(
                            f'{default_module}::{refname}',
                        )
            else:
                refname = type.get_displayname_static(refname)

        raise errors.InvalidReferenceError(
            f'{label} {refname!r} does not exist',
            context=sourcectx)

    def has_object(self, object_id: uuid.UUID) -> bool:
        return object_id in self._id_to_type

    def has_module(self, module: str) -> bool:
        return self.get_global(s_mod.Module, module, None) is not None

    def get_objects(
        self,
        *,
        exclude_stdlib: bool = False,
        included_modules: Optional[Iterable[str]] = None,
        excluded_modules: Optional[Iterable[str]] = None,
        included_items: Optional[Iterable[str]] = None,
        excluded_items: Optional[Iterable[str]] = None,
        type: Optional[Type[so.Object_T]] = None,
        extra_filters: Iterable[Callable[[Schema, so.Object], bool]] = (),
    ) -> SchemaIterator[so.Object_T]:
        return SchemaIterator[so.Object_T](
            self,
            self._id_to_type.values(),
            exclude_stdlib=exclude_stdlib,
            included_modules=included_modules,
            excluded_modules=excluded_modules,
            included_items=included_items,
            excluded_items=excluded_items,
            type=type,
            extra_filters=extra_filters,
        )

    def get_all_objects(self) -> Iterable[so.Object]:
        return self._id_to_type.values()

    def get_modules(self) -> Tuple[s_mod.Module, ...]:
        modules = []
        for (objtype, _), objid in self._globalname_to_id.items():
            if objtype is s_mod.Module:
                modules.append(self.get_by_id(objid, type=s_mod.Module))
        return tuple(modules)

    def get_last_migration(self) -> Optional[s_migrations.Migration]:
        return _get_last_migration(self)

    def __repr__(self) -> str:
        return (
            f'<{type(self).__name__} gen:{self._generation} at {id(self):#x}>')


class SchemaIterator(Generic[so.Object_T]):
    def __init__(
        self,
        schema: Schema,
        objects: Iterable[so.Object],
        *,
        exclude_stdlib: bool = False,
        included_modules: Optional[Iterable[str]],
        excluded_modules: Optional[Iterable[str]],
        included_items: Optional[Iterable[str]] = None,
        excluded_items: Optional[Iterable[str]] = None,
        type: Optional[Type[so.Object_T]] = None,
        extra_filters: Iterable[Callable[[Schema, so.Object], bool]] = (),
    ) -> None:

        filters = []

        if type is not None:
            t = type
            filters.append(lambda schema, obj: isinstance(obj, t))

        if included_modules:
            modules = frozenset(included_modules)
            filters.append(
                lambda schema, obj:
                    isinstance(obj, so.QualifiedObject) and
                    obj.get_name(schema).module in modules)

        if excluded_modules or exclude_stdlib:
            excmod: Set[str] = set()
            if excluded_modules:
                excmod.update(excluded_modules)
            if exclude_stdlib:
                excmod.update(STD_MODULES)
            filters.append(
                lambda schema, obj: (
                    not isinstance(obj, so.QualifiedObject)
                    or obj.get_name(schema).module not in excmod
                )
            )

        if included_items:
            objs = frozenset(included_items)
            filters.append(
                lambda schema, obj: obj.get_name(schema) in objs)

        if excluded_items:
            objs = frozenset(excluded_items)
            filters.append(
                lambda schema, obj: obj.get_name(schema) not in objs)

        if exclude_stdlib:
            filters.append(
                lambda schema, obj: not isinstance(obj, s_pseudo.PseudoType)
            )

        # Extra filters are last, because they might depend on type.
        filters.extend(extra_filters)

        self._filters = filters
        self._schema = schema
        self._objects = objects

    def __iter__(self) -> Iterator[so.Object_T]:
        filters = self._filters
        for obj in self._objects:
            if all(f(self._schema, obj) for f in filters):
                yield obj  # type: ignore


class ChainedSchema(Schema):

    __slots__ = ('_base_schema', '_top_schema')

    def __init__(
        self,
        base_schema: FlatSchema,
        top_schema: FlatSchema,
    ) -> None:
        self._base_schema = base_schema
        self._top_schema = top_schema

    def add(
        self,
        id: uuid.UUID,
        scls: so.Object,
        data: Mapping[str, Any],
    ) -> ChainedSchema:
        return ChainedSchema(
            self._base_schema,
            self._top_schema.add(id, scls, data),
        )

    def discard(self, obj: so.Object) -> ChainedSchema:
        return ChainedSchema(
            self._base_schema,
            self._top_schema.discard(obj),
        )

    def delete(self, obj: so.Object) -> ChainedSchema:
        return ChainedSchema(
            self._base_schema,
            self._top_schema.delete(obj),
        )

    def update_obj(
        self,
        obj_id: uuid.UUID,
        updates: Mapping[str, Any],
    ) -> ChainedSchema:
        base_obj = self._base_schema.get_by_id(obj_id, default=None)
        if base_obj is not None and not self._top_schema.has_object(obj_id):
            top_schema = self._top_schema.add(
                obj_id, base_obj, self._base_schema._id_to_data[obj_id])
        else:
            top_schema = self._top_schema

        return ChainedSchema(
            self._base_schema,
            top_schema.update_obj(obj_id, updates),
        )

    def get_obj_field(
        self,
        obj_id: uuid.UUID,
        field: str,
    ) -> Any:
        if obj_id in self._base_schema._id_to_data:
            return self._base_schema.get_obj_field(obj_id, field)
        else:
            return self._top_schema.get_obj_field(obj_id, field)

    def set_obj_field(
        self,
        obj_id: uuid.UUID,
        field: str,
        value: Any,
    ) -> ChainedSchema:
        return ChainedSchema(
            self._base_schema,
            self._top_schema.set_obj_field(obj_id, field, value),
        )

    def unset_obj_field(
        self,
        obj_id: uuid.UUID,
        field: str,
    ) -> ChainedSchema:
        return ChainedSchema(
            self._base_schema,
            self._top_schema.unset_obj_field(obj_id, field),
        )

    def get_functions(
        self,
        name: str,
        default: Union[
            Tuple[s_func.Function, ...], so.NoDefaultT
        ] = so.NoDefault,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
    ) -> Tuple[s_func.Function, ...]:
        try:
            return self._top_schema.get_functions(
                name, module_aliases=module_aliases)
        except errors.InvalidReferenceError:
            return self._base_schema.get_functions(
                name, default=default, module_aliases=module_aliases)

    def get_operators(
        self,
        name: str,
        default: Union[
            Tuple[s_oper.Operator, ...], so.NoDefaultT
        ] = so.NoDefault,
        *,
        module_aliases: Optional[Mapping[Optional[str], str]] = None,
    ) -> Tuple[s_oper.Operator, ...]:
        try:
            return self._top_schema.get_operators(
                name, module_aliases=module_aliases)
        except errors.InvalidReferenceError:
            return self._base_schema.get_operators(
                name, default=default, module_aliases=module_aliases)

    def get_casts_to_type(
        self,
        to_type: s_types.Type,
        *,
        implicit: bool = False,
        assignment: bool = False,
    ) -> FrozenSet[s_casts.Cast]:
        return (
            self._base_schema.get_casts_to_type(
                to_type,
                implicit=implicit,
                assignment=assignment,
            )
            | self._top_schema.get_casts_to_type(
                to_type,
                implicit=implicit,
                assignment=assignment,
            )
        )

    def get_casts_from_type(
        self,
        from_type: s_types.Type,
        *,
        implicit: bool = False,
        assignment: bool = False,
    ) -> FrozenSet[s_casts.Cast]:
        return (
            self._base_schema.get_casts_from_type(
                from_type,
                implicit=implicit,
                assignment=assignment,
            )
            | self._top_schema.get_casts_from_type(
                from_type,
                implicit=implicit,
                assignment=assignment,
            )
        )

    def get_referrers(
        self,
        scls: so.Object,
        *,
        scls_type: Optional[Type[so.Object]] = None,
        field_name: Optional[str] = None,
    ) -> FrozenSet[so.Object]:
        return (
            self._base_schema.get_referrers(
                scls,
                scls_type=scls_type,
                field_name=field_name,
            )
            | self._top_schema.get_referrers(
                scls,
                scls_type=scls_type,
                field_name=field_name,
            )
        )

    def get_referrers_ex(
        self,
        scls: so.Object,
    ) -> Dict[
        Tuple[Type[so.Object], str],
        FrozenSet[so.Object],
    ]:
        base = self._base_schema.get_referrers_ex(scls)
        top = self._top_schema.get_referrers_ex(scls)
        return {
            k: base.get(k, frozenset()) | top.get(k, frozenset())
            for k in itertools.chain(base, top)
        }

    @overload
    def get_by_id(
        self,
        obj_id: uuid.UUID,
        default: Union[so.Object, so.NoDefaultT] = so.NoDefault,
        *,
        type: None = None,
    ) -> so.Object:
        ...

    @overload
    def get_by_id(  # NoQA: F811
        self,
        obj_id: uuid.UUID,
        default: Union[so.Object_T, so.NoDefaultT] = so.NoDefault,
        *,
        type: Optional[Type[so.Object_T]] = None,
    ) -> so.Object_T:
        ...

    @overload
    def get_by_id(  # NoQA: F811
        self,
        obj_id: uuid.UUID,
        default: None = None,
        *,
        type: Optional[Type[so.Object_T]] = None,
    ) -> Optional[so.Object_T]:
        ...

    def get_by_id(  # NoQA: F811
        self,
        obj_id: uuid.UUID,
        default: Union[so.Object_T, so.NoDefaultT, None] = so.NoDefault,
        *,
        type: Optional[Type[so.Object_T]] = None,
    ) -> Optional[so.Object_T]:
        obj = self._top_schema.get_by_id(obj_id, type=type, default=None)
        if obj is None:
            return self._base_schema.get_by_id(
                obj_id, default=default, type=type)
        else:
            return obj

    @overload
    def get_global(
        self,
        objtype: Type[so.Object_T],
        name: str,
        default: Union[so.Object_T, so.NoDefaultT] = so.NoDefault,
    ) -> so.Object_T:
        ...

    @overload
    def get_global(  # NoQA: F811
        self,
        objtype: Type[so.Object_T],
        name: str,
        default: None = None,
    ) -> Optional[so.Object_T]:
        ...

    def get_global(  # NoQA: F811
        self,
        objtype: Type[so.Object_T],
        name: str,
        default: Union[so.Object_T, so.NoDefaultT, None] = so.NoDefault,
    ) -> Optional[so.Object_T]:
        try:
            return self._top_schema.get_global(objtype, name)
        except errors.InvalidReferenceError:
            return self._base_schema.get_global(objtype, name, default=default)

    def get_generic(  # NoQA: F811
        self,
        name: str,
        default: Union[so.Object, so.NoDefaultT, None],
        *,
        module_aliases: Optional[Mapping[Optional[str], str]],
        type: Optional[Type[so.Object_T]],
        condition: Optional[Callable[[so.Object], bool]],
        label: Optional[str],
        sourcectx: Optional[parsing.ParserContext],
    ) -> Optional[so.Object]:
        obj = self._top_schema.get(
            name,
            module_aliases=module_aliases,
            type=type,
            default=None,
            condition=condition,
            label=label,
            sourcectx=sourcectx,
        )
        if obj is None:
            return self._base_schema.get(
                name,
                default=default,
                module_aliases=module_aliases,
                type=type,
                condition=condition,
                label=label,
                sourcectx=sourcectx,
            )
        else:
            return obj

    def has_object(self, object_id: uuid.UUID) -> bool:
        return (
            self._base_schema.has_object(object_id)
            or self._top_schema.has_object(object_id)
        )

    def has_module(self, module: str) -> bool:
        return (
            self._base_schema.has_module(module)
            or self._top_schema.has_module(module)
        )

    def get_objects(
        self,
        *,
        exclude_stdlib: bool = False,
        included_modules: Optional[Iterable[str]] = None,
        excluded_modules: Optional[Iterable[str]] = None,
        included_items: Optional[Iterable[str]] = None,
        excluded_items: Optional[Iterable[str]] = None,
        type: Optional[Type[so.Object_T]] = None,
        extra_filters: Iterable[Callable[[Schema, so.Object], bool]] = (),
    ) -> SchemaIterator[so.Object_T]:
        return SchemaIterator[so.Object_T](
            self,
            itertools.chain(
                self._base_schema._id_to_type.values(),
                self._top_schema._id_to_type.values(),
            ),
            exclude_stdlib=exclude_stdlib,
            included_modules=included_modules,
            excluded_modules=excluded_modules,
            included_items=included_items,
            excluded_items=excluded_items,
            type=type,
            extra_filters=extra_filters,
        )

    def get_modules(self) -> Tuple[s_mod.Module, ...]:
        return (
            self._base_schema.get_modules()
            + self._top_schema.get_modules()
        )

    def get_last_migration(self) -> Optional[s_migrations.Migration]:
        migration = self._top_schema.get_last_migration()
        if migration is None:
            migration = self._base_schema.get_last_migration()
        return migration


@functools.lru_cache()
def _get_functions(
    schema: FlatSchema,
    name: str,
) -> Optional[Tuple[s_func.Function, ...]]:
    objids = schema._shortname_to_id.get((s_func.Function, name))
    if objids is None:
        return None
    return cast(
        Tuple[s_func.Function, ...],
        tuple(schema._id_to_type[oid] for oid in objids),
    )


@functools.lru_cache()
def _get_operators(
    schema: FlatSchema,
    name: str,
) -> Optional[Tuple[s_oper.Operator, ...]]:
    objids = schema._shortname_to_id.get((s_oper.Operator, name))
    if objids is None:
        return
    return cast(
        Tuple[s_oper.Operator, ...],
        tuple(schema._id_to_type[oid] for oid in objids),
    )


@functools.lru_cache()
def _get_last_migration(
    schema: FlatSchema,
) -> Optional[s_migrations.Migration]:

    migrations = cast(
        List[s_migrations.Migration],
        [
            schema._id_to_type[mid]
            for (t, _), mid in schema._globalname_to_id.items()
            if t is s_migrations.Migration
        ],
    )

    if not migrations:
        return None

    migration_map = collections.defaultdict(list)
    root = None
    for m in migrations:
        parents = m.get_parents(schema).objects(schema)
        if not parents:
            if root is not None:
                raise errors.InternalServerError(
                    'multiple migration roots found')
            root = m
        for parent in parents:
            migration_map[parent].append(m)

    if root is None:
        raise errors.InternalServerError('cannot find migration root')

    latest = root
    while children := migration_map[latest]:
        if len(children) > 1:
            raise errors.InternalServerError(
                'nonlinear migration history detected')
        latest = children[0]

    return latest
