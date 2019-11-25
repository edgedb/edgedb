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
from typing_extensions import Final

import collections
import collections.abc
import enum
import itertools
import sys
import types
import uuid

import immutables as immu

from edb import errors
from edb.edgeql import qltypes

from edb.common import checked
from edb.common import markup
from edb.common import ordered
from edb.common import parsing
from edb.common import struct
from edb.common import topological
from edb.common import uuidgen

from . import abc as s_abc
from . import name as sn
from . import _types


if TYPE_CHECKING:
    from edb.schema import objtypes
    from edb.schema import delta as sd
    from edb.schema import schema as s_schema

    if sys.version_info <= (3, 7):
        from typing_extensions import Protocol  # type: ignore

    CovT = TypeVar("CovT", covariant=True)

    class MergeFunction(Protocol):
        def __call__(
            self,  # not actually part of the signature
            target: InheritingObjectBase,
            sources: List[Object],
            field_name: str,
            *,
            schema: s_schema.Schema,
        ) -> Any:
            ...

    class CollectionFactory(Collection[CovT], Protocol):
        """An unknown collection that can be instantiated from an iterable."""

        def __init__(self, from_iter: Optional[Iterable[CovT]] = None) -> None:
            ...


class NoDefaultT(enum.Enum):
    """Used as a sentinel indicating that a named argument wasn't passed.

    Trick from https://github.com/python/mypy/issues/7642.
    """
    NoDefault = 0


NoDefault: Final = NoDefaultT.NoDefault
T = TypeVar("T")
Type_T = TypeVar("Type_T", bound=type)
Object_T = TypeVar("Object_T", bound="Object")
ObjectLS_T = TypeVar("ObjectLS_T", "ObjectList", "ObjectSet")
ObjectCollection_T = TypeVar("ObjectCollection_T", bound="ObjectCollection")
Pair = Tuple[Optional["Object"], Optional["Object"]]
HashCriterion = Union[Type[Object_T], Tuple[str, Any]]


def default_field_merge(
    target: InheritingObjectBase,
    sources: List[Object],
    field_name: str,
    *,
    schema: s_schema.Schema,
) -> Any:
    """The default `MergeFunction`."""
    ours = target.get_explicit_local_field_value(schema, field_name, None)
    if ours is not None:
        return ours

    for source in sources:
        theirs = source.get_explicit_field_value(schema, field_name, None)
        if theirs is not None:
            return theirs

    return None


def get_known_type_id(
    typename: Any, default: Union[uuid.UUID, NoDefaultT] = NoDefault
) -> uuid.UUID:
    try:
        return _types.TYPE_IDS[typename]
    except KeyError:
        pass

    if default is NoDefault:
        raise errors.SchemaError(
            f'failed to lookup named type id for {typename!r}')

    return default


class ComparisonContextWrapper:
    def __init__(self, context: ComparisonContext, pair: Pair) -> None:
        self.context = context
        self.pair = pair

    def __enter__(self) -> None:
        self.context.push(self.pair)

    def __exit__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_value: Exception,
        traceback: Optional[types.TracebackType],
    ) -> None:
        self.context.pop()


class ComparisonContext:
    stacks: Dict[Type[Object], List[Pair]]
    ptrs: List[Type[Object]]

    def __init__(self) -> None:
        self.stacks = collections.defaultdict(list)
        self.ptrs = []

    def push(self, pair: Pair) -> None:
        obj = pair[1] if pair[0] is None else pair[0]
        cls = type(obj)

        if not issubclass(cls, Object):
            raise ValueError(
                f'invalid argument type {cls!r} for comparison context')

        self.stacks[cls].append(pair)
        self.ptrs.append(cls)

    def pop(self, cls: Optional[Type[Object]] = None) -> Pair:
        cls = cls or self.ptrs.pop()
        return self.stacks[cls].pop()

    def get(self, cls: Type[Object]) -> Optional[Pair]:
        stack = self.stacks[cls]
        if stack:
            return stack[-1]
        return None

    def __call__(
        self, left: Optional[Object], right: Optional[Object]
    ) -> ComparisonContextWrapper:
        return ComparisonContextWrapper(self, (left, right))


# derived from ProtoField for validation
class Field(struct.ProtoField, Generic[T]):

    __slots__ = ('name', 'type', 'coerce',
                 'compcoef', 'inheritable', 'simpledelta',
                 'merge_fn', 'ephemeral', 'introspectable',
                 'allow_ddl_set', 'weak_ref')

    #: Name of the field on the target class; assigned by ObjectMeta
    name: str
    #: The type of the value stored in the field
    type: Type[T]
    #: Whether the field is allowed to automatically coerce
    #: the input value to the declared type of the field.
    coerce: bool
    #: The diffing coefficient to use when comparing field
    #: values in objects from 0 to 1.
    compcoef: Optional[float]
    #: Whether the field value can be inherited.
    inheritable: bool
    #: Wheter the field uses the generic AlterObjectProperty
    #: delta op, or a custom delta command.
    simpledelta: bool
    #: If true, the value of the field is not persisted in the
    #: database.
    ephemeral: bool
    #: If true, the field value can be introspected.
    introspectable: bool
    #: Whether the field can be set directly using the `SET`
    #: command in DDL.
    allow_ddl_set: bool
    #: Used for fields holding references to objects.  If True,
    #: the reference is considered "weak", i.e. not essential for
    #: object definition.  The schema and delta linearization
    #: rely on this to break object reference cycles.
    weak_ref: bool
    #: A callable used to merge the value of the field from
    #: multiple objects.  Most oftenly used by inheritance.
    merge_fn: MergeFunction

    def __init__(
        self,
        type_: Type[T],
        *,
        coerce: bool = False,
        compcoef: Optional[float] = None,
        inheritable: bool = True,
        simpledelta: bool = True,
        merge_fn: MergeFunction = default_field_merge,
        ephemeral: bool = False,
        introspectable: bool = True,
        weak_ref: bool = False,
        allow_ddl_set: bool = False,
        **kwargs: Any,
    ) -> None:
        """Schema item core attribute definition.

        """
        if not isinstance(type_, type):
            raise ValueError(f'{type_!r} is not a type')

        self.type = type_
        self.coerce = coerce
        self.allow_ddl_set = allow_ddl_set

        self.compcoef = compcoef
        self.inheritable = inheritable
        self.simpledelta = simpledelta
        self.introspectable = introspectable
        self.weak_ref = weak_ref

        if (
            merge_fn is default_field_merge
            and callable(getattr(self.type, 'merge_values', None))
        ):
            # type ignore due to https://github.com/python/mypy/issues/1424
            self.merge_fn = self.type.merge_values  # type: ignore
        else:
            self.merge_fn = merge_fn

        self.ephemeral = ephemeral

    def coerce_value(self, schema: s_schema.Schema, value: Any) -> Optional[T]:
        # cast() below due to https://github.com/python/mypy/issues/7920
        ctype = cast(type, self.type)
        ftype = self.type

        if value is None or isinstance(value, ftype):
            return value

        if not self.coerce:
            raise TypeError(
                f'{self.name} field: expected {ftype} but got {value!r}')

        if issubclass(ftype, (checked.CheckedList,
                              checked.CheckedSet,
                              checked.FrozenCheckedList,
                              checked.FrozenCheckedSet)):
            casted_list = []
            for v in value:
                if v is not None and not isinstance(v, ftype.type):
                    v = ftype.type(v)
                casted_list.append(v)
            return ctype(casted_list)

        if issubclass(ftype, checked.CheckedDict):
            casted_dict = {}
            for k, v in value.items():
                if k is not None and not isinstance(k, ftype.keytype):
                    k = ftype.keytype(k)
                if v is not None and not isinstance(v, ftype.valuetype):
                    v = ftype.valuetype(v)
                casted_dict[k] = v
            return ctype(casted_dict)

        if issubclass(ctype, ObjectCollection):
            # Type ignore below because with ctype we lost information that
            # it is indeed a Type[T].
            return ctype.create(schema, value)  # type: ignore

        try:
            # Type ignore below because Mypy doesn't trust we can instantiate
            # the type using the value.  We don't trust that either but this
            # is why there's the try-except block.
            return ctype(value)  # type: ignore
        except Exception:
            raise TypeError(
                f'cannot coerce {self.name!r} value {value!r} to {ftype}')

    @property
    def required(self):
        return True

    @property
    def is_schema_field(self) -> bool:
        return False

    @overload
    def __get__(self, instance: None, owner: Type[Object]) -> Field[Type[T]]:
        ...

    @overload  # NoQA: F811
    def __get__(self, instance: Object, owner: Type[Object]) -> T:
        ...

    def __get__(self, instance, owner):  # NoQA: F811
        if instance is not None:
            return None
        else:
            return self

    def __repr__(self) -> str:
        return (
            f'<{type(self).__name__} name={self.name!r} '
            f'type={self.type} {id(self):#x}>'
        )


class SchemaField(Field[Type_T]):

    __slots__ = ('default', 'hashable')

    #: The default value to use for the field.
    default: Any
    #: Whether the field participates in object hash.
    hashable: bool

    def __init__(
        self,
        type: Type_T,
        *,
        default: Any = NoDefault,
        hashable: bool = True,
        allow_ddl_set: bool = False,
        **kwargs: Any,
    ):
        super().__init__(type, **kwargs)
        self.default = default
        self.hashable = hashable
        self.allow_ddl_set = allow_ddl_set

    @property
    def required(self) -> bool:
        return self.default is NoDefault

    @property
    def is_schema_field(self) -> bool:
        return True

    # Breaking Liskov Substitution Principle
    @overload  # type: ignore
    def __get__(self, instance: None, owner: Type) -> SchemaField[Type_T]:
        ...

    @overload  # NoQA: F811
    def __get__(self, instance: T, owner: Type[T]) -> Type_T:
        ...

    def __get__(self, instance, owner):  # NoQA: F811
        if instance is not None:
            raise FieldValueNotFoundError(self.name)
        else:
            return self


class RefDict(struct.Struct):

    attr = struct.Field(str, frozen=True)
    backref_attr = struct.Field(str, default='subject', frozen=True)
    requires_explicit_inherit = struct.Field(bool, default=False, frozen=True)
    ref_cls = struct.Field(type, frozen=True)


class ObjectMeta(type):

    _all_types: List[ObjectMeta] = []
    _schema_types: Set[ObjectMeta] = set()
    _ql_map: Dict[qltypes.SchemaObjectClass, ObjectMeta] = {}

    # Instance fields (i.e. class fields on types built with ObjectMeta)
    _fields: Dict[str, Field]
    _hashable_fields: Set[Field]  # if f.is_schema_field and f.hashable
    _sorted_fields: collections.OrderedDict[str, Field]
    _refdicts: collections.OrderedDict[str, RefDict]
    _refdicts_by_refclass: Dict[type, RefDict]
    _refdicts_by_field: Dict[str, RefDict]  # key is rd.attr
    _ql_class: Optional[qltypes.SchemaObjectClass]

    def __new__(
        mcls,
        name: str,
        bases: Tuple[type, ...],
        clsdict: Dict[str, Any],
        *,
        qlkind: Optional[qltypes.SchemaObjectClass] = None,
    ) -> ObjectMeta:
        refdicts: collections.OrderedDict[str, RefDict]

        fields = {}
        myfields = {}
        refdicts = collections.OrderedDict()
        mydicts = {}

        if '__slots__' in clsdict:
            raise TypeError(
                f'cannot create {name} class: __slots__ are not supported')

        for k, v in tuple(clsdict.items()):
            if isinstance(v, RefDict):
                mydicts[k] = v
                continue
            if not isinstance(v, struct.ProtoField):
                continue
            if not isinstance(v, Field):
                raise TypeError(
                    f'cannot create {name} class: schema.objects.Field '
                    f'expected, got {type(v)}')

            v.name = k
            myfields[k] = v

            if v.is_schema_field:
                getter_name = f'get_{v.name}'
                if getter_name in clsdict:
                    raise TypeError(
                        f'cannot create {name} class: schema field getter '
                        f'{getter_name}() is already defined')
                clsdict[getter_name] = (
                    lambda self, schema, *, _fn=v.name:
                        self._get_schema_field_value(schema, _fn)
                )

        try:
            cls = cast(ObjectMeta, super().__new__(mcls, name, bases, clsdict))
        except TypeError as ex:
            raise TypeError(
                f'Object metaclass has failed to create class {name}: {ex}')

        for parent in reversed(cls.__mro__):
            if parent is cls:
                fields.update(myfields)
                refdicts.update(mydicts)
            elif isinstance(parent, ObjectMeta):
                fields.update(parent.get_ownfields())
                refdicts.update({k: d.copy()
                                for k, d in parent.get_own_refdicts().items()})

        cls._fields = fields
        cls._hashable_fields = {f for f in fields.values()
                                if isinstance(f, SchemaField) and f.hashable}
        cls._sorted_fields = collections.OrderedDict(
            sorted(fields.items(), key=lambda e: e[0]))
        # Populated lazily
        cls._object_fields = None

        fa = '{}.{}_fields'.format(cls.__module__, cls.__name__)
        setattr(cls, fa, myfields)

        non_schema_fields = {field.name for field in fields.values()
                             if not field.is_schema_field}
        if non_schema_fields == {'id'} and len(fields) > 1:
            mcls._schema_types.add(cls)
            if qlkind is not None:
                mcls._ql_map[qlkind] = cls

        cls._refdicts_by_refclass = {}

        for dct in refdicts.values():
            if dct.attr not in cls._fields:
                raise RuntimeError(
                    f'object {name} has no refdict field {dct.attr}')

            if cls._fields[dct.attr].inheritable:
                raise RuntimeError(
                    f'{name}.{dct.attr} field must not be inheritable')
            if not cls._fields[dct.attr].ephemeral:
                raise RuntimeError(
                    f'{name}.{dct.attr} field must be ephemeral')
            if not cls._fields[dct.attr].coerce:
                raise RuntimeError(
                    f'{name}.{dct.attr} field must be coerced')

            if isinstance(dct.ref_cls, str):
                ref_cls_getter = getattr(cls, dct.ref_cls)
                try:
                    dct.ref_cls = ref_cls_getter()
                except NotImplementedError:
                    pass

            if not isinstance(dct.ref_cls, str):
                other_dct = cls._refdicts_by_refclass.get(dct.ref_cls)
                if other_dct is not None:
                    raise TypeError(
                        'multiple reference dicts for {!r} in '
                        '{!r}: {!r} and {!r}'.format(dct.ref_cls, cls,
                                                     dct.attr, other_dct.attr))

                cls._refdicts_by_refclass[dct.ref_cls] = dct

        # Refdicts need to be reversed here to respect the __mro__,
        # as we have iterated over it in reverse above.
        cls._refdicts = collections.OrderedDict(reversed(refdicts.items()))

        cls._refdicts_by_field = {rd.attr: rd for rd in cls._refdicts.values()}

        setattr(cls, '{}.{}_refdicts'.format(cls.__module__, cls.__name__),
                     mydicts)

        cls._ql_class = qlkind
        mcls._all_types.append(cls)

        return cls

    def get_object_fields(cls):
        if cls._object_fields is None:
            cls._object_fields = frozenset(
                f for f in cls._fields.values()
                if issubclass(f.type, s_abc.ObjectContainer)
            )
        return cls._object_fields

    def get_field(cls, name):
        return cls._fields.get(name)

    def get_fields(cls, sorted=False):
        return cls._sorted_fields if sorted else cls._fields

    def get_ownfields(cls):
        return getattr(
            cls, '{}.{}_fields'.format(cls.__module__, cls.__name__))

    def get_own_refdicts(cls):
        return getattr(cls, '{}.{}_refdicts'.format(
            cls.__module__, cls.__name__))

    def get_refdicts(cls):
        return iter(cls._refdicts.values())

    def get_refdict(cls, name):
        return cls._refdicts_by_field.get(name)

    def get_refdict_for_class(cls, refcls):
        for rcls in refcls.__mro__:
            try:
                return cls._refdicts_by_refclass[rcls]
            except KeyError:
                pass
        else:
            raise KeyError(f'{cls} has no refdict for {refcls}')

    @property
    def is_schema_object(cls) -> bool:
        return cls in ObjectMeta._schema_types

    @classmethod
    def get_schema_metaclasses(mcls) -> Iterator[ObjectMeta]:
        return iter(mcls._all_types)

    @classmethod
    def get_schema_metaclass_for_ql_class(
        mcls,
        qlkind: qltypes.SchemaObjectClass
    ) -> ObjectMeta:
        cls = mcls._ql_map.get(qlkind)
        if cls is None:
            raise LookupError(f'no schema metaclass for {qlkind}')
        return cls

    def get_ql_class(cls) -> Optional[qltypes.SchemaObjectClass]:
        return cls._ql_class


class FieldValueNotFoundError(Exception):
    pass


class Object(s_abc.Object, s_abc.ObjectContainer, metaclass=ObjectMeta):
    """Base schema item class."""

    # Unique ID for this schema item.
    id = Field[uuid.UUID](
        uuid.UUID,
        inheritable=False, simpledelta=False, allow_ddl_set=True)

    # Schema source context for this object
    sourcectx = SchemaField(
        parsing.ParserContext,
        default=None, compcoef=None,
        inheritable=False, introspectable=False, hashable=False,
        ephemeral=True)

    name = SchemaField(
        sn.Name,
        inheritable=False, compcoef=0.670)

    # The path_id_name field is solely for the purposes of the compiler
    # so that this item can act as a transparent proxy for the item
    # it has been derived from, specifically in path ids.
    path_id_name = SchemaField(
        sn.Name,
        inheritable=False, ephemeral=True,
        introspectable=False, default=None)

    _fields: Dict[str, SchemaField]

    def get_shortname(self, schema: s_schema.Schema) -> sn.Name:
        return sn.shortname_from_fullname(self.get_name(schema))

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return str(self.get_shortname(schema))

    def get_verbosename(
        self, schema: s_schema.Schema, *, with_parent: bool = False
    ) -> str:
        clsname = self.get_schema_class_displayname()
        dname = self.get_displayname(schema)
        return f"{clsname} '{dname}'"

    def __init__(self, *, _private_init) -> None:
        pass

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return NotImplemented
        return self.id == other.id

    def __hash__(self) -> int:
        return hash((self.id, type(self)))

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return cls.__name__.lower()

    @classmethod
    def _prepare_id(
        cls, id: Optional[uuid.UUID], data: Dict[str, Any]
    ) -> uuid.UUID:
        if id is not None:
            return id

        try:
            return get_known_type_id(data.get('name'))
        except errors.SchemaError:
            return uuidgen.uuid1mc()

    @classmethod
    def _create_from_id(cls: Type[Object_T], id: uuid.UUID) -> Object_T:
        assert id is not None
        obj = cls(_private_init=True)
        obj.__dict__['id'] = id
        return obj

    @classmethod
    def create_in_schema(
        cls: Type[Object_T],
        schema: s_schema.Schema, *,
        id=None,
        **data
    ) -> Tuple[s_schema.Schema, Object_T]:

        if not cls.is_schema_object:
            raise TypeError(f'{cls.__name__} type cannot be created in schema')

        if not data.get('name'):
            raise RuntimeError(f'cannot create {cls} without a name')

        obj_data = {}
        for field_name, value in data.items():
            try:
                field = cls._fields[field_name]
            except KeyError:
                raise TypeError(
                    f'type {cls.__name__} has no schema field for '
                    f'keyword argument {field_name!r}') from None

            assert isinstance(field, SchemaField)

            value = field.coerce_value(schema, value)
            if value is None:
                continue

            obj_data[field_name] = value

        id = cls._prepare_id(id, data)
        scls = cls._create_from_id(id)
        schema = schema._add(id, scls, obj_data)

        return schema, scls

    @classmethod
    def _create(
        cls: Type[Object_T],
        schema: Optional[s_schema.Schema],
        *,
        id: Optional[uuid.UUID] = None,
        **data: Any,
    ) -> Object_T:
        if cls.is_schema_object:
            raise TypeError(
                f'{cls.__name__} type cannot be created outside of a schema')

        obj = cls(_private_init=True)

        id = cls._prepare_id(id, data)
        obj.__dict__['id'] = id

        for field_name, value in data.items():
            try:
                field = cls._fields[field_name]
            except KeyError:
                raise TypeError(
                    f'type {cls.__name__} has no field for '
                    f'keyword argument {field_name!r}') from None

            assert not field.is_schema_field
            obj.__dict__[field_name] = value

        return obj

    def __setattr__(self, name: str, value: Any) -> None:
        raise RuntimeError(
            f'cannot set value to attribute {self}.{name} directly')

    def _getdefault(
        self,
        field_name: str,
        field: SchemaField[Type[T]],
        relaxrequired: bool = False,
    ) -> Optional[T]:
        if field.default == field.type:
            if issubclass(field.default, ObjectCollection):
                value = field.default.create_empty()
            else:
                value = field.default()
        elif field.default is NoDefault:
            if relaxrequired:
                value = None
            else:
                raise TypeError(
                    '%s.%s.%s is required' % (
                        self.__class__.__module__, self.__class__.__name__,
                        field_name))
        else:
            value = field.default
        return value

    # XXX sadly, in the methods below, statically we don't know any better than
    # "Any" since providing the field name as a `str` is the equivalent of
    # getattr() on a regular class.

    def _get_schema_field_value(
        self,
        schema: s_schema.Schema,
        field_name: str,
        *,
        allow_default: bool = True,
    ) -> Any:
        val = schema._get_obj_field(self.id, field_name)
        if val is not None:
            return val

        if allow_default:
            field = type(self).get_field(field_name)

            try:
                return self._getdefault(field_name, field)
            except TypeError:
                pass

        raise FieldValueNotFoundError(
            f'{self!r} object has no value for field {field_name!r}')

    def get_field_value(
        self,
        schema: s_schema.Schema,
        field_name: str,
        *,
        allow_default: bool = True,
    ) -> Any:
        field = type(self).get_field(field_name)

        if field.is_schema_field:
            return self._get_schema_field_value(
                schema, field_name, allow_default=allow_default)
        else:
            try:
                return self.__dict__[field_name]
            except KeyError:
                pass

        raise FieldValueNotFoundError(
            f'{self!r} object has no value for field {field_name!r}')

    def get_explicit_field_value(
        self,
        schema: s_schema.Schema,
        field_name: str,
        default: Any = NoDefault,
    ) -> Any:
        field = type(self).get_field(field_name)

        if field.is_schema_field:
            val = schema._get_obj_field(self.id, field_name)
            if val is not None:
                return val
            elif default is not NoDefault:
                return default
            else:
                raise FieldValueNotFoundError(
                    f'{self!r} object has no value for field {field_name!r}')

        else:
            try:
                return self.__dict__[field_name]
            except KeyError:
                if default is not NoDefault:
                    return default

            raise FieldValueNotFoundError(
                f'{self!r} object has no value for field {field_name!r}')

    def set_field_value(
        self,
        schema: s_schema.Schema,
        name: str,
        value: Any,
    ) -> s_schema.Schema:
        field = type(self)._fields[name]
        assert field.is_schema_field

        if value is None:
            return schema._unset_obj_field(self.id, name)
        else:
            value = field.coerce_value(schema, value)
            return schema._set_obj_field(self.__dict__['id'], name, value)

    def update(
        self, schema: s_schema.Schema, updates: Dict[str, Any]
    ) -> s_schema.Schema:
        fields = type(self)._fields

        updates = updates.copy()
        for field_name in updates:
            field = fields[field_name]
            assert field.is_schema_field

            new_val = updates[field_name]
            if new_val is not None:
                new_val = field.coerce_value(schema, new_val)
                updates[field_name] = new_val

        return schema._update_obj(self.__dict__['id'], updates)

    def is_type(self) -> bool:
        return False

    def hash_criteria(
        self: Object_T, schema: s_schema.Schema
    ) -> FrozenSet[HashCriterion]:
        cls = type(self)

        sig: List[Union[Type[Object_T], Tuple[str, Any]]] = [cls]
        for f in cls._hashable_fields:
            fn = f.name
            val = schema._get_obj_field(self.id, fn)
            if val is None:
                continue
            sig.append((fn, val))

        return frozenset(sig)

    def compare(
        self,
        other: Object,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: Optional[ComparisonContext] = None
    ) -> float:
        if (not isinstance(other, self.__class__) and
                not isinstance(self, other.__class__)):
            return NotImplemented

        context = context or ComparisonContext()
        cls = type(self)

        with context(self, other):
            similarity = 1.0

            fields = cls.get_fields(sorted=True)

            for field_name, field in fields.items():
                if field.compcoef is None:
                    continue

                our_value = self.get_field_value(our_schema, field_name)
                their_value = other.get_field_value(their_schema, field_name)

                fcoef = cls.compare_field_value(
                    field,
                    our_value,
                    their_value,
                    our_schema=our_schema,
                    their_schema=their_schema,
                    context=context)

                # XXX to be fixed in a follow-up PR
                similarity *= fcoef  # type: ignore

        return similarity

    def is_blocking_ref(
        self, schema: s_schema.Schema, reference: InheritingObjectBase
    ) -> bool:
        return True

    @classmethod
    def compare_field_value(
        cls,
        field: SchemaField[Type[T]],
        our_value: T,
        their_value: T,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: Optional[ComparisonContext],
    ) -> Optional[float]:
        comparator = getattr(field.type, 'compare_values', None)
        if callable(comparator):
            return comparator(our_value, their_value, context=context,
                              our_schema=our_schema,
                              their_schema=their_schema,
                              compcoef=field.compcoef or 0.5)

        if our_value != their_value:
            return field.compcoef

        return 1.0

    @classmethod
    def compare_values(
        cls,
        ours: Optional[Object],
        theirs: Optional[Object],
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: Optional[ComparisonContext],
        compcoef: float,
    ) -> float:
        """Compare two values and return a coefficient of similarity.

        This is a common callback that is used when we do schema comparisons.
        *ours* and *theirs* are instances of this class, and *our_schema* and
        *their_schema* are the corresponding schemas in which the values are
        defined.  *compcoef* is whatever was specified for the field. The
        method returns a coefficient of similarity of the values, from ``0``
        to ``1``.
        """
        similarity = 1.0

        if ours is not None and theirs is not None:
            if type(ours) is not type(theirs):
                similarity /= 1.4
            elif ours.get_name(our_schema) != theirs.get_name(their_schema):
                similarity /= 1.2
        elif ours is not None or theirs is not None:
            # one is None but not both
            similarity /= 1.2

        if similarity < 1.0:
            return compcoef
        else:
            return 1.0

    @classmethod
    def delta(
        cls,
        old: Optional[Object],
        new: Optional[Object],
        *,
        context: ComparisonContext = None,
        old_schema: Optional[s_schema.Schema],
        new_schema: s_schema.Schema,
    ) -> sd.ObjectCommand:
        from . import delta as sd

        if context is None:
            context = ComparisonContext()

        with context(old, new):
            command_args: Dict[str, Any] = {'canonical': True}

            if old and new:
                try:
                    name = old.get_name(old_schema)  # type: ignore
                except AttributeError:
                    pass
                else:
                    command_args['classname'] = name

                alter_class = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.AlterObject, type(old))
                delta = alter_class(**command_args)
                cls.delta_properties(delta, old, new, context=context,
                                     old_schema=old_schema,
                                     new_schema=new_schema)

            elif new:
                try:
                    name = new.get_name(new_schema)
                except AttributeError:
                    pass
                else:
                    command_args['classname'] = name

                create_class = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.CreateObject, type(new))
                delta = create_class(**command_args)
                cls.delta_properties(delta, old, new, context=context,
                                     old_schema=old_schema,
                                     new_schema=new_schema)

            elif old:
                try:
                    name = old.get_name(old_schema)  # type: ignore
                except AttributeError:
                    pass
                else:
                    command_args['classname'] = name

                delete_class = sd.ObjectCommandMeta.get_command_class_or_die(
                    sd.DeleteObject, type(old))
                delta = delete_class(**command_args)

            for refdict in cls.get_refdicts():
                cls._delta_refdict(
                    old, new, delta=delta,
                    refdict=refdict, context=context,
                    old_schema=old_schema, new_schema=new_schema)

        return delta

    @classmethod
    def _delta_refdict(
        cls,
        old: Optional[Object],
        new: Optional[Object],
        *,
        delta: sd.ObjectCommand,
        refdict: RefDict,
        context: ComparisonContext,
        old_schema: Optional[s_schema.Schema],
        new_schema: s_schema.Schema,
    ) -> None:

        old_idx_key = lambda o: o.get_name(old_schema)
        new_idx_key = lambda o: o.get_name(new_schema)

        def _delta_subdict(attr):
            if old:
                oldcoll = old.get_field_value(old_schema, attr)
                oldcoll_idx = sorted(
                    set(oldcoll.objects(old_schema)), key=old_idx_key
                )
            else:
                oldcoll_idx = []

            if new:
                newcoll = new.get_field_value(new_schema, attr)
                newcoll_idx = sorted(
                    set(newcoll.objects(new_schema)), key=new_idx_key
                )
            else:
                newcoll_idx = []

            delta.update(cls.delta_sets(
                oldcoll_idx, newcoll_idx, context,
                old_schema=old_schema, new_schema=new_schema))

        _delta_subdict(refdict.attr)

    def add_classref(
        self,
        schema: s_schema.Schema,
        collection: str,
        obj: Object,
        replace: bool = False,
    ) -> s_schema.Schema:
        refdict = type(self).get_refdict(collection)
        attr = refdict.attr

        colltype = type(self).get_field(attr).type

        coll = self.get_explicit_field_value(schema, attr, None)

        if coll is not None:
            schema, all_coll = coll.update(schema, [obj])
        else:
            all_coll = colltype.create(schema, [obj])

        schema = self.set_field_value(schema, attr, all_coll)

        return schema

    def del_classref(
        self,
        schema: s_schema.Schema,
        collection: str,
        key: str,
    ) -> s_schema.Schema:
        refdict = type(self).get_refdict(collection)
        attr = refdict.attr
        coll = self.get_field_value(schema, attr)

        if coll and coll.has(schema, key):
            schema, coll = coll.delete(schema, [key])
            schema = self.set_field_value(schema, attr, coll)

        return schema

    def _reduce_to_ref(self, schema: s_schema.Schema) -> Tuple[Object, Any]:
        return ObjectRef(name=self.get_name(schema)), self.get_name(schema)

    def _resolve_ref(self, schema: s_schema.Schema) -> Object:
        return self

    def _reduce_obj_coll(
        self, schema: s_schema.Schema, v: ObjectLS_T
    ) -> Tuple[ObjectLS_T, Tuple[str, ...]]:
        result = []
        comparison_v = []

        for scls in v.objects(schema):
            ref, comp = scls._reduce_to_ref(schema)
            result.append(ref)
            comparison_v.append(comp)

        return type(v).create(schema, result), tuple(comparison_v)

    _reduce_obj_list = _reduce_obj_coll

    def _reduce_obj_set(
        self, schema: s_schema.Schema, v: ObjectSet
    ) -> Tuple[ObjectSet, FrozenSet[str]]:
        result, comparison_v = self._reduce_obj_coll(schema, v)
        return result, frozenset(comparison_v)

    def _reduce_refs(
        self, schema: s_schema.Schema, value: Any
    ) -> Tuple[Any, Any]:
        if isinstance(value, ObjectList):
            return self._reduce_obj_list(schema, value)

        elif isinstance(value, ObjectSet):
            return self._reduce_obj_set(schema, value)

        elif isinstance(value, Object):
            return value._reduce_to_ref(schema)

        elif isinstance(value, ObjectCollection):
            return value._reduce_to_ref(schema, value)

        elif isinstance(value, s_abc.ObjectContainer):
            return value._reduce_to_ref(schema)

        return value, value

    @classmethod
    def delta_properties(
        cls,
        delta: sd.ObjectCommand,
        old: Optional[Object],
        new: Object,
        *,
        context: ComparisonContext = None,
        old_schema: Optional[s_schema.Schema],
        new_schema: s_schema.Schema,
    ) -> None:
        from edb.schema import delta as sd

        ff = type(new).get_fields(sorted=True).items()
        fields = {fn: f for fn, f in ff
                  if f.simpledelta and not f.ephemeral and f.introspectable}

        if old and new:
            if old_schema is None:
                raise ValueError("`old` provided but `old_schema is None")
            if old.get_name(old_schema) != new.get_name(new_schema):
                delta.add(old.delta_rename(old, new.get_name(new_schema),
                                           old_schema=old_schema,
                                           new_schema=new_schema))

            for fn, f in fields.items():
                oldattr_v = old.get_explicit_field_value(old_schema, fn, None)
                newattr_v = new.get_explicit_field_value(new_schema, fn, None)

                oldattr_v, oldattr_v1 = old._reduce_refs(old_schema, oldattr_v)
                newattr_v, newattr_v1 = new._reduce_refs(new_schema, newattr_v)

                fcoef = cls.compare_field_value(
                    f,
                    oldattr_v,
                    newattr_v,
                    our_schema=old_schema,
                    their_schema=new_schema,
                    context=context)

                if fcoef != 1.0:
                    delta.add(sd.AlterObjectProperty(
                        property=fn, old_value=oldattr_v, new_value=newattr_v))
        elif new:
            # IDs are assigned once when the object is created and
            # never changed.
            id_value = new.get_explicit_field_value(new_schema, 'id')
            delta.add(sd.AlterObjectProperty(
                property='id', old_value=None, new_value=id_value))

            for fn in fields:
                value = new.get_explicit_field_value(new_schema, fn, None)
                if value is not None:
                    value, _ = new._reduce_refs(new_schema, value)
                    cls.delta_property(new_schema, new, delta, fn, value)

    @classmethod
    def delta_property(
        cls,
        schema: s_schema.Schema,
        scls: Object,
        delta: sd.ObjectCommand,
        fname: str,
        value: Any,
    ) -> None:
        from edb.schema import delta as sd

        delta.add(sd.AlterObjectProperty(
            property=fname, old_value=None, new_value=value))

    @classmethod
    def delta_rename(
        cls,
        obj: Object,
        new_name: sn.Name,
        *,
        old_schema: s_schema.Schema,
        new_schema: s_schema.Schema,
    ) -> Object:
        from . import delta as sd

        rename_class = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.RenameObject, type(obj))

        return rename_class(classname=obj.get_name(old_schema),
                            new_name=new_name,
                            metaclass=type(obj))

    @classmethod
    def _sort_set(
        cls, schema: s_schema.Schema, items: ordered.OrderedSet[Object_T]
    ) -> ordered.OrderedSet[Object_T]:
        from . import inheriting as s_inh

        if items:
            probe = next(iter(items))

            if isinstance(probe, s_inh.InheritingObject):
                items_idx = {p.get_name(schema): p for p in items}
                g = {}
                c_items = cast(ordered.OrderedSet[InheritingObjectBase], items)
                for x in c_items:
                    deps = {b for b in x._get_deps(schema) if b in items_idx}
                    g[x.get_name(schema)] = {'item': x, 'deps': deps}
                items = topological.sort(g)

        return items

    @classmethod
    def delta_sets(
        cls,
        old: Optional[Iterable[Object]],
        new: Optional[Iterable[Object]],
        context: Optional[ComparisonContext] = None,
        *,
        old_schema: Optional[s_schema.Schema],
        new_schema: s_schema.Schema,
    ) -> Union[sd.DeltaRoot, Tuple[sd.DeltaRoot, ...]]:
        from edb.schema import delta as sd
        from edb.schema import inheriting as s_inh

        adds_mods = sd.DeltaRoot()
        dels = sd.DeltaRoot()

        if old is None:
            if new is None:
                raise ValueError("`old` and `new` cannot be both None.")
            for n in new:
                adds_mods.add(n.delta(None, n, context=context,
                                      old_schema=old_schema,
                                      new_schema=new_schema))
            return adds_mods, dels
        elif new is None:
            if old is None:
                raise ValueError("`old` and `new` cannot be both None.")
            for o in old:
                dels.add(o.delta(o, None, context=context,
                                 old_schema=old_schema,
                                 new_schema=new_schema))
            return adds_mods, dels

        old = list(old)
        new = list(new)

        if old and old_schema is None:
            raise ValueError("`old` is present but `old_schema` is None")

        oldkeys = {
            o.id: o.hash_criteria(old_schema) for o in old  # type: ignore
        }
        newkeys = {o.id: o.hash_criteria(new_schema) for o in new}

        unchanged = set(oldkeys.values()) & set(newkeys.values())

        old = ordered.OrderedSet[Object](
            o for o in old
            if oldkeys[o.id] not in unchanged)
        new = ordered.OrderedSet[Object](
            o for o in new
            if newkeys[o.id] not in unchanged)

        comparison: List[Tuple[float, Object, Object]] = []
        for x, y in itertools.product(new, old):
            comp = x.compare(y, our_schema=new_schema,
                             their_schema=old_schema)  # type: ignore
            comparison.append((comp, x, y))

        used_x: Set[Object] = set()
        used_y: Set[Object] = set()
        altered = ordered.OrderedSet[sd.ObjectCommand]()

        comparison = sorted(comparison, key=lambda item: item[0], reverse=True)

        for s, x, y in comparison:
            if x not in used_x and y not in used_y:
                if s != 1.0:
                    if s > 0.6:
                        altered.add(x.delta(y, x, context=context,
                                            old_schema=old_schema,
                                            new_schema=new_schema))
                        used_x.add(x)
                        used_y.add(y)
                else:
                    used_x.add(x)
                    used_y.add(y)

        deleted = old - used_y
        created = new - used_x

        if created:
            created = cls._sort_set(new_schema, created)
            for x in created:
                adds_mods.add(x.delta(None, x, context=context,
                                      old_schema=old_schema,
                                      new_schema=new_schema))

        if old_schema is not None and new_schema is not None:
            probe: Optional[Object]
            if old:
                probe = next(iter(old))
            elif new:
                probe = next(iter(new))
            else:
                probe = None

            if probe is not None:
                has_bases = isinstance(probe, s_inh.InheritingObject)
            else:
                has_bases = False

            if has_bases:
                g = {}

                altered_idx = {p.classname: p for p in altered}
                for p in altered:
                    for op in p.get_subcommands(
                            type=sd.RenameObject):
                        altered_idx[op.new_name] = p

                for p in altered:
                    old_class = old_schema.get(p.classname)

                    for op in p.get_subcommands(
                            type=sd.RenameObject):
                        new_name = op.new_name
                        break
                    else:
                        new_name = p.classname

                    new_class = new_schema.get(new_name)

                    old_bases = \
                        old_class.get_bases(old_schema).objects(old_schema)
                    new_bases = \
                        new_class.get_bases(new_schema).objects(new_schema)

                    bases = (
                        {b.get_name(old_schema) for b in old_bases} |
                        {b.get_name(new_schema) for b in new_bases}
                    )

                    deps = {b for b in bases if b in altered_idx}

                    g[p.classname] = {'item': p, 'deps': deps}
                    if new_name != p.classname:
                        g[new_name] = {'item': p, 'deps': deps}

                altered = topological.sort(g)

        for p in altered:
            adds_mods.add(p)

        if deleted:
            deleted = cls._sort_set(old_schema, deleted)  # type: ignore
            for y in reversed(list(deleted)):
                dels.add(y.delta(y, None, context=context,
                                 old_schema=old_schema,
                                 new_schema=new_schema))

        adds_mods.update(dels)
        return adds_mods

    def dump(self, schema: s_schema.Schema) -> str:
        return (
            f'<{type(self).__name__} name={self.get_name(schema)!r} '
            f'at {id(self):#x}>'
        )

    def __repr__(self) -> str:
        return f'<{type(self).__name__} {self.id} at 0x{id(self):#x}>'


class ObjectFragment(Object):
    """A part of another object that cannot exist independently."""


class UnqualifiedObject(Object):

    name = SchemaField(
        # ignore below because Mypy doesn't understand fields which are not
        # inheritable.
        str,  # type: ignore
        inheritable=False,
        compcoef=0.670,
    )

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return self.get_name(schema)


class GlobalObject(UnqualifiedObject):
    pass


class ObjectRef(Object):
    _name: str
    _origname: Optional[str]
    _schemaclass: Optional[ObjectMeta]
    _sourcectx: Optional[parsing.ParserContext]

    def __init__(
        self,
        *,
        name: str,
        origname: Optional[str] = None,
        schemaclass: Optional[ObjectMeta] = None,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> None:
        self.__dict__['_name'] = name
        self.__dict__['_origname'] = origname
        self.__dict__['_sourcectx'] = sourcectx
        self.__dict__['_schemaclass'] = schemaclass

    # `name` and `get_name` are deliberately incompatible with the base
    # `Object` equivalents so we type-ignore them

    @property
    def name(self) -> str:  # type: ignore
        return self._name

    def get_name(self, schema: s_schema.Schema) -> str:  # type: ignore
        return self._name

    def get_refname(self, schema: s_schema.Schema) -> str:
        return self._origname if self._origname is not None else self._name

    def get_sourcectx(self, schema: s_schema.Schema) -> Any:
        return self._sourcectx

    def __repr__(self) -> str:
        return '<ObjectRef "{}" at 0x{:x}>'.format(self._name, id(self))

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return NotImplemented
        return self._name == other._name

    def __hash__(self) -> int:
        return hash((self._name, type(self)))

    def _reduce_to_ref(self, schema: s_schema.Schema) -> Tuple[ObjectRef, Any]:
        return self, self.get_name(schema)

    def _resolve_ref(self, schema: s_schema.Schema) -> Object:
        return schema.get(
            self.get_name(schema),
            type=self._schemaclass,
            refname=self._origname,
            sourcectx=self.get_sourcectx(schema),
        )


class ObjectCollectionDuplicateNameError(Exception):
    pass


class ObjectCollection(s_abc.ObjectContainer, Generic[Object_T]):
    _type: Type[Object_T]
    _container: Type[CollectionFactory]

    def __init_subclass__(
        cls,
        *,
        type: Type[Object_T] = Object,  # type: ignore
        container: Optional[Type[Collection]] = None,
    ) -> None:
        # Type ignore above due to https://github.com/python/mypy/issues/7927

        cls._type = type
        if container is not None:
            cls._container = container

    def __init__(
        self,
        ids: Union[Collection[uuid.UUID], Collection[ObjectRef]],
        *,
        _private_init: bool,
    ) -> None:
        self._ids = ids

    def __len__(self) -> int:
        return len(self._ids)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._ids == other._ids

    def __hash__(self) -> int:
        return hash(self._ids)

    def dump(self, schema: s_schema.Schema) -> str:
        return (
            f'<{type(self).__name__} objects='
            f'[{", ".join(o.dump(schema) for o in self.objects(schema))}] '
            f'at {id(self):#x}>'
        )

    @classmethod
    def create(
        cls: Type[ObjectCollection_T],
        schema: s_schema.Schema,
        data: Iterable[Object_T],
    ) -> ObjectCollection_T:
        ids = []

        if isinstance(data, ObjectCollection):
            ids = data._ids
        elif data:
            for v in data:
                ids.append(cls._validate_value(schema, v))
        container: CollectionFactory[uuid.UUID] = cls._container(ids)
        return cls(container, _private_init=True)

    @classmethod
    def create_empty(cls) -> ObjectCollection:
        return cls(cls._container(), _private_init=True)

    @classmethod
    def _validate_value(
        cls, schema: s_schema.Schema, v: Object
    ) -> Union[ObjectRef, uuid.UUID]:
        if not isinstance(v, cls._type):
            raise TypeError(
                f'invalid input data for ObjectIndexByShortname: '
                f'expected {cls._type} values, got {type(v)}')

        if v.id is not None:
            return v.id
        elif isinstance(v, ObjectRef):
            return v
        else:
            raise TypeError(f'object {v!r} has no ID!')

        return v

    def ids(self, schema: s_schema.Schema) -> Tuple[uuid.UUID, ...]:
        result: List[uuid.UUID] = []

        for item_id in self._ids:
            if isinstance(item_id, ObjectRef):
                resolved = item_id._resolve_ref(schema)
                # type ignore due to https://github.com/python/mypy/issues/7153
                result.append(resolved.id)
            else:
                result.append(item_id)

        return tuple(result)

    def names(
        self, schema: s_schema.Schema, *, allow_unresolved: bool = False
    ) -> Collection[str]:
        result = []

        for item_id in self._ids:
            if isinstance(item_id, ObjectRef):
                try:
                    obj = item_id._resolve_ref(schema)
                except errors.InvalidReferenceError:
                    if allow_unresolved:
                        result.append(item_id.get_name(schema))
                    else:
                        raise
                else:
                    result.append(obj.get_name(schema))
            else:
                obj = schema.get_by_id(item_id)
                result.append(obj.get_name(schema))

        return type(self)._container(result)

    def objects(self, schema: s_schema.Schema) -> Tuple[Any, ...]:
        # The `Any` return type is so that using methods on Object subclasses
        # doesn't cause Mypy to complain.
        result = []

        for item_id in self._ids:
            if isinstance(item_id, ObjectRef):
                result.append(item_id._resolve_ref(schema))
            else:
                result.append(schema.get_by_id(item_id))

        return tuple(result)

    @classmethod
    def compare_values(
        cls,
        ours: ObjectCollection,
        theirs: ObjectCollection,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: Optional[ComparisonContext],
        compcoef: float,
    ) -> float:
        if ours is not None:
            our_names = ours.names(our_schema, allow_unresolved=True)
        else:
            our_names = cls._container()

        if theirs is not None:
            their_names = theirs.names(their_schema, allow_unresolved=True)
        else:
            their_names = cls._container()

        if frozenset(our_names) != frozenset(their_names):
            return compcoef
        else:
            return 1.0

    # Breaking Liskov Substitution Principle below (by adding `v`).
    def _reduce_to_ref(  # type: ignore
        self: T, schema: s_schema.Schema, v: ObjectCollection
    ) -> Tuple[T, Any]:
        raise NotImplementedError


KeyFunction = Callable[["s_schema.Schema", Object], str]
OIBT = TypeVar("OIBT", bound="ObjectIndexBase")


class ObjectIndexBase(ObjectCollection, container=tuple):
    _key: KeyFunction

    def __init_subclass__(cls, *, key: KeyFunction):
        cls._key = key

    @classmethod
    def get_key_for(cls, schema: s_schema.Schema, obj: Object) -> str:
        return cls._key(schema, obj)

    @classmethod
    def get_key_for_name(cls, schema: s_schema.Schema, name: str) -> str:
        return name

    @classmethod
    def create(
        cls: Type[OIBT], schema: s_schema.Schema, data: Iterable[Object]
    ) -> OIBT:
        coll = super().create(schema, data)
        coll._check_duplicates(schema)
        return coll

    def _check_duplicates(self, schema: s_schema.Schema) -> None:
        counts = collections.Counter(self.keys(schema))
        duplicates = [v for v, count in counts.items() if count > 1]
        if duplicates:
            raise ObjectCollectionDuplicateNameError(
                'object index contains duplicate key(s): ' +
                ', '.join(repr(duplicates)))

    @classmethod
    def compare_values(
        cls,
        ours: ObjectCollection,
        theirs: ObjectCollection,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: Optional[ComparisonContext],
        compcoef: float,
    ) -> float:

        if not ours and not theirs:
            basecoef = 1.0
        elif not ours or not theirs:
            basecoef = 0.2
        else:
            assert isinstance(ours, ObjectIndexBase)
            assert isinstance(theirs, ObjectIndexBase)
            similarity: List[float] = []
            for k, v in ours.items(our_schema):
                try:
                    theirsv = theirs.get(their_schema, k)
                except KeyError:
                    # key only in ours
                    similarity.append(0.2)
                else:
                    similarity.append(
                        v.compare(theirsv, our_schema=our_schema,
                                  their_schema=their_schema, context=context))

            diff = (
                set(theirs.keys(their_schema)) -
                set(ours.keys(our_schema))
            )
            similarity.extend(0.2 for k in diff)

            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef

    def add(
        self: OIBT, schema: s_schema.Schema, item: Object
    ) -> Tuple[s_schema.Schema, OIBT]:
        """Return a copy of this collection containing the given item.

        If the item is already present in the collection, an
        ``ObjectIndexDuplicateNameError`` is raised.
        """

        key = type(self)._key(schema, item)
        if self.has(schema, key):
            raise ObjectCollectionDuplicateNameError(
                f'object index already contains the {key!r} key')

        return self.update(schema, [item])

    def update(
        self: OIBT, schema: s_schema.Schema, reps: Iterable[Object]
    ) -> Tuple[s_schema.Schema, OIBT]:
        items = dict(self.items(schema))
        keyfunc = type(self)._key

        for obj in reps:
            items[keyfunc(schema, obj)] = obj

        return schema, type(self).create(schema, items.values())

    def delete(
        self: OIBT,
        schema: s_schema.Schema,
        names: Iterable[str],
    ) -> Tuple[s_schema.Schema, OIBT]:
        items = dict(self.items(schema))
        for name in names:
            items.pop(name)
        return schema, type(self).create(schema, items.values())

    def items(self, schema: s_schema.Schema) -> Tuple[Tuple[str, Any], ...]:
        result = []
        keyfunc = type(self)._key

        for obj in self.objects(schema):
            result.append((keyfunc(schema, obj), obj))

        return tuple(result)

    def keys(self, schema: s_schema.Schema) -> Tuple[str, ...]:
        result = []
        keyfunc = type(self)._key

        for obj in self.objects(schema):
            result.append(keyfunc(schema, obj))

        return tuple(result)

    def has(self, schema: s_schema.Schema, name: str) -> bool:
        return name in self.keys(schema)

    def get(
        self, schema: s_schema.Schema, name: str, default: Any = NoDefault
    ):
        items = dict(self.items(schema))
        if default is NoDefault:
            return items[name]
        else:
            return items.get(name, default)


class ObjectIndexByFullname(
        ObjectIndexBase,
        key=lambda schema, o: o.get_name(schema)):
    pass


class ObjectIndexByShortname(
        ObjectIndexBase,
        key=lambda schema, o: o.get_shortname(schema)):

    @classmethod
    def get_key_for_name(cls, schema: s_schema.Schema, name: str) -> str:
        return sn.shortname_from_fullname(name)


class ObjectIndexByUnqualifiedName(
        ObjectIndexBase,
        key=lambda schema, o: o.get_shortname(schema).name):

    @classmethod
    def get_key_for_name(cls, schema: s_schema.Schema, name: str) -> str:
        return sn.shortname_from_fullname(name).name


class ObjectDict(ObjectCollection, container=tuple):
    _keys: Tuple[Any, ...]

    # Breaking the Liskov Substitution Principle
    @classmethod
    def create(  # type: ignore
        cls,
        schema: s_schema.Schema,
        data: Mapping[Any, Object],
    ) -> ObjectDict:

        result = super().create(schema, data.values())
        result._keys = tuple(data.keys())
        return result

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._ids == other._ids and self._keys == other._keys

    def __hash__(self) -> int:
        return hash((self._ids, self._keys))

    def dump(self, schema: s_schema.Schema) -> str:
        objs = ", ".join(f"{self._keys[i]}: {o.dump(schema)}"
                         for i, o in enumerate(self.objects(schema)))
        return f'<{type(self).__name__} objects={objs} at {id(self):#x}>'

    def __repr__(self) -> str:
        items = [f"{self._keys[i]}: {id}" for i, id in enumerate(self._ids)]
        return f'{{{", ".join(items)}}}'

    def keys(self, schema: s_schema.Schema) -> Tuple[Any, ...]:
        return self._keys

    def values(self, schema: s_schema.Schema) -> Tuple[Object, ...]:
        return self.objects(schema)

    def items(self, schema: s_schema.Schema) -> Tuple[Tuple[Any, Object], ...]:
        return tuple(zip(self._keys, self.objects(schema)))

    # Breaking Liskov Substitution Principle below (by adding `v`).
    def _reduce_to_ref(  # type: ignore
        self: ObjectDict, schema: s_schema.Schema, v: ObjectDict
    ) -> Tuple[ObjectDict, Any]:
        result = {}
        comparison_v = []

        for key, scls in v.items(schema):
            ref, comp = scls._reduce_to_ref(schema)
            result[key] = ref
            comparison_v.append((key, comp))

        return type(v).create(schema, result), frozenset(comparison_v)


class ObjectSet(ObjectCollection, container=frozenset):

    def __repr__(self) -> str:
        return f'{{{", ".join(str(id) for id in self._ids)}}}'

    @staticmethod
    def merge_values(
        target: Object,
        sources: Iterable[Object],
        field_name: str,
        *,
        schema: s_schema.Schema,
    ):
        result = target.get_explicit_field_value(schema, field_name, None)
        for source in sources:
            if source.__class__.get_field(field_name) is None:
                continue
            theirs = source.get_explicit_field_value(schema, field_name, None)
            if theirs:
                if result is None:
                    result = theirs
                else:
                    result._ids |= theirs._ids

        return result


class ObjectList(ObjectCollection, container=tuple):

    def __repr__(self) -> str:
        return f'[{", ".join(str(id) for id in self._ids)}]'

    def first(
        self, schema: s_schema.Schema, default: Any = NoDefault
    ) -> Any:
        # The `Any` return type is so that using methods on Object subclasses
        # doesn't cause Mypy to complain.
        try:
            return next(iter(self.objects(schema)))
        except StopIteration:
            pass

        if default is NoDefault:
            raise IndexError('ObjectList is empty')
        else:
            return default


class InheritingObjectBase(Object):

    bases = SchemaField(
        ObjectList,
        default=ObjectList,
        coerce=True,
        inheritable=False,
        compcoef=0.714,
    )

    ancestors = SchemaField(
        ObjectList,
        default=ObjectList,
        coerce=True,
        inheritable=False,
        hashable=False,
    )

    # Attributes that have been set locally as opposed to inherited.
    inherited_fields = SchemaField(
        immu.Map,
        default=immu.Map(),
        inheritable=False,
        hashable=False,
    )

    is_derived = SchemaField(
        bool,
        default=False, compcoef=0.909)

    is_abstract = SchemaField(
        bool,
        default=False,
        inheritable=False, compcoef=0.909)

    is_final = SchemaField(
        bool,
        default=False, compcoef=0.909)

    def _issubclass(
        self, schema: s_schema.Schema, parent: InheritingObjectBase
    ) -> bool:
        lineage = compute_lineage(schema, self)
        return parent in lineage

    def issubclass(
        self,
        schema: s_schema.Schema,
        parent: Union[InheritingObjectBase, Tuple[InheritingObjectBase, ...]],
    ) -> bool:
        from . import types as s_types
        if isinstance(parent, tuple):
            return any(self.issubclass(schema, p) for p in parent)
        else:
            if isinstance(parent, s_types.Type) and parent.is_any():
                return True
            else:
                return self._issubclass(schema, parent)

    def descendants(
        self, schema: s_schema.Schema
    ) -> FrozenSet[objtypes.ObjectType]:
        return schema.get_descendants(self)

    def ordered_descendants(
        self, schema: s_schema.Schema
    ) -> List[objtypes.ObjectType]:
        """Return class descendants in ancestral order."""
        graph = {}
        for descendant in self.descendants(schema):
            graph[descendant] = {
                'item': descendant,
                'deps': descendant.get_bases(schema).objects(schema),
            }

        return list(topological.sort(graph, allow_unresolved=True))

    def children(self, schema: s_schema.Schema) -> frozenset:
        return schema.get_children(self)

    def get_nearest_non_derived_parent(
        self, schema: s_schema.Schema
    ) -> Object:
        obj = self
        while obj.get_is_derived(schema):
            obj = cast(
                InheritingObjectBase, obj.get_bases(schema).first(schema)
            )
        return obj

    def get_explicit_local_field_value(
        self,
        schema: s_schema.Schema,
        field_name: str,
        default: Any = NoDefault,
    ) -> Any:
        inherited_fields = self.get_inherited_fields(schema)
        if not inherited_fields.get(field_name):
            return self.get_explicit_field_value(schema, field_name, default)
        elif default is not NoDefault:
            return default
        else:
            raise FieldValueNotFoundError(
                f'{self!r} object has no non-inherited value for '
                f'field {field_name!r}'
            )

    def _get_deps(self, schema: s_schema.Schema) -> Set[sn.Name]:
        return {
            b.get_name(schema)
            for b in self.get_bases(schema).objects(schema)
        }


@markup.serializer.serializer.register(Object)
@markup.serializer.serializer.register(ObjectCollection)
def _serialize_to_markup(o: Object, *, ctx):
    if 'schema' not in ctx.kwargs:
        orepr = repr(o)
    else:
        orepr = o.dump(ctx.kwargs['schema'])

    return markup.elements.lang.Object(
        id=id(o), class_module=type(o).__module__,
        classname=type(o).__name__,
        repr=orepr)


def _merge_lineage(
    schema: s_schema.Schema, obj: Object, lineage: Iterable[Any]
) -> List[Any]:
    result: List[Any] = []

    while True:
        nonempty = [line for line in lineage if line]
        if not nonempty:
            return result

        for line in nonempty:
            candidate = line[0]
            tails = [m for m in nonempty
                     if id(candidate) in {id(c) for c in m[1:]}]
            if not tails:
                break
        else:
            name = obj.get_verbosename(schema)
            raise errors.SchemaError(
                f"Could not find consistent ancestor order for {name}"
            )

        result.append(candidate)

        for line in nonempty:
            if line[0] is candidate:
                del line[0]

    return result


def compute_lineage(
    schema: s_schema.Schema, obj: InheritingObjectBase
) -> List[Any]:
    bases = tuple(obj.get_bases(schema).objects(schema))
    lineage = [[obj]]

    for base in bases:
        # ObjectList could be made generic then this ignore would not be
        # necessary.  Mypy sees `base` as Object whereas it's an IOB for sure.
        lineage.append(compute_lineage(schema, base))  # type: ignore

    return _merge_lineage(schema, obj, lineage)


def compute_ancestors(
    schema: s_schema.Schema, obj: InheritingObjectBase
) -> List[Any]:
    return compute_lineage(schema, obj)[1:]
