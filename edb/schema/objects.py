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
from typing_extensions import Final

import builtins
import collections
import collections.abc
import enum
import itertools
import sys
import uuid

from edb import errors
from edb.edgeql import qltypes

from edb.common import checked
from edb.common import markup
from edb.common import ordered
from edb.common import parametric
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
            target: InheritingObject,
            sources: Iterable[Object],
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


class DefaultConstructorT(enum.Enum):
    DefaultConstructor = 0


DEFAULT_CONSTRUCTOR: Final = DefaultConstructorT.DefaultConstructor


T = TypeVar("T")
Type_T = TypeVar("Type_T", bound=type)
Object_T = TypeVar("Object_T", bound="Object")
ObjectCollection_T = TypeVar(
    "ObjectCollection_T",
    bound="ObjectCollection[Object]",
)
HashCriterion = Union[Type["Object"], Tuple[str, Any]]

_EMPTY_FIELD_FROZENSET: FrozenSet[Field[Any]] = frozenset()


def default_field_merge(
    target: InheritingObject,
    sources: Iterable[Object],
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


class ComparisonContext:

    def __init__(
        self,
        *,
        related_schemas: bool = False,
    ) -> None:
        self.related_schemas = related_schemas


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

    def coerce_value(
        self,
        schema: s_schema.Schema,
        value: Any,
    ) -> Optional[T]:
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
            # Mypy complains about ambiguity and generics in class vars here,
            # although the generic in SingleParameter is clearly a type.
            valtype = ftype.type  # type: ignore
            for v in value:
                if v is not None and not isinstance(v, valtype):
                    v = valtype(v)
                casted_list.append(v)

            value = casted_list

        elif issubclass(ftype, checked.CheckedDict):
            casted_dict = {}
            for k, v in value.items():
                if k is not None and not isinstance(k, ftype.keytype):
                    k = ftype.keytype(k)
                if v is not None and not isinstance(v, ftype.valuetype):
                    v = ftype.valuetype(v)
                casted_dict[k] = v

            value = casted_dict

        elif issubclass(ftype, ObjectCollection):
            # Type ignore below because mypy narrowed ftype to
            # Type[ObjectCollection] and lost track that it's actually
            # Type[T]
            return ftype.create(schema, value)  # type: ignore

        try:
            # Type ignore below because Mypy doesn't trust we can instantiate
            # the type using the value.  We don't trust that either but this
            # is why there's the try-except block.
            return ftype(value)  # type: ignore
        except Exception:
            raise TypeError(
                f'cannot coerce {self.name!r} value {value!r} to {ftype}')

    @property
    def required(self) -> bool:
        return True

    @property
    def is_schema_field(self) -> bool:
        return False

    def __get__(
        self,
        instance: Optional[Object],
        owner: Type[Object],
    ) -> Optional[T]:
        if instance is not None:
            return None
        else:
            raise AttributeError(
                f"type object {owner.__name__!r} "
                f"has no attribute {self.name!r}"
            )

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

    def __get__(
        self,
        instance: Optional[Object],
        owner: Type[Object],
    ) -> Optional[T]:
        if instance is not None:
            raise FieldValueNotFoundError(self.name)
        else:
            raise AttributeError(
                f"type object {owner.__name__!r} "
                f"has no attribute {self.name!r}"
            )


class RefDict(struct.Struct):

    attr = struct.Field(
        str, frozen=True)

    backref_attr = struct.Field(
        str, default='subject', frozen=True)

    requires_explicit_overloaded = struct.Field(
        bool, default=False, frozen=True)

    ref_cls = struct.Field(
        type, frozen=True)


class ObjectMeta(type):

    _all_types: List[ObjectMeta] = []
    _schema_types: Set[ObjectMeta] = set()
    _ql_map: Dict[qltypes.SchemaObjectClass, ObjectMeta] = {}

    # Instance fields (i.e. class fields on types built with ObjectMeta)
    _fields: Dict[str, Field[Any]]
    _hashable_fields: Set[Field[Any]]  # if f.is_schema_field and f.hashable
    _sorted_fields: collections.OrderedDict[str, Field[Any]]
    _object_fields: FrozenSet[Field[Any]]
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
                    # The getter was defined explicitly, move on.
                    continue
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
        cls._object_fields = _EMPTY_FIELD_FROZENSET

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

        for f in myfields.values():
            if (issubclass(f.type, parametric.ParametricType)
                    and not f.type.is_fully_resolved()):
                f.type.resolve_types({cls.__name__: cls})

        cls._ql_class = qlkind
        mcls._all_types.append(cls)

        return cls

    def get_object_fields(cls) -> FrozenSet[Field[Any]]:
        if cls._object_fields is _EMPTY_FIELD_FROZENSET:
            cls._object_fields = frozenset(
                f for f in cls._fields.values()
                if issubclass(f.type, s_abc.ObjectContainer)
            )
        return cls._object_fields

    def has_field(cls, name: str) -> bool:
        return name in cls._fields

    def get_field(cls, name: str) -> Field[Any]:
        field = cls._fields.get(name)
        if field is None:
            raise LookupError(
                f'schema class {cls.__name__!r} has no field {name!r}'
            )
        return field

    def get_fields(cls, sorted: bool = False) -> Mapping[str, Field[Any]]:
        return cls._sorted_fields if sorted else cls._fields

    def get_ownfields(cls) -> Mapping[str, Field[Any]]:
        return getattr(  # type: ignore
            cls,
            f'{cls.__module__}.{cls.__name__}_fields',
        )

    def get_own_refdicts(cls) -> Mapping[str, RefDict]:
        return getattr(  # type: ignore
            cls,
            f'{cls.__module__}.{cls.__name__}_refdicts',
        )

    def get_refdicts(cls) -> Iterator[RefDict]:
        return iter(cls._refdicts.values())

    def has_refdict(cls, name: str) -> bool:
        return name in cls._refdicts_by_field

    def get_refdict(cls, name: str) -> RefDict:
        refdict = cls._refdicts_by_field.get(name)
        if refdict is None:
            raise LookupError(
                f'schema class {cls.__name__!r} has no refdict {name!r}'
            )
        return refdict

    def get_refdict_for_class(cls, refcls: type) -> RefDict:
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
    ) -> Type[Object]:
        cls = mcls._ql_map.get(qlkind)
        if cls is None:
            raise LookupError(f'no schema metaclass for {qlkind}')
        return cast(Type[Object], cls)

    def get_ql_class(cls) -> Optional[qltypes.SchemaObjectClass]:
        return cls._ql_class

    def get_ql_class_or_die(cls) -> qltypes.SchemaObjectClass:
        if cls._ql_class is not None:
            return cls._ql_class
        else:
            raise LookupError(f'{cls} has no edgeql class string assigned')


class FieldValueNotFoundError(Exception):
    pass


class Object(s_abc.Object, s_abc.ObjectContainer, metaclass=ObjectMeta):
    """Base schema item class."""

    # Unique ID for this schema item.
    id = Field(
        uuid.UUID,
        inheritable=False,
        simpledelta=False,
        allow_ddl_set=True,
    )

    # Schema source context for this object
    sourcectx = SchemaField(
        parsing.ParserContext,
        default=None,
        compcoef=None,
        inheritable=False,
        introspectable=False,
        hashable=False,
        ephemeral=True,
    )

    name = SchemaField(
        str,
        inheritable=False,
        compcoef=0.670,
    )

    # The path_id_name field is solely for the purposes of the compiler
    # so that this item can act as a transparent proxy for the item
    # it has been derived from, specifically in path ids.
    path_id_name = SchemaField(
        sn.Name,
        inheritable=False, ephemeral=True,
        introspectable=False, default=None)

    _fields: Dict[str, SchemaField[Any]]

    def get_id(self, schema: s_schema.Schema) -> uuid.UUID:
        return self.id

    def get_shortname(self, schema: s_schema.Schema) -> str:
        return sn.shortname_from_fullname(self.get_name(schema))

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return str(self.get_shortname(schema))

    def get_verbosename(
        self, schema: s_schema.Schema, *, with_parent: bool = False
    ) -> str:
        clsname = self.get_schema_class_displayname()
        dname = self.get_displayname(schema)
        return f"{clsname} '{dname}'"

    def __init__(self, *, _private_init: bool) -> None:
        pass

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return NotImplemented
        return self.id == other.id  # type: ignore

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
        schema: s_schema.Schema,
        *,
        id: Optional[uuid.UUID] = None,
        **data: Any,
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
    ) -> Optional[T]:
        value: Optional[T]

        if field.default is NoDefault:
            raise TypeError(f'{type(self).__name__}.{field_name} is required')
        elif field.default is DEFAULT_CONSTRUCTOR:
            if issubclass(field.type, ObjectCollection):
                value = field.type.create_empty()
            else:
                # The dance below is required to workaround a bug in mypy:
                # Unsupported type Type["Type[T]"]
                t = cast(type, field.type)
                value = t()
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
            assert isinstance(field, SchemaField)

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
        context: ComparisonContext,
    ) -> float:
        if (not isinstance(other, self.__class__) and
                not isinstance(self, other.__class__)):
            raise NotImplementedError(
                f'class {self.__class__.__name__!r} and '
                f'class {other.__class__.__name__!r} are not comparable'
            )

        cls = type(self)

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
                context=context,
            )

            similarity *= fcoef

        return similarity

    def is_blocking_ref(
        self, schema: s_schema.Schema, reference: Object
    ) -> bool:
        return True

    @classmethod
    def compare_field_value(
        cls,
        field: Field[Type[T]],
        our_value: T,
        their_value: T,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: ComparisonContext,
    ) -> float:
        comparator = getattr(field.type, 'compare_values', None)
        assert field.compcoef is not None
        if callable(comparator):
            result = comparator(
                our_value,
                their_value,
                context=context,
                our_schema=our_schema,
                their_schema=their_schema,
                compcoef=field.compcoef,
            )
            assert isinstance(result, (float, int))
            return result

        if our_value != their_value:
            return field.compcoef
        else:
            return 1.0

    @classmethod
    def compare_values(
        cls: Type[Object_T],
        ours: Optional[Object_T],
        theirs: Optional[Object_T],
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: ComparisonContext,
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
        context: ComparisonContext,
        old_schema: Optional[s_schema.Schema],
        new_schema: s_schema.Schema,
    ) -> sd.ObjectCommand[Object]:
        from . import delta as sd

        command_args: Dict[str, Any] = {'canonical': True}

        delta: sd.ObjectCommand[Object]

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
            cls.delta_properties(
                delta,
                old,
                new,
                context=context,
                old_schema=old_schema,
                new_schema=new_schema,
            )

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
            cls.delta_properties(
                delta,
                old,
                new,
                context=context,
                old_schema=old_schema,
                new_schema=new_schema,
            )

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
        delta: sd.ObjectCommand[Object],
        refdict: RefDict,
        context: ComparisonContext,
        old_schema: Optional[s_schema.Schema],
        new_schema: s_schema.Schema,
    ) -> None:

        old_idx_key = lambda o: o.get_name(old_schema)
        new_idx_key = lambda o: o.get_name(new_schema)

        def _delta_subdict(attr: str) -> None:
            if old:
                assert old_schema is not None
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

            delta.add(
                cls.delta_sets(
                    oldcoll_idx,
                    newcoll_idx,
                    context=context,
                    old_schema=old_schema,
                    new_schema=new_schema,
                )
            )

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

    def as_shell(self, schema: s_schema.Schema) -> ObjectShell:
        return ObjectShell(
            name=self.get_name(schema),
            displayname=self.get_displayname(schema),
            schemaclass=type(self),
        )

    @classmethod
    def delta_properties(
        cls,
        delta: sd.ObjectCommand[Object],
        old: Optional[Object],
        new: Object,
        *,
        context: ComparisonContext,
        old_schema: Optional[s_schema.Schema],
        new_schema: s_schema.Schema,
    ) -> None:
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

                old_v: Any
                new_v: Any

                if (issubclass(f.type, s_abc.ObjectContainer)
                        and not context.related_schemas):
                    if oldattr_v is not None:
                        old_v = oldattr_v.as_shell(old_schema)
                    else:
                        old_v = None
                    if newattr_v is not None:
                        new_v = newattr_v.as_shell(new_schema)
                    else:
                        new_v = None
                else:
                    old_v = oldattr_v
                    new_v = newattr_v

                if f.compcoef is not None:
                    fcoef = cls.compare_field_value(
                        f,
                        oldattr_v,
                        newattr_v,
                        our_schema=old_schema,
                        their_schema=new_schema,
                        context=context)

                    if fcoef != 1.0:
                        delta.set_attribute_value(
                            fn,
                            new_v,
                            orig_value=old_v,
                        )
        elif new:
            # IDs are assigned once when the object is created and
            # never changed.
            id_value = new.get_explicit_field_value(new_schema, 'id')
            delta.set_attribute_value('id', id_value)

            for fn, f in fields.items():
                value = new.get_explicit_field_value(new_schema, fn, None)
                if value is not None:
                    v: Any
                    if (issubclass(f.type, s_abc.ObjectContainer)
                            and not context.related_schemas):
                        v = value.as_shell(new_schema)
                    else:
                        v = value
                    cls.delta_property(new_schema, new, delta, fn, v)

    @classmethod
    def delta_property(
        cls,
        schema: s_schema.Schema,
        scls: Object,
        delta: sd.ObjectCommand[Object],
        fname: str,
        value: Any,
    ) -> None:
        delta.set_attribute_value(fname, value)

    @classmethod
    def delta_rename(
        cls,
        obj: Object,
        new_name: str,
        *,
        old_schema: s_schema.Schema,
        new_schema: s_schema.Schema,
    ) -> sd.RenameObject:
        from . import delta as sd

        rename_class = sd.ObjectCommandMeta.get_command_class_or_die(
            sd.RenameObject, type(obj))

        return rename_class(classname=obj.get_name(old_schema),
                            new_name=new_name,
                            metaclass=type(obj))

    @classmethod
    def delta_sets(
        cls,
        old: Optional[Iterable[Object]],
        new: Optional[Iterable[Object]],
        context: ComparisonContext,
        *,
        old_schema: Optional[s_schema.Schema],
        new_schema: s_schema.Schema,
    ) -> sd.DeltaRoot:
        from edb.schema import delta as sd

        adds_mods = sd.DeltaRoot()
        dels = sd.DeltaRoot()

        if old is None:
            if new is None:
                raise ValueError("`old` and `new` cannot be both None.")
            for n in new:
                adds_mods.add(n.delta(None, n, context=context,
                                      old_schema=old_schema,
                                      new_schema=new_schema))
            return adds_mods
        elif new is None:
            if old is None:
                raise ValueError("`old` and `new` cannot be both None.")
            for o in old:
                dels.add(o.delta(o, None, context=context,
                                 old_schema=old_schema,
                                 new_schema=new_schema))
            return dels

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
            # type ignore below, because mypy does not correlate `old`
            # and `old_schema`.
            comp = x.compare(
                y,
                our_schema=new_schema,
                their_schema=old_schema,  # type: ignore
                context=context,
            )
            comparison.append((comp, x, y))

        used_x: Set[Object] = set()
        used_y: Set[Object] = set()
        altered = ordered.OrderedSet[sd.ObjectCommand[Object]]()

        def _key(item: Tuple[float, Object, Object]) -> Tuple[float, str]:
            return item[0], item[1].get_name(new_schema)

        comparison = sorted(comparison, key=_key, reverse=True)

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
                has_bases = isinstance(probe, InheritingObject)
            else:
                has_bases = False

            if has_bases:
                g = {}

                altered_idx = {p.classname: p for p in altered}
                for p in altered:
                    for op in p.get_subcommands(type=sd.RenameObject):
                        altered_idx[op.new_name] = p

                for p in altered:
                    old_class: Object = old_schema.get(p.classname)

                    for op in p.get_subcommands(type=sd.RenameObject):
                        new_name = op.new_name
                        break
                    else:
                        new_name = p.classname

                    new_class: Object = new_schema.get(new_name)

                    assert isinstance(old_class, InheritingObject)
                    assert isinstance(new_class, InheritingObject)

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
            for y in deleted:
                dels.add(y.delta(y, None, context=context,
                                 old_schema=old_schema,
                                 new_schema=new_schema))

        adds_mods.add(dels)
        return adds_mods

    def dump(self, schema: s_schema.Schema) -> str:
        return (
            f'<{type(self).__name__} name={self.get_name(schema)!r} '
            f'at {id(self):#x}>'
        )

    def __repr__(self) -> str:
        return f'<{type(self).__name__} {self.id} at 0x{id(self):#x}>'


class QualifiedObject(Object):

    name = SchemaField(
        # ignore below because Mypy doesn't understand fields which are not
        # inheritable.
        sn.Name,  # type: ignore
        inheritable=False,
        compcoef=0.670,
    )

    def get_shortname(self, schema: s_schema.Schema) -> sn.Name:
        shortname = super().get_shortname(schema)
        assert isinstance(shortname, sn.Name)
        return shortname


QualifiedObject_T = TypeVar('QualifiedObject_T', bound='QualifiedObject')


class ObjectFragment(QualifiedObject):
    """A part of another object that cannot exist independently."""


class GlobalObject(Object):
    pass


class DerivableObject(QualifiedObject):

    def derive_name(
        self,
        schema: s_schema.Schema,
        source: QualifiedObject,
        *qualifiers: str,
        derived_name_base: Optional[str] = None,
        module: Optional[str] = None,
    ) -> sn.SchemaName:
        if module is None:
            module = source.get_name(schema).module
        source_name = source.get_name(schema)
        qualifiers = (source_name,) + qualifiers

        return derive_name(
            schema,
            *qualifiers,
            module=module,
            parent=self,
            derived_name_base=derived_name_base,
        )

    def generic(self, schema: s_schema.Schema) -> bool:
        return self.get_shortname(schema) == self.get_name(schema)

    def get_derived_name_base(self, schema: s_schema.Schema) -> str:
        return self.get_shortname(schema)

    def get_derived_name(
        self,
        schema: s_schema.Schema,
        source: QualifiedObject,
        *qualifiers: str,
        mark_derived: bool = False,
        derived_name_base: Optional[str] = None,
        module: Optional[str] = None,
    ) -> sn.Name:
        return self.derive_name(
            schema, source, *qualifiers,
            derived_name_base=derived_name_base,
            module=module)


class Shell:

    def resolve(self, schema: s_schema.Schema) -> Any:
        raise NotImplementedError


class ObjectShell(Shell):

    def __init__(
        self,
        *,
        name: str,
        schemaclass: Type[Object] = Object,
        displayname: Optional[str] = None,
        origname: Optional[str] = None,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> None:
        self.name = name
        self.origname = origname
        self.displayname = displayname
        self.schemaclass = schemaclass
        self.sourcectx = sourcectx

    def get_id(self, schema: s_schema.Schema) -> uuid.UUID:
        return self.resolve(schema).get_id(schema)

    def resolve(self, schema: s_schema.Schema) -> Object:
        if self.name is None:
            raise TypeError(
                'cannot resolve anonymous ObjectShell'
            )

        return schema.get(
            self.name,
            type=self.schemaclass,
            refname=self.origname,
            sourcectx=self.sourcectx,
        )

    def get_refname(self, schema: s_schema.Schema) -> str:
        if self.origname is not None:
            return self.origname
        else:
            return self.get_displayname(schema)

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return self.displayname or self.name

    def get_schema_class_displayname(self) -> str:
        return self.schemaclass.get_schema_class_displayname()

    def __repr__(self) -> str:
        if self.schemaclass is not None:
            dn = self.schemaclass.__name__
        else:
            dn = 'Object'

        n = self.name or '<anonymous>'

        return f'<{type(self).__name__} {dn}({n!r}) at 0x{id(self):x}>'


class ObjectCollectionDuplicateNameError(Exception):
    pass


class ObjectCollection(
    s_abc.ObjectContainer,
    parametric.SingleParametricType[Object_T],
    Generic[Object_T],
):

    # Even though Object_T would be a correct annotation below,
    # we want the type to default to base `Object` for cases
    # when a TypeVar is passed as Object_T.  This is a hack,
    # of course, because, ideally we'd want to at least default
    # to the bounds or constraints of the TypeVar, or, even better,
    # pass the actual type at the call site, but there seems to be
    # no easy solution to do that.
    type: ClassVar[Type[Object]] = Object  # type: ignore
    _container: ClassVar[Type[CollectionFactory[Any]]]
    _ids: Collection[uuid.UUID]

    def __init_subclass__(
        cls,
        *,
        container: Optional[Type[CollectionFactory[Any]]] = None,
    ) -> None:
        super().__init_subclass__()
        if container is not None:
            cls._container = container

    def __init__(
        self,
        ids: Collection[uuid.UUID],
        *,
        _private_init: bool,
    ) -> None:
        if not self.is_fully_resolved():
            raise TypeError(
                f"{type(self)!r} unresolved type parameters"
            )
        self._ids = ids

    def __len__(self) -> int:
        return len(self._ids)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._ids == other._ids

    def __hash__(self) -> int:
        return hash(self._ids)

    def __reduce__(self) -> Tuple[Any, ...]:
        assert type(self).is_fully_resolved(), \
            f'{type(self)} parameters are not resolved'
        types: Optional[Tuple[Optional[type], ...]] = self.types
        if types is None:
            types = (None,)
        cls: Type[ObjectCollection[Object_T]] = self.__class__
        if cls.__name__.endswith("]"):
            # Parametrized type.
            cls = cls.__bases__[0]
        else:
            # A subclass of a parametrized type.
            types = (None,)

        typeargs = types[0] if len(types) == 1 else types
        return cls.__restore__, (typeargs, self.__dict__)

    @classmethod
    def __restore__(
        cls,
        params: Optional[Tuple[builtins.type, ...]],
        objdict: Dict[str, Any],
    ) -> ObjectCollection[Object_T]:
        ids = objdict.pop('_ids')

        if params is None:
            obj = cls(ids=ids, _private_init=True)
        else:
            obj = cls[params](ids=ids, _private_init=True)  # type: ignore

        if objdict:
            obj.__dict__.update(objdict)

        return obj

    def dump(self, schema: s_schema.Schema) -> str:
        return (
            f'<{type(self).__name__} objects='
            f'[{", ".join(o.dump(schema) for o in self.objects(schema))}] '
            f'at {id(self):#x}>'
        )

    @classmethod
    def create(
        cls: Type[ObjectCollection[Object_T]],
        schema: s_schema.Schema,
        data: Iterable[Object_T],
    ) -> ObjectCollection[Object_T]:
        ids: List[uuid.UUID] = []

        if isinstance(data, ObjectCollection):
            ids.extend(data._ids)
        elif data:
            for v in data:
                ids.append(cls._validate_value(schema, v))
        container: Collection[uuid.UUID] = cls._container(ids)
        return cls(container, _private_init=True)

    @classmethod
    def create_empty(cls) -> ObjectCollection[Object_T]:
        return cls(cls._container(), _private_init=True)

    @classmethod
    def _validate_value(
        cls, schema: s_schema.Schema, v: Object
    ) -> uuid.UUID:
        if not isinstance(v, cls.type):
            raise TypeError(
                f'invalid input data for ObjectIndexByShortname: '
                f'expected {cls.type} values, got {type(v)}')

        if v.id is not None:
            return v.id
        else:
            raise TypeError(f'object {v!r} has no ID!')

    def ids(self, schema: s_schema.Schema) -> Tuple[uuid.UUID, ...]:
        return tuple(self._ids)

    def names(self, schema: s_schema.Schema) -> Collection[str]:
        result = []

        for item_id in self._ids:
            obj = schema.get_by_id(item_id)
            result.append(obj.get_name(schema))

        return type(self)._container(result)

    def objects(self, schema: s_schema.Schema) -> Tuple[Object_T, ...]:
        return tuple(
            schema.get_by_id(iid) for iid in self._ids  # type: ignore
        )

    @classmethod
    def compare_values(
        cls,
        ours: ObjectCollection[Object_T],
        theirs: ObjectCollection[Object_T],
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: ComparisonContext,
        compcoef: float,
    ) -> float:
        if ours is not None:
            our_names = ours.names(our_schema)
        else:
            our_names = cls._container()

        if theirs is not None:
            their_names = theirs.names(their_schema)
        else:
            their_names = cls._container()

        if our_names != their_names:
            return compcoef
        else:
            return 1.0

    def as_shell(
        self,
        schema: s_schema.Schema,
    ) -> ObjectCollectionShell[Object_T]:
        return ObjectCollectionShell[Object_T](
            items=[o.as_shell(schema) for o in self.objects(schema)],
            collection_type=type(self),
        )


class ObjectCollectionShell(Shell, Generic[Object_T]):

    def __init__(
        self,
        items: Iterable[ObjectShell],
        collection_type: Type[ObjectCollection[Object_T]],
    ) -> None:
        self.items = items
        self.collection_type = collection_type

    def resolve(self, schema: s_schema.Schema) -> ObjectCollection[Object_T]:
        return self.collection_type.create(
            schema,
            [cast(Object_T, s.resolve(schema)) for s in self.items],
        )

    def __repr__(self) -> str:
        tn = self.__class__.__name__
        cn = self.collection_type.__name__
        items = ', '.join(e.name or '<anonymous>' for e in self.items)
        return f'<{tn} {cn}({items}) at 0x{id(self):x}>'


OIBT = TypeVar("OIBT", bound="ObjectIndexBase[Object]")
KeyFunction = Callable[["s_schema.Schema", Object_T], str]


class ObjectIndexBase(
    ObjectCollection[Object_T],
    container=tuple,
):
    _key: KeyFunction[Object_T]

    def __init_subclass__(
        cls,
        *,
        key: Optional[KeyFunction[Object_T]] = None,
    ) -> None:
        super().__init_subclass__()
        if key is not None:
            cls._key = key
        elif cls._key is None:
            raise TypeError('missing required "key" class argument')

    @classmethod
    def get_key_for(cls, schema: s_schema.Schema, obj: Object) -> str:
        return cls._key(schema, obj)

    @classmethod
    def get_key_for_name(cls, schema: s_schema.Schema, name: str) -> str:
        return name

    @classmethod
    def create(
        cls: Type[ObjectIndexBase[Object_T]],
        schema: s_schema.Schema,
        data: Iterable[Object_T],
    ) -> ObjectIndexBase[Object_T]:
        coll = cast(
            ObjectIndexBase[Object_T],
            super().create(schema, data)
        )
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
        ours: ObjectCollection[Object_T],
        theirs: ObjectCollection[Object_T],
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: ComparisonContext,
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

        return (
            schema,
            cast(OIBT, type(self).create(schema, items.values())),
        )

    def delete(
        self: OIBT,
        schema: s_schema.Schema,
        names: Iterable[str],
    ) -> Tuple[s_schema.Schema, OIBT]:
        items = dict(self.items(schema))
        for name in names:
            items.pop(name)
        return (
            schema,
            cast(OIBT, type(self).create(schema, items.values())),
        )

    def items(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[Tuple[str, Object_T], ...]:
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
        self,
        schema: s_schema.Schema,
        name: str,
        default: Any = NoDefault,
    ) -> Optional[Object_T]:
        items = dict(self.items(schema))
        if default is NoDefault:
            return items[name]
        else:
            return items.get(name, default)


def _fullname_object_key(schema: s_schema.Schema, o: Object) -> str:
    return o.get_name(schema)


class ObjectIndexByFullname(
    ObjectIndexBase[Object_T],
    key=_fullname_object_key,
):
    pass


def _shortname_object_key(schema: s_schema.Schema, o: Object) -> str:
    return o.get_shortname(schema)


class ObjectIndexByShortname(
    ObjectIndexBase[Object_T],
    key=_shortname_object_key,
):

    @classmethod
    def get_key_for_name(cls, schema: s_schema.Schema, name: str) -> str:
        return sn.shortname_from_fullname(name)


def _unqualified_object_key(schema: s_schema.Schema, o: Object) -> str:
    assert isinstance(o, QualifiedObject)
    return o.get_shortname(schema).name


class ObjectIndexByUnqualifiedName(
    ObjectIndexBase[QualifiedObject_T],
    Generic[QualifiedObject_T],
    key=_unqualified_object_key,
):

    @classmethod
    def get_key_for_name(cls, schema: s_schema.Schema, name: str) -> str:
        return sn.shortname_from_fullname(name).name


Key_T = TypeVar("Key_T")


class ObjectDict(
    Generic[Key_T, Object_T],
    ObjectCollection[Object_T],
    container=tuple,
):
    _keys: Tuple[Key_T, ...]

    # Breaking the Liskov Substitution Principle
    @classmethod
    def create(  # type: ignore
        cls,
        schema: s_schema.Schema,
        data: Mapping[Key_T, Object_T],
    ) -> ObjectDict[Key_T, Object_T]:

        result = cast(
            ObjectDict[Key_T, Object_T],
            super().create(schema, data.values()),
        )

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

    def keys(self, schema: s_schema.Schema) -> Tuple[Key_T, ...]:
        return self._keys

    def values(self, schema: s_schema.Schema) -> Tuple[Object_T, ...]:
        return self.objects(schema)

    def items(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[Tuple[Key_T, Object_T], ...]:
        return tuple(zip(self._keys, self.objects(schema)))

    def as_shell(
        self,
        schema: s_schema.Schema,
    ) -> ObjectDictShell[Key_T, Object_T]:
        return ObjectDictShell(
            items={k: o.as_shell(schema) for k, o in self.items(schema)},
            collection_type=type(self),
        )


class ObjectDictShell(
    ObjectCollectionShell[Object_T],
    Generic[Key_T, Object_T],
):

    items: Mapping[Any, ObjectShell]
    collection_type: Type[ObjectDict[Key_T, Object_T]]

    def __init__(
        self,
        items: Mapping[Any, ObjectShell],
        collection_type: Type[ObjectDict[Key_T, Object_T]],
    ) -> None:
        self.items = items
        self.collection_type = collection_type

    def __repr__(self) -> str:
        tn = self.__class__.__name__
        cn = self.collection_type.__name__
        items = ', '.join(f'{k}: {v.name}' for k, v in self.items.items())
        return f'<{tn} {cn}({items}) at 0x{id(self):x}>'

    def resolve(self, schema: s_schema.Schema) -> ObjectDict[Key_T, Object_T]:
        return self.collection_type.create(
            schema,
            {
                k: cast(Object_T, s.resolve(schema))
                for k, s in self.items.items()
            },
        )


class ObjectSet(
    ObjectCollection[Object_T],
    Generic[Object_T],
    container=frozenset,
):

    def __repr__(self) -> str:
        return f'{{{", ".join(str(id) for id in self._ids)}}}'

    @classmethod
    def merge_values(
        cls: Type[ObjectSet[Object_T]],
        target: Object,
        sources: Iterable[Object],
        field_name: str,
        *,
        schema: s_schema.Schema,
    ) -> ObjectSet[Object_T]:
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

        return cast(ObjectSet[Object_T], result)


class ObjectList(
    ObjectCollection[Object_T],
    Generic[Object_T],
    container=tuple,
):

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

    # Unfortunately, mypy does not support self generics over types with
    # typevars, so we have to resort to method redifinition.
    @classmethod
    def create(
        cls,
        schema: s_schema.Schema,
        data: Iterable[Object_T],
    ) -> ObjectList[Object_T]:
        return super().create(schema, data)  # type: ignore


class SubclassableObject(Object):

    is_abstract = SchemaField(
        bool,
        default=False,
        inheritable=False, compcoef=0.909)

    is_final = SchemaField(
        bool,
        default=False, compcoef=0.909)

    def _issubclass(
        self, schema: s_schema.Schema, parent: SubclassableObject
    ) -> bool:
        return parent == self

    def issubclass(
        self,
        schema: s_schema.Schema,
        parent: Union[SubclassableObject, Tuple[SubclassableObject, ...]],
    ) -> bool:
        from . import types as s_types
        if isinstance(parent, tuple):
            return any(self.issubclass(schema, p) for p in parent)
        else:
            if isinstance(parent, s_types.Type) and parent.is_any():
                return True
            else:
                return self._issubclass(schema, parent)


InheritingObjectT = TypeVar('InheritingObjectT', bound='InheritingObject')


class InheritingObject(SubclassableObject):

    bases = SchemaField(
        ObjectList['InheritingObject'],
        default=DEFAULT_CONSTRUCTOR,
        coerce=True,
        inheritable=False,
        compcoef=0.714,
    )

    ancestors = SchemaField(
        ObjectList['InheritingObject'],
        default=DEFAULT_CONSTRUCTOR,
        coerce=True,
        inheritable=False,
        compcoef=0.999,
    )

    # Attributes that have been set locally as opposed to inherited.
    inherited_fields = SchemaField(
        checked.FrozenCheckedSet[str],
        default=DEFAULT_CONSTRUCTOR,
        coerce=True,
        inheritable=False,
        hashable=False,
    )

    is_derived = SchemaField(
        bool,
        default=False, compcoef=0.909)

    def inheritable_fields(self) -> Iterable[str]:
        for fn, f in self.__class__.get_fields().items():
            if f.inheritable:
                yield fn

    @classmethod
    def get_default_base_name(self) -> Optional[str]:
        return None

    # Redefinining bases and ancestors accessors to make them generic
    def get_bases(
        self: InheritingObjectT,
        schema: s_schema.Schema,
    ) -> ObjectList[InheritingObjectT]:
        return self.get_field_value(schema, 'bases')  # type: ignore

    def get_ancestors(
        self: InheritingObjectT,
        schema: s_schema.Schema,
    ) -> ObjectList[InheritingObjectT]:
        return self.get_field_value(schema, 'ancestors')  # type: ignore

    def get_base_names(self, schema: s_schema.Schema) -> Collection[str]:
        return self.get_bases(schema).names(schema)

    def get_topmost_concrete_base(
        self: InheritingObjectT,
        schema: s_schema.Schema
    ) -> InheritingObjectT:
        """Get the topmost non-abstract base."""
        lineage = [self]
        lineage.extend(self.get_ancestors(schema).objects(schema))
        for ancestor in reversed(lineage):
            if not ancestor.get_is_abstract(schema):
                return ancestor

        if not self.get_is_abstract(schema):
            return self

        raise errors.SchemaError(
            f'{self.get_verbosename(schema)} has no non-abstract ancestors')

    def get_base_for_cast(self, schema: s_schema.Schema) -> Object:
        return self.get_topmost_concrete_base(schema)

    @classmethod
    def get_root_classes(cls) -> Tuple[sn.Name, ...]:
        return tuple()

    def _issubclass(
        self,
        schema: s_schema.Schema,
        parent: SubclassableObject,
    ) -> bool:
        if parent == self:
            return True

        lineage = self.get_ancestors(schema).objects(schema)
        return parent in lineage

    def descendants(
        self: InheritingObjectT, schema: s_schema.Schema
    ) -> FrozenSet[InheritingObjectT]:
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

    def children(
        self: InheritingObjectT,
        schema: s_schema.Schema,
    ) -> FrozenSet[InheritingObjectT]:
        return schema.get_children(self)

    def get_explicit_local_field_value(
        self,
        schema: s_schema.Schema,
        field_name: str,
        default: Any = NoDefault,
    ) -> Any:
        inherited_fields = self.get_inherited_fields(schema)
        if field_name not in inherited_fields:
            return self.get_explicit_field_value(schema, field_name, default)
        elif default is not NoDefault:
            return default
        else:
            raise FieldValueNotFoundError(
                f'{self!r} object has no non-inherited value for '
                f'field {field_name!r}'
            )

    def allow_ref_propagation(
        self,
        schema: s_schema.Schema,
        constext: sd.CommandContext,
        refdict: RefDict,
    ) -> bool:
        return True

    @classmethod
    def delta(
        cls,
        old: Optional[Object],
        new: Optional[Object],
        *,
        context: ComparisonContext,
        old_schema: Optional[s_schema.Schema],
        new_schema: s_schema.Schema,
    ) -> sd.ObjectCommand[Object]:
        from . import delta as sd
        from . import inheriting as s_inh

        delta = super().delta(
            old,
            new,
            context=context,
            old_schema=old_schema,
            new_schema=new_schema,
        )

        if old and new:
            assert isinstance(old, InheritingObject)
            assert isinstance(new, InheritingObject)

            rebase = sd.ObjectCommandMeta.get_command_class(
                s_inh.RebaseInheritingObject, type(new))

            assert old_schema is not None

            old_base_names = old.get_base_names(old_schema)
            new_base_names = new.get_base_names(new_schema)

            if old_base_names != new_base_names and rebase is not None:
                removed, added = s_inh.delta_bases(
                    old_base_names, new_base_names)

                rebase_cmd = rebase(
                    classname=new.get_name(new_schema),
                    metaclass=type(new),
                    removed_bases=removed,
                    added_bases=added,
                )

                rebase_cmd.set_attribute_value(
                    'bases',
                    new.get_bases(new_schema).as_shell(new_schema),
                )

                rebase_cmd.set_attribute_value(
                    'ancestors',
                    new.get_ancestors(new_schema).as_shell(new_schema),
                )

                delta.add(rebase_cmd)

        return delta

    @classmethod
    def delta_property(
        cls,
        schema: s_schema.Schema,
        scls: Object,
        delta: sd.Command,
        fname: str,
        value: Any,
    ) -> None:
        assert isinstance(scls, InheritingObject)
        inherited_fields = scls.get_inherited_fields(schema)
        delta.set_attribute_value(
            fname,
            value,
            inherited=fname in inherited_fields,
        )


DerivableInheritingObjectT = TypeVar(
    'DerivableInheritingObjectT',
    bound='DerivableInheritingObject',
)


class DerivableInheritingObject(DerivableObject, InheritingObject):

    def get_nearest_non_derived_parent(
        self: DerivableInheritingObjectT,
        schema: s_schema.Schema,
    ) -> DerivableInheritingObjectT:
        obj = self
        while obj.get_is_derived(schema):
            obj = cast(
                DerivableInheritingObjectT,
                obj.get_bases(schema).first(schema),
            )
        return obj


@markup.serializer.serializer.register(Object)
@markup.serializer.serializer.register(ObjectCollection)
def _serialize_to_markup(o: Object, *, ctx: markup.Context) -> markup.Markup:
    if 'schema' not in ctx.kwargs:
        orepr = repr(o)
    else:
        orepr = o.dump(ctx.kwargs['schema'])

    return markup.elements.lang.Object(
        id=id(o),
        class_module=type(o).__module__,
        classname=type(o).__name__,
        repr=orepr,
    )


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
    schema: s_schema.Schema, obj: InheritingObject
) -> List[Any]:
    bases = tuple(obj.get_bases(schema).objects(schema))
    lineage = [[obj]]

    for base in bases:
        lineage.append(compute_lineage(schema, base))

    return _merge_lineage(schema, obj, lineage)


def compute_ancestors(
    schema: s_schema.Schema, obj: InheritingObject
) -> List[Any]:
    return compute_lineage(schema, obj)[1:]


def derive_name(
    schema: s_schema.Schema,
    *qualifiers: str,
    module: str,
    parent: Optional[DerivableObject] = None,
    derived_name_base: Optional[str] = None,
) -> sn.Name:
    if derived_name_base is None:
        assert parent is not None
        derived_name_base = parent.get_derived_name_base(schema)

    name = sn.get_specialized_name(derived_name_base, *qualifiers)

    return sn.Name(name=name, module=module)
