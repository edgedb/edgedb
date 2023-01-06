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

import builtins
import collections
import collections.abc
import copy
import enum
import functools
import uuid

from edb import errors
from edb.edgeql import qltypes
from edb.common.typeutils import not_none

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
    from edb.schema import delta as sd
    from edb.schema import schema as s_schema

    CovT = TypeVar("CovT", covariant=True)

    class MergeFunction(Protocol):
        def __call__(
            self,  # not actually part of the signature
            target: InheritingObject,
            sources: Iterable[Object],
            field_name: str,
            *,
            ignore_local: bool = False,
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
ObjectContainer_T = TypeVar('ObjectContainer_T', bound='ObjectContainer')
Object_T = TypeVar("Object_T", bound="Object")
Object_T_co = TypeVar("Object_T_co", bound="Object", covariant=True)
ObjectCollection_T = TypeVar(
    "ObjectCollection_T",
    bound="ObjectCollection[Object]",
)
HashCriterion = Union[Type["Object"], Tuple[str, Any]]


class ReflectionMethod(enum.Enum):
    """Annotation on schema classes telling how to reflect in metaschema."""

    #: Straight 1:1 reflection (the default)
    REGULAR = enum.auto()

    #: Object type for schema class is elided and its properties
    #: are reflected as link properties.  This is used for certain
    #: Referenced classes, like AnnotationValue.
    AS_LINK = enum.auto()

    #: No metaschema reflection at all.
    NONE = enum.auto()


def default_field_merge(
    target: InheritingObject,
    sources: Iterable[Object],
    field_name: str,
    *,
    ignore_local: bool = False,
    schema: s_schema.Schema,
) -> Any:
    """The default `MergeFunction`."""
    if not ignore_local:
        ours = target.get_explicit_local_field_value(schema, field_name, None)
        if ours is not None:
            return ours

    for source in sources:
        theirs = source.get_explicit_field_value(schema, field_name, None)
        if theirs is not None:
            return theirs

    return None


def get_known_type_id(
    typename: Union[str, sn.Name],
    default: Union[uuid.UUID, NoDefaultT] = NoDefault
) -> uuid.UUID:
    if isinstance(typename, str):
        typename = sn.name_from_string(typename)
    try:
        return _types.TYPE_IDS[typename]
    except KeyError:
        pass

    if default is NoDefault:
        raise errors.SchemaError(
            f'failed to lookup named type id for {typename!r}')

    return default


class DeltaGuidance(NamedTuple):

    banned_creations: FrozenSet[Tuple[Type[Object], sn.Name]] = frozenset()
    banned_deletions: FrozenSet[Tuple[Type[Object], sn.Name]] = frozenset()
    banned_alters: FrozenSet[
        Tuple[Type[Object], Tuple[sn.Name, sn.Name]]
    ] = frozenset()


class DescribeVisibilityFlags(enum.IntFlag):

    #: Show the field if it is set explicitly, i.e. not inherited or computed.
    SHOW_IF_EXPLICIT = 1 << 0
    #: Show the field if it is inherited or computed.
    SHOW_IF_DERIVED = 1 << 1
    #: Show if the field value matches the default.
    SHOW_IF_DEFAULT = 1 << 2


class DescribeVisibilityPolicy(enum.IntEnum):

    SHOW_IF_EXPLICIT = (
        DescribeVisibilityFlags.SHOW_IF_EXPLICIT
    )

    SHOW_IF_EXPLICIT_OR_DERIVED = (
        DescribeVisibilityFlags.SHOW_IF_EXPLICIT
        | DescribeVisibilityFlags.SHOW_IF_DERIVED
        | DescribeVisibilityFlags.SHOW_IF_DEFAULT
    )

    SHOW_IF_EXPLICIT_OR_DERIVED_NOT_DEFAULT = (
        DescribeVisibilityFlags.SHOW_IF_EXPLICIT
        | DescribeVisibilityFlags.SHOW_IF_DERIVED
    )


class ComparisonContext:

    renames: Dict[Tuple[Type[Object], sn.Name], sd.RenameObject[Object]]
    deletions: Dict[Tuple[Type[Object], sn.Name], sd.DeleteObject[Object]]
    guidance: Optional[DeltaGuidance]
    parent_ops: List[sd.ObjectCommand[Any]]

    def __init__(
        self,
        *,
        generate_prompts: bool = False,
        descriptive_mode: bool = False,
        guidance: Optional[DeltaGuidance] = None,
    ) -> None:
        self.generate_prompts = generate_prompts
        self.descriptive_mode = descriptive_mode
        self.guidance = guidance
        self.renames = {}
        self.deletions = {}
        self.placeholder_ctr: Dict[str, int] = collections.Counter()
        self.parent_ops = []

    def is_deleting(self, schema: s_schema.Schema, obj: Object) -> bool:
        return (type(obj), obj.get_name(schema)) in self.deletions

    def record_rename(
        self,
        op: sd.RenameObject[Object],
    ) -> None:
        self.renames[op.get_schema_metaclass(), op.classname] = op

    def is_renaming(self, schema: s_schema.Schema, obj: Object) -> bool:
        return (type(obj), obj.get_name(schema)) in self.renames

    def get_obj_name(self, schema: s_schema.Schema, obj: Object) -> sn.Name:
        obj_name = obj.get_name(schema)
        rename_op = self.renames.get((type(obj), obj_name))
        if rename_op is not None:
            return rename_op.new_name
        else:
            return obj_name

    def get_placeholder(self, prefix: str) -> str:
        ctr = self.placeholder_ctr[prefix]
        self.placeholder_ctr[prefix] += 1
        if ctr == 0:
            return f'{prefix}'
        else:
            return f'{prefix}_{ctr}'


# derived from ProtoField for validation
class Field(struct.ProtoField, Generic[T]):

    __slots__ = ('name', 'sname', 'type', 'coerce',
                 'compcoef', 'inheritable', 'simpledelta',
                 'merge_fn', 'ephemeral',
                 'allow_ddl_set', 'ddl_identity', 'special_ddl_syntax',
                 'aux_cmd_data', 'describe_visibility',
                 'weak_ref', 'reflection_method', 'reflection_proxy',
                 'type_is_generic_self', 'is_reducible')

    #: Name of the field on the target class; assigned by ObjectMeta
    name: str
    #: The name to use when reflecting the field into the schema.
    #: The same as name by default, but can be overridden.
    sname: str
    #: The type of the value stored in the field
    type: Type[T]
    #: Specifies if *type* is a generic type of the host object
    #: this field is defined on.
    type_is_generic_self: bool
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
    #: Whether the field can be set directly using the `SET`
    #: command in DDL.
    allow_ddl_set: bool
    #: Whether the field is used to identify the object
    #: in DDL operations and schema reflection when object
    #: name is insufficient.
    ddl_identity: bool
    #: Whether the value of this field should be included in the
    #: aux_object_data for delta commands of objects containing the field.
    aux_cmd_data: bool
    #: Whether this field is set using special DDL syntax or a generic
    #: SET command.
    special_ddl_syntax: bool
    #: Determines when this field is shown in
    #: DESCRIBE AS TEXT [VERBOSE].
    describe_visibility: DescribeVisibilityPolicy
    #: Used for fields holding references to objects.  If True,
    #: the reference is considered "weak", i.e. not essential for
    #: object definition.  The schema and delta linearization
    #: rely on this to break object reference cycles.
    weak_ref: bool
    #: A callable used to merge the value of the field from
    #: multiple objects.  Most oftenly used by inheritance.
    merge_fn: MergeFunction
    #: Defines how the field is reflected into the backend schema storage.
    reflection_method: ReflectionMethod
    #: In cases when the value of the field cannot be reflected as a
    #: direct link (for example, if the value is a non-distinct set),
    #: this specifies a (ProxyType, linkname) pair of a proxy object type
    #: and the name of the link within that proxy type.
    reflection_proxy: Optional[Tuple[str, str]]

    def __init__(
        self,
        type_: Type[T],
        *,
        type_is_generic_self: bool = False,
        coerce: bool = False,
        compcoef: Optional[float] = None,
        inheritable: bool = True,
        simpledelta: bool = True,
        merge_fn: MergeFunction = default_field_merge,
        ephemeral: bool = False,
        weak_ref: bool = False,
        allow_ddl_set: bool = False,
        describe_visibility: DescribeVisibilityPolicy = (
            DescribeVisibilityPolicy.SHOW_IF_EXPLICIT),
        ddl_identity: bool = False,
        aux_cmd_data: bool = False,
        special_ddl_syntax: bool = False,
        reflection_method: ReflectionMethod = ReflectionMethod.REGULAR,
        reflection_proxy: Optional[Tuple[str, str]] = None,
        name: Optional[str] = None,
        reflection_name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Schema item core attribute definition.

        """
        if not isinstance(type_, type):
            raise ValueError(f'{type_!r} is not a type')

        self.type = type_
        self.type_is_generic_self = type_is_generic_self
        self.coerce = coerce
        self.allow_ddl_set = allow_ddl_set
        self.ddl_identity = ddl_identity
        self.aux_cmd_data = aux_cmd_data
        self.special_ddl_syntax = special_ddl_syntax
        self.describe_visibility = describe_visibility

        self.compcoef = compcoef
        self.inheritable = inheritable
        self.simpledelta = simpledelta
        self.weak_ref = weak_ref
        self.reflection_method = reflection_method
        self.reflection_proxy = reflection_proxy
        self.is_reducible = issubclass(type_, s_abc.Reducible)

        if name is not None:
            self.name = name
        if reflection_name is not None:
            self.sname = reflection_name

        if (
            merge_fn is default_field_merge
            and callable(
                type_merge_fn := getattr(self.type, 'merge_values', None)
            )
        ):
            self.merge_fn = type_merge_fn
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

        elif issubclass(ftype, sn.QualName):
            return ftype.from_string(value)  # type: ignore

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

    def maybe_get_default(self) -> Any:
        return NoDefault

    def get_default(self) -> Any:
        raise ValueError(f'field {self.name!r} is required and has no default')

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

    __slots__ = ('default', 'hashable', 'allow_ddl_set', 'index')

    #: The default value to use for the field.
    default: Any
    #: Whether the field participates in object hash.
    hashable: bool
    #: Whether it's possible to set the field in DDL.
    allow_ddl_set: bool
    #: Field index within the object data tuple
    index: int

    def __init__(
        self,
        type: Type_T,
        *,
        default: Any = NoDefault,
        hashable: bool = True,
        allow_ddl_set: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(type, **kwargs)
        self.default = default
        self.hashable = hashable
        self.allow_ddl_set = allow_ddl_set
        self.index = -1

    @property
    def required(self) -> bool:
        return self.default is NoDefault

    @property
    def is_schema_field(self) -> bool:
        return True

    def maybe_get_default(self) -> Any:
        if self.default is DEFAULT_CONSTRUCTOR:
            if issubclass(self.type, ObjectCollection):
                value = self.type.create_empty()
            else:
                value = self.type()  # type: ignore
        else:
            value = self.default
        return value

    def get_default(self) -> Any:
        value = self.maybe_get_default()
        if value is NoDefault:
            raise ValueError(
                f'field {self.name!r} is required and has no default')
        else:
            return value

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


class RefDict(struct.RTStruct):

    attr = struct.Field(
        str, frozen=True)

    backref_attr = struct.Field(
        str, default='subject', frozen=True)

    requires_explicit_overloaded = struct.Field(
        bool, default=False, frozen=True)

    ref_cls: Type[Object] = struct.Field(
        type, frozen=True)


class ObjectContainer(s_abc.Reducible):

    @classmethod
    def schema_refs_from_data(
        cls,
        data: Any,
    ) -> FrozenSet[uuid.UUID]:
        raise NotImplementedError


class ObjectMeta(type):

    _all_types: ClassVar[Dict[str, Type[Object]]] = {}
    _schema_types: ClassVar[Set[ObjectMeta]] = set()
    _ql_map: ClassVar[Dict[qltypes.SchemaObjectClass, ObjectMeta]] = {}
    _refdicts_to: ClassVar[
        Dict[ObjectMeta, List[Tuple[RefDict, ObjectMeta]]]
    ] = {}

    # Instance fields (i.e. class fields on types built with ObjectMeta)
    _fields: Dict[str, Field[Any]]
    _schema_fields: Dict[str, SchemaField[Any]]
    _hashable_fields: Set[Field[Any]]  # if f.is_schema_field and f.hashable
    _sorted_fields: collections.OrderedDict[str, Field[Any]]
    #: Fields that contain references to objects either directly or
    #: indirectly.
    _objref_fields: FrozenSet[SchemaField[Any]]
    _reducible_fields: FrozenSet[SchemaField[Any]]
    _aux_cmd_data_fields: FrozenSet[SchemaField[Any]]  # if f.aux_cmd_data
    _refdicts: collections.OrderedDict[str, RefDict]
    _refdicts_by_refclass: Dict[type, RefDict]
    _refdicts_by_field: Dict[str, RefDict]  # key is rd.attr
    _ql_class: Optional[qltypes.SchemaObjectClass]
    _reflection_method: ReflectionMethod
    _reflection_link: Optional[str]
    #: Indicates that ALL changes to this object class are safe from the
    #: standpoint of persistent data.  In other words, changes to the
    #: object are fully reversible without possible data loss.
    _data_safe: bool

    def __new__(
        mcls,
        name: str,
        bases: Tuple[type, ...],
        clsdict: Dict[str, Any],
        *,
        qlkind: Optional[qltypes.SchemaObjectClass] = None,
        reflection: ReflectionMethod = ReflectionMethod.REGULAR,
        reflection_link: Optional[str] = None,
        data_safe: bool = False,
        **kwargs: Any,
    ) -> ObjectMeta:
        refdicts: collections.OrderedDict[str, RefDict]

        fields = {}
        myfields = {}
        refdicts = collections.OrderedDict()
        mydicts = {}

        if name in mcls._all_types:
            raise TypeError(
                f'duplicate name for schema class: {name}, already defined'
                f' as {mcls._all_types[name]!r}'
            )

        for k, field in tuple(clsdict.items()):
            if isinstance(field, RefDict):
                mydicts[k] = field
                continue
            if not isinstance(field, struct.ProtoField):
                continue
            if not isinstance(field, Field):
                raise TypeError(
                    f'cannot create {name} class: schema.objects.Field '
                    f'expected, got {type(field)}')

            field.name = k
            if not hasattr(field, 'sname'):
                field.sname = k
            myfields[k] = field
            del clsdict[k]

        try:
            cls = super().__new__(mcls, name, bases, clsdict, **kwargs)
        except TypeError as ex:
            raise TypeError(
                f'Object metaclass has failed to create class {name}: {ex}')

        for parent in reversed(cls.__mro__):
            if parent is cls:
                fields.update(myfields)
                refdicts.update(mydicts)
            elif isinstance(parent, ObjectMeta):
                fields.update({
                    fn: copy.copy(f)
                    for fn, f in parent.get_ownfields().items()
                })
                refdicts.update({
                    k: d.copy()
                    for k, d in parent.get_own_refdicts().items()
                })

        cls._data_safe = data_safe
        cls._fields = fields
        cls._schema_fields = {
            fn: f for fn, f in fields.items()
            if isinstance(f, SchemaField)
        }
        cls._hashable_fields = {
            f for f in cls._schema_fields.values()
            if f.hashable
        }
        cls._aux_cmd_data_fields = frozenset(
            f for f in cls._schema_fields.values()
            if f.aux_cmd_data
        )
        cls._sorted_fields = collections.OrderedDict(
            sorted(fields.items(), key=lambda e: e[0]))
        cls._objref_fields = frozenset(
            f for f in cls._schema_fields.values()
            if issubclass(f.type, ObjectContainer)
        )
        cls._reducible_fields = frozenset(
            f for f in cls._schema_fields.values()
            if issubclass(f.type, s_abc.Reducible)
        )

        fa = '{}.{}_fields'.format(cls.__module__, cls.__name__)
        setattr(cls, fa, myfields)

        for findex, field in enumerate(cls._schema_fields.values()):
            field.index = findex
            getter_name = f'get_{field.name}'
            if getter_name in clsdict:
                # The getter was defined explicitly, move on.
                continue

            ftype = field.type
            # The field getters are hot code as they're essentially
            # attribute access, so be mindful about what you are adding
            # into the callables below.
            if issubclass(ftype, s_abc.Reducible):
                def reducible_getter(
                    self: Any,
                    schema: s_schema.Schema,
                    *,
                    _f: SchemaField[Any] = field,
                    _fn: str = field.name,
                    _fi: int = findex,
                    _sr: Callable[[Any], s_abc.Reducible] = (
                        ftype.schema_restore
                    ),
                ) -> Any:
                    data = schema.get_obj_data_raw(self)
                    v = data[_fi]
                    if v is not None:
                        return _sr(v)
                    else:
                        try:
                            return _f.get_default()
                        except ValueError:
                            pass

                        raise FieldValueNotFoundError(
                            f'{self!r} object has no value '
                            f'for field {_fn!r}'
                        )

                setattr(cls, getter_name, reducible_getter)
            else:
                def regular_getter(
                    self: Any,
                    schema: s_schema.Schema,
                    *,
                    _f: SchemaField[Any] = field,
                    _fn: str = field.name,
                    _fi: int = findex,
                ) -> Any:
                    data = schema.get_obj_data_raw(self)
                    v = data[_fi]
                    if v is not None:
                        return v
                    else:
                        try:
                            return _f.get_default()
                        except ValueError:
                            pass

                        raise FieldValueNotFoundError(
                            f'{self!r} object has no value '
                            f'for field {_fn!r}'
                        )

                setattr(cls, getter_name, regular_getter)

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

            other_dct = cls._refdicts_by_refclass.get(dct.ref_cls)
            if other_dct is not None:
                raise TypeError(
                    'multiple reference dicts for {!r} in '
                    '{!r}: {!r} and {!r}'.format(dct.ref_cls, cls,
                                                 dct.attr, other_dct.attr))

            cls._refdicts_by_refclass[dct.ref_cls] = dct

            try:
                refdicts_to = mcls._refdicts_to[dct.ref_cls]
            except KeyError:
                refdicts_to = mcls._refdicts_to[dct.ref_cls] = []

            refdicts_to.append((dct, cls))

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
        cls._reflection_method = reflection
        if reflection is ReflectionMethod.AS_LINK:
            if reflection_link is None:
                raise TypeError(
                    'reflection AS_LINK requires reflection_link to be passed'
                    ' also'
                )
            cls._reflection_link = reflection_link
        mcls._all_types[name] = cast(Type['Object'], cls)

        return cls

    def get_object_reference_fields(cls) -> FrozenSet[SchemaField[Any]]:
        return cls._objref_fields

    def get_reducible_fields(cls) -> FrozenSet[SchemaField[Any]]:
        return cls._reducible_fields

    def get_aux_cmd_data_fields(cls) -> FrozenSet[SchemaField[Any]]:
        return cls._aux_cmd_data_fields

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

    def get_schema_field(cls, name: str) -> SchemaField[Any]:
        field = cls._schema_fields.get(name)
        if field is None:
            raise LookupError(
                f'schema class {cls.__name__!r} has no schema field {name!r}'
            )
        return field

    def get_schema_fields(cls) -> Mapping[str, SchemaField[Any]]:
        return cls._schema_fields

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

    def get_referring_classes(cls) -> FrozenSet[Tuple[RefDict, ObjectMeta]]:
        try:
            refdicts_to = type(cls)._refdicts_to[cls]
        except KeyError:
            return frozenset()
        else:
            return frozenset(refdicts_to)

    @property
    def is_schema_object(cls) -> bool:
        return cls in ObjectMeta._schema_types

    @classmethod
    def get_schema_metaclasses(mcls) -> Iterator[Type[Object]]:
        return iter(mcls._all_types.values())

    @classmethod
    def get_schema_class(mcls, name: str) -> Type[Object]:
        return mcls._all_types[name]

    @classmethod
    def maybe_get_schema_class(mcls, name: str) -> Optional[Type[Object]]:
        return mcls._all_types.get(name)

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

    def get_reflection_method(cls) -> ReflectionMethod:
        return cls._reflection_method

    def get_reflection_link(cls) -> Optional[str]:
        return cls._reflection_link


class FieldValueNotFoundError(Exception):
    pass


class Object(s_abc.Object, ObjectContainer, metaclass=ObjectMeta):
    """Base schema item class."""

    __slots__ = ('id',)

    # Unique ID for this schema item.
    id = Field(
        uuid.UUID,
        inheritable=False,
        simpledelta=False,
        allow_ddl_set=True,
    )

    internal = SchemaField(
        bool,
        inheritable=False,
    )

    # Schema source context for this object
    sourcectx = SchemaField(
        parsing.ParserContext,
        default=None,
        compcoef=None,
        inheritable=False,
        hashable=False,
        ephemeral=True,
    )

    name = SchemaField(
        sn.Name,
        inheritable=False,
        compcoef=0.670,
    )

    builtin = SchemaField(
        bool,
        default=False,
        compcoef=0.01,
        inheritable=False,
    )

    # Fields that have been computed by the system as opposed to
    # set explicitly or inherited.
    computed_fields = SchemaField(
        checked.FrozenCheckedSet[str],
        default=DEFAULT_CONSTRUCTOR,
        coerce=True,
        inheritable=False,
        compcoef=0.999,
    )

    _fields: Dict[str, SchemaField[Any]]

    def schema_reduce(self) -> Tuple[str, uuid.UUID]:
        return type(self).__name__, self.id

    @classmethod
    @functools.lru_cache(maxsize=10240)
    def schema_restore(
        cls,
        data: Tuple[str, uuid.UUID],
    ) -> Object:
        sclass_name, obj_id = data
        sclass = ObjectMeta.get_schema_class(sclass_name)
        return sclass(_private_id=obj_id)

    @classmethod
    def schema_refs_from_data(
        cls,
        data: Tuple[str, uuid.UUID],
    ) -> FrozenSet[uuid.UUID]:
        return frozenset((data[1],))

    def get_id(self, schema: s_schema.Schema) -> uuid.UUID:
        return self.id

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return cls.__name__.lower()

    @classmethod
    def get_shortname_static(cls, name: sn.Name) -> sn.Name:
        return name

    @classmethod
    def get_local_name_static(cls, name: sn.Name) -> sn.UnqualName:
        return cls.get_shortname_static(name).get_local_name()

    @classmethod
    def get_displayname_static(cls, name: sn.Name) -> str:
        return str(cls.get_shortname_static(name))

    @classmethod
    def get_verbosename_static(
        cls,
        name: sn.Name,
        *,
        parent: Optional[str] = None,
    ) -> str:
        clsname = cls.get_schema_class_displayname()
        dname = cls.get_displayname_static(name)
        if parent is not None:
            return f"{clsname} '{dname}' of {parent}"
        else:
            return f"{clsname} '{dname}'"

    @classmethod
    def is_abstract(cls) -> bool:
        """Return True if this type does NOT represent a concrete schema class.
        """
        return cls.get_ql_class() is None

    def get_shortname(self, schema: s_schema.Schema) -> sn.Name:
        return type(self).get_shortname_static(self.get_name(schema))

    def get_local_name(self, schema: s_schema.Schema) -> sn.UnqualName:
        return type(self).get_local_name_static(self.get_name(schema))

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return type(self).get_displayname_static(self.get_name(schema))

    def get_verbosename(
        self, schema: s_schema.Schema, *, with_parent: bool = False
    ) -> str:
        clsname = self.get_schema_class_displayname()
        dname = self.get_displayname(schema)
        return f"{clsname} '{dname}'"

    def __init__(self, *, _private_id: uuid.UUID) -> None:
        self.id = _private_id

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Object):
            return self.id == other.id
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self.id)

    @classmethod
    def generate_id(
        cls,
        schema: s_schema.Schema,
        data: Dict[str, Any],
    ) -> uuid.UUID:
        return uuidgen.uuid1mc()

    @classmethod
    def _prepare_id(
        cls,
        schema: s_schema.Schema,
        data: Dict[str, Any],
    ) -> uuid.UUID:
        name = data.get('name')
        assert isinstance(name, (str, sn.Name))

        try:
            return get_known_type_id(name)
        except errors.SchemaError:
            return cls.generate_id(schema, data)

    @classmethod
    def _create_from_id(cls: Type[Object_T], id: uuid.UUID) -> Object_T:
        assert id is not None
        return cls(_private_id=id)

    @classmethod
    def create_in_schema(
        cls: Type[Object_T],
        schema: s_schema.Schema_T,
        *,
        id: Optional[uuid.UUID] = None,
        **data: Any,
    ) -> Tuple[s_schema.Schema_T, Object_T]:

        if not cls.is_schema_object:
            raise TypeError(f'{cls.__name__} type cannot be created in schema')

        if not data.get('name'):
            raise RuntimeError(f'cannot create {cls} without a name')

        all_fields = cls.get_schema_fields()
        obj_data = [None] * len(all_fields)
        for field_name, value in data.items():
            field = cls.get_schema_field(field_name)
            value = field.coerce_value(schema, value)
            obj_data[field.index] = value

        if id is None:
            id = cls._prepare_id(schema, data)
        scls = cls._create_from_id(id)
        schema = schema.add(id, cls, tuple(obj_data))

        return schema, scls

    # XXX sadly, in the methods below, statically we don't know any better than
    # "Any" since providing the field name as a `str` is the equivalent of
    # getattr() on a regular class.
    def get_field_value(
        self,
        schema: s_schema.Schema,
        field_name: str,
    ) -> Any:
        field = type(self).get_field(field_name)

        if isinstance(field, SchemaField):
            data = schema.get_obj_data_raw(self)
            val = data[field.index]
            if val is not None:
                if field.is_reducible:
                    return field.type.schema_restore(val)
                else:
                    return val
            else:
                try:
                    return field.get_default()
                except ValueError:
                    pass
        else:
            try:
                return object.__getattribute__(self, field_name)
            except AttributeError:
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

        if isinstance(field, SchemaField):
            data = schema.get_obj_data_raw(self)
            val = data[field.index]
            if val is not None:
                if field.is_reducible:
                    return field.type.schema_restore(val)
                else:
                    return val
            elif default is not NoDefault:
                return default

        else:
            try:
                return object.__getattribute__(self, field_name)
            except AttributeError:
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
            return schema.unset_obj_field(self, name)
        else:
            value = field.coerce_value(schema, value)
            return schema.set_obj_field(self, name, value)

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

        return schema.update_obj(self, updates)

    def is_type(self) -> bool:
        return False

    def hash_criteria(
        self: Object_T, schema: s_schema.Schema
    ) -> FrozenSet[HashCriterion]:
        cls = type(self)

        sig: List[Union[Type[Object_T], Tuple[str, Any]]] = [cls]
        for f in cls._hashable_fields:
            fn = f.name
            val = self.get_explicit_field_value(schema, fn, default=None)
            if val is None:
                continue
            elif isinstance(val, collections.abc.MutableSequence):
                # Turn the list into something hashable so it can be
                # put in a set.
                val = tuple(val)
            elif isinstance(val, collections.abc.MutableMapping):
                # Turn the dict into something hashable so it can be
                # put in a set.
                val = tuple((k, v) for k, v in val.items())
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

        for field in fields.values():
            if field.compcoef is None:
                continue

            fcoef = cls.compare_obj_field_value(
                field,
                self,
                other,
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

    def is_parent_ref(
        self,
        schema: s_schema.Schema,
        reference: Object,
    ) -> bool:
        """Return True if *reference* is a structural ancestor of self."""
        return False

    def is_generated(self, schema: s_schema.Schema) -> bool:
        return False

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
        if (
            our_value is not None
            and their_value is not None
            and type(our_value) == type(their_value)
        ):
            comparator = getattr(type(our_value), 'compare_values', None)
        else:
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
    def compare_obj_field_value(
        cls: Type[Object_T],
        field: Field[Type[T]],
        ours: Object_T,
        theirs: Object_T,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: ComparisonContext,
        explicit: bool = False,
    ) -> float:
        fname = field.name

        # If a field is not inheritable (and thus cannot be affected
        # by other objects) and the value is missing, it is exactly
        # equivalent to that field having the default value instead,
        # so we should use the default for comparisons. This means
        # that we perform the comparison as if explicit = False.
        #
        # E.g. 'owned' being None and False is semantically
        # identical and should not be considered a change.
        if (isinstance(field, SchemaField) and not field.inheritable):
            explicit = False

        if explicit:
            our_value = ours.get_explicit_field_value(
                our_schema, fname, None)
            their_value = theirs.get_explicit_field_value(
                their_schema, fname, None)
        else:
            our_value = ours.get_field_value(our_schema, fname)
            their_value = theirs.get_field_value(their_schema, fname)

        similarity = cls.compare_field_value(
            field,
            our_value,
            their_value,
            our_schema=our_schema,
            their_schema=their_schema,
            context=context,
        )

        # Check to see if this field's computed status has changed.
        our_cfs = ours.get_computed_fields(our_schema)
        their_cfs = theirs.get_computed_fields(their_schema)

        fname = field.name
        if (fname in our_cfs) != (fname in their_cfs):
            # The change in computed status decreases the similarity.
            similarity *= 0.95

        return similarity

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
            else:
                our_name = context.get_obj_name(our_schema, ours)
                their_name = theirs.get_name(their_schema)
                if our_name != their_name:
                    similarity /= 1.2
                else:
                    # If the new and old versions share a reference to
                    # an object that is being deleted, then we must
                    # delete this object as well.
                    if (type(ours), our_name) in context.deletions:
                        return 0.0

        elif ours is not None or theirs is not None:
            # one is None but not both
            similarity /= 1.2

        if similarity < 1.0:
            return compcoef
        else:
            return 1.0

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

    def field_is_computed(
        self,
        schema: s_schema.Schema,
        field_name: str,
    ) -> bool:
        return field_name in self.get_computed_fields(schema)

    def field_is_inherited(
        self,
        schema: s_schema.Schema,
        field_name: str,
    ) -> bool:
        return False

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

    def as_shell(
        self: Object_T,
        schema: s_schema.Schema,
    ) -> ObjectShell[Object_T]:
        return ObjectShell(
            name=self.get_name(schema),
            displayname=self.get_displayname(schema),
            schemaclass=type(self),
        )

    def get_ddl_identity(
        self,
        schema: s_schema.Schema,
    ) -> Optional[Dict[str, str]]:
        ddl_id_fields = [
            fn for fn, f in type(self).get_fields().items() if f.ddl_identity
        ]

        ddl_identity: Optional[Dict[str, Any]]
        if ddl_id_fields:
            ddl_identity = {}
            for fn in ddl_id_fields:
                v = self.get_field_value(schema, fn)
                if v is not None:
                    ddl_identity[fn] = v
        else:
            ddl_identity = None

        return ddl_identity

    def init_delta_command(
        self: Object_T,
        schema: s_schema.Schema,
        cmdtype: Type[sd.ObjectCommand_T],
        *,
        classname: Optional[sn.Name] = None,
        **kwargs: Any,
    ) -> sd.ObjectCommand_T:
        from . import delta as sd

        cls = type(self)
        cmd = sd.get_object_delta_command(
            objtype=cls,
            cmdtype=cmdtype,
            schema=schema,
            name=classname or self.get_name(schema),
            ddl_identity=self.get_ddl_identity(schema),
            **kwargs,
        )
        cmd.scls = self
        self.record_cmd_object_aux_data(schema, cmd)
        return cmd

    def record_cmd_object_aux_data(
        self: Object_T,
        schema: s_schema.Schema,
        cmd: sd.ObjectCommand[Any],
    ) -> None:
        for field in type(self).get_aux_cmd_data_fields():
            cmd.set_object_aux_data(
                field.name,
                self.get_field_value(schema, field.name),
            )

    def init_parent_delta_branch(
        self: Object_T,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        referrer: Optional[Object] = None,
    ) -> Tuple[sd.CommandGroup, sd.Command, sd.ContextStack]:
        """Prepare a parent portion of a command tree for this object.

        This returns a tuple containing:

        - the root (as a ``CommandGroup``) of a nested ``AlterObject`` tree
          with nodes for each enclosing referrer object;
        - direct reference to the innermost command in the above tree
          (may be root if there are no referring objects);
        - a ``ContextStack`` instance representing the nested CommandContext
          corresponding to the returned command tree.
        """
        from . import delta as sd
        root = sd.CommandGroup()
        return root, root, sd.ContextStack(())

    def init_delta_branch(
        self: Object_T,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        cmdtype: Type[sd.ObjectCommand_T],
        *,
        classname: Optional[sn.Name] = None,
        referrer: Optional[Object] = None,
        **kwargs: Any,
    ) -> Tuple[sd.CommandGroup, sd.ObjectCommand_T, sd.ContextStack]:
        """Make a command subtree for this object.

        This returns a tuple containing:

        - the root (as a ``CommandGroup``) of a nested ``AlterObject`` tree
          with nodes for each enclosing referrer object and an instance of
          *cmdtype* as the innermost command;
        - direct reference to the innermost command in the above tree;
        - a ``ContextStack`` instance representing the nested CommandContext
          corresponding to the returned command tree.
        """
        root_cmd, parent_cmd, ctx_stack = self.init_parent_delta_branch(
            schema=schema,
            context=context,
            referrer=referrer,
        )

        self_cmd = self.init_delta_command(
            schema,
            cmdtype=cmdtype,
            classname=classname,
            **kwargs,
        )

        parent_cmd.add(self_cmd)
        ctx_stack.push(self_cmd.new_context(schema, context, self))
        return root_cmd, self_cmd, ctx_stack

    def as_create_delta(
        self: Object_T,
        schema: s_schema.Schema,
        context: ComparisonContext,
    ) -> sd.ObjectCommand[Object_T]:
        from . import delta as sd

        cls = type(self)
        delta = self.init_delta_command(
            schema,
            sd.CreateObject,
            canonical=True,
        )

        if context.generate_prompts:
            delta.set_annotation('orig_cmdclass', type(delta))

        ff = cls.get_fields(sorted=True).items()
        fields = {fn: f for fn, f in ff if f.simpledelta and not f.ephemeral}
        for fn, f in fields.items():
            value = self.get_explicit_field_value(schema, fn, None)

            if (
                value is None
                and context.descriptive_mode
                and (
                    f.describe_visibility
                    & DescribeVisibilityFlags.SHOW_IF_DERIVED
                )
            ):
                value = self.get_field_value(schema, fn)
                value_from_default = True
            else:
                value_from_default = False

            if f.aux_cmd_data:
                delta.set_object_aux_data(fn, value)
            if value is not None:
                v: Any
                if issubclass(f.type, ObjectContainer):
                    v = value.as_shell(schema)
                else:
                    v = value
                self.record_field_create_delta(
                    schema,
                    delta,
                    context=context,
                    fname=fn,
                    value=v,
                    from_default=value_from_default,
                )

        for refdict in cls.get_refdicts():
            refcoll: ObjectCollection[Object] = (
                self.get_field_value(schema, refdict.attr))
            sorted_refcoll = sorted(
                refcoll.objects(schema),
                key=lambda o: o.get_name(schema),
            )
            for ref in sorted_refcoll:
                delta.add(ref.as_create_delta(schema, context))

        return delta

    def as_alter_delta(
        self: Object_T,
        other: Object_T,
        *,
        self_schema: s_schema.Schema,
        other_schema: s_schema.Schema,
        confidence: float,
        context: ComparisonContext,
    ) -> sd.ObjectCommand[Object_T]:
        from . import delta as sd

        cls = type(self)
        delta = self.init_delta_command(
            self_schema,
            sd.AlterObject,
            canonical=True,
        )

        delta.set_annotation('confidence', confidence)

        if context.generate_prompts:
            other_name = other.get_name(other_schema)
            if self.get_name(self_schema) != other_name:
                delta.set_annotation('new_name', other_name)
            delta.set_annotation('orig_cmdclass', type(delta))

        ff = cls.get_fields(sorted=True).items()
        fields = {fn: f for fn, f in ff if f.simpledelta and not f.ephemeral}
        for fn, f in fields.items():
            oldattr_v = self.get_explicit_field_value(self_schema, fn, None)
            newattr_v = other.get_explicit_field_value(other_schema, fn, None)
            if f.aux_cmd_data:
                delta.set_object_aux_data(fn, newattr_v)

            old_v: Any
            new_v: Any

            if issubclass(f.type, ObjectContainer):
                if oldattr_v is not None:
                    old_v = oldattr_v.as_shell(self_schema)
                else:
                    old_v = None
                if newattr_v is not None:
                    new_v = newattr_v.as_shell(other_schema)
                else:
                    new_v = None
            else:
                old_v = oldattr_v
                new_v = newattr_v

            if f.compcoef is not None:
                fcoef = cls.compare_obj_field_value(
                    f,
                    self,
                    other,
                    our_schema=self_schema,
                    their_schema=other_schema,
                    context=context,
                    explicit=True,
                )

                if fcoef != 1.0:
                    other.record_field_alter_delta(
                        other_schema,
                        delta,
                        context,
                        fname=fn,
                        value=new_v,
                        orig_value=old_v,
                        orig_schema=self_schema,
                        orig_object=self,
                        confidence=confidence,
                    )

        for refdict in cls.get_refdicts():
            oldcoll: ObjectCollection[Object] = (
                self.get_field_value(self_schema, refdict.attr))
            oldcoll_idx = sorted(
                oldcoll.objects(self_schema),
                key=lambda o: o.get_name(self_schema)
            )

            newcoll: ObjectCollection[Object] = (
                other.get_field_value(other_schema, refdict.attr))
            newcoll_idx = sorted(
                newcoll.objects(other_schema),
                key=lambda o: o.get_name(other_schema),
            )

            context.parent_ops.append(delta)

            delta.add(
                sd.delta_objects(
                    oldcoll_idx,
                    newcoll_idx,
                    sclass=refdict.ref_cls,
                    parent_confidence=confidence,
                    context=context,
                    old_schema=self_schema,
                    new_schema=other_schema,
                ),
            )

            context.parent_ops.pop()

        return delta

    def as_delete_delta(
        self: Object_T,
        *,
        schema: s_schema.Schema,
        context: ComparisonContext,
    ) -> sd.ObjectCommand[Object_T]:
        from . import delta as sd

        cls = type(self)
        delta = self.init_delta_command(
            schema,
            sd.DeleteObject,
            canonical=True,
        )

        if context.generate_prompts:
            delta.set_annotation('orig_cmdclass', type(delta))

        context.deletions[type(self), delta.classname] = delta

        ff = cls.get_fields(sorted=True).items()
        fields = {fn: f for fn, f in ff if f.simpledelta and not f.ephemeral}
        for fn, f in fields.items():
            value = self.get_explicit_field_value(schema, fn, None)
            if f.aux_cmd_data:
                delta.set_object_aux_data(fn, value)
            if value is not None:
                if issubclass(f.type, ObjectContainer):
                    v = value.as_shell(schema)
                else:
                    v = value

                self.record_field_delete_delta(
                    schema,
                    delta,
                    context,
                    fn,
                    orig_value=v,
                )

        for refdict in cls.get_refdicts():
            refcoll = self.get_field_value(schema, refdict.attr)
            for ref in refcoll.objects(schema):
                delta.add(ref.as_delete_delta(schema=schema, context=context))

        return delta

    def record_simple_field_delta(
        self: Object_T,
        schema: s_schema.Schema,
        delta: sd.ObjectCommand[Object_T],
        context: ComparisonContext,
        *,
        fname: str,
        value: Any,
        orig_value: Any,
        orig_schema: Optional[s_schema.Schema],
        orig_object: Optional[Object_T],
        from_default: bool = False,
    ) -> None:
        computed_fields = self.get_computed_fields(schema)
        is_computed = fname in computed_fields
        if orig_schema is not None and orig_object is not None:
            orig_computed_fields = (
                orig_object.get_computed_fields(orig_schema))
            orig_is_computed = fname in orig_computed_fields
        else:
            orig_is_computed = is_computed

        cmd = delta.set_attribute_value(
            fname,
            value,
            orig_value=orig_value,
            computed=is_computed,
            orig_computed=orig_is_computed,
            from_default=from_default,
        )

        context.parent_ops.append(delta)
        cmd.record_diff_annotations(
            schema=schema,
            orig_schema=orig_schema,
            context=context,
        )
        context.parent_ops.pop()

    def record_field_create_delta(
        self: Object_T,
        schema: s_schema.Schema,
        delta: sd.ObjectCommand[Object_T],
        context: ComparisonContext,
        *,
        fname: str,
        value: Any,
        from_default: bool,
    ) -> None:
        self.record_simple_field_delta(
            schema,
            delta,
            context,
            fname=fname,
            value=value,
            orig_value=None,
            orig_schema=None,
            orig_object=None,
            from_default=from_default,
        )

    def record_field_alter_delta(
        self: Object_T,
        schema: s_schema.Schema,
        delta: sd.ObjectCommand[Object_T],
        context: ComparisonContext,
        *,
        fname: str,
        value: Any,
        orig_value: Any,
        orig_schema: s_schema.Schema,
        orig_object: Object_T,
        confidence: float,
    ) -> None:
        from . import delta as sd

        if fname == 'name':
            rename_op = orig_object.init_delta_command(
                orig_schema,
                sd.RenameObject,
                new_name=value,
            )

            rename_op.set_annotation('confidence', confidence)

            self.record_simple_field_delta(
                schema,
                rename_op,
                context,
                fname=fname,
                value=value,
                orig_value=orig_value,
                orig_schema=orig_schema,
                orig_object=orig_object,
            )

            delta.add(rename_op)

            context.record_rename(rename_op)
        else:
            self.record_simple_field_delta(
                schema,
                delta,
                context,
                fname=fname,
                value=value,
                orig_value=orig_value,
                orig_schema=orig_schema,
                orig_object=orig_object,
            )

    def record_field_delete_delta(
        self: Object_T,
        schema: s_schema.Schema,
        delta: sd.ObjectCommand[Object_T],
        context: ComparisonContext,
        fname: str,
        orig_value: Any,
    ) -> None:
        self.record_simple_field_delta(
            schema,
            delta,
            context,
            fname=fname,
            value=None,
            orig_value=orig_value,
            orig_schema=None,
            orig_object=None,
        )

    def dump(self, schema: s_schema.Schema) -> str:
        return (
            f'<{type(self).__name__} name={self.get_name(schema)!r} '
            f'at {id(self):#x}>'
        )

    def __repr__(self) -> str:
        return f'<{type(self).__name__} {self.id} at 0x{id(self):#x}>'


class InternalObject(Object):
    """A schema object that is used by the system internally.

    Instances of InternalObject should not appear in schema dumps.
    """

    @classmethod
    def is_abstract(cls) -> bool:
        """Return True if this type does NOT represent a concrete schema class.
        """
        return cls is InternalObject


class QualifiedObject(Object):

    name = SchemaField(
        # ignore below because Mypy doesn't understand fields which are not
        # inheritable.
        sn.QualName,  # type: ignore
        inheritable=False,
        compcoef=0.670,
    )

    @classmethod
    def get_shortname_static(cls, name: sn.Name) -> sn.QualName:
        result = sn.shortname_from_fullname(name)
        if not isinstance(result, sn.QualName):
            assert isinstance(name, sn.QualName)
            result = sn.QualName(module=name.module, name=result.name)
        return result

    def get_shortname(self, schema: s_schema.Schema) -> sn.QualName:
        return type(self).get_shortname_static(self.get_name(schema))


QualifiedObject_T = TypeVar('QualifiedObject_T', bound='QualifiedObject')


class ObjectFragment(QualifiedObject):
    """A part of another object that cannot exist independently."""


class GlobalObject(Object):
    pass


GlobalObject_T = TypeVar('GlobalObject_T', bound='GlobalObject')


class ExternalObject(GlobalObject):
    """An object that is not tracked in a schema, but some external state."""
    pass


ExternalObject_T = TypeVar('ExternalObject_T', bound='ExternalObject')


class DerivableObject(QualifiedObject):

    def derive_name(
        self,
        schema: s_schema.Schema,
        source: QualifiedObject,
        *qualifiers: str,
        derived_name_base: Optional[sn.Name] = None,
        module: Optional[str] = None,
    ) -> sn.QualName:
        source_name = source.get_name(schema)
        if module is None:
            module = source_name.module
        qualifiers = (str(source_name),) + qualifiers

        return derive_name(
            schema,
            *qualifiers,
            module=module,
            parent=self,
            derived_name_base=derived_name_base,
        )

    def generic(self, schema: s_schema.Schema) -> bool:
        return self.get_shortname(schema) == self.get_name(schema)

    def get_derived_name_base(self, schema: s_schema.Schema) -> sn.Name:
        return self.get_shortname(schema)

    def get_derived_name(
        self,
        schema: s_schema.Schema,
        source: QualifiedObject,
        *qualifiers: str,
        mark_derived: bool = False,
        derived_name_base: Optional[sn.Name] = None,
        module: Optional[str] = None,
    ) -> sn.QualName:
        return self.derive_name(
            schema, source, *qualifiers,
            derived_name_base=derived_name_base,
            module=module)


class Shell:

    def resolve(self, schema: s_schema.Schema) -> Any:
        raise NotImplementedError


class ObjectShell(Shell, Generic[Object_T_co]):

    def __init__(
        self,
        *,
        name: sn.Name,
        schemaclass: Type[Object_T_co],
        displayname: Optional[str] = None,
        origname: Optional[sn.Name] = None,
        sourcectx: Optional[parsing.ParserContext] = None,
    ) -> None:
        self.name = name
        self.origname = origname
        self.displayname = displayname
        self.schemaclass = schemaclass
        self.sourcectx = sourcectx

    def get_id(self, schema: s_schema.Schema) -> uuid.UUID:
        return self.resolve(schema).get_id(schema)

    def resolve(self, schema: s_schema.Schema) -> Object_T_co:
        if self.name is None:
            raise TypeError(
                'cannot resolve anonymous ObjectShell'
            )

        if (
            self.schemaclass is Object
            or issubclass(self.schemaclass, QualifiedObject)
        ):
            return schema.get(
                self.name,
                type=self.schemaclass,
                sourcectx=self.sourcectx,
            )
        else:
            return schema.get_global(self.schemaclass, self.name)

    def get_refname(self, schema: s_schema.Schema) -> sn.Name:
        if self.origname is not None:
            return self.origname
        else:
            # XXX: change get_displayname to return Name
            return sn.name_from_string(self.get_displayname(schema))

    def get_name(self, schema: s_schema.Schema) -> sn.Name:
        return self.name

    def get_displayname(self, schema: s_schema.Schema) -> str:
        return self.displayname or str(self.name)

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


# A set of scalars that should be reflected as a multi prop, not as an
# array.
class MultiPropSet(
    checked.FrozenCheckedSet[T],
):
    pass


class ObjectCollection(
    ObjectContainer,
    parametric.SingleParametricType[Object_T],
    Generic[Object_T],
):
    __slots__ = ('_ids',)

    # Even though Object_T would be a correct annotation below,
    # we want the type to default to base `Object` for cases
    # when a TypeVar is passed as Object_T.  This is a hack,
    # of course, because, ideally we'd want to at least default
    # to the bounds or constraints of the TypeVar, or, even better,
    # pass the actual type at the call site, but there seems to be
    # no easy solution to do that.
    type: ClassVar[Type[Object]] = Object  # type: ignore
    _registry: ClassVar[Dict[str, Type[ObjectCollection[Object]]]] = {}

    _container: ClassVar[Type[CollectionFactory[Any]]]

    def __init_subclass__(
        cls,
        *,
        container: Optional[Type[CollectionFactory[Any]]] = None,
    ) -> None:
        super().__init_subclass__()
        if container is not None:
            cls._container = container

        if not cls.is_anon_parametrized():
            name = cls.__name__
            if name in cls._registry:
                raise TypeError(
                    f'duplicate name for schema collection class: {name},'
                    f'already defined as {cls._registry[name]!r}'
                )
            else:
                cls._registry[name] = cls  # type: ignore

    @classmethod
    def get_subclass(cls, name: str) -> Type[ObjectCollection[Object]]:
        return cls._registry[name]

    def __init__(
        self,
        _ids: Collection[uuid.UUID],
        *,
        _private_init: bool,
    ) -> None:
        if not self.is_fully_resolved():
            raise TypeError(
                f"{type(self)!r} unresolved type parameters"
            )
        self._ids = _ids

    def __len__(self) -> int:
        return len(self._ids)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._ids == other._ids

    def __hash__(self) -> int:
        return hash(self._ids)

    def schema_reduce(
        self,
    ) -> Tuple[
        str,
        Optional[Union[Tuple[builtins.type, ...], builtins.type]],
        Tuple[uuid.UUID, ...],
        Tuple[Tuple[str, Any], ...],
    ]:
        cls = type(self)
        _, (typeargs, ids, attrs) = self.__reduce__()
        if cls.is_anon_parametrized():
            clsname = cls.__bases__[0].__name__
        else:
            clsname = cls.__name__
        return (clsname, typeargs, ids, tuple(attrs.items()))

    @classmethod
    @functools.lru_cache(maxsize=10240)
    def schema_restore(
        cls,
        data: Tuple[
            str,
            Optional[Union[Tuple[builtins.type, ...], builtins.type]],
            Tuple[uuid.UUID, ...],
            Tuple[Tuple[str, Any], ...],
        ],
    ) -> ObjectCollection[Object]:
        clsname, typeargs, ids, attrs = data
        scoll_class = ObjectCollection.get_subclass(clsname)
        return scoll_class.__restore__(typeargs, ids, dict(attrs))

    @classmethod
    def schema_refs_from_data(
        cls,
        data: Tuple[
            str,
            Optional[Union[Tuple[builtins.type, ...], builtins.type]],
            Tuple[uuid.UUID, ...],
            Tuple[Tuple[str, Any], ...],
        ],
    ) -> FrozenSet[uuid.UUID]:
        return frozenset(data[2])

    def __reduce__(
        self
    ) -> Tuple[
        Callable[..., ObjectCollection[Any]],
        Tuple[
            Optional[Union[Tuple[builtins.type, ...], builtins.type]],
            Tuple[uuid.UUID, ...],
            Dict[str, Any],
        ],
    ]:
        assert type(self).is_fully_resolved(), \
            f'{type(self)} parameters are not resolved'

        cls: Type[ObjectCollection[Object_T]] = self.__class__
        types: Optional[Tuple[type, ...]] = self.orig_args
        if types is None or not cls.is_anon_parametrized():
            typeargs = None
        else:
            typeargs = types[0] if len(types) == 1 else types
        attrs = {k: getattr(self, k) for k in self.__slots__ if k != '_ids'}
        # Mypy fails to resolve typeargs properly
        return (
            cls.__restore__,
            (typeargs, tuple(self._ids), attrs)  # type: ignore
        )

    @classmethod
    def __restore__(
        cls,
        typeargs: Optional[Union[Tuple[builtins.type, ...], builtins.type]],
        ids: Tuple[uuid.UUID, ...],
        attrs: Dict[str, Any],
    ) -> ObjectCollection[Object_T]:
        if typeargs is None or cls.is_anon_parametrized():
            obj = cls(_ids=ids, **attrs, _private_init=True)
        else:
            obj = cls[typeargs](  # type: ignore
                _ids=ids, **attrs, _private_init=True)

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
        **kwargs: Any,
    ) -> ObjectCollection[Object_T]:
        ids: List[uuid.UUID] = []

        if isinstance(data, ObjectCollection):
            ids.extend(data._ids)
        elif data:
            for v in data:
                ids.append(cls._validate_value(schema, v))
        container: Collection[uuid.UUID] = cls._container(ids)
        return cls(container, **kwargs, _private_init=True)

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

    def names(self, schema: s_schema.Schema) -> Collection[sn.Name]:
        result = []

        for item_id in self._ids:
            obj = schema.get_by_id(item_id)
            result.append(obj.get_name(schema))

        return type(self)._container(result)

    def objects(self, schema: s_schema.Schema) -> Tuple[Object_T, ...]:
        return tuple(
            schema.get_by_id(iid) for iid in self._ids  # type: ignore
        )

    def _object_keys(self, schema: s_schema.Schema) -> Set[
            Tuple[Type[Object], sn.Name]]:
        return {
            (type(x), x.get_name(schema))
            for x in (self.objects(schema))
        }

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
        # If the new and old versions share a reference to an object
        # that is being deleted, then we must delete this object as well.
        our_keys = set(ours._object_keys(our_schema) if ours else {})
        their_keys = set(theirs._object_keys(their_schema) if theirs else {})
        if (our_keys & their_keys) & context.deletions.keys():
            return 0.0

        if ours is not None:
            our_names = cls._container(
                context.get_obj_name(our_schema, obj)
                for obj in ours.objects(our_schema)
            )
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
        items: Iterable[ObjectShell[Object_T]],
        collection_type: Type[ObjectCollection[Object_T]],
    ) -> None:
        self.items = items
        self.collection_type = collection_type

    def __iter__(self) -> Iterator[ObjectShell[Object_T]]:
        return iter(self.items)

    def resolve(self, schema: s_schema.Schema) -> ObjectCollection[Object_T]:
        return self.collection_type.create(
            schema,
            [s.resolve(schema) for s in self.items],
        )

    def __repr__(self) -> str:
        tn = self.__class__.__name__
        cn = self.collection_type.__name__
        items = ', '.join(str(e.name) or '<anonymous>' for e in self.items)
        return f'<{tn} {cn}({items}) at 0x{id(self):x}>'


Key_T = TypeVar("Key_T")
KeyFunction = Callable[["s_schema.Schema", Object_T], Key_T]
OIBT = TypeVar("OIBT", bound="ObjectIndexBase[Object, Any]")


class ObjectIndexBase(
    Generic[Key_T, Object_T],
    ObjectCollection[Object_T],
    container=tuple,
):
    _key: KeyFunction[Object_T, Key_T]

    def __init_subclass__(
        cls,
        *,
        key: Optional[KeyFunction[Object_T, Key_T]] = None,
    ) -> None:
        super().__init_subclass__()
        if key is not None:
            cls._key = key

    @classmethod
    def get_key_for(cls, schema: s_schema.Schema, obj: Object) -> Key_T:
        return cls._key(schema, obj)  # type: ignore  # mypy bug?

    @classmethod
    def get_key_for_name(
        cls,
        schema: s_schema.Schema,
        name: sn.Name,
    ) -> Key_T:
        raise NotImplementedError

    @classmethod
    def create(
        cls: Type[ObjectIndexBase[Key_T, Object_T]],
        schema: s_schema.Schema,
        data: Iterable[Object_T],
        **kwargs: Any,
    ) -> ObjectIndexBase[Key_T, Object_T]:
        coll = cast(
            ObjectIndexBase[Key_T, Object_T],
            super().create(schema, data, **kwargs)
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
        names: Iterable[Key_T],
    ) -> Tuple[s_schema.Schema, OIBT]:
        items = dict(self.items(schema))
        for name in names:
            items.pop(name)  # type: ignore[call-overload]  # mypy bug
        return (
            schema,
            cast(OIBT, type(self).create(schema, items.values())),
        )

    def items(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[Tuple[Key_T, Object_T], ...]:
        result = []
        keyfunc = type(self)._key

        for obj in self.objects(schema):
            result.append((keyfunc(schema, obj), obj))

        return tuple(result)

    def keys(self, schema: s_schema.Schema) -> Tuple[Key_T, ...]:
        result = []
        keyfunc = type(self)._key

        for obj in self.objects(schema):
            result.append(keyfunc(schema, obj))

        return tuple(result)

    def has(self, schema: s_schema.Schema, name: Key_T) -> bool:
        return name in self.keys(schema)

    def get(
        self,
        schema: s_schema.Schema,
        name: Key_T,
        default: Any = NoDefault,
    ) -> Optional[Object_T]:
        items = dict(self.items(schema))
        if default is NoDefault:
            return items[name]
        else:
            return items.get(name, default)


def _fullname_object_key(schema: s_schema.Schema, o: Object) -> sn.Name:
    return o.get_name(schema)


class ObjectIndexByFullname(
    ObjectIndexBase[sn.Name, Object_T],
    key=_fullname_object_key,
):

    @classmethod
    def get_key_for_name(
        cls,
        schema: s_schema.Schema,
        name: sn.Name,
    ) -> sn.Name:
        return name


def _shortname_object_key(schema: s_schema.Schema, o: Object) -> sn.Name:
    return o.get_shortname(schema)


class ObjectIndexByShortname(
    ObjectIndexBase[sn.Name, Object_T],
    key=_shortname_object_key,
):

    @classmethod
    def get_key_for_name(
        cls,
        schema: s_schema.Schema,
        name: sn.Name,
    ) -> sn.Name:
        return sn.shortname_from_fullname(name)


def _unqualified_object_key(
    schema: s_schema.Schema,
    o: QualifiedObject,
) -> sn.UnqualName:
    assert isinstance(o, QualifiedObject)
    return sn.UnqualName(o.get_shortname(schema).name)


class ObjectIndexByUnqualifiedName(
    ObjectIndexBase[sn.UnqualName, QualifiedObject_T],
    key=_unqualified_object_key,
):

    @classmethod
    def get_key_for_name(
        cls,
        schema: s_schema.Schema,
        name: sn.Name,
    ) -> sn.UnqualName:
        return sn.UnqualName(sn.shortname_from_fullname(name).name)


class ObjectDict(
    Generic[Key_T, Object_T],
    ObjectCollection[Object_T],
    container=tuple,
):
    __slots__ = ('_ids', '_keys')

    # Breaking the Liskov Substitution Principle
    @classmethod
    def create(  # type: ignore
        cls,
        schema: s_schema.Schema,
        data: Mapping[Key_T, Object_T],
        **kwargs: Any,
    ) -> ObjectDict[Key_T, Object_T]:
        return cast(
            ObjectDict[Key_T, Object_T],
            super().create(schema, data.values(), _keys=tuple(data.keys())),
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
        assert isinstance(ours, ObjectDict)
        assert isinstance(theirs, ObjectDict)
        if ours.keys(our_schema) != theirs.keys(their_schema):
            return compcoef
        return super().compare_values(
            ours, theirs,
            our_schema=our_schema, their_schema=their_schema,
            context=context, compcoef=compcoef)

    def __init__(
        self,
        _ids: Collection[uuid.UUID],
        _keys: Tuple[Key_T, ...],
        *,
        _private_init: bool,
    ) -> None:
        super().__init__(_ids, _private_init=_private_init)
        self._keys = _keys

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

    items: Mapping[Any, ObjectShell[Object_T]]
    collection_type: Type[ObjectDict[Key_T, Object_T]]

    def __init__(
        self,
        items: Mapping[Any, ObjectShell[Object_T]],
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
            {k: s.resolve(schema) for k, s in self.items.items()},
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
        ignore_local: bool = False,
        schema: s_schema.Schema,
    ) -> ObjectSet[Object_T]:
        if not ignore_local:
            result = target.get_explicit_field_value(schema, field_name, None)
        else:
            result = None
        for source in sources:
            if source.__class__.get_field(field_name) is None:
                continue
            theirs = source.get_explicit_field_value(schema, field_name, None)
            if theirs:
                if result is None:
                    result = theirs
                else:
                    result._ids |= theirs._ids

        return result  # type: ignore


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
        **kwargs: Any,
    ) -> ObjectList[Object_T]:
        return super().create(schema, data, **kwargs)  # type: ignore


class SubclassableObject(Object):

    abstract = SchemaField(
        bool,
        default=False,
        inheritable=False,
        special_ddl_syntax=True,
        compcoef=0.909,
    )

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
            if isinstance(parent, s_types.Type) and parent.is_any(schema):
                return True
            else:
                return self._issubclass(schema, parent)


InheritingObjectT = TypeVar('InheritingObjectT', bound='InheritingObject')


class InheritingObject(SubclassableObject):

    bases = SchemaField(
        ObjectList['InheritingObject'],
        type_is_generic_self=True,
        default=DEFAULT_CONSTRUCTOR,
        coerce=True,
        inheritable=False,
        compcoef=0.900,
    )

    ancestors = SchemaField(
        ObjectList['InheritingObject'],
        type_is_generic_self=True,
        default=DEFAULT_CONSTRUCTOR,
        coerce=True,
        inheritable=False,
        compcoef=0.999,
    )

    # Fields that have been inherited as opposed to set explicitly.
    inherited_fields = SchemaField(
        checked.FrozenCheckedSet[str],
        default=DEFAULT_CONSTRUCTOR,
        coerce=True,
        inheritable=False,
        compcoef=0.999,
    )

    is_derived = SchemaField(
        bool,
        default=False, compcoef=0.909)

    def inheritable_fields(self) -> Iterable[str]:
        for fn, f in self.__class__.get_fields().items():
            if f.inheritable and not f.ephemeral:
                yield fn

    @classmethod
    def get_default_base_name(self) -> Optional[sn.Name]:
        return None

    # Redefining bases and ancestors accessors to make them generic
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

    def get_base_names(self, schema: s_schema.Schema) -> Collection[sn.Name]:
        return self.get_bases(schema).names(schema)

    def maybe_get_topmost_concrete_base(
        self: InheritingObjectT,
        schema: s_schema.Schema
    ) -> Optional[InheritingObjectT]:
        """Get the topmost non-abstract base."""
        lineage = self.get_ancestors(schema).objects(schema)
        for ancestor in reversed(lineage):
            if not ancestor.get_abstract(schema):
                return ancestor

        if not self.get_abstract(schema):
            return self

        return None

    def get_topmost_concrete_base(
        self: InheritingObjectT,
        schema: s_schema.Schema
    ) -> InheritingObjectT:
        """Get the topmost non-abstract base."""
        base = self.maybe_get_topmost_concrete_base(schema)
        if not base:
            raise errors.SchemaError(
                f'{self.get_verbosename(schema)} has no non-abstract ancestors'
            )
        return base

    def get_base_for_cast(self, schema: s_schema.Schema) -> Object:
        return self.get_topmost_concrete_base(schema)

    @classmethod
    def get_root_classes(cls) -> Tuple[sn.QualName, ...]:
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
        self: InheritingObjectT, schema: s_schema.Schema
    ) -> List[InheritingObjectT]:
        """Return class descendants in ancestral order."""
        graph = {}
        for descendant in self.descendants(schema):
            graph[descendant] = topological.DepGraphEntry(
                item=descendant,
                deps=ordered.OrderedSet(
                    descendant.get_bases(schema).objects(schema),
                ),
                extra=False,
            )

        return list(topological.sort(graph, allow_unresolved=True))

    def children(
        self: InheritingObjectT,
        schema: s_schema.Schema,
    ) -> FrozenSet[InheritingObjectT]:
        return schema.get_children(self)

    def field_is_inherited(
        self,
        schema: s_schema.Schema,
        field_name: str,
    ) -> bool:
        inherited_fields = self.get_inherited_fields(schema)
        return field_name in inherited_fields

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

    def as_alter_delta(
        self: InheritingObjectT,
        other: InheritingObjectT,
        *,
        self_schema: s_schema.Schema,
        other_schema: s_schema.Schema,
        confidence: float,
        context: ComparisonContext,
    ) -> sd.ObjectCommand[InheritingObjectT]:
        from . import delta as sd
        from . import inheriting as s_inh

        delta = super().as_alter_delta(
            other,
            self_schema=self_schema,
            other_schema=other_schema,
            confidence=confidence,
            context=context,
        )

        rebase = sd.get_object_command_class(
            s_inh.RebaseInheritingObject, type(self))

        old_base_names = tuple(
            context.get_obj_name(self_schema, base)
            for base in self.get_bases(self_schema).objects(self_schema)
        )
        new_base_names = other.get_bases(other_schema).names(other_schema)

        if old_base_names != new_base_names and rebase is not None:
            removed, added = s_inh.delta_bases(
                old_base_names,
                new_base_names,
                t=type(self),
            )

            rebase_cmd = rebase(
                classname=other.get_name(other_schema),
                removed_bases=removed,
                added_bases=added,
            )

            rebase_cmd.set_attribute_value(
                'bases',
                other.get_bases(other_schema).as_shell(other_schema),
            )

            rebase_cmd.set_attribute_value(
                'ancestors',
                other.get_ancestors(other_schema).as_shell(other_schema),
            )
            # Trim these from the base alter since they are redundant
            # and clog up debug output.
            delta.discard(not_none(delta._get_attribute_set_cmd('bases')))
            # ancestors might not be in the delta, if it didn't change
            if anc := delta._get_attribute_set_cmd('ancestors'):
                delta.discard(anc)

            delta.add(rebase_cmd)

        return delta

    def record_simple_field_delta(
        self: InheritingObjectT,
        schema: s_schema.Schema,
        delta: sd.ObjectCommand[InheritingObjectT],
        context: ComparisonContext,
        *,
        fname: str,
        value: Any,
        orig_value: Any,
        orig_schema: Optional[s_schema.Schema],
        orig_object: Optional[InheritingObjectT],
        from_default: bool = False,
    ) -> None:
        inherited_fields = self.get_inherited_fields(schema)
        is_inherited = fname in inherited_fields
        if orig_schema is not None and orig_object is not None:
            orig_inherited_fields = (
                orig_object.get_inherited_fields(orig_schema))
            orig_is_inherited = fname in orig_inherited_fields
        else:
            orig_is_inherited = is_inherited

        computed_fields = self.get_computed_fields(schema)
        is_computed = fname in computed_fields
        if orig_schema is not None and orig_object is not None:
            orig_computed_fields = (
                orig_object.get_computed_fields(orig_schema))
            orig_is_computed = fname in orig_computed_fields
        else:
            orig_is_computed = is_computed

        cmd = delta.set_attribute_value(
            fname,
            value=value,
            orig_value=orig_value,
            inherited=is_inherited,
            orig_inherited=orig_is_inherited,
            computed=is_computed,
            orig_computed=orig_is_computed,
            from_default=from_default,
        )

        context.parent_ops.append(delta)
        cmd.record_diff_annotations(
            schema=schema,
            orig_schema=orig_schema,
            context=context,
        )
        context.parent_ops.pop()

    def get_field_create_delta(
        self: InheritingObjectT,
        schema: s_schema.Schema,
        delta: sd.ObjectCommand[InheritingObjectT],
        fname: str,
        value: Any,
    ) -> None:
        inherited_fields = self.get_inherited_fields(schema)
        delta.set_attribute_value(
            fname,
            value=value,
            inherited=fname in inherited_fields,
        )

    def get_field_alter_delta(
        self: InheritingObjectT,
        old_schema: s_schema.Schema,
        new_schema: s_schema.Schema,
        delta: sd.ObjectCommand[InheritingObjectT],
        fname: str,
        value: Any,
        orig_value: Any,
    ) -> None:
        inherited_fields = self.get_inherited_fields(new_schema)
        delta.set_attribute_value(
            fname,
            value,
            orig_value=orig_value,
            inherited=fname in inherited_fields,
        )

    def get_field_delete_delta(
        self: InheritingObjectT,
        schema: s_schema.Schema,
        delta: sd.ObjectCommand[InheritingObjectT],
        fname: str,
        orig_value: Any,
    ) -> None:
        inherited_fields = self.get_inherited_fields(schema)
        delta.set_attribute_value(
            fname,
            value=None,
            orig_value=orig_value,
            inherited=fname in inherited_fields,
        )

    @classmethod
    def compare_obj_field_value(
        cls: Type[InheritingObjectT],
        field: Field[Type[T]],
        ours: InheritingObjectT,
        theirs: InheritingObjectT,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: ComparisonContext,
        explicit: bool = False,
    ) -> float:
        similarity = super().compare_obj_field_value(
            field,
            ours,
            theirs,
            our_schema=our_schema,
            their_schema=their_schema,
            context=context,
            explicit=explicit,
        )

        # Check to see if this field's inherited status has changed.
        # If so, this is definitely a change.
        our_ifs = ours.get_inherited_fields(our_schema)
        their_ifs = theirs.get_inherited_fields(their_schema)

        fname = field.name
        if (fname in our_ifs) != (fname in their_ifs):
            # The change in inherited status decreases the similarity.
            similarity *= 0.95

        return similarity


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
            obj = obj.get_bases(schema).first(schema)
        return obj

    def get_nearest_generic_parent(
        self: DerivableInheritingObjectT,
        schema: s_schema.Schema,
    ) -> DerivableInheritingObjectT:
        obj = self
        while not obj.generic(schema):
            obj = obj.get_bases(schema).first(schema)
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
    lineage: Iterable[List[InheritingObjectT]],
    subject_name: str,
) -> List[InheritingObjectT]:
    result: List[Any] = []

    while True:
        nonempty = [line for line in lineage if line]
        if not nonempty:
            return result

        for line in nonempty:
            candidate = line[0]
            tails = [m for m in nonempty if candidate in m[1:]]
            if not tails:
                break
        else:
            raise errors.SchemaError(
                f"Could not find consistent ancestor order for {subject_name}"
            )

        result.append(candidate)

        for line in nonempty:
            if line[0] == candidate:
                del line[0]


def _compute_lineage(
    schema: s_schema.Schema,
    obj: InheritingObjectT,
    subject_name: str,
) -> List[InheritingObjectT]:
    bases = tuple(obj.get_bases(schema).objects(schema))
    lineage = [[obj]]

    for base in bases:
        lineage.append(_compute_lineage(schema, base, subject_name))

    return _merge_lineage(lineage, subject_name)


def compute_lineage(
    schema: s_schema.Schema,
    bases: Iterable[InheritingObjectT],
    subject_name: str,
) -> List[InheritingObjectT]:
    lineage = []
    for base in bases:
        lineage.append(_compute_lineage(schema, base, subject_name))

    return _merge_lineage(lineage, subject_name)


def compute_ancestors(
    schema: s_schema.Schema,
    obj: InheritingObjectT,
) -> List[InheritingObjectT]:
    return compute_lineage(
        schema,
        obj.get_bases(schema).objects(schema),
        obj.get_verbosename(schema),
    )


def derive_name(
    schema: s_schema.Schema,
    *qualifiers: str,
    module: str,
    parent: Optional[DerivableObject] = None,
    derived_name_base: Optional[sn.Name] = None,
) -> sn.QualName:
    if derived_name_base is None:
        assert parent is not None
        derived_name_base = parent.get_derived_name_base(schema)
    name = sn.get_specialized_name(derived_name_base, *qualifiers)
    return sn.QualName(name=name, module=module)
