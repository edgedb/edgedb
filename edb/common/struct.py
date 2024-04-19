#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2009-present MagicStack Inc. and the EdgeDB authors.
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
    Any,
    Callable,
    Final,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    Iterable,
    Iterator,
    Mapping,
    Dict,
    List,
    cast,
)

import collections
import enum

from . import checked


class ProtoField:
    __slots__ = ()


class NoDefaultT(enum.Enum):
    NoDefault = 0


NoDefault: Final = NoDefaultT.NoDefault


T = TypeVar("T")


class Field(ProtoField, Generic[T]):
    """``Field`` objects: attributes of :class:`Struct`."""

    __slots__ = ('name', 'type', 'default', 'coerce', 'formatters',
                 'frozen')

    name: str

    def __init__(
        self,
        type_: Type[T],
        default: Union[T, NoDefaultT] = NoDefault,
        *,
        coerce: bool = False,
        str_formatter: Callable[[T], str] = str,
        repr_formatter: Callable[[T], str] = repr,
        frozen: bool = False,
    ) -> None:
        """
        :param type:
            The type of the value in the field.
        :param default:
            Default field value.  If not specified, the field would
            be considered required and a failure to specify its
            value when initializing a ``Struct`` will raise
            :exc:`TypeError`.  `default` can be a callable taking
            no arguments.
        :param bool coerce:
            If set to ``True`` - coerce field's value to its type.
        """
        self.type = type_
        self.default = default
        self.coerce = coerce
        self.frozen = frozen
        self.formatters = {'str': str_formatter, 'repr': repr_formatter}

    def copy(self) -> Field[T]:
        return self.__class__(
            self.type, self.default, coerce=self.coerce,
            str_formatter=self.formatters['str'],
            repr_formatter=self.formatters['repr'])

    def adapt(self, value: Any) -> T:
        # cast() below due to https://github.com/python/mypy/issues/7920
        ctype = cast(type, self.type)

        if not isinstance(value, ctype):
            value = ctype(value)

        # Type ignore below because with ctype we lost information that
        # it is indeed a Type[T].
        return value  # type: ignore

    @property
    def required(self) -> bool:
        return self.default is NoDefault


StructMeta_T = TypeVar("StructMeta_T", bound="StructMeta")


class StructMeta(type):

    _fields: Dict[str, Field[Any]]
    _sorted_fields: Dict[str, Field[Any]]

    def __new__(
        mcls: Type[StructMeta_T],
        name: str,
        bases: Tuple[type, ...],
        clsdict: Dict[str, Any],
        *,
        use_slots: bool = True,
        **kwargs: Any,
    ) -> StructMeta_T:
        fields = {}
        myfields = {}

        for k, v in clsdict.items():
            if not isinstance(v, ProtoField):
                continue
            if not isinstance(v, Field):
                raise TypeError(
                    f'cannot create {name} class: struct.Field expected, '
                    f'got {type(v)}')

            v.name = k
            myfields[k] = v

        if '__slots__' not in clsdict:
            if use_slots is None:
                for base in bases:
                    sa = '{}.{}_slots'.format(base.__module__, base.__name__)
                    if isinstance(base, StructMeta) and hasattr(base, sa):
                        use_slots = True
                        break

            if use_slots:
                clsdict['__slots__'] = tuple(myfields.keys())
                for key in myfields.keys():
                    del clsdict[key]

        cls = super().__new__(mcls, name, bases, clsdict, **kwargs)

        if use_slots:
            sa = '{}.{}_slots'.format(cls.__module__, cls.__name__)
            setattr(cls, sa, True)

        for parent in reversed(cls.__mro__):
            if parent is cls:
                fields.update(myfields)
            elif isinstance(parent, StructMeta):
                fields.update(parent.get_ownfields())

        for field in fields.values():
            if field.coerce and not issubclass(cls, RTStruct):
                raise TypeError(
                    f'{cls.__name__}.{field.name} cannot be declared '
                    f'with coerce=True: {cls.__name__} is not an RTStruct',
                )
            if field.frozen and not issubclass(cls, RTStruct):
                raise TypeError(
                    f'{cls.__name__}.{field.name} cannot be declared '
                    f'with frozen=True: {cls.__name__} is not an RTStruct',
                )

        cls._fields = fields
        cls._sorted_fields = collections.OrderedDict(
            sorted(fields.items(), key=lambda e: e[0]))
        fa = '{}.{}_fields'.format(cls.__module__, cls.__name__)
        setattr(cls, fa, myfields)
        return cls

    def get_field(cls, name: str) -> Optional[Field[Any]]:
        return cls._fields.get(name)

    def get_fields(cls, sorted: bool = False) -> Dict[str, Field[Any]]:
        return cls._sorted_fields if sorted else cls._fields

    def get_ownfields(cls) -> Dict[str, Field[Any]]:
        return getattr(  # type: ignore
            cls, '{}.{}_fields'.format(cls.__module__, cls.__name__))


Struct_T = TypeVar("Struct_T", bound="Struct")


class Struct(metaclass=StructMeta):
    """A base class allowing implementation of attribute objects protocols.

    Each struct has a collection of ``Field`` objects, which should be defined
    as class attributes of the ``Struct`` subclass.  Unlike
    ``collections.namedtuple``, ``Struct`` is much easier to mix in and define.
    Furthermore, fields are strictly typed and can be declared as required.  By
    default, Struct will reject attributes, which have not been declared as
    fields.  A ``MixedStruct`` subclass does have this restriction.

    .. code-block:: pycon

        >>> from edb.common.struct import Struct, Field

        >>> class MyStruct(Struct):
        ...    name = Field(type=str)
        ...    description = Field(type=str, default=None)
        ...
        >>> MyStruct(name='Spam')
        <MyStruct name=Spam>
        >>> MyStruct(name='Ham', description='Good Ham')
        <MyStruct name=Ham, description=Good Ham>

    If ``use_slots`` is set to ``True`` in a class signature, ``__slots__``
    will be used to create dictless instances, with reduced memory footprint:

    .. code-block:: pycon

        >>> class S1(Struct, use_slots=True):
        ...     foo = Field(str, None)

        >>> class S2(S1):
        ...     bar = Field(str, None)

        >>> S2().foo = '1'
        >>> S2().bar = '2'

        >>> S2().spam = '2'
        AttributeError: 'S2' object has no attribute 'spam'
    """

    def __init__(self, **kwargs: Any) -> None:
        """
        :raises: TypeError if invalid field value was provided or a value was
                 not provided for a field without a default value.
        """
        self._check_init_argnames(kwargs)
        self._init_fields(kwargs)

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        if isinstance(state, tuple) and len(state) == 2:
            state, slotstate = state
        else:
            slotstate = None

        if state:
            self.update(**state)

        if slotstate:
            self.update(**slotstate)

    def update(self, *args: Any, **kwargs: Any) -> None:
        """Update the field values."""
        values: Dict[str, Any] = {}
        values.update(*args, **kwargs)

        self._check_init_argnames(values)

        for k, v in values.items():
            setattr(self, k, v)

    def setdefaults(self) -> List[str]:
        """Initialize unset fields with default values."""
        fields_set = []
        for field_name, field in self.__class__._fields.items():
            value = getattr(self, field_name)
            if value is None and field.default is not None:
                value = self._getdefault(field_name, field)
                self.set_default_value(field_name, value)
                fields_set.append(field_name)
        return fields_set

    def set_default_value(self, field_name: str, value: Any) -> None:
        setattr(self, field_name, value)

    def formatfields(
        self,
        formatter: str = 'str',
    ) -> Iterator[Tuple[str, str]]:
        """Return an iterator over fields formatted using `formatter`."""
        for name, field in self.__class__._fields.items():
            formatter_obj = field.formatters.get(formatter)
            if formatter_obj:
                yield (name, formatter_obj(getattr(self, name)))

    def _copy_and_replace(
        self,
        cls: Type[Struct_T],
        **replacements: Any,
    ) -> Struct_T:
        args = {f: getattr(self, f) for f in cls._fields.keys()}
        if replacements:
            args.update(replacements)
        return cls(**args)

    def copy_with_class(self, cls: Type[Struct_T]) -> Struct_T:
        return self._copy_and_replace(cls)

    def copy(self: Struct_T) -> Struct_T:
        return self.copy_with_class(type(self))

    def replace(self: Struct_T, **replacements: Any) -> Struct_T:
        return self._copy_and_replace(type(self), **replacements)

    def items(self) -> Iterator[Tuple[str, Any]]:
        for field in self.__class__._fields:
            yield field, getattr(self, field, None)

    def as_tuple(self) -> Tuple[Any, ...]:
        result = []
        for field in self.__class__._fields:
            result.append(getattr(self, field, None))
        return tuple(result)

    __copy__ = copy

    def __iter__(self) -> Iterator[str]:
        return iter(self.__class__._fields)

    def __str__(self) -> str:
        fields = ', '.join(
            f'{name}={value}'
            for name, value in self.formatfields('str')
        )
        if fields:
            fields = f' {fields}'
        return f'<{self.__class__.__name__}{fields} at {id(self):#x}>'

    def __repr__(self) -> str:
        fields = ', '.join(
            f'{name}={value}'
            for name, value in self.formatfields('repr')
        )
        if fields:
            fields = f' {fields}'
        return f'<{self.__class__.__name__}{fields} at {id(self):#x}>'

    def _init_fields(
        self,
        values: Mapping[str, Any],
    ) -> None:
        for field_name, field in self.__class__._fields.items():
            value = values.get(field_name)

            if value is None and field.default is not None:
                value = self._getdefault(field_name, field)

            setattr(self, field_name, value)

    def _check_init_argnames(self, args: Iterable[str]) -> None:
        extra = set(args) - set(self.__class__._fields) - {'_in_init_'}
        if extra:
            fmt = '{} {} invalid argument{} for struct {}.{}'
            plural = len(extra) > 1
            msg = fmt.format(
                ', '.join(extra), 'are' if plural else 'is an', 's' if plural
                else '', self.__class__.__module__, self.__class__.__name__)
            raise TypeError(msg)

    def _getdefault(
        self,
        field_name: str,
        field: Field[T],
    ) -> T:
        ftype = field.type
        if field.default == ftype:
            value = field.default()  # type: ignore
        elif field.default is NoDefault:
            raise TypeError(
                '%s.%s.%s is required' % (
                    self.__class__.__module__, self.__class__.__name__,
                    field_name))
        else:
            value = field.default

        return value  # type: ignore

    def get_field_value(self, field_name: str) -> Any:
        try:
            return self.__dict__[field_name]
        except KeyError as e:
            field = self.__class__.get_field(field_name)
            if field is None:
                raise TypeError(
                    f'{field_name} is not a valid field in this struct')
            try:
                return self._getdefault(field_name, field)
            except TypeError:
                raise e


class RTStruct(Struct):
    """A variant of Struct with runtime type validation"""

    __slots__ = ('_in_init_',)

    def __init__(self, **kwargs: Any) -> None:
        """
        :raises: TypeError if invalid field value was provided or a value was
                 not provided for a field without a default value.
        """
        self._check_init_argnames(kwargs)

        self._in_init_ = True
        try:
            self._init_fields(kwargs)
        finally:
            self._in_init_ = False

    def __setstate__(self, state: Mapping[str, Any]) -> None:
        self._in_init_ = True
        try:
            super().__setstate__(state)
        finally:
            self._in_init_ = False

    def __setattr__(self, name: str, value: Any) -> None:
        field = type(self)._fields.get(name)
        if field is not None:
            value = self._check_field_type(field, name, value)
            if field.frozen and not self._in_init_:
                raise ValueError(f'cannot assign to frozen field {name!r}')
        super().__setattr__(name, value)

    def _check_field_type(self, field: Field[T], name: str, value: Any) -> T:
        if (field.type and value is not None and
                not isinstance(value, field.type)):
            if field.coerce:
                ftype = field.type

                if issubclass(ftype, (checked.AbstractCheckedList,
                                      checked.AbstractCheckedSet)):
                    val_list = []
                    for v in value:
                        if v is not None and not isinstance(v, ftype.type):
                            v = ftype.type(v)
                        val_list.append(v)
                    value = val_list
                elif issubclass(ftype, checked.CheckedDict):
                    val_dict = {}
                    for k, v in value.items():
                        if k is not None and not isinstance(k, ftype.keytype):
                            k = ftype.keytype(k)
                        if (v is not None and
                                not isinstance(v, ftype.valuetype)):
                            v = ftype.valuetype(v)
                        val_dict[k] = v

                    value = val_dict

                try:
                    return ftype(value)  # type: ignore
                except Exception as ex:
                    raise TypeError(
                        'cannot coerce {!r} value {!r} '
                        'to {}'.format(name, value, ftype)) from ex

            raise TypeError(
                '{}.{}.{}: expected {} but got {!r}'.format(
                    self.__class__.__module__, self.__class__.__name__, name,
                    field.type.__name__, value))

        return value  # type: ignore


class MixedStructMeta(StructMeta):
    def __new__(
        mcls,
        name: str,
        bases: Tuple[type, ...],
        clsdict: Dict[str, Any],
        *,
        use_slots: bool = False,
        **kwargs: Any,
    ) -> MixedStructMeta:
        return super().__new__(
            mcls,
            name,
            bases,
            clsdict,
            use_slots=use_slots,
            **kwargs,
        )


class MixedStruct(Struct, metaclass=MixedStructMeta):
    def _check_init_argnames(self, args: Iterable[Any]) -> None:
        pass


class MixedRTStruct(RTStruct, metaclass=MixedStructMeta):
    def _check_init_argnames(self, args: Iterable[Any]) -> None:
        pass
