#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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
    ClassVar,
    Generic,
    Mapping,
    Optional,
    Self,
    TypeVar,
    TYPE_CHECKING,
)

import dataclasses
import datetime
import decimal
import enum
import functools
import re
import struct
import uuid

import immutables

from edb import errors
from edb.common import parametric
from edb.common import uuidgen

from edb.schema import name as s_name
from edb.schema import objects as s_obj

if TYPE_CHECKING:
    from edb.edgeql import qltypes

MISSING: Any = object()


@dataclasses.dataclass(frozen=True)
class CompositeTypeSpecField:
    name: str
    type: type | CompositeTypeSpec
    _: dataclasses.KW_ONLY
    unique: bool = False
    default: Any = MISSING
    secret: bool = False
    protected: bool = False


@dataclasses.dataclass(frozen=True, kw_only=True)
class CompositeTypeSpec:
    name: str
    fields: immutables.Map[str, CompositeTypeSpecField]
    parent: Optional[CompositeTypeSpec] = None
    children: list[CompositeTypeSpec] = dataclasses.field(
        default_factory=list, hash=False, compare=False
    )
    has_secret: bool = False

    def __post_init__(self) -> None:
        has_secret = any(
            field.secret
            or (
                isinstance(field, CompositeTypeSpec)
                # We look at children of pointer targets, and not
                # children of the object itself, on the idea that for
                # config objects, omitting individual top level
                # objects with secrets should be fine.
                and (
                    field.has_secret
                    or any(child.has_secret for child in field.children)
                )
            )
            for field in self.fields.values()
        )
        object.__setattr__(self, 'has_secret', has_secret)

    @property
    def __name__(self) -> str:
        return self.name

    def get_field_unique_site(self, name: str) -> Optional[CompositeTypeSpec]:
        typ: Optional[CompositeTypeSpec] = self
        site: Optional[CompositeTypeSpec] = None
        while typ:
            if name in typ.fields and typ.fields[name].unique:
                site = typ
            typ = typ.parent
        return site


class CompositeType:
    _tspec: CompositeTypeSpec

    def to_json_value(self, redacted: bool = False) -> dict[str, Any]:
        raise NotImplementedError


class ScalarType:
    def __init__(self, val: str, /) -> None:
        raise NotImplementedError

    def to_backend_str(self) -> str:
        raise NotImplementedError

    @classmethod
    def to_backend_expr(cls, expr: str) -> str:
        raise NotImplementedError("{cls}.to_backend_expr()")

    @classmethod
    def to_frontend_expr(cls, expr: str) -> Optional[str]:
        raise NotImplementedError("{cls}.to_frontend_expr()")

    def to_json(self) -> str:
        raise NotImplementedError

    def encode(self) -> bytes:
        raise NotImplementedError

    @classmethod
    def decode(cls, data: bytes) -> ScalarType:
        raise NotImplementedError


@functools.total_ordering
class Duration(ScalarType):

    _pg_simple_parser = re.compile(r'''
        ^
        \s*
        (
            (?P<sign>(\+|\-)?)
        )
        (
            (?P<hours>\d+)
        )
        :
        (
            (?P<minutes>\d+)?
            (
                :(?P<seconds>\d+)
                (
                    \.(?P<milliseconds>\d{0,3})
                    (?P<microseconds>\d{0,3})
                    (?P<submicro>\d*)
                )?
            )?
        )?
        \s*
        $
    ''', re.X)

    _pg_parser = re.compile(r'''
        (
            (
                \s*
                (?P<hours>(\+|\-)?\d+)
                \s*
                (h|hr|hrs|hour|hours)
                \s*
            )
            |
            (
                \s*
                (?P<minutes>(\+|\-)?\d+)
                \s*
                (m|min|mins|minute|minutes)
                \s*
            )
            |
            (
                \s*
                (?P<milliseconds>(\+|\-)?\d+)
                \s*
                (ms | (millisecon(s|d|ds)?))  # '12 millisecon' is valid
                \s*
            )
            |
            (
                \s*
                (?P<microseconds>(\+|\-)?\d+)
                \s*
                (us | (microsecond(s)?))
                \s*
            )
            |
            (
                \s*
                (?P<seconds>(\+|\-)?\d+)
                (
                    (\s* $) | (\s* (s|sec|secs|second|seconds))
                )
                \s*
            )
        )(?=$ | \d | \s)
        |
        (
            \s*
            (?P<error>.+)
        )
    ''', re.X | re.I)

    _iso_parser = re.compile(r'''
        ^
        PT
        (
            (?P<hours>(\+|\-)?\d+) H
        )?
        (
            (?P<minutes>(\+|\-)?\d+) M
        )?
        (
            (
                (?P<secsign>\+|\-)?
                (?P<seconds>\d+)
                (
                    \.
                    (?P<microseconds>\d+)
                )?
            ) S
        )?
        $
    ''', re.X)

    _codec = struct.Struct('!QLL')

    _value: int  # microseconds

    def __init__(
        self, pg_text: str = '', /, *, microseconds: Optional[int] = None
    ) -> None:
        if pg_text == '' and microseconds is not None:
            self._value = microseconds
        else:
            self._value = self._us_from_pg_text(pg_text)

    def _us_from_pg_text(self, input: str, /) -> int:
        try:
            seconds = int(input)
        except ValueError:
            pass
        else:
            return seconds * 1000 * 1000

        m = self._pg_simple_parser.match(input)
        if m is not None:
            value = 0
            parsed = m.groupdict()
            if parsed['hours']:
                hours = int(parsed['hours'])
                if 0 <= hours <= 2147483647:
                    value += hours * 3600_000_000
                else:
                    raise errors.NumericOutOfRangeError(
                        'interval field value out of range')
            if parsed['minutes']:
                mins = int(parsed['minutes'])
                if 0 <= mins <= 59:
                    value += mins * 60_000_000
                else:
                    raise errors.NumericOutOfRangeError(
                        'interval field value out of range')
            if parsed['seconds']:
                secs = int(parsed['seconds'])
                if 0 <= secs <= 59:
                    value += secs * 1_000_000
                else:
                    raise errors.NumericOutOfRangeError(
                        'interval field value out of range')
            if parsed['milliseconds']:
                value += int(parsed['milliseconds'].ljust(3, '0')) * 1_000
            if parsed['microseconds']:
                value += int(parsed['microseconds'].ljust(3, '0'))
            if parsed['submicro'] and int(parsed['submicro'][:1]) >= 5:
                value += 1
            if parsed['sign'] == '-':
                value = -value

            return value

        if (parsed_iso := self._parse_iso8601(input)) is not None:
            return parsed_iso

        value = 0
        seen: set[str] = set()
        for m in self._pg_parser.finditer(input):
            filtered = {
                k: v for k, v in m.groupdict().items()
                if v is not None
            }
            if len(filtered) != 1:
                raise errors.InvalidValueError(
                    'invalid input syntax for type std::duration')

            kind, val = next(iter(filtered.items()))
            if kind == 'error':
                raise errors.InvalidValueError(
                    f'invalid input syntax for type std::duration: '
                    f'unable to parse {val!r}')
            if kind in seen:
                raise errors.InvalidValueError(
                    f'invalid input syntax for type std::duration: '
                    f'the {kind!r} component has been specified '
                    f'more than once')
            seen.add(kind)

            intval = int(val)
            if kind == 'hours':
                value += intval * 3600_000_000
            elif kind == 'minutes':
                value += intval * 60_000_000
            elif kind == 'seconds':
                value += intval * 1_000_000
            elif kind == 'milliseconds':
                value += intval * 1_000
            elif kind == 'microseconds':
                value += intval

        return value

    @classmethod
    def _parse_iso8601(cls, input: str, /) -> Optional[int]:
        m = cls._iso_parser.match(input)
        if not m:
            return None

        value = 0
        if m['hours']:
            value += int(m['hours']) * 3600_000_000
        if m['minutes']:
            value += int(m['minutes']) * 60_000_000

        secsign = -1 if m['secsign'] == '-' else +1
        if m['seconds']:
            value += int(m['seconds']) * 1_000_000 * secsign
        if m['microseconds']:
            ms = m['microseconds'][:6]
            ms = ms.ljust(6, '0')
            value += int(ms) * secsign

        return value

    @classmethod
    def from_iso8601(cls, input: str, /) -> Duration:
        val = cls._parse_iso8601(input)
        if val is None:
            raise errors.InvalidValueError(
                f'invalid input syntax for type std::duration: '
                f'cannot parse {input!r} as ISO 8601')
        return cls(microseconds=val)

    @classmethod
    def from_microseconds(cls, input: int, /) -> Duration:
        return cls(microseconds=input)

    def to_microseconds(self) -> int:
        return self._value

    def __lt__(self, other: Duration) -> bool:
        return self._value < other._value

    def to_iso8601(self) -> str:
        neg = '-' if self._value < 0 else ''
        seconds, usecs = divmod(abs(self._value), 1_000_000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        ret = ['PT']
        if hours:
            ret.append(f'{neg}{hours}H')
        if minutes:
            ret.append(f'{neg}{minutes}M')
        if seconds or usecs:
            if usecs:
                ret.append(f"{neg}{seconds}.")
                ret.append(f"{str(usecs).rjust(6, '0')}"[:6].rstrip('0'))
            else:
                ret.append(f'{neg}{seconds}')
            ret.append('S')
        if ret == ['PT']:
            ret.append('0S')
        return ''.join(ret)

    def to_timedelta(self) -> datetime.timedelta:
        return datetime.timedelta(microseconds=self.to_microseconds())

    def to_backend_str(self) -> str:
        return f'{self.to_microseconds()}us'

    @classmethod
    def to_backend_expr(cls, expr: str) -> str:
        return f"edgedb_VER._interval_to_ms(({expr})::interval)::text || 'ms'"

    @classmethod
    def to_frontend_expr(cls, expr: str) -> Optional[str]:
        return None

    def to_json(self) -> str:
        return self.to_iso8601()

    def __repr__(self) -> str:
        return f'<statypes.Duration {self.to_iso8601()!r}>'

    def encode(self) -> bytes:
        return self._codec.pack(self._value, 0, 0)

    @classmethod
    def decode(cls, data: bytes) -> Duration:
        return cls(microseconds=cls._codec.unpack(data)[0])

    def __hash__(self) -> int:
        return hash(self._value)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Duration):
            return self._value == other._value
        else:
            return False


@functools.total_ordering
class ConfigMemory(ScalarType):

    PiB = 1024 * 1024 * 1024 * 1024 * 1024
    TiB = 1024 * 1024 * 1024 * 1024
    GiB = 1024 * 1024 * 1024
    MiB = 1024 * 1024
    KiB = 1024

    _parser = re.compile(r'''
        ^
        (?P<num>\d+)
        (?P<unit>B|KiB|MiB|GiB|TiB|PiB)
        $
    ''', re.X)

    _value: int

    def __init__(
        self,
        val: str | int,
        /,
    ) -> None:
        if isinstance(val, int):
            self._value = val
        elif isinstance(val, str):
            text = val
            if text == '0':
                self._value = 0
                return

            m = self._parser.match(text)
            if m is None:
                raise errors.InvalidValueError(
                    f'unable to parse memory size: {text!r}')

            num = int(m.group('num'))
            unit = m.group('unit')

            if unit == 'B':
                self._value = num
            elif unit == 'KiB':
                self._value = num * self.KiB
            elif unit == 'MiB':
                self._value = num * self.MiB
            elif unit == 'GiB':
                self._value = num * self.GiB
            elif unit == 'TiB':
                self._value = num * self.TiB
            elif unit == 'PiB':
                self._value = num * self.PiB
            else:
                raise AssertionError('unexpected unit')
        else:
            raise ValueError(
                f"invalid ConfigMemory value: {type(val)}, expected int | str")

    def __lt__(self, other: ConfigMemory) -> bool:
        return self._value < other._value

    def to_nbytes(self) -> int:
        return self._value

    def to_str(self) -> str:
        if self._value >= self.PiB and self._value % self.PiB == 0:
            return f'{self._value // self.PiB}PiB'

        if self._value >= self.TiB and self._value % self.TiB == 0:
            return f'{self._value // self.TiB}TiB'

        if self._value >= self.GiB and self._value % self.GiB == 0:
            return f'{self._value // self.GiB}GiB'

        if self._value >= self.MiB and self._value % self.MiB == 0:
            return f'{self._value // self.MiB}MiB'

        if self._value >= self.KiB and self._value % self.KiB == 0:
            return f'{self._value // self.KiB}KiB'

        return f'{self._value}B'

    def to_backend_str(self) -> str:
        if self._value >= self.TiB and self._value % self.TiB == 0:
            return f'{self._value // self.TiB}TB'

        if self._value >= self.GiB and self._value % self.GiB == 0:
            return f'{self._value // self.GiB}GB'

        if self._value >= self.MiB and self._value % self.MiB == 0:
            return f'{self._value // self.MiB}MB'

        if self._value >= self.KiB and self._value % self.KiB == 0:
            return f'{self._value // self.KiB}kB'

        return f'{self._value}B'

    @classmethod
    def to_backend_expr(cls, expr: str) -> str:
        return f"edgedb_VER.cfg_memory_to_str({expr})"

    @classmethod
    def to_frontend_expr(cls, expr: str) -> Optional[str]:
        return f"(edgedb_VER.str_to_cfg_memory({expr})::text || 'B')"

    def to_json(self) -> str:
        return self.to_str()

    def __repr__(self) -> str:
        return f'<statypes.ConfigMemory {self.to_str()!r}>'

    def __hash__(self) -> int:
        return hash(self._value)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ConfigMemory):
            return self._value == other._value
        else:
            return False


typemap = {
    'std::str': str,
    'std::anyint': int,
    'std::anyfloat': float,
    'std::decimal': decimal.Decimal,
    'std::bigint': decimal.Decimal,
    'std::bool': bool,
    'std::json': str,
    'std::uuid': uuidgen.UUID,
    'std::duration': Duration,
    'cfg::memory': ConfigMemory,
}


def maybe_get_python_type_for_scalar_type_name(name: str) -> Optional[type]:
    return typemap.get(name)


E = TypeVar("E", bound=enum.StrEnum)


class EnumScalarType(
    ScalarType,
    parametric.SingleParametricType[E],
    Generic[E],
):
    """Configuration value represented by a custom string enum type that
    supports arbitrary value mapping to backend (Postgres) configuration
    values, e.g mapping "Enabled"/"Disabled" enum to a bool value, etc.

    We use SingleParametricType to obtain runtime access to the Generic
    type arg to avoid having to copy-paste the constructors.
    """

    _val: E
    _eql_type: ClassVar[Optional[s_name.QualName]]

    def __init_subclass__(
        cls,
        *,
        edgeql_type: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        global typemap
        super().__init_subclass__(**kwargs)
        if edgeql_type is not None:
            if edgeql_type in typemap:
                raise TypeError(
                    f"{edgeql_type} is already a registered EnumScalarType")
            typemap[edgeql_type] = cls
            cls._eql_type = s_name.QualName.from_string(edgeql_type)

    def __init__(
        self,
        val: E | str,
    ) -> None:
        if isinstance(val, self.type):
            self._val = val
        elif isinstance(val, str):
            try:
                self._val = self.type(val)
            except ValueError:
                raise errors.InvalidValueError(
                    f'unexpected backend value for '
                    f'{self.__class__.__name__}: {val!r}'
                ) from None

    def to_str(self) -> str:
        return str(self._val)

    def to_json(self) -> str:
        return self._val

    def encode(self) -> bytes:
        return self._val.encode("utf8")

    @classmethod
    def get_translation_map(cls) -> Mapping[E, str]:
        raise NotImplementedError

    @classmethod
    def decode(cls, data: bytes) -> Self:
        return cls(val=cls.type(data.decode("utf8")))

    def __repr__(self) -> str:
        return f"<statypes.{self.__class__.__name__} '{self._val}'>"

    def __hash__(self) -> int:
        return hash(self._val)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, type(self)):
            return self._val == other._val
        else:
            return NotImplemented

    def __reduce__(self) -> tuple[
        Callable[..., EnumScalarType[Any]],
        tuple[
            Optional[tuple[type, ...] | type],
            E,
        ],
    ]:
        assert type(self).is_fully_resolved(), \
            f'{type(self)} parameters are not resolved'

        cls: type[EnumScalarType[E]] = self.__class__
        types: Optional[tuple[type, ...]] = self.orig_args
        if types is None or not cls.is_anon_parametrized():
            typeargs = None
        else:
            typeargs = types[0] if len(types) == 1 else types
        return (cls.__restore__, (typeargs, self._val))

    @classmethod
    def __restore__(
        cls,
        typeargs: Optional[tuple[type, ...] | type],
        val: E,
    ) -> Self:
        if typeargs is None or cls.is_anon_parametrized():
            obj = cls(val)
        else:
            obj = cls[typeargs](val)  # type: ignore[index]

        return obj

    @classmethod
    def get_edgeql_typeid(cls) -> uuid.UUID:
        return s_obj.get_known_type_id('std::str')

    @classmethod
    def get_edgeql_type(cls) -> s_name.QualName:
        """Return fully-qualified name of the scalar type for this setting."""
        assert cls._eql_type is not None
        return cls._eql_type

    def to_backend_str(self) -> str:
        """Convert static frontend config value to backend config value."""
        return self.get_translation_map()[self._val]

    @classmethod
    def to_backend_expr(cls, expr: str) -> str:
        """Convert dynamic backend config value to frontend config value."""
        cases_list = []
        for fe_val, be_val in cls.get_translation_map().items():
            cases_list.append(f"WHEN lower('{fe_val}') THEN '{be_val}'")
        cases = "\n".join(cases_list)
        errmsg = f"unexpected frontend value for {cls.__name__}: %s"
        err = f"edgedb_VER.raise(NULL::text, msg => format('{errmsg}', v))"
        return (
            f"(SELECT CASE v\n{cases}\nELSE\n{err}\nEND "
            f"FROM lower(({expr})) AS f(v))"
        )

    @classmethod
    def to_frontend_expr(cls, expr: str) -> Optional[str]:
        """Convert dynamic frontend config value to backend config value."""
        cases_list = []
        for fe_val, be_val in cls.get_translation_map().items():
            cases_list.append(f"WHEN lower('{be_val}') THEN '{fe_val}'")
        cases = "\n".join(cases_list)
        errmsg = f"unexpected backend value for {cls.__name__}: %s"
        err = f"edgedb_VER.raise(NULL::text, msg => format('{errmsg}', v))"
        return (
            f"(SELECT CASE v\n{cases}\nELSE\n{err}\nEND "
            f"FROM lower(({expr})) AS f(v))"
        )


class EnabledDisabledEnum(enum.StrEnum):
    Enabled = "Enabled"
    Disabled = "Disabled"


class EnabledDisabledType(
    EnumScalarType[EnabledDisabledEnum],
    edgeql_type="cfg::TestEnabledDisabledEnum",
):
    @classmethod
    def get_translation_map(cls) -> Mapping[EnabledDisabledEnum, str]:
        return {
            EnabledDisabledEnum.Enabled: "true",
            EnabledDisabledEnum.Disabled: "false",
        }


class TransactionAccessModeEnum(enum.StrEnum):
    ReadOnly = "ReadOnly"
    ReadWrite = "ReadWrite"


class TransactionAccessMode(
    EnumScalarType[TransactionAccessModeEnum],
    edgeql_type="sys::TransactionAccessMode",
):
    @classmethod
    def get_translation_map(cls) -> Mapping[TransactionAccessModeEnum, str]:
        return {
            TransactionAccessModeEnum.ReadOnly: "true",
            TransactionAccessModeEnum.ReadWrite: "false",
        }

    def to_qltypes(self) -> qltypes.TransactionAccessMode:
        from edb.edgeql import qltypes
        match self._val:
            case TransactionAccessModeEnum.ReadOnly:
                return qltypes.TransactionAccessMode.READ_ONLY
            case TransactionAccessModeEnum.ReadWrite:
                return qltypes.TransactionAccessMode.READ_WRITE
            case _:
                raise AssertionError(f"unexpected value: {self._val!r}")


class TransactionDeferrabilityEnum(enum.StrEnum):
    Deferrable = "Deferrable"
    NotDeferrable = "NotDeferrable"


class TransactionDeferrability(
    EnumScalarType[TransactionDeferrabilityEnum],
    edgeql_type="sys::TransactionDeferrability",
):
    @classmethod
    def get_translation_map(cls) -> Mapping[TransactionDeferrabilityEnum, str]:
        return {
            TransactionDeferrabilityEnum.Deferrable: "true",
            TransactionDeferrabilityEnum.NotDeferrable: "false",
        }


class TransactionIsolationEnum(enum.StrEnum):
    Serializable = "Serializable"
    RepeatableRead = "RepeatableRead"


class TransactionIsolation(
    EnumScalarType[TransactionIsolationEnum],
    edgeql_type="sys::TransactionIsolation",
):
    @classmethod
    def get_translation_map(cls) -> Mapping[TransactionIsolationEnum, str]:
        return {
            TransactionIsolationEnum.Serializable: "serializable",
            TransactionIsolationEnum.RepeatableRead: "repeatable read",
        }

    def to_qltypes(self) -> qltypes.TransactionIsolationLevel:
        from edb.edgeql import qltypes
        match self._val:
            case TransactionIsolationEnum.Serializable:
                return qltypes.TransactionIsolationLevel.SERIALIZABLE
            case TransactionIsolationEnum.RepeatableRead:
                return qltypes.TransactionIsolationLevel.REPEATABLE_READ
            case _:
                raise AssertionError(f"unexpected value: {self._val!r}")
