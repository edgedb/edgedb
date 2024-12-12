#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

from typing import Any, TypeVar, TYPE_CHECKING, TypeGuard

import enum
import platform

from edb import errors
from edb.common import typeutils
from edb.common import typing_inspect
from edb.schema import objects as s_obj

from edb.ir import statypes


if TYPE_CHECKING:
    from . import spec


T_type = TypeVar('T_type', bound=type)


def _issubclass(
    typ: type | statypes.CompositeTypeSpec, parent: T_type
) -> TypeGuard[T_type]:
    return isinstance(typ, type) and issubclass(typ, parent)


class ConfigTypeSpec(statypes.CompositeTypeSpec):
    def __call__(self, **kwargs) -> CompositeConfigType:
        return CompositeConfigType(self, **kwargs)

    def from_pyvalue(
        self, v, *, spec, allow_missing=False
    ) -> CompositeConfigType:
        return CompositeConfigType.from_pyvalue(
            v, tspec=self, spec=spec, allow_missing=allow_missing
        )


class ConfigType:

    @classmethod
    def from_pyvalue(cls, v, *, tspec, spec, allow_missing=False):
        """Subclasses override this to allow creation from Python scalars."""
        raise NotImplementedError

    @classmethod
    def from_json_value(cls, v, *, tspec, spec):
        raise NotImplementedError

    def to_json_value(self):
        raise NotImplementedError

    @classmethod
    def get_edgeql_typeid(cls):
        raise NotImplementedError


class CompositeConfigType(ConfigType, statypes.CompositeType):
    _compare_keys: tuple[str, ...]

    def __init__(self, tspec: statypes.CompositeTypeSpec, **kwargs) -> None:
        object.__setattr__(self, '_tspec', tspec)
        for f in tspec.fields.values():
            if f.name in kwargs:
                object.__setattr__(self, f.name, kwargs[f.name])
            elif f.default is not statypes.MISSING:
                object.__setattr__(self, f.name, f.default)
        object.__setattr__(self, '_compare_keys', tuple(
            f.name for f in tspec.fields.values() if f.unique
        ))

    def __setattr__(self, k, v) -> None:
        raise TypeError(f"{self._tspec.name} is immutable")

    def __eq__(self, rhs: Any) -> bool:
        if (
            not isinstance(rhs, CompositeConfigType)
            or self._tspec != rhs._tspec
        ):
            return NotImplemented
        compare_keys = self._compare_keys
        return (
            tuple(getattr(self, k) for k in compare_keys)
            == tuple(getattr(rhs, k) for k in compare_keys)
        )

    def __hash__(self) -> int:
        return hash(tuple(getattr(self, k) for k in self._compare_keys))

    def __repr__(self) -> str:
        body = ', '.join(
            f'{f.name}={getattr(self, f.name)!r}'
            for f in self._tspec.fields.values()
            if hasattr(self, f.name)
        )
        return f'{self._tspec.name}({body})'

    @classmethod
    def from_pyvalue(
        cls,
        data,
        *,
        tspec: statypes.CompositeTypeSpec,
        spec: spec.Spec,
        allow_missing=False,
    ) -> CompositeConfigType:
        if allow_missing and data is None:
            return None  # type: ignore

        if not isinstance(data, dict):
            raise cls._err(tspec, f'expected a dict value, got {type(data)!r}')

        data = dict(data)
        tname = data.pop('_tname', None)
        if tname is not None:
            tspec = spec.get_type_by_name(tname)
        assert tspec

        fields = tspec.fields

        items = {}
        inv_keys = []
        for fieldname, value in data.items():
            field = fields.get(fieldname)
            if field is None:
                if value is None:
                    # This may happen when data is produced by
                    # a polymorphic config query.
                    pass
                else:
                    inv_keys.append(fieldname)

                continue

            f_type = field.type

            if value is None:
                # Config queries return empty pointer values as None.
                continue

            if typing_inspect.is_generic_type(f_type):
                container = typing_inspect.get_origin(f_type)
                if container not in (frozenset, list):
                    raise RuntimeError(
                        f'invalid type annotation on '
                        f'{tspec.name}.{fieldname}: '
                        f'{f_type!r} is not supported')

                eltype = typing_inspect.get_args(f_type, evaluate=True)[0]
                if isinstance(value, eltype):
                    value = container((value,))
                elif (typeutils.is_container(value)
                        and all(isinstance(v, eltype) for v in value)):
                    value = container(value)
                else:
                    raise cls._err(
                        tspec,
                        f'invalid {fieldname!r} field value: expecting '
                        f'{eltype.__name__} or a list thereof, but got '
                        f'{type(value).__name__}'
                    )

            elif (isinstance(f_type, ConfigTypeSpec)
                    and isinstance(value, dict)):

                tname = value.get('_tname', None)
                if tname is not None:
                    actual_f_type = spec.get_type_by_name(tname)
                else:
                    actual_f_type = f_type
                    value['_tname'] = f_type.name

                value = cls.from_pyvalue(value, tspec=actual_f_type, spec=spec)

            elif (
                _issubclass(f_type, statypes.Duration)
                and isinstance(value, str)
            ):
                value = statypes.Duration.from_iso8601(value)
            elif (
                _issubclass(f_type, statypes.ConfigMemory)
                and isinstance(value, str | int)
            ):
                value = statypes.ConfigMemory(value)

            elif not isinstance(f_type, type) or not isinstance(value, f_type):
                raise cls._err(
                    tspec,
                    f'invalid {fieldname!r} field value: expecting '
                    f'{f_type.__name__}, but got {type(value).__name__}'
                )

            items[fieldname] = value

        if inv_keys:
            sinv_keys = ', '.join(repr(r) for r in inv_keys)
            raise cls._err(tspec, f'unknown fields: {sinv_keys}')

        for fieldname, field in fields.items():
            if fieldname not in items and field.default is statypes.MISSING:
                if allow_missing:
                    items[fieldname] = None
                else:
                    raise cls._err(
                        tspec, f'missing required field: {fieldname!r}'
                    )

        try:
            return cls(tspec, **items)
        except (TypeError, ValueError) as ex:
            raise cls._err(tspec, str(ex))

    @classmethod
    def get_edgeql_typeid(cls):
        return s_obj.get_known_type_id('std::json')

    @classmethod
    def from_json_value(cls, s, *, tspec: statypes.CompositeTypeSpec, spec):
        return cls.from_pyvalue(s, tspec=tspec, spec=spec)

    def to_json_value(self, redacted: bool = False):
        dct = {}
        dct['_tname'] = self._tspec.name

        for f in self._tspec.fields.values():
            f_type = f.type
            value = getattr(self, f.name)
            if redacted and f.secret and value is not None:
                value = {'redacted': True}
            elif (isinstance(f_type, statypes.CompositeTypeSpec)
                    and value is not None):
                value = value.to_json_value(redacted=redacted)
            elif typing_inspect.is_generic_type(f_type):
                value = list(value) if value is not None else []
            elif (_issubclass(f_type, statypes.ScalarType) and
                  value is not None):
                value = value.to_json()

            dct[f.name] = value

        return dct

    @classmethod
    def _err(
        cls, tspec: statypes.CompositeTypeSpec, msg: str
    ) -> errors.ConfigurationError:
        return errors.ConfigurationError(
            f'invalid {tspec.name.lower()!r} value: {msg}')


class QueryCacheMode(enum.StrEnum):
    InMemory = "InMemory"
    RegInline = "RegInline"
    PgFunc = "PgFunc"
    Default = "Default"

    @classmethod
    def effective(cls, value: str | None) -> QueryCacheMode:
        if value is None:
            rv = cls.Default
        else:
            rv = cls(value)
        if rv is QueryCacheMode.Default:
            # Persistent cache disabled for now by default on arm64 linux
            # because of observed problems in CI test runs.
            if platform.system() == 'Linux' and platform.machine() == 'arm64':
                rv = QueryCacheMode.InMemory
            else:
                rv = QueryCacheMode.PgFunc
        return rv
