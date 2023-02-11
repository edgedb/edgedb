#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

from edb.common import context as pctx
from edb.common import exceptions as ex

import contextlib


__all__ = (
    'EdgeDBError', 'EdgeDBMessage', 'ensure_context',
)


class EdgeDBErrorMeta(type):
    _error_map: Dict[int, Type[EdgeDBError]] = {}
    _name_map: Dict[str, Type[EdgeDBError]] = {}

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)

        assert name not in mcls._name_map
        mcls._name_map[name] = cls

        code = dct.get('_code')
        if code is not None:
            mcls._error_map[code] = cls

        return cls

    def __init__(cls, name, bases, dct):
        if cls._code is None and cls.__module__ != __name__:
            # We don't want any EdgeDBError subclasses to not
            # have a code.
            raise RuntimeError(
                'direct subclassing of EdgeDBError is prohibited; '
                'subclass one of its subclasses in edb.errors')

    @classmethod
    def get_error_class_from_code(mcls, code: int) -> Type[EdgeDBError]:
        return mcls._error_map[code]

    @classmethod
    def get_error_class_from_name(mcls, name: str) -> Type[EdgeDBError]:
        return mcls._name_map[name]


class EdgeDBMessage(Warning):

    _code: Optional[int] = None

    @classmethod
    def get_code(cls):
        if cls._code is None:
            raise RuntimeError(
                f'EdgeDB message code is not set (type: {cls.__name__})')
        return cls._code


class EdgeDBError(Exception, metaclass=EdgeDBErrorMeta):

    _code: Optional[int] = None
    _attrs: Dict[int, str]
    _pgext_code: Optional[str] = None

    def __init__(
        self,
        msg: Optional[str] = None,
        *,
        hint: Optional[str] = None,
        details: Optional[str] = None,
        context=None,
        position: Optional[tuple[Optional[int], ...]] = None,
        filename: Optional[str] = None,
        token=None,
        pgext_code: Optional[str] = None,
    ):
        if type(self) is EdgeDBError:
            raise RuntimeError(
                'EdgeDBError is not supposed to be instantiated directly')

        self.token = token
        self._attrs = {}
        self._pgext_code = pgext_code

        if isinstance(context, pctx.ParserContext):
            self.set_source_context(context)
        elif position:
            self.set_position(*position)

        if filename is not None:
            self.set_filename(filename)

        self.set_hint_and_details(hint, details)

        super().__init__(msg)

    @classmethod
    def get_code(cls):
        if cls._code is None:
            raise RuntimeError(
                f'EdgeDB message code is not set (type: {cls.__name__})')
        return cls._code

    def set_filename(self, filename):
        self._attrs[FIELD_FILENAME] = filename

    def set_linecol(self, line, col):
        if line is not None:
            self._attrs[FIELD_LINE_START] = str(line)
        if col is not None:
            self._attrs[FIELD_COLUMN_START] = str(col)

    def set_hint_and_details(self, hint, details=None):
        ex.replace_context(
            self, ex.DefaultExceptionContext(hint=hint, details=details))

        if hint is not None:
            self._attrs[FIELD_HINT] = hint
        if details is not None:
            self._attrs[FIELD_DETAILS] = details

    def has_source_context(self):
        return FIELD_DETAILS in self._attrs

    def set_source_context(self, context):
        start = context.start_point
        end = context.end_point
        ex.replace_context(self, context)

        self._attrs[FIELD_POSITION_START] = str(start.offset)
        self._attrs[FIELD_POSITION_END] = str(end.offset)
        self._attrs[FIELD_CHARACTER_START] = str(start.char_offset)
        self._attrs[FIELD_CHARACTER_END] = str(end.char_offset)
        self._attrs[FIELD_LINE_START] = str(start.line)
        self._attrs[FIELD_COLUMN_START] = str(start.column)
        self._attrs[FIELD_UTF16_COLUMN_START] = str(start.utf16column)
        self._attrs[FIELD_LINE_END] = str(end.line)
        self._attrs[FIELD_COLUMN_END] = str(end.column)
        self._attrs[FIELD_UTF16_COLUMN_END] = str(end.utf16column)
        if context.name and context.name != '<string>':
            self._attrs[FIELD_FILENAME] = context.name

    def set_position(
        self,
        line: Optional[int] = None,
        column: Optional[int] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ):
        self.set_linecol(line, column)
        if start is not None:
            self._attrs[FIELD_POSITION_START] = str(start)
        end = end or start
        if end is not None:
            self._attrs[FIELD_POSITION_END] = str(end)

    @property
    def line(self):
        return int(self._attrs.get(FIELD_LINE_START, -1))

    @property
    def col(self):
        return int(self._attrs.get(FIELD_COLUMN_START, -1))

    @property
    def position(self):
        return int(self._attrs.get(FIELD_POSITION_START, -1))

    @property
    def hint(self):
        return self._attrs.get(FIELD_HINT)

    @property
    def details(self):
        return self._attrs.get(FIELD_DETAILS)

    @property
    def pgext_code(self):
        return self._pgext_code


@contextlib.contextmanager
def ensure_context(context: Any) -> Iterator[None]:
    try:
        yield
    except EdgeDBError as e:
        if not e.has_source_context():
            e.set_source_context(context)
        raise


FIELD_HINT = 0x_00_01
FIELD_DETAILS = 0x_00_02
FIELD_SERVER_TRACEBACK = 0x_01_01

# XXX: Subject to be changed/deprecated.
FIELD_POSITION_START = 0x_FF_F1
FIELD_POSITION_END = 0x_FF_F2
FIELD_LINE_START = 0x_FF_F3
FIELD_COLUMN_START = 0x_FF_F4
FIELD_UTF16_COLUMN_START = 0x_FF_F5
FIELD_LINE_END = 0x_FF_F6
FIELD_COLUMN_END = 0x_FF_F7
FIELD_UTF16_COLUMN_END = 0x_FF_F8
FIELD_CHARACTER_START = 0x_FF_F9
FIELD_CHARACTER_END = 0x_FF_FA
FIELD_FILENAME = 0x_FF_FB
