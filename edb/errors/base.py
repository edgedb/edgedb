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

from typing import *  # NoQA

from edb.common import context as pctx
from edb.common import exceptions as ex


__all__ = (
    'EdgeDBError', 'EdgeDBMessage',
)


class EdgeDBErrorMeta(type):
    _error_map = {}

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)

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
    def get_error_class_from_code(mcls, code):
        return mcls._error_map[code]


class EdgeDBMessage(Warning):

    _code = None

    @classmethod
    def get_code(cls):
        if cls._code is None:
            raise RuntimeError(
                f'EdgeDB message code is not set (type: {cls.__name__})')
        return cls._code


class EdgeDBError(Exception, metaclass=EdgeDBErrorMeta):

    _code = None
    _attrs: Mapping[str, str]

    def __init__(self, msg: str=None, *,
                 hint: str=None, details: str=None, context=None,
                 token=None):
        if type(self) is EdgeDBError:
            raise RuntimeError(
                'EdgeDBError is not supposed to be instantiated directly')

        self.token = token
        self._attrs = {}

        if isinstance(context, pctx.ParserContext):
            self.set_source_context(context)

        self.set_hint_and_details(hint, details)

        if details:
            msg = f'{msg}\n\nDETAILS: {details}'

        super().__init__(msg)

    @classmethod
    def get_code(cls):
        if cls._code is None:
            raise RuntimeError(
                f'EdgeDB message code is not set (type: {cls.__name__})')
        return cls._code

    def set_linecol(self, line, col):
        self._attrs[FIELD_LINE] = str(line)
        self._attrs[FIELD_COLUMN] = str(col)

    def set_hint_and_details(self, hint, details=None):
        ex.replace_context(
            self, ex.DefaultExceptionContext(hint=hint, details=details))

        if hint is not None:
            self._attrs[FIELD_HINT] = hint
        if details is not None:
            self._attrs[FIELD_DETAILS] = details

    def set_source_context(self, context):
        self.set_linecol(context.start.line, context.start.column)
        ex.replace_context(self, context)

        if context.start is not None:
            self._attrs[FIELD_POSITION_START] = str(context.start.pointer)
            self._attrs[FIELD_POSITION_END] = str(context.end.pointer)

    @property
    def line(self):
        return int(self._attrs.get(FIELD_LINE, -1))

    @property
    def col(self):
        return int(self._attrs.get(FIELD_COLUMN, -1))

    @property
    def position(self):
        return int(self._attrs.get(FIELD_POSITION_START))

    @property
    def hint(self):
        return self._attrs.get(FIELD_HINT)

    @property
    def details(self):
        return self._attrs.get(FIELD_DETAILS)


FIELD_HINT = 0x_00_01
FIELD_DETAILS = 0x_00_02
FIELD_SERVER_TRACEBACK = 0x_01_01

# XXX: Subject to be changed/deprecated.
FIELD_POSITION_START = 0x_FF_F1
FIELD_POSITION_END = 0x_FF_F2
FIELD_LINE = 0x_FF_F3
FIELD_COLUMN = 0x_FF_F4
