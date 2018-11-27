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


import typing

from edb.lang.common import context as pctx
from edb.lang.common import exceptions as ex


__all__ = (
    'EdgeDBError',
)


class EdgeDBErrorMeta(type):
    _error_map = {}

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)

        code = dct.get('_code')
        if code is not None:
            mcls._error_map[code] = cls

        return cls


class EdgeDBError(Exception, metaclass=EdgeDBErrorMeta):

    _code = None
    _attrs: typing.Mapping[str, str]

    def __init__(self, msg: str=None, *,
                 hint: str=None, details: str=None, context=None,
                 token=None):
        if type(self) is EdgeDBError:
            raise RuntimeError(
                'EdgeDBError is not supposed to be instantiated directly')

        self.token = token
        self._attrs = {}

        if isinstance(context, pctx.ParserContext):
            self._attrs['L'] = context.start.line
            self._attrs['C'] = context.start.column
            self.set_source_context(context)

        self.set_hint_and_details(hint, details)

        if details:
            msg = f'{msg}\n\nDETAILS: {details}'

        super().__init__(msg)

    @classmethod
    def get_code(cls):
        if cls._code is None:
            raise RuntimeError('EdgeDB exception code is not set')
        return cls._code

    def set_hint_and_details(self, hint, details=None):
        ex.replace_context(
            self, ex.DefaultExceptionContext(hint=hint, details=details))

        if hint is not None:
            self._attrs['H'] = hint
        if details is not None:
            self._attrs['D'] = details

    def set_source_context(self, context):
        ex.replace_context(self, context)

        if context.start is not None:
            self._attrs['P'] = str(context.start.pointer)
            self._attrs['p'] = str(context.end.pointer)

    @property
    def attrs(self):
        return self._attrs

    @property
    def line(self):
        return int(self._attrs.get('L', -1))

    @property
    def col(self):
        return int(self._attrs.get('C', -1))

    @property
    def position(self):
        return int(self._attrs.get('P'))

    @property
    def hint(self):
        return self._attrs.get('H')

    @property
    def details(self):
        return self._attrs.get('D')

    def as_text(self):
        buffer = ''

        for context in ex.iter_contexts(self):
            buffer += context.as_text()

        return buffer
