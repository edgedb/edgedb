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

import textwrap
import traceback


class MultiError(Exception):

    def __init__(self, msg, *args, errors=()):
        if errors:
            types = set(type(e).__name__ for e in errors)
            msg = f'{msg}; {len(errors)} sub errors: ({", ".join(types)})'
            for er in errors:
                exc_fmt = traceback.format_exception(er)
                msg += f'\n + {exc_fmt[0]}'
                er_tb = ''.join(exc_fmt[1:])
                er_tb = textwrap.indent(er_tb, ' | ', lambda _: True)
                msg += f'{er_tb}\n'
        super().__init__(msg, *args)
        self.__errors__ = tuple(errors)

    def get_error_types(self):
        return {type(e) for e in self.__errors__}

    def __reduce__(self):
        return (type(self), (self.args,), {'__errors__': self.__errors__})
