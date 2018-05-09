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


"""IR Compiler Exceptions."""


from edgedb.lang.common import markup
from edgedb.server.pgsql import exceptions as pg_errors


class IRCompilerError(pg_errors.BackendError):
    pass


class IRCompilerInternalError(IRCompilerError):
    pass


class IRCompilerErrorContext(markup.MarkupExceptionContext):
    title = 'EdgeDB PgSQL IR Compiler Error Context'

    def __init__(self, tree):
        super().__init__()
        self.tree = tree

    @classmethod
    def as_markup(cls, self, *, ctx):
        tree = markup.serialize(self.tree, ctx=ctx)
        return markup.elements.lang.ExceptionContext(
            title=self.title, body=[tree])
