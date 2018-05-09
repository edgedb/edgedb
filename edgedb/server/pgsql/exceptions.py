#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2013-present MagicStack Inc. and the EdgeDB authors.
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


from edgedb.lang.common import exceptions as edgedb_error

from edgedb.lang.common.exceptions import replace_context
from edgedb.lang.common import markup
from edgedb.lang.common import parsing


class BackendError(edgedb_error.EdgeDBBackendError):
    pass


class QueryExceptionContext(markup.MarkupExceptionContext):
    title = 'SQL Query Context'

    def __init__(self, query, position=None):
        super().__init__()
        self.query = query
        self.position = position

    @classmethod
    def as_markup(cls, self, *, ctx):
        me = markup.elements

        if self.position:
            lineno, colno = parsing.line_col_from_char_offset(
                self.query, self.position)
        else:
            lineno = colno = None

        tbp = me.lang.TracebackPoint(
            name='SQL query', filename='<string>', lineno=lineno, colno=colno)
        tbp.load_source(window=5, lines=self.query.split('\n'))

        src = markup.serializer.serialize_code(self.query, lexer='sql')

        tb_section = me.doc.Section(title='Error Point', body=[tbp])
        src_section = me.doc.Section(
            title='Full Source', body=[src], collapsed=True)

        return me.lang.ExceptionContext(
            title=self.title, body=[tb_section, src_section])


class QueryError(BackendError):
    def __init__(self, driver_err, query_text, query_offset=0):
        super().__init__(driver_err.message)
        err_details = getattr(driver_err, 'details', None)
        try:
            position = err_details['position']
        except KeyError:
            position = None
        else:
            try:
                position = int(position) - 1
            except ValueError:
                position = None
            else:
                position -= query_offset

        ctx = QueryExceptionContext(query=query_text, position=position)
        replace_context(self, ctx)
