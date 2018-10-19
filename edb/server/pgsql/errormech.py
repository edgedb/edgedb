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


import collections
import json
import re

import asyncpg

from edb.lang.common import exceptions as edgedb_error
from edb.lang.schema import name as sn


class ErrorMech:
    error_res = {
        asyncpg.IntegrityConstraintViolationError: collections.OrderedDict((
            ('cardinality', re.compile(r'^.*".*_cardinality_idx".*$')),
            ('link_target', re.compile(r'^.*link target constraint$')),
            ('constraint', re.compile(r'^.*;schemaconstr(?:#\d+)?".*$')),
            ('id', re.compile(r'^.*"(?:\w+)_data_pkey".*$')),
            ('link_target_del', re.compile(r'^.*link target policy$')),
        ))
    }

    @classmethod
    async def _interpret_db_error(
            cls, intro_mech, constr_mech, type_mech, err):
        if isinstance(err, asyncpg.NotNullViolationError):
            source_name = pointer_name = None

            if err.schema_name and err.table_name:
                tabname = (err.schema_name, err.table_name)

                source_name = intro_mech.table_name_to_object_name(tabname)

                if err.column_name:
                    cols = await type_mech.get_table_columns(
                        tabname, connection=intro_mech.connection)
                    col = cols.get(err.column_name)
                    pointer_name = col['column_comment']

            if pointer_name is not None:
                pname = '{{{}}}.{{{}}}'.format(source_name, pointer_name)

                return edgedb_error.MissingRequiredPointerError(
                    'missing value for required pointer {}'.format(pname),
                    source_name=source_name, pointer_name=pointer_name)

            else:
                return edgedb_error.EdgeDBBackendError(err.message)

        elif isinstance(err, asyncpg.IntegrityConstraintViolationError):
            connection = intro_mech.connection
            schema = intro_mech.schema
            source = pointer = None

            for ecls, eres in cls.error_res.items():
                if isinstance(err, ecls):
                    break
            else:
                eres = {}

            for type, ere in eres.items():
                m = ere.match(err.message)
                if m:
                    error_type = type
                    break
            else:
                return edgedb_error.EdgeDBBackendError(err.message)

            if error_type == 'cardinality':
                err = 'cardinality violation'
                errcls = edgedb_error.PointerCardinalityViolationError
                return errcls(err, source=source, pointer=pointer)

            elif error_type == 'link_target':
                if err.detail:
                    try:
                        detail = json.loads(err.detail)
                    except ValueError:
                        detail = None

                if detail is not None:
                    srcname = detail.get('source')
                    ptrname = detail.get('pointer')
                    target = detail.get('target')
                    expected = detail.get('expected')

                    if srcname and ptrname:
                        srcname = sn.Name(srcname)
                        ptrname = sn.Name(ptrname)
                        lname = '{}.{}'.format(srcname, ptrname.name)
                    else:
                        lname = ''

                    msg = 'invalid target for link {!r}: {!r} (' \
                          'expecting {!r})'.format(lname, target,
                                                   ' or '.join(expected))

                else:
                    msg = 'invalid target for link'

                return edgedb_error.InvalidPointerTargetError(msg)

            elif error_type == 'link_target_del':
                return edgedb_error.ConstraintViolationError(
                    err.message, detail=err.detail)

            elif error_type == 'constraint':
                constraint_name = \
                    await constr_mech.constraint_name_from_pg_name(
                        connection, err.constraint_name)

                if constraint_name is None:
                    return edgedb_error.EdgeDBBackendError(err.message)

                constraint = schema.get(constraint_name)

                return edgedb_error.ConstraintViolationError(
                    constraint.format_error_message())

            elif error_type == 'id':
                msg = 'unique link constraint violation'
                errcls = edgedb_error.UniqueConstraintViolationError
                return errcls(msg=msg)

        else:
            return edgedb_error.EdgeDBBackendError(err.message)
