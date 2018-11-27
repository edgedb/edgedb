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
import uuid

import asyncpg

from edb import errors

from edb.lang.schema import name as sn
from edb.lang.schema import objtypes as s_objtypes

from . import common


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
            cls, schema, intro_mech, constr_mech, err):
        if isinstance(err, asyncpg.NotNullViolationError):
            source_name = pointer_name = None

            if err.schema_name and err.table_name:
                tabname = (err.schema_name, err.table_name)

                source = common.get_object_from_backend_name(
                    schema, s_objtypes.ObjectType, tabname)
                source_name = source.get_displayname(schema)

                if err.column_name:
                    pointer_name = err.column_name

            if pointer_name is not None:
                pname = f'{source_name}.{pointer_name}'

                return errors.MissingRequiredError(
                    'missing value for required property {}'.format(pname),
                    source_name=source_name, pointer_name=pointer_name)

            else:
                return errors.InternalServerError(err.message)

        elif isinstance(err, asyncpg.IntegrityConstraintViolationError):
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
                return errors.InternalServerError(err.message)

            if error_type == 'cardinality':
                err = 'cardinality violation'
                errcls = errors.CardinalityViolationError
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

                return errors.UnknownLinkError(msg)

            elif error_type == 'link_target_del':
                return errors.ConstraintViolationError(
                    err.message, detail=err.detail)

            elif error_type == 'constraint':
                if err.constraint_name is None:
                    return errors.InternalServerError(err.message)

                constraint_id, _, _ = err.constraint_name.rpartition(';')

                try:
                    constraint_id = uuid.UUID(constraint_id)
                except ValueError:
                    return errors.InternalServerError(err.message)

                constraint = schema.get_by_id(constraint_id)

                return errors.ConstraintViolationError(
                    constraint.format_error_message(schema))

            elif error_type == 'id':
                msg = 'unique link constraint violation'
                errcls = errors.ConstraintViolationError
                return errcls(msg=msg)

        else:
            return errors.InternalServerError(err.message)
