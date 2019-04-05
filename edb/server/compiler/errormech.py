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


import enum
import json
import re
import uuid

from edb import errors

from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes

from edb.pgsql import common
from edb.pgsql import types


class PGError(enum.Enum):

    IntegrityConstraintViolationError = '23000'
    RestrictViolationError = '23001'
    NotNullViolationError = '23502'
    ForeignKeyViolationError = '23503'
    UniqueViolationError = '23505'
    CheckViolationError = '23514'
    ExclusionViolationError = '23P01'

    NumericValueOutOfRange = '22003'
    InvalidParameterValue = '22023'
    InvalidTextRepresentation = '22P02'

    DivisionByZeroError = '22012'

    ReadOnlySQLTransactionError = '25006'

    InvalidDatetimeFormatError = '22007'
    DatetimeError = '22008'

    TransactionSerializationFailure = '40001'
    TransactionDeadlockDetected = '40P01'


constraint_errors = frozenset({
    PGError.IntegrityConstraintViolationError,
    PGError.RestrictViolationError,
    PGError.NotNullViolationError,
    PGError.ForeignKeyViolationError,
    PGError.UniqueViolationError,
    PGError.CheckViolationError,
    PGError.ExclusionViolationError,
})


constraint_res = {
    'cardinality': re.compile(r'^.*".*_cardinality_idx".*$'),
    'link_target': re.compile(r'^.*link target constraint$'),
    'constraint': re.compile(r'^.*;schemaconstr(?:#\d+)?".*$'),
    'id': re.compile(r'^.*"(?:\w+)_data_pkey".*$'),
    'link_target_del': re.compile(r'^.*link target policy$'),
}


pgtype_re = re.compile(
    '|'.join(fr'\b{key}\b' for key in types.base_type_name_map_r))
enum_re = re.compile(
    r'(?P<p>enum) (?P<v>"edgedb_([\w-]+)"."(?P<id>[\w-]+)_domain")')


def translate_pgtype(schema, msg):
    translated = pgtype_re.sub(
        lambda r: types.base_type_name_map_r.get(r.group(0), r.group(0)),
        msg)

    if translated != msg:
        return translated

    def replace(r):
        type_id = uuid.UUID(r.group('id'))
        stype = schema.get_by_id(type_id, None)
        if stype:
            return f'{r.group("p")} {stype.get_displayname(schema)!r}'
        else:
            return f'{r.group("p")} {r.group("v")}'

    translated = enum_re.sub(replace, msg)

    return translated


def interpret_backend_error(schema, fields):
    # See https://www.postgresql.org/docs/current/protocol-error-fields.html
    # for the full list of PostgreSQL error message fields.
    message = fields.get('M')

    detail = fields.get('D')
    detail_json = None
    if detail and detail.startswith('{'):
        detail_json = json.loads(detail)
        detail = None

    if detail_json:
        errcode = detail_json.get('code')
        if errcode:
            try:
                errcls = type(errors.EdgeDBError).get_error_class_from_code(
                    errcode)
            except LookupError:
                pass
            else:
                err = errcls(message)
                err.set_linecol(
                    detail_json.get('line', -1),
                    detail_json.get('column', -1))
                return err

    try:
        code = PGError(fields['C'])
    except ValueError:
        return errors.InternalServerError(message)

    schema_name = fields.get('s')
    table_name = fields.get('t')
    column_name = fields.get('c')
    constraint_name = fields.get('n')

    if code == PGError.NotNullViolationError:
        source_name = pointer_name = None

        if schema_name and table_name:
            tabname = (schema_name, table_name)

            source = common.get_object_from_backend_name(
                schema, s_objtypes.ObjectType, tabname)
            source_name = source.get_displayname(schema)

            if column_name:
                pointer_name = column_name

        if pointer_name is not None:
            pname = f'{source_name}.{pointer_name}'

            return errors.MissingRequiredError(
                f'missing value for required property {pname}')

        else:
            return errors.InternalServerError(message)

    elif code in constraint_errors:
        source = pointer = None

        for errtype, ere in constraint_res.items():
            m = ere.match(message)
            if m:
                error_type = errtype
                break
        else:
            return errors.InternalServerError(message)

        if error_type == 'cardinality':
            return errors.CardinalityViolationError(
                'cardinality violation',
                source=source, pointer=pointer)

        elif error_type == 'link_target':
            if detail_json:
                srcname = detail_json.get('source')
                ptrname = detail_json.get('pointer')
                target = detail_json.get('target')
                expected = detail_json.get('expected')

                if srcname and ptrname:
                    srcname = sn.Name(srcname)
                    ptrname = sn.Name(ptrname)
                    lname = '{}.{}'.format(srcname, ptrname.name)
                else:
                    lname = ''

                msg = (
                    f'invalid target for link {lname!r}: {target!r} '
                    f'(expecting {expected!r})'
                )

            else:
                msg = 'invalid target for link'

            return errors.UnknownLinkError(msg)

        elif error_type == 'link_target_del':
            return errors.ConstraintViolationError(
                message, details=detail)

        elif error_type == 'constraint':
            if constraint_name is None:
                return errors.InternalServerError(message)

            constraint_id, _, _ = constraint_name.rpartition(';')

            try:
                constraint_id = uuid.UUID(constraint_id)
            except ValueError:
                return errors.InternalServerError(message)

            constraint = schema.get_by_id(constraint_id)

            return errors.ConstraintViolationError(
                constraint.format_error_message(schema))

        elif error_type == 'id':
            return errors.ConstraintViolationError(
                'unique link constraint violation')

    elif code == PGError.InvalidParameterValue:
        return errors.InvalidValueError(
            message, details=detail if detail else None)

    elif code == PGError.InvalidTextRepresentation:
        return errors.InvalidValueError(translate_pgtype(schema, message))

    elif code == PGError.NumericValueOutOfRange:
        return errors.NumericOutOfRangeError(translate_pgtype(schema, message))

    elif code == PGError.DivisionByZeroError:
        return errors.DivisionByZeroError(message)

    elif code == PGError.ReadOnlySQLTransactionError:
        return errors.TransactionError(
            'cannot execute query in a read-only transaction')

    elif code in {PGError.InvalidDatetimeFormatError, PGError.DatetimeError}:
        return errors.InvalidValueError(translate_pgtype(schema, message))

    elif code == PGError.TransactionSerializationFailure:
        return errors.TransactionSerializationError(message)

    elif code == PGError.TransactionDeadlockDetected:
        return errors.TransactionDeadlockError(message)

    return errors.InternalServerError(message)
