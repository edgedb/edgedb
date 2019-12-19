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


from __future__ import annotations

import enum
import json
import re
from typing import *  # NoQA

from edb import errors
from edb.common import uuidgen

from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes

from edb.pgsql import common
from edb.pgsql import types


class PGErrorCode(enum.Enum):

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

    InvalidCatalogNameError = '3D000'


class SchemaRequired:
    '''A sentinel used to signal that a particular error requires a schema.'''


# Error codes that always require the schema to be resolved. There are
# other error codes that only require the schema under certain
# circumstances.
SCHEMA_CODES = frozenset({
    PGErrorCode.InvalidTextRepresentation,
    PGErrorCode.NumericValueOutOfRange,
    PGErrorCode.InvalidDatetimeFormatError,
    PGErrorCode.DatetimeError,
})


class ErrorDetails(NamedTuple):
    message: str
    detail: str = None
    detail_json: dict = None
    code: PGErrorCode = None
    schema_name: str = None
    table_name: str = None
    column_name: str = None
    constraint_name: str = None
    errcls: errors.EdgeDBError = None


constraint_errors = frozenset({
    PGErrorCode.IntegrityConstraintViolationError,
    PGErrorCode.RestrictViolationError,
    PGErrorCode.NotNullViolationError,
    PGErrorCode.ForeignKeyViolationError,
    PGErrorCode.UniqueViolationError,
    PGErrorCode.CheckViolationError,
    PGErrorCode.ExclusionViolationError,
})


constraint_res = {
    'cardinality': re.compile(r'^.*".*_cardinality_idx".*$'),
    'link_target': re.compile(r'^.*link target constraint$'),
    'constraint': re.compile(r'^.*;schemaconstr(?:#\d+)?".*$'),
    'newconstraint': re.compile(r'^.*violate the new constraint.*$'),
    'id': re.compile(r'^.*"(?:\w+)_data_pkey".*$'),
    'link_target_del': re.compile(r'^.*link target policy$'),
    'scalar': re.compile(r'^value for domain (\w+) violates check constraint'),
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
        type_id = uuidgen.UUID(r.group('id'))
        stype = schema.get_by_id(type_id, None)
        if stype:
            return f'{r.group("p")} {stype.get_displayname(schema)!r}'
        else:
            return f'{r.group("p")} {r.group("v")}'

    translated = enum_re.sub(replace, msg)

    return translated


def get_error_details(fields):
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
                return ErrorDetails(
                    errcls=errcls, message=message, detail_json=detail_json)

    try:
        code = PGErrorCode(fields['C'])
    except ValueError:
        return ErrorDetails(errcls=errors.InternalServerError, message=message)

    schema_name = fields.get('s')
    table_name = fields.get('t')
    column_name = fields.get('c')
    constraint_name = fields.get('n')

    return ErrorDetails(
        message=message, detail=detail, detail_json=detail_json, code=code,
        schema_name=schema_name, table_name=table_name,
        column_name=column_name, constraint_name=constraint_name
    )


def get_generic_exception_from_err_details(err_details):
    err = None
    if err_details.errcls is not None:
        err = err_details.errcls(err_details.message)
        if err_details.errcls is not errors.InternalServerError:
            err.set_linecol(
                err_details.detail_json.get('line', -1),
                err_details.detail_json.get('column', -1))
    return err


def static_interpret_backend_error(fields):
    err_details = get_error_details(fields)
    # handle some generic errors if possible
    err = get_generic_exception_from_err_details(err_details)
    if err is not None:
        return err

    if err_details.code == PGErrorCode.NotNullViolationError:
        if err_details.schema_name and err_details.table_name:
            return SchemaRequired

        else:
            return errors.InternalServerError(err_details.message)

    elif err_details.code in constraint_errors:
        source = pointer = None

        for errtype, ere in constraint_res.items():
            m = ere.match(err_details.message)
            if m:
                error_type = errtype
                break
        else:
            return errors.InternalServerError(err_details.message)

        if error_type == 'cardinality':
            return errors.CardinalityViolationError(
                'cardinality violation',
                source=source, pointer=pointer)

        elif error_type == 'link_target':
            if err_details.detail_json:
                srcname = err_details.detail_json.get('source')
                ptrname = err_details.detail_json.get('pointer')
                target = err_details.detail_json.get('target')
                expected = err_details.detail_json.get('expected')

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
                err_details.message, details=err_details.detail)

        elif error_type == 'constraint':
            if err_details.constraint_name is None:
                return errors.InternalServerError(err_details.message)

            constraint_id, _, _ = err_details.constraint_name.rpartition(';')

            try:
                constraint_id = uuidgen.UUID(constraint_id)
            except ValueError:
                return errors.InternalServerError(err_details.message)

            return SchemaRequired

        elif error_type == 'newconstraint':
            # We can reconstruct what went wrong from the schema_name,
            # table_name, and column_name. But we don't expect
            # constraint_name to be present (because the constraint is
            # not yet present in the schema?).
            if (err_details.schema_name and err_details.table_name and
                    err_details.column_name):
                return SchemaRequired

            else:
                return errors.InternalServerError(err_details.message)

        elif error_type == 'scalar':
            return SchemaRequired

        elif error_type == 'id':
            return errors.ConstraintViolationError(
                'unique link constraint violation')

    elif err_details.code in SCHEMA_CODES:
        if err_details.code == PGErrorCode.InvalidDatetimeFormatError:
            hint = None
            if err_details.detail_json:
                hint = err_details.detail_json.get('hint')

            if err_details.message.startswith('missing required time zone'):
                return errors.InvalidValueError(err_details.message, hint=hint)
            elif err_details.message.startswith('unexpected time zone'):
                return errors.InvalidValueError(err_details.message, hint=hint)

        return SchemaRequired

    elif err_details.code == PGErrorCode.InvalidParameterValue:
        return errors.InvalidValueError(
            err_details.message,
            details=err_details.detail if err_details.detail else None
        )

    elif err_details.code == PGErrorCode.DivisionByZeroError:
        return errors.DivisionByZeroError(err_details.message)

    elif err_details.code == PGErrorCode.ReadOnlySQLTransactionError:
        return errors.TransactionError(
            'cannot execute query in a read-only transaction')

    elif err_details.code == PGErrorCode.TransactionSerializationFailure:
        return errors.TransactionSerializationError(err_details.message)

    elif err_details.code == PGErrorCode.TransactionDeadlockDetected:
        return errors.TransactionDeadlockError(err_details.message)

    elif err_details.code == PGErrorCode.InvalidCatalogNameError:
        return errors.AuthenticationError(err_details.message)

    return errors.InternalServerError(err_details.message)


def interpret_backend_error(schema, fields):
    err_details = get_error_details(fields)
    hint = None
    if err_details.detail_json:
        hint = err_details.detail_json.get('hint')

    # all generic errors are static and have been handled by this point

    if err_details.code == PGErrorCode.NotNullViolationError:
        source_name = pointer_name = None

        if err_details.schema_name and err_details.table_name:
            tabname = (err_details.schema_name, err_details.table_name)

            source = common.get_object_from_backend_name(
                schema, s_objtypes.ObjectType, tabname)
            source_name = source.get_displayname(schema)

            if err_details.column_name:
                pointer_name = err_details.column_name

        if pointer_name is not None:
            pname = f'{source_name}.{pointer_name}'

            return errors.MissingRequiredError(
                f'missing value for required property {pname}')

        else:
            return errors.InternalServerError(err_details.message)

    elif err_details.code in constraint_errors:
        error_type = None
        match = None

        for errtype, ere in constraint_res.items():
            m = ere.match(err_details.message)
            if m:
                error_type = errtype
                match = m
                break
        # no need for else clause since it would have been handled by
        # the static version

        if error_type == 'constraint':
            # similarly, if we're here it's because we have a constraint_id
            constraint_id, _, _ = err_details.constraint_name.rpartition(';')
            constraint_id = uuidgen.UUID(constraint_id)

            constraint = schema.get_by_id(constraint_id)

            return errors.ConstraintViolationError(
                constraint.format_error_message(schema))
        elif error_type == 'newconstraint':
            # If we're here, it means that we already validated that
            # schema_name, table_name and column_name all exist.
            tabname = (err_details.schema_name, err_details.table_name)
            source = common.get_object_from_backend_name(
                schema, s_objtypes.ObjectType, tabname)
            source_name = source.get_displayname(schema)
            pname = f'{source_name}.{err_details.column_name}'

            return errors.ConstraintViolationError(
                f'Existing {pname} values violate the new constraint')
        elif error_type == 'scalar':
            domain_name = match.group(1)
            stype_name = types.base_type_name_map_r.get(domain_name)
            if stype_name:
                msg = f'invalid value for scalar type {stype_name!r}'
            else:
                msg = translate_pgtype(schema, err_details.message)
            return errors.InvalidValueError(msg)

    elif err_details.code == PGErrorCode.InvalidTextRepresentation:
        return errors.InvalidValueError(
            translate_pgtype(schema, err_details.message))

    elif err_details.code == PGErrorCode.NumericValueOutOfRange:
        return errors.NumericOutOfRangeError(
            translate_pgtype(schema, err_details.message))

    elif err_details.code in {PGErrorCode.InvalidDatetimeFormatError,
                              PGErrorCode.DatetimeError}:
        return errors.InvalidValueError(
            translate_pgtype(schema, err_details.message),
            hint=hint)

    return errors.InternalServerError(err_details.message)
