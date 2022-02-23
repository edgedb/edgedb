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
from typing import *

import json
import re

from edb import errors
from edb.common import value_dispatch
from edb.common import uuidgen

from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers

from edb.pgsql import common
from edb.pgsql import types

from edb.server.pgcon import errors as pgerrors


class SchemaRequired:
    '''A sentinel used to signal that a particular error requires a schema.'''


# Error codes that always require the schema to be resolved. There are
# other error codes that only require the schema under certain
# circumstances.
SCHEMA_CODES = frozenset({
    pgerrors.ERROR_INVALID_TEXT_REPRESENTATION,
    pgerrors.ERROR_NUMERIC_VALUE_OUT_OF_RANGE,
    pgerrors.ERROR_INVALID_DATETIME_FORMAT,
    pgerrors.ERROR_DATETIME_FIELD_OVERFLOW,
})


class ErrorDetails(NamedTuple):
    message: str
    detail: Optional[str] = None
    detail_json: Optional[Dict[str, Any]] = None
    code: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    constraint_name: Optional[str] = None
    errcls: Optional[errors.EdgeDBError] = None


constraint_errors = frozenset({
    pgerrors.ERROR_INTEGRITY_CONSTRAINT_VIOLATION,
    pgerrors.ERROR_RESTRICT_VIOLATION,
    pgerrors.ERROR_NOT_NULL_VIOLATION,
    pgerrors.ERROR_FOREIGN_KEY_VIOLATION,
    pgerrors.ERROR_UNIQUE_VIOLATION,
    pgerrors.ERROR_CHECK_VIOLATION,
    pgerrors.ERROR_EXCLUSION_VIOLATION,
})

directly_mappable = {
    pgerrors.ERROR_DIVISION_BY_ZERO: errors.DivisionByZeroError,
    pgerrors.ERROR_INTERVAL_FIELD_OVERFLOW: errors.NumericOutOfRangeError,
    pgerrors.ERROR_READ_ONLY_SQL_TRANSACTION: errors.TransactionError,
    pgerrors.ERROR_SERIALIZATION_FAILURE: errors.TransactionSerializationError,
    pgerrors.ERROR_DEADLOCK_DETECTED: errors.TransactionDeadlockError,
    pgerrors.ERROR_INVALID_CATALOG_NAME: errors.UnknownDatabaseError,
    pgerrors.ERROR_OBJECT_IN_USE: errors.ExecutionError,
    pgerrors.ERROR_DUPLICATE_DATABASE: errors.DuplicateDatabaseDefinitionError,
    pgerrors.ERROR_IDLE_IN_TRANSACTION_TIMEOUT:
        errors.IdleTransactionTimeoutError,
    pgerrors.ERROR_QUERY_CANCELLED: errors.QueryTimeoutError,
    pgerrors.ERROR_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE: errors.InvalidValueError,
    pgerrors.ERROR_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE: (
        errors.InvalidValueError),
    pgerrors.ERROR_INVALID_REGULAR_EXPRESSION: errors.InvalidValueError,
}


constraint_res = {
    'cardinality': re.compile(r'^.*".*_cardinality_idx".*$'),
    'link_target': re.compile(r'^.*link target constraint$'),
    'constraint': re.compile(r'^.*;schemaconstr(?:#\d+)?".*$'),
    'newconstraint': re.compile(r'^.*violate the new constraint.*$'),
    'id': re.compile(r'^.*"(?:\w+)_data_pkey".*$'),
    'link_target_del': re.compile(r'^.*link target policy$'),
    'scalar': re.compile(
        r'^value for domain (\w+) violates check constraint "(.+)"'
    ),
}


range_constraints = frozenset({
    'timestamptz_t_check',
    'timestamp_t_check',
    'date_t_check',
})


pgtype_re = re.compile(
    '|'.join(fr'\b{key}\b' for key in types.base_type_name_map_r))
enum_re = re.compile(
    r'(?P<p>enum) (?P<v>edgedb([\w-]+)."(?P<id>[\w-]+)_domain")')


def translate_pgtype(schema, msg):
    translated = pgtype_re.sub(
        lambda r: str(types.base_type_name_map_r.get(r.group(0), r.group(0))),
        msg,
    )

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

    code = fields['C']
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


#########################################################################
# Static errors interpretation
#########################################################################


def static_interpret_backend_error(fields):
    err_details = get_error_details(fields)
    # handle some generic errors if possible
    err = get_generic_exception_from_err_details(err_details)
    if err is not None:
        return err

    return static_interpret_by_code(
        err_details.code, err_details)


@value_dispatch.value_dispatch
def static_interpret_by_code(code, err_details):
    return errors.InternalServerError(err_details.message)


@static_interpret_by_code.register_for_all(
    directly_mappable.keys())
def _static_interpret_directly_mappable(code, err_details):
    errcls = directly_mappable[code]
    return errcls(err_details.message)


@static_interpret_by_code.register_for_all(constraint_errors)
def _static_interpret_constraint_errors(code, err_details):
    if code == pgerrors.ERROR_NOT_NULL_VIOLATION:
        if err_details.table_name or err_details.column_name:
            return SchemaRequired
        else:
            return errors.InternalServerError(err_details.message)

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
                srcname = sn.QualName.from_string(srcname)
                ptrname = sn.QualName.from_string(ptrname)
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


@static_interpret_by_code.register_for_all(SCHEMA_CODES)
def _static_interpret_schema_errors(code, err_details):
    if code == pgerrors.ERROR_INVALID_DATETIME_FORMAT:
        hint = None
        if err_details.detail_json:
            hint = err_details.detail_json.get('hint')

        if err_details.message.startswith('missing required time zone'):
            return errors.InvalidValueError(err_details.message, hint=hint)
        elif err_details.message.startswith('unexpected time zone'):
            return errors.InvalidValueError(err_details.message, hint=hint)

    return SchemaRequired


@static_interpret_by_code.register(
    pgerrors.ERROR_INVALID_PARAMETER_VALUE)
def _static_interpret_invalid_param_value(_code, err_details):
    return errors.InvalidValueError(
        err_details.message,
        details=err_details.detail if err_details.detail else None
    )


@static_interpret_by_code.register(
    pgerrors.ERROR_WRONG_OBJECT_TYPE)
def _static_interpret_wrong_object_type(_code, err_details):
    if err_details.column_name:
        return SchemaRequired

    hint = None
    if err_details.detail_json:
        hint = err_details.detail_json.get('hint')

    return errors.InvalidValueError(
        err_details.message,
        details=err_details.detail if err_details.detail else None,
        hint=hint,
    )


@static_interpret_by_code.register(
    pgerrors.ERROR_CARDINALITY_VIOLATION)
def _static_interpret_cardinality_violation(_code, err_details):

    if (err_details.constraint_name == 'std::assert_single'
            or err_details.constraint_name == 'std::assert_exists'):
        return errors.CardinalityViolationError(err_details.message)

    elif err_details.constraint_name == 'std::assert_distinct':
        return errors.ConstraintViolationError(err_details.message)

    elif err_details.constraint_name == 'set abstract':
        return errors.ConstraintViolationError(err_details.message)

    return errors.InternalServerError(err_details.message)


@static_interpret_by_code.register(
    pgerrors.ERROR_FEATURE_NOT_SUPPORTED)
def _static_interpret_feature_not_supported(_code, err_details):
    return errors.UnsupportedBackendFeatureError(err_details.message)


#########################################################################
# Errors interpretation that requires a schema
#########################################################################


def interpret_backend_error(schema, fields):
    # all generic errors are static and have been handled by this point

    err_details = get_error_details(fields)
    hint = None
    if err_details.detail_json:
        hint = err_details.detail_json.get('hint')

    return interpret_by_code(err_details.code, schema, err_details, hint)


@value_dispatch.value_dispatch
def interpret_by_code(code, schema, err_details, hint):
    return errors.InternalServerError(err_details.message)


@interpret_by_code.register_for_all(constraint_errors)
def _interpret_constraint_errors(code, schema, err_details, hint):
    details = None
    if code == pgerrors.ERROR_NOT_NULL_VIOLATION:
        colname = err_details.column_name
        if colname:
            if colname.startswith('??'):
                ptr_id, *_ = colname[2:].partition('_')
            else:
                ptr_id = colname
            pointer = common.get_object_from_backend_name(
                schema, s_pointers.Pointer, ptr_id)
            pname = pointer.get_verbosename(schema, with_parent=True)
        else:
            pname = None

        if pname is not None:
            if err_details.detail_json:
                object_id = err_details.detail_json.get('object_id')
                if object_id is not None:
                    details = f'Failing object id is {str(object_id)!r}.'

            return errors.MissingRequiredError(
                f'missing value for required {pname}',
                details=details,
                hint=hint,
            )
        else:
            return errors.InternalServerError(err_details.message)

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

        msg = constraint.format_error(schema)
        subject = constraint.get_subject(schema)
        vname = subject.get_verbosename(schema, with_parent=True)
        subjtitle = f"value of {vname}"
        details = constraint.format_error_message(schema, subjtitle)
        return errors.ConstraintViolationError(msg, details=details)
    elif error_type == 'newconstraint':
        # If we're here, it means that we already validated that
        # schema_name, table_name and column_name all exist.
        tabname = (err_details.schema_name, err_details.table_name)
        source = common.get_object_from_backend_name(
            schema, s_objtypes.ObjectType, tabname)
        source_name = source.get_displayname(schema)
        pointer = common.get_object_from_backend_name(
            schema, s_pointers.Pointer, err_details.column_name)
        pointer_name = pointer.get_shortname(schema).name

        return errors.ConstraintViolationError(
            f'Existing {source_name}.{pointer_name} '
            f'values violate the new constraint')
    elif error_type == 'scalar':
        domain_name = match.group(1)
        stype_name = types.base_type_name_map_r.get(domain_name)
        if stype_name:
            if match.group(2) in range_constraints:
                msg = f'{str(stype_name)!r} value out of range'
            else:
                msg = f'invalid value for scalar type {str(stype_name)!r}'
        else:
            msg = translate_pgtype(schema, err_details.message)
        return errors.InvalidValueError(msg)

    return errors.InternalServerError(err_details.message)


@interpret_by_code.register(pgerrors.ERROR_INVALID_TEXT_REPRESENTATION)
def _interpret_invalid_text_repr(code, schema, err_details, hint):
    return errors.InvalidValueError(
        translate_pgtype(schema, err_details.message))


@interpret_by_code.register(pgerrors.ERROR_NUMERIC_VALUE_OUT_OF_RANGE)
def _interpret_numeric_out_of_range(code, schema, err_details, hint):
    return errors.NumericOutOfRangeError(
        translate_pgtype(schema, err_details.message))


@interpret_by_code.register(pgerrors.ERROR_INVALID_DATETIME_FORMAT)
@interpret_by_code.register(pgerrors.ERROR_DATETIME_FIELD_OVERFLOW)
def _interpret_invalid_datetime(code, schema, err_details, hint):
    return errors.InvalidValueError(
        translate_pgtype(schema, err_details.message),
        hint=hint)


@interpret_by_code.register(pgerrors.ERROR_WRONG_OBJECT_TYPE)
def _interpret_wrong_object_type(code, schema, err_details, hint):
    if err_details.message == 'covariance error':
        ptr = schema.get_by_id(uuidgen.UUID(err_details.column_name))
        wrong_obj = schema.get_by_id(uuidgen.UUID(err_details.table_name))

        vn = ptr.get_verbosename(schema, with_parent=True)
        return errors.InvalidLinkTargetError(
            f"invalid target for {vn}: '{wrong_obj.get_name(schema)}'"
            f" (expecting '{ptr.get_target(schema).get_name(schema)}')"
        )

    return errors.InternalServerError(err_details.message)
