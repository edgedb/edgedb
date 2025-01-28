# mypy: allow-untyped-defs, allow-incomplete-defs

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
from typing import Any, Optional, Type, Dict, NamedTuple

import json
import re

from edb import errors
from edb.common import value_dispatch
from edb.common import uuidgen

from edb.graphql import types as gql_types

from edb.pgsql.parser import exceptions as parser_errors

from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema
from edb.schema import constraints as s_constraints

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
    errcls: Optional[Type[errors.EdgeDBError]] = None


constraint_errors = frozenset({
    pgerrors.ERROR_INTEGRITY_CONSTRAINT_VIOLATION,
    pgerrors.ERROR_RESTRICT_VIOLATION,
    pgerrors.ERROR_NOT_NULL_VIOLATION,
    pgerrors.ERROR_FOREIGN_KEY_VIOLATION,
    pgerrors.ERROR_UNIQUE_VIOLATION,
    pgerrors.ERROR_CHECK_VIOLATION,
    pgerrors.ERROR_EXCLUSION_VIOLATION,
})

branch_errors = {
    pgerrors.ERROR_INVALID_CATALOG_NAME: errors.UnknownDatabaseError,
    pgerrors.ERROR_DUPLICATE_DATABASE: errors.DuplicateDatabaseDefinitionError,
}

directly_mappable = {
    pgerrors.ERROR_DIVISION_BY_ZERO: errors.DivisionByZeroError,
    pgerrors.ERROR_INTERVAL_FIELD_OVERFLOW: errors.NumericOutOfRangeError,
    pgerrors.ERROR_READ_ONLY_SQL_TRANSACTION: errors.TransactionError,
    pgerrors.ERROR_SERIALIZATION_FAILURE: errors.TransactionSerializationError,
    pgerrors.ERROR_DEADLOCK_DETECTED: errors.TransactionDeadlockError,
    pgerrors.ERROR_OBJECT_IN_USE: errors.ExecutionError,
    pgerrors.ERROR_IDLE_IN_TRANSACTION_TIMEOUT:
        errors.IdleTransactionTimeoutError,
    pgerrors.ERROR_QUERY_CANCELLED: errors.QueryTimeoutError,
    pgerrors.ERROR_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE: errors.InvalidValueError,
    pgerrors.ERROR_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE: (
        errors.InvalidValueError),
    pgerrors.ERROR_INVALID_REGULAR_EXPRESSION: errors.InvalidValueError,
    pgerrors.ERROR_INVALID_LOGARITHM_ARGUMENT: errors.InvalidValueError,
    pgerrors.ERROR_INVALID_POWER_ARGUMENT: errors.InvalidValueError,
    pgerrors.ERROR_INSUFFICIENT_PRIVILEGE: errors.AccessPolicyError,
    pgerrors.ERROR_PROGRAM_LIMIT_EXCEEDED: errors.InvalidValueError,
    pgerrors.ERROR_DATA_EXCEPTION: errors.InvalidValueError,
    pgerrors.ERROR_CHARACTER_NOT_IN_REPERTOIRE: errors.InvalidValueError,
}


constraint_res = {
    'cardinality': re.compile(r'^.*".*_cardinality_idx".*$'),
    'link_target': re.compile(r'^.*link target constraint$'),
    'constraint': re.compile(r'^.*;schemaconstr(?:#\d+)?".*$'),
    'idconstraint': re.compile(r'^.*".*_pkey".*$'),
    'newconstraint': re.compile(r'^.*violate the new constraint.*$'),
    'id': re.compile(r'^.*"(?:\w+)_data_pkey".*$'),
    'link_target_del': re.compile(r'^.*link target policy$'),
    'scalar': re.compile(
        r'^value for domain ([\w\.]+) violates check constraint "(.+)"'
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


type_in_access_policy_re = re.compile(r'(\w+|`.+?`)::(\w+|`.+?`)')


def gql_translate_pgtype_inner(schema, msg):
    """Try to replace any internal pg type name with a GraphQL type name"""

    # Mapping base types
    def base_type_map(name: str) -> str:
        result = gql_types.EDB_TO_GQL_SCALARS_MAP.get(
            str(types.base_type_name_map_r.get(name))
        )

        if result is None:
            return name
        else:
            return result.name

    translated = pgtype_re.sub(
        lambda r: base_type_map(r.group(0)),
        msg,
    )

    if translated != msg:
        return translated

    def replace(r):
        type_id = uuidgen.UUID(r.group('id'))
        stype = schema.get_by_id(type_id, None)
        gql_name = gql_types.GQLCoreSchema.get_gql_name(
            stype.get_name(schema))
        if stype:
            return f'{r.group("p")} {gql_name!r}'
        else:
            return f'{r.group("p")} {r.group("v")}'

    translated = enum_re.sub(replace, msg)

    return translated


def gql_replace_type_names_in_text(msg):
    return type_in_access_policy_re.sub(
        lambda m: gql_types.GQLCoreSchema.get_gql_name(
            sn.QualName.from_string(m.group(0))),
        msg,
    )


def eql_translate_pgtype_inner(schema, msg):
    """Try to replace any internal pg type name with an edgedb type name"""
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


def translate_pgtype(schema, msg, from_graphql=False):
    """Try to translate a message that might refer to internal pg types.

    We *want* to replace internal pg type names with edgedb names, but only
    when they actually refer to types.
    The messages aren't really structured well enough to support this properly,
    so we approximate it by only doing the replacement *before* the first colon
    in the message, so if a user does `<int64>"bigint"`, and we get the message
    'invalid input syntax for type bigint: "bigint"', we do the right thing.
    """

    leading, *rest = msg.split(':')
    if from_graphql:
        leading_translated = gql_translate_pgtype_inner(schema, leading)
    else:
        leading_translated = eql_translate_pgtype_inner(schema, leading)
    return ':'.join([leading_translated, *rest])


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


def static_interpret_backend_error(fields, from_graphql=False):
    err_details = get_error_details(fields)
    # handle some generic errors if possible
    err = get_generic_exception_from_err_details(err_details)
    if err is not None:
        return err

    return static_interpret_by_code(
        err_details.code, err_details, from_graphql=from_graphql)


@value_dispatch.value_dispatch
def static_interpret_by_code(
    _code: str,
    err_details: ErrorDetails,
    from_graphql: bool = False,
):
    return errors.InternalServerError(err_details.message)


@static_interpret_by_code.register_for_all(branch_errors.keys())
def _static_interpret_branch_errors(
    code: str,
    err_details: ErrorDetails,
    from_graphql: bool = False,
):
    errcls = branch_errors[code]

    msg = err_details.message.replace('database', 'branch', 1)

    return errcls(msg)


@static_interpret_by_code.register_for_all(directly_mappable.keys())
def _static_interpret_directly_mappable(
    code: str,
    err_details: ErrorDetails,
    from_graphql: bool = False,
):
    errcls = directly_mappable[code]

    if from_graphql:
        msg = gql_replace_type_names_in_text(err_details.message)
    else:
        msg = err_details.message

    return errcls(msg)


@static_interpret_by_code.register_for_all(constraint_errors)
def _static_interpret_constraint_errors(
    code: str,
    err_details: ErrorDetails,
    from_graphql: bool = False,
):
    if code == pgerrors.ERROR_NOT_NULL_VIOLATION:
        if err_details.table_name or err_details.column_name:
            return SchemaRequired
        else:
            return errors.InternalServerError(err_details.message)

    for errtype, ere in constraint_res.items():
        m = ere.match(err_details.message)
        if m:
            error_type = errtype
            break
    else:
        return errors.InternalServerError(err_details.message)

    if error_type == 'cardinality':
        return errors.CardinalityViolationError('cardinality violation')

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
        if from_graphql:
            msg = gql_replace_type_names_in_text(err_details.message)
        else:
            msg = err_details.message

        return errors.ConstraintViolationError(
            msg, details=err_details.detail)

    elif error_type == 'constraint':
        if err_details.constraint_name is None:
            return errors.InternalServerError(err_details.message)

        constraint_id, _, _ = err_details.constraint_name.rpartition(';')

        try:
            uuidgen.UUID(constraint_id)
        except ValueError:
            return errors.InternalServerError(err_details.message)

        return SchemaRequired

    elif error_type == 'idconstraint':
        if err_details.constraint_name is None:
            return errors.InternalServerError(err_details.message)

        constraint_id, _, _ = err_details.constraint_name.rpartition('_')

        try:
            uuidgen.UUID(constraint_id)
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
def _static_interpret_schema_errors(
    code: str,
    err_details: ErrorDetails,
    from_graphql: bool = False,
):
    if code == pgerrors.ERROR_INVALID_DATETIME_FORMAT:
        hint = None
        if err_details.detail_json:
            hint = err_details.detail_json.get('hint')

        if err_details.message.startswith('missing required time zone'):
            return errors.InvalidValueError(err_details.message, hint=hint)
        elif err_details.message.startswith('unexpected time zone'):
            return errors.InvalidValueError(err_details.message, hint=hint)

    return SchemaRequired


@static_interpret_by_code.register(pgerrors.ERROR_INVALID_PARAMETER_VALUE)
def _static_interpret_invalid_param_value(
    _code: str,
    err_details: ErrorDetails,
    from_graphql: bool = False,
):
    error_message_context = ''
    if err_details.detail_json:
        error_message_context = (
            err_details.detail_json.get('error_message_context', '')
        )

    return errors.InvalidValueError(
        error_message_context + err_details.message,
        details=err_details.detail if err_details.detail else None,
    )


@static_interpret_by_code.register(pgerrors.ERROR_WRONG_OBJECT_TYPE)
def _static_interpret_wrong_object_type(
    _code: str,
    err_details: ErrorDetails,
    from_graphql: bool = False,
):
    if err_details.column_name:
        return SchemaRequired

    hint = None
    error_message_context = ''
    if err_details.detail_json:
        hint = err_details.detail_json.get('hint')
        error_message_context = (
            err_details.detail_json.get('error_message_context', '')
        )

    return errors.InvalidValueError(
        error_message_context + err_details.message,
        details=err_details.detail if err_details.detail else None,
        hint=hint,
    )


@static_interpret_by_code.register(pgerrors.ERROR_CARDINALITY_VIOLATION)
def _static_interpret_cardinality_violation(
    _code: str,
    err_details: ErrorDetails,
    from_graphql: bool = False,
):

    if (err_details.constraint_name == 'std::assert_single'
            or err_details.constraint_name == 'std::assert_exists'):
        return errors.CardinalityViolationError(err_details.message)

    elif err_details.constraint_name == 'std::assert_distinct':
        return errors.ConstraintViolationError(err_details.message)

    elif err_details.constraint_name == 'std::assert':
        return errors.QueryAssertionError(err_details.message)

    elif err_details.constraint_name == 'set abstract':
        return errors.ConstraintViolationError(err_details.message)

    return errors.InternalServerError(err_details.message)


@static_interpret_by_code.register(pgerrors.ERROR_FEATURE_NOT_SUPPORTED)
def _static_interpret_feature_not_supported(
    _code: str,
    err_details: ErrorDetails,
    from_graphql: bool = False,
):
    return errors.UnsupportedBackendFeatureError(err_details.message)


#########################################################################
# Errors interpretation that requires a schema
#########################################################################


def interpret_backend_error(schema, fields, from_graphql=False):
    # all generic errors are static and have been handled by this point

    err_details = get_error_details(fields)
    hint = None
    if err_details.detail_json:
        hint = err_details.detail_json.get('hint')

    return interpret_by_code(err_details.code, schema, err_details, hint,
                             from_graphql=from_graphql)


@value_dispatch.value_dispatch
def interpret_by_code(code, schema, err_details, hint, from_graphql=False):
    return errors.InternalServerError(err_details.message)


@interpret_by_code.register_for_all(constraint_errors)
def _interpret_constraint_errors(
    code: str,
    schema: s_schema.Schema,
    err_details: ErrorDetails,
    hint: Optional[str],
    from_graphql: bool = False,
):
    details = None
    if code == pgerrors.ERROR_NOT_NULL_VIOLATION:
        colname = err_details.column_name
        if colname:
            if colname.startswith('??'):
                ptr_id, *_ = colname[2:].partition('_')
            else:
                ptr_id = colname
            if ptr_id == 'id':
                assert err_details.table_name
                obj_type: s_objtypes.ObjectType = schema.get_by_id(
                    uuidgen.UUID(err_details.table_name),
                    type=s_objtypes.ObjectType,
                )
                pointer = obj_type.getptr(schema, sn.UnqualName('id'))
            else:
                pointer = common.get_object_from_backend_name(
                    schema, s_pointers.Pointer, ptr_id
                )
            pname = pointer.get_verbosename(schema, with_parent=True)
        else:
            pname = None

        if pname is not None:
            if err_details.detail_json:
                object_id = err_details.detail_json.get('object_id')
                if object_id is not None:
                    details = f'Failing object id is {str(object_id)!r}.'

            if from_graphql:
                pname = gql_replace_type_names_in_text(pname)

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

    if error_type == 'constraint' or error_type == 'idconstraint':
        assert err_details.constraint_name

        # similarly, if we're here it's because we have a constraint_id
        if error_type == 'constraint':
            constraint_id_s, _, _ = err_details.constraint_name.rpartition(';')
            assert err_details.constraint_name
            constraint_id = uuidgen.UUID(constraint_id_s)
            constraint = schema.get_by_id(
                constraint_id, type=s_constraints.Constraint
            )
        else:
            # Primary key violations give us the table name, so
            # look through that for the constraint
            obj_id, _, _ = err_details.constraint_name.rpartition('_')
            obj_type = schema.get_by_id(
                uuidgen.UUID(obj_id), type=s_objtypes.ObjectType
            )
            obj_ptr = obj_type.getptr(schema, sn.UnqualName('id'))
            constraint = obj_ptr.get_exclusive_constraints(schema)[0]

        # msg is for the "end user" that should not mention pointers and object
        # type it is also affected by setting `errmessage` in user schema.
        msg = constraint.format_error_message(schema)

        # details is for the "developer" that must explain what's going on
        # under the hood. It contains verbose descriptions of object involved.
        subject = constraint.get_subject(schema)
        subject_description = subject.get_verbosename(schema, with_parent=True)
        constraint_description = constraint.get_verbosename(schema)
        details = f'violated {constraint_description} on {subject_description}'

        if from_graphql:
            msg = gql_replace_type_names_in_text(msg)
            details = gql_replace_type_names_in_text(details)

        return errors.ConstraintViolationError(msg, details=details)
    elif error_type == 'newconstraint':
        # If we're here, it means that we already validated that
        # schema_name, table_name and column_name all exist.
        #
        # NOTE: this should never occur in GraphQL mode.
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
        assert match
        domain_name = match.group(1)
        # NOTE: We don't attempt to change the name of the scalar type even if
        # the error comes from GraphQL because we map custom scalars onto
        # their underlying base types.
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
def _interpret_invalid_text_repr(
    code: str,
    schema: s_schema.Schema,
    err_details: ErrorDetails,
    hint: Optional[str],
    from_graphql: bool = False,
):
    return errors.InvalidValueError(
        translate_pgtype(schema, err_details.message,
                         from_graphql=from_graphql)
    )


@interpret_by_code.register(pgerrors.ERROR_NUMERIC_VALUE_OUT_OF_RANGE)
def _interpret_numeric_out_of_range(
    code: str,
    schema: s_schema.Schema,
    err_details: ErrorDetails,
    hint: Optional[str],
    from_graphql: bool = False,
):
    return errors.NumericOutOfRangeError(
        translate_pgtype(schema, err_details.message,
                         from_graphql=from_graphql)
    )


@interpret_by_code.register(pgerrors.ERROR_INVALID_DATETIME_FORMAT)
@interpret_by_code.register(pgerrors.ERROR_DATETIME_FIELD_OVERFLOW)
def _interpret_invalid_datetime(
    code: str,
    schema: s_schema.Schema,
    err_details: ErrorDetails,
    hint: Optional[str],
    from_graphql: bool = False,
):
    return errors.InvalidValueError(
        translate_pgtype(schema, err_details.message,
                         from_graphql=from_graphql),
        hint=hint,
    )


@interpret_by_code.register(pgerrors.ERROR_WRONG_OBJECT_TYPE)
def _interpret_wrong_object_type(
    code: str,
    schema: s_schema.Schema,
    err_details: ErrorDetails,
    hint: Optional[str],
    from_graphql: bool = False,
):
    # NOTE: this should never occur in GraphQL mode due to schema/query
    # validation.

    if (
        err_details.message == 'covariance error' and
        err_details.column_name is not None and
        err_details.table_name is not None
    ):
        ptr = schema.get_by_id(uuidgen.UUID(err_details.column_name))
        wrong_obj = schema.get_by_id(uuidgen.UUID(err_details.table_name))
        assert isinstance(ptr, (s_pointers.Pointer, s_pointers.PseudoPointer))
        target = ptr.get_target(schema)
        assert target is not None

        vn = ptr.get_verbosename(schema, with_parent=True)
        return errors.InvalidLinkTargetError(
            f"invalid target for {vn}: '{wrong_obj.get_name(schema)}'"
            f" (expecting '{target.get_name(schema)}')"
        )

    return errors.InternalServerError(err_details.message)


def static_interpret_psql_parse_error(
    exc: parser_errors.PSqlParseError
) -> errors.EdgeDBError:
    res: errors.EdgeDBError
    if isinstance(exc, parser_errors.PSqlSyntaxError):
        res = errors.EdgeQLSyntaxError(str(exc))
        res.set_position(exc.lineno, 0, exc.cursorpos - 1, None)
    elif isinstance(exc, parser_errors.PSqlUnsupportedError):
        res = errors.UnsupportedFeatureError(str(exc))
    else:
        res = errors.InternalServerError(str(exc))

    return res
