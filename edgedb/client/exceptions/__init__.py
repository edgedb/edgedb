##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from ._base import *  # NOQA
from . import _base


class IntegrityConstraintViolationError(_base.EdgeDBError):
    code = '23000'


class MissingRequiredPointerError(IntegrityConstraintViolationError):
    code = '23502'


class InvalidPointerTargetError(IntegrityConstraintViolationError):
    code = '23503'


class ConstraintViolationError(IntegrityConstraintViolationError):
    code = '23514'


class InvalidTransactionStateError(_base.EdgeDBError):
    code = '25000'


class NoActiveTransactionError(InvalidTransactionStateError):
    code = '25P01'


class SchemaError(_base.EdgeDBError):
    code = '32000'


class SchemaDefinitionError(SchemaError):
    code = '32100'


class InvalidConstraintDefinitionError(SchemaDefinitionError):
    code = '32101'


class EdgeDBLanguageError(_base.EdgeDBError):
    code = '42600'


class EdgeQLError(EdgeDBLanguageError):
    code = '42601'


class EdgeQLSyntaxError(EdgeQLError):
    code = '42602'


__all__ = _base.__all__ + (
    'IntegrityConstraintViolationError',
    'InvalidTransactionStateError',
    'NoActiveTransactionError',
    'MissingRequiredPointerError',
    'ConstraintViolationError',
    'EdgeDBLanguageError',
    'EdgeQLError',
    'EdgeQLSyntaxError'
)
