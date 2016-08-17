##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from ._base import *  # NOQA
from . import _base


class IntergrityConstraintViolationError(EdgeDBError):
    code = '23000'


class MissingRequiredPointerError(IntergrityConstraintViolationError):
    code = '23502'


class ConstraintViolationError(IntergrityConstraintViolationError):
    code = '23514'


class InvalidTransactionStateError(EdgeDBError):
    code = '25000'


class NoActiveTransactionError(InvalidTransactionStateError):
    code = '25P01'


class EdgeDBSyntaxError(EdgeDBError):
    code = '42600'


class EdgeQLError(EdgeDBSyntaxError):
    code = '42601'


class EdgeQLSyntaxError(EdgeDBSyntaxError):
    code = '42602'


__all__ = _base.__all__ + (
    'IntergrityConstraintViolationError',
    'InvalidTransactionStateError',
    'NoActiveTransactionError',
    'MissingRequiredPointerError',
    'ConstraintViolationError',
    'EdgeDBSyntaxError',
    'EdgeQLError',
    'EdgeQLSyntaxError'
)
