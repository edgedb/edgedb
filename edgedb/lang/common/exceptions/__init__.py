##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from . import _base
from ._base import *  # NOQA


class InternalError(_base.EdgeDBError):
    code = 'XX000'


class EdgeDBBackendError(InternalError):
    code = 'XX001'


class IntegrityConstraintViolationError(_base.EdgeDBError):
    code = '23000'


class MissingRequiredPointerError(IntegrityConstraintViolationError):
    code = '23502'

    def __init__(self, msg, *, source_name=None, pointer_name=None):
        super().__init__(msg)
        self._attrs['s'] = source_name
        self._attrs['p'] = pointer_name


class InvalidPointerTargetError(IntegrityConstraintViolationError):
    code = '23503'

    def __init__(self, msg):
        super().__init__(msg)


class ConstraintViolationError(IntegrityConstraintViolationError):
    code = '23514'


class PointerCardinalityViolationError(IntegrityConstraintViolationError):
    code = '23600'


class EdgeDBSyntaxError(_base.EdgeDBError):
    code = '42600'


class InvalidTransactionStateError(_base.EdgeDBError):
    code = '25000'


class NoActiveTransactionError(InvalidTransactionStateError):
    code = '25P01'
