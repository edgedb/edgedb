##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from ._base import *  # NOQA


class InternalError(EdgeDBError):
    code = 'XX000'


class EdgeDBBackendError(InternalError):
    code = 'XX001'


class IntergrityConstraintViolationError(EdgeDBError):
    code = '23000'


class MissingRequiredPointerError(IntergrityConstraintViolationError):
    code = '23502'

    def __init__(self, msg, *, source_name=None, pointer_name=None):
        super().__init__(msg)
        self._attrs['s'] = source_name
        self._attrs['p'] = pointer_name


class ConstraintViolationError(IntergrityConstraintViolationError):
    code = '23514'


class LinkMappingCardinalityViolationError(IntergrityConstraintViolationError):
    code = '23600'


class EdgeDBSyntaxError(EdgeDBError):
    code = '42600'


class InvalidTransactionStateError(EdgeDBError):
    code = '25000'


class NoActiveTransactionError(InvalidTransactionStateError):
    code = '25P01'
