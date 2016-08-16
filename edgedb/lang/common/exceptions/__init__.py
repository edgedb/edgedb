##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from ._base import *  # NOQA


class EdgeDBBackendError(EdgeDBError):
    code = '23000'


class IntergrityConstraintViolationError(EdgeDBError):
    code = '23000'


class MissingRequiredPointerError(IntergrityConstraintViolationError):
    code = '23502'

    def __init__(self, msg, *, source_name=None, pointer_name=None):
        super().__init__(msg)
        self._attrs['s'] = source_name
        self._attrs['p'] = pointer_name


class UniqueConstraintViolationError(IntergrityConstraintViolationError):
    code = '23505'


class ConstraintViolationError(IntergrityConstraintViolationError):
    code = '23514'


class EdgeDBSyntaxError(EdgeDBError):
    code = '42600'
