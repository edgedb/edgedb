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


class UniqueConstraintViolationError(IntergrityConstraintViolationError):
    code = '23505'


class ConstraintViolationError(IntergrityConstraintViolationError):
    code = '23514'


class EdgeDBSyntaxError(EdgeDBError):
    code = '42600'


class EdgeQLError(EdgeDBSyntaxError):
    code = '42601'


class EdgeQLSyntaxError(EdgeDBSyntaxError):
    code = '42602'


__all__ = _base.__all__ + (
    'IntergrityConstraintViolationError',
    'MissingRequiredPointerError',
    'UniqueConstraintViolationError',
    'ConstraintViolationError',
    'EdgeDBSyntaxError',
    'EdgeQLError',
    'EdgeQLSyntaxError'
)
