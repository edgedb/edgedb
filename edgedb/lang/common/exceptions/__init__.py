#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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
