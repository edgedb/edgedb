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
