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

ERROR_FEATURE_NOT_SUPPORTED = '0A000'

ERROR_CARDINALITY_VIOLATION = '21000'

# Class 22 — Data Exception
ERROR_NUMERIC_VALUE_OUT_OF_RANGE = '22003'
ERROR_INVALID_DATETIME_FORMAT = '22007'
ERROR_DATETIME_FIELD_OVERFLOW = '22008'
ERROR_DIVISION_BY_ZERO = '22012'
ERROR_INTERVAL_FIELD_OVERFLOW = '22015'
ERROR_INVALID_PARAMETER_VALUE = '22023'
ERROR_INVALID_TEXT_REPRESENTATION = '22P02'

# Class 23 — Integrity Constraint Violation
ERROR_INTEGRITY_CONSTRAINT_VIOLATION = '23000'
ERROR_RESTRICT_VIOLATION = '23001'
ERROR_NOT_NULL_VIOLATION = '23502'
ERROR_FOREIGN_KEY_VIOLATION = '23503'
ERROR_UNIQUE_VIOLATION = '23505'
ERROR_CHECK_VIOLATION = '23514'
ERROR_EXCLUSION_VIOLATION = '23P01'

# Class 25 - Invalid Transaction State
ERROR_IDLE_IN_TRANSACTION_TIMEOUT = '25P03'
ERROR_READ_ONLY_SQL_TRANSACTION = '25006'

ERROR_INVALID_AUTHORIZATION_SPECIFICATION = '28000'
ERROR_INVALID_PASSWORD = '28P01'

ERROR_INVALID_CATALOG_NAME = '3D000'

ERROR_SERIALIZATION_FAILURE = '40001'
ERROR_DEADLOCK_DETECTED = '40P01'

ERROR_WRONG_OBJECT_TYPE = '42809'
ERROR_DUPLICATE_DATABASE = '42P04'

ERROR_OBJECT_IN_USE = '55006'

ERROR_QUERY_CANCELLED = '57014'
ERROR_CANNOT_CONNECT_NOW = '57P03'


class BackendError(Exception):

    def __init__(self, *, fields):
        msg = fields.get('M', f'error code {fields["C"]}')
        self.fields = fields
        super().__init__(msg)

    def code_is(self, code: str) -> bool:
        return self.fields["C"] == code


class BackendQueryCancelledError(BackendError):
    pass
