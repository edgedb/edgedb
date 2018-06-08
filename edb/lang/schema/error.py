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


from edb.lang.common.exceptions import EdgeDBError, EdgeDBSyntaxError
from edb.lang.common.parsing import ParserError


class SchemaSyntaxError(ParserError, EdgeDBSyntaxError):
    pass


class SchemaError(EdgeDBError):
    code = '32000'


class SchemaNameError(SchemaError):
    pass


class SchemaModuleNotFoundError(SchemaError):
    pass


class ItemNotFoundError(SchemaError):
    pass


class SchemaDefinitionError(SchemaError):
    code = '32100'


class InvalidConstraintDefinitionError(SchemaDefinitionError):
    code = '32101'
