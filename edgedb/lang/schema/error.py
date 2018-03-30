##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.exceptions import EdgeDBError, EdgeDBSyntaxError
from edgedb.lang.common.parsing import ParserError


class SchemaSyntaxError(ParserError, EdgeDBSyntaxError):
    pass


class SchemaError(EdgeDBError):
    code = '32000'


class SchemaNameError(SchemaError):
    pass


class NoObjectError(SchemaError):
    pass


class SchemaDefinitionError(SchemaError):
    code = '32100'


class InvalidConstraintDefinitionError(SchemaDefinitionError):
    code = '32101'
