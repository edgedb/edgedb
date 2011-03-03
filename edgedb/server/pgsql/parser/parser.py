##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import debug, parsing
from .error import PgSQLParserError


class PgSQLParser(parsing.Parser):
    def get_parser_spec_module(self):
        from . import pgsql
        return pgsql

    def get_debug(self):
        return 'caos.pgsql.parser' in debug.channels

    def get_exception(self, native_err, context):
        return PgSQLParserError(native_err.args[0], context=context)
