##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import compiler
from edgedb.server.pgsql import common


class Alias(str):
    def __new__(cls, value=''):
        return super(Alias, cls).__new__(
            cls, common.edgedb_name_to_pg_name(value))


class AliasGenerator(compiler.AliasGenerator):
    def get(self, hint=None):
        alias = super().get(hint)
        return Alias(alias)
