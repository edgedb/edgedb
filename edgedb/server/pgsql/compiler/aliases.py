##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import compiler
from edgedb.server.pgsql import common


class AliasGenerator(compiler.AliasGenerator):
    def get(self, hint=None):
        alias = super().get(hint)
        return common.edgedb_name_to_pg_name(alias)
