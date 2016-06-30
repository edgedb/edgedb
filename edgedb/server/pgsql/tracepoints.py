##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.tracepoints import Trace


class Query(Trace):
    caption = 'edgedb.pgsql.query'
    merge_descendants = True
    merge_same_id_only = True


class ResultUnpack(Trace):
    caption = 'edgedb.pgsql.unpack'
    merge_descendants = True
