##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.tracepoints import Trace


class Query(Trace):
    caption = 'caos.pgsql.query'
    merge_descendants = True
    merge_same_id_only = True


class ResultUnpack(Trace):
    caption = 'caos.pgsql.unpack'
    merge_descendants = True
