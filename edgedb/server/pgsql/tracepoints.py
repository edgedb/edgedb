##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.tracepoints import Trace


class Query(Trace):
    caption = 'caos.pgsql.query'


class ResultUnpack(Trace):
    caption = 'caos.pgsql.unpack'
