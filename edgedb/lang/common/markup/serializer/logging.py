##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import logging

from . import base


@base.serializer(handles=logging.LogRecord)
def serialize_logging_record(obj, *, ctx):
    return base._serialize_known_object(obj,
                                        (attr for attr in dir(obj) if not attr.startswith('_')),
                                        ctx=ctx)
