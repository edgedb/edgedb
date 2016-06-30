##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import abc
import numbers
import uuid

from edgedb.lang.common import datetime
from edgedb.lang.common.gis import proto as gis_proto


class Literal(metaclass=abc.ABCMeta):
    pass


Literal.register(numbers.Number)
Literal.register(uuid.UUID)
Literal.register(str)
Literal.register(bytes)
Literal.register(datetime.DateTime)
Literal.register(datetime.Date)
Literal.register(datetime.TimeDelta)
Literal.register(datetime.Time)
Literal.register(type(None))
Literal.register(gis_proto.geometry.Geometry)
