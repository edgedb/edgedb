##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import nlang

from . import objects as so
from . import referencing


class PrimaryClass(referencing.ReferencingClass):

    title = so.Field(nlang.WordCombination,
                     default=None, compcoef=0.909, coerce=True)

    description = so.Field(str, default=None, compcoef=0.909)
