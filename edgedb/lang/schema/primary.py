##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.nlang import morphology

from . import objects as so
from . import referencing


class PrimaryClass(referencing.ReferencingClass):

    title = so.Field(morphology.WordCombination,
                     default=None, compcoef=0.909, coerce=True)

    description = so.Field(str, default=None, compcoef=0.909)
