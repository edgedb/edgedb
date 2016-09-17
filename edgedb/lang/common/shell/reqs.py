##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common.exceptions import EdgeDBError


class UnsatisfiedRequirementError(EdgeDBError):
    pass


class CommandRequirement:
    pass
