##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import SemantixError

from . import ast


class CaosQLError(SemantixError):
    pass


class CaosQLQueryError(CaosQLError):
    pass
