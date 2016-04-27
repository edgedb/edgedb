##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.datastructures import typed

from . import literal


class ExpressionText(str):
    pass


class ExpressionList(typed.TypedList, type=literal.Literal):
    @classmethod
    def merge_values(cls, ours, theirs, schema):
        if not ours:
            if theirs:
                ours = theirs[:]
        elif theirs and isinstance(ours[-1], ExpressionText):
            ours.extend(theirs)

        return ours
