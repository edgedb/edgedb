##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.datastructures import typed

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


class ExpressionDict(typed.TypedDict, keytype=str, valuetype=literal.Literal):
    @classmethod
    def compare_values(cls, ours, theirs, context, compcoef):
        if not ours and not theirs:
            basecoef = 1.0
        elif not ours or not theirs:
            basecoef = 0.2
        else:
            similarity = []

            for k, v in ours.items():
                try:
                    theirsv = theirs[k]
                except KeyError:
                    # key only in ours
                    similarity.append(0.2)
                else:
                    similarity.append(1.0 if v == theirsv else 0.4)

            similarity.extend(0.2 for k in set(theirs) - set(ours))
            basecoef = sum(similarity) / len(similarity)

        return basecoef + (1 - basecoef) * compcoef
