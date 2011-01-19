##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


def levenshtein_distance(s, t):
    """Calculates Levenshtein distance between s and t."""

    m, n = len(s), len(t)

    if m > n:
        s, t = t, s
        m, n = n, m

    ri = list(range(m + 1))

    for i in range(1, n + 1):
        ri_1, ri = ri, [i] + [0] * m

        for j in range(1, m + 1):
            ri[j] = min(ri_1[j] + 1, ri[j - 1] + 1, ri_1[j - 1] + int(s[j - 1] != t[i - 1]))

    return ri[m]
