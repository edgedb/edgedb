##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import math

from .minimize_impl.qm import minimize


def ints_to_terms(*args):
    size = max(args) + 1
    numargs = int(math.ceil(math.log(size, 2)))

    result = [tuple([1 if arg & 1 << i else 0 for i in range(numargs)]) for arg in args]
    return result


def terms_to_ints(terms):
    result = [sum(1 << i for i, bit in enumerate(term) if bit) for term in terms]
    return result
