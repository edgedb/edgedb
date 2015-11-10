##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Implementation of continuous interval"""

import functools
import operator


__all__ = 'Interval',


# Smallest
class _Smallest:
    def __neg__(self):
        return Largest

    def __lt__(self, other):
        return other is not self

    def __le__(self, other):
        return True

    def __gt__(self, other):
        return False

    def __ge__(self, other):
        return other is self

    def __eq__(self, other):
        return other is self

    def __ne__(self, other):
        return other is not self

Smallest = _Smallest()


# Largest
class _Largest:
    def __neg__(self):
        return Smallest

    def __lt__(self, other):
        return False

    def __le__(self, other):
        return other is self

    def __gt__(self, other):
        return other is not self

    def __ge__(self, other):
        return True

    def __eq__(self, other):
        return other is self

    def __ne__(self, other):
        return other is not self

Largest = _Largest()


class Interval:
    __slots__ = 'lower', 'within_lower', 'closed_lower', 'upper', 'within_upper', 'closed_upper'

    def __init__(self, lower=Smallest, upper=Largest, closed_lower=True, closed_upper=True):
        self.lower = lower
        lower_op = operator.ge if closed_lower else operator.gt
        self.within_lower = functools.partial(lower_op, lower)
        self.closed_lower = closed_lower

        self.upper = upper
        upper_op = operator.le if closed_lower else operator.lt
        self.within_upper = functools.partial(upper_op, upper)
        self.closed_upper = closed_upper

    def __bool__(self):
        """False if interval is empty, True otherwise."""
        return (self.closed_lower and self.closed_upper) or self.lower != self.upper

    def __contains__(self, other):
        """Check if other Interval is fully contained within this Interval"""

        if not isinstance(other, Interval):
            return NotImplemented
        else:
            return self.within_lower(other.lower) and self.within_upper(other.upper)
