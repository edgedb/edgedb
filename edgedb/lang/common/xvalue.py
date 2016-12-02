##
# Copyright (c) 2008-2012, 2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


__all__ = 'xvalue',


class xvalue:
    """A "rich" value with an arbitrary set of additional attributes."""

    __slots__ = ('value', 'attrs')

    def __init__(self, value, **attrs):
        self.value = value
        self.attrs = attrs

    def __repr__(self):
        attrs = ', '.join('%s=%r' % (k, v) for k, v in self.attrs.items())
        return '<xvalue "%r"; %s>' % (self.value, attrs)

    def __eq__(self, other):
        if not isinstance(other, xvalue):
            return NotImplemented

        return self.value == other.value and self.attrs == other.attrs

    def __hash__(self):
        return hash((self.value, frozenset(self.attrs.items())))

    __str__ = __repr__
