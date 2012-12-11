##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.functional import cachedproperty, checktypes


__all__ = ['Point', 'Rectangle']


class Point:
    __slots__ = ('x', 'y',)

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __eq__(self, other):
        return (self.x, self.y) == (other.x, other.y)

    def __hash__(self):
        return hash((Point, self.x, self.y))

    def __str__(self):
        return 'Point(%s, %s)' % (self.x, self.y,)

    __repr__ = __str__


@checktypes
class Rectangle:
    def __init__(self, width, height, position:Point=None, *, data=None, children=None):
        self.width = width
        self.height = height
        self.position = position if position is not None else Point(0, 0)
        self.data = data
        self._children = set(children) if children is not None else set()

    @property
    def top_left(self):
        return Point(self.position.x, self.position.y)

    @property
    def top_right(self):
        return Point(self.position.x + self.width, self.position.y)

    @property
    def bottom_left(self):
        return Point(self.position.x, self.position.y + self.height)

    @property
    def bottom_right(self):
        return Point(self.position.x + self.width, self.position.y + self.height)

    @property
    def area(self):
        return self.width * self.height

    @property
    def children(self):
        return self._children

    @children.setter
    def children(self, children):
        self._children = set(children)

    def add_child(self, child):
        self._children.add(child)

    def remove_child(self, child):
        self._children.remove(child)

    def move(self, point:Point):
        x_diff, y_diff = point.x - self.position.x, point.y - self.position.y
        return Rectangle(self.width, self.height, point,
                         data=self.data,
                         children=(c.move_relative(x_diff, y_diff) for c in self.children))

    def move_relative(self, x_diff, y_diff):
        return self.move(Point(self.position.x + x_diff, self.position.y + y_diff))

    def encloses(self, other):
        return (other.top_left.x >= self.top_left.x and
                other.top_left.y > self.top_left.y and
                other.bottom_right.x <= self.bottom_right.x and
                other.bottom_right.y <= self.bottom_right.y)

    def __eq__(self, other): # should I take children into account?
        return (self.position == other.position and
                self.width == other.width and
                self.height == other.height)

    def __hash__(self):
        return hash((Rectangle, self.position, self.width, self.height))

    def __str__(self):
        return 'Rectangle(%sx%s at [%s, %s])' % (self.width, self.height,
                                                 self.position.x, self.position.y)

    __repr__ = __str__
