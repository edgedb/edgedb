##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common.algos.packing.rectangles import Point, Rectangle


class TestRectangles:
    def test_rectangles_points(self):
        point = Point(128, 64)
        assert str(point) == 'Point(128, 64)'
        assert Point(128, 64) == Point(128, 64)

    def test_rectangles_basic(self):
        rect = Rectangle(50, 50)
        assert str(rect) == 'Rectangle(50x50 at [0, 0])'
        assert rect == Rectangle(50, 50)
        assert rect != Rectangle(50, 50, Point(1, 1))

    def test_rectangles_properties(self):
        rect = Rectangle(67, 64, Point(14, 12))
        assert rect.area == 67 * 64
        assert rect.top_left == rect.position == Point(14, 12)
        assert rect.top_right == Point(81, 12)
        assert rect.bottom_left == Point(14, 76)
        assert rect.bottom_right == Point(81, 76)

    def test_rectangles_methods1(self):
        rect = Rectangle(67, 64, Point(14, 12))
        assert rect.encloses(Rectangle(10, 10, Point(20, 20)))
        assert not rect.encloses(Rectangle(80, 10, Point(20, 20)))

    def test_rectangles_data1(self):
        rect = Rectangle(10, 10, Point(1, 1), data='rect1')
        rect.add_child(Rectangle(10, 10, data='child_rect1'))
        rect.add_child(Rectangle(100, 10, data='child_rect2'))
        rect = rect.move(Point(10, 20))
        assert rect.data == 'rect1'
        assert {c.data
                for c in rect.children} == {'child_rect1', 'child_rect2'}

    def test_rectangles_children(self):
        children = (Rectangle(20, 20, Point(1, 2)), Rectangle(1, 1))
        rect = Rectangle(10, 20, children=children)
        assert rect.children == set(children)

    def test_rectangles_moving1(self):
        rect = Rectangle(50, 50)
        assert rect.move(Point(1, 1)) == Rectangle(50, 50, Point(1, 1))

    def test_rectangles_moving2(self):
        children = (
            Rectangle(20, 20, Point(1, 2)), Rectangle(1, 1, Point(1, 1)))
        rect = Rectangle(100, 100, Point(1, 1), children=children)
        rect = rect.move(Point(20, 40))
        assert rect == Rectangle(100, 100, Point(20, 40))
        assert rect.children == set(
            [Rectangle(20, 20, Point(20, 41)), Rectangle(1, 1, Point(20, 40))])
