##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import pytest
from metamagic.utils.debug import assert_raises

from metamagic.utils.algos.packing.rectangles import *


class TestRectanglePacking:
    def test_rectangle_packing_cygon1(self):
        packer = CygonRectanglePacker(Rectangle(31, 31))
        rects = {packer.pack(Rectangle(10, 10)) for i in range(9)}
        assert rects == {Rectangle(10, 10, p) for p in (Point(0, 0),
                                                        Point(10, 0), Point(20, 0),
                                                        Point(0, 10), Point(10, 10),
                                                        Point(20, 10), Point(0, 20),
                                                        Point(10, 20), Point(20, 20))}

        with assert_raises(RectanglePackingError):
            packer.pack(Rectangle(10, 10))

    def test_rectangle_packing1(self):
        packed = pack_rectangles(Rectangle(w, h) for w, h in
                                 [(235, 122), (117, 61), (58, 30), (29, 15), (14, 7)])

    def test_rectangle_packing2(self):
        rect = Rectangle(100, 200)
        packed = pack_rectangles([rect])
        assert packed.width == rect.width and packed.height == rect.height
        assert packed.children == {rect}

    def test_rectangle_packing3(self):
        with assert_raises(ValueError, error_re='no rectangles to pack'):
            pack_rectangles([])
