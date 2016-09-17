##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common.algos.packing.rectangles import *  # NOQA


class TestRectanglePacking:
    def test_rectangle_packing_cygon1(self):
        packer = CygonRectanglePacker(Rectangle(31, 31))
        rects = {packer.pack(Rectangle(10, 10)) for i in range(9)}
        assert rects == {
            Rectangle(10, 10, p)
            for p in (
                Point(0, 0), Point(10, 0), Point(20, 0), Point(0, 10), Point(
                    10, 10), Point(20, 10), Point(0, 20), Point(10, 20), Point(
                        20, 20))
        }

        with self.assertRaises(RectanglePackingError):
            packer.pack(Rectangle(10, 10))

    def test_rectangle_packing1(self):
        pack_rectangles(
            Rectangle(w, h)
            for w, h in [(235, 122), (117, 61), (58, 30), (29, 15), (14, 7)])

    def test_rectangle_packing2(self):
        rect = Rectangle(100, 200)
        packed = pack_rectangles([rect])
        assert packed.width == rect.width and packed.height == rect.height
        assert packed.children == {rect}

    def test_rectangle_packing3(self):
        with self.assertRaisesRegex(ValueError, 'no rectangles to pack'):
            pack_rectangles([])
