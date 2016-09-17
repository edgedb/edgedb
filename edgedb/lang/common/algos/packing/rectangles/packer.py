##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import operator
import math
import itertools
from bisect import bisect_left
from .base import Point as BasePoint, Rectangle

__all__ = (
    'RectanglePacker', 'CygonRectanglePacker', 'RectanglePackingError',
    'pack_rectangles'
)


class RectanglePackingError(Exception):
    def __init__(self):
        super().__init__('Rectangle does not fit in the packing area')


class Point(BasePoint):
    def __lt__(self, other):
        return self.x < other.x  # starting position of height slices


class RectanglePacker:
    def __init__(self, packing_area):
        self.packing_area = packing_area

    def pack(self, rect):
        raise NotImplementedError

    @classmethod
    def pack_rectangles(cls, packing_area, rectangles):
        packer = cls(packing_area)
        rectangles = sorted(
            rectangles, key=operator.attrgetter('area'), reverse=True)
        return [packer.pack(rect) for rect in rectangles]


class CygonRectanglePacker(RectanglePacker):
    def __init__(self, packing_area):
        super().__init__(packing_area)
        self.height_slices = [Point(0, 0)]

    def pack(self, rect):
        if (
                rect.width > self.packing_area.width or
                rect.height > self.packing_area.height):
            raise RectanglePackingError

        rect = self.place_rectangle(rect)
        self.integrate_rectangle(rect)

        return rect

    def place_rectangle(self, rect):
        best_slice_index = -1
        best_slice_y = 0

        best_score = self.packing_area.width * self.packing_area.height

        left_slice_index = 0

        right_slice_index = bisect_left(
            self.height_slices, Point(rect.width, 0))
        if right_slice_index < 0:
            right_slice_index = ~right_slice_index

        while right_slice_index <= len(self.height_slices):
            highest = self.height_slices[left_slice_index].y
            for index in range(left_slice_index + 1, right_slice_index):
                if self.height_slices[index].y > highest:
                    highest = self.height_slices[index].y

            if highest + rect.height < self.packing_area.height:
                score = highest

                if score < best_score:
                    best_slice_index = left_slice_index
                    best_slice_y = highest
                    best_score = score

            left_slice_index += 1
            if left_slice_index >= len(self.height_slices):
                break

            right_rectangle_end = self.height_slices[
                left_slice_index].x + rect.width
            while right_slice_index <= len(self.height_slices):
                if right_slice_index == len(self.height_slices):
                    right_slice_start = self.packing_area.width
                else:
                    right_slice_start = self.height_slices[right_slice_index].x

                if right_slice_start > right_rectangle_end:
                    break

                right_slice_index += 1

            if right_slice_index > len(self.height_slices):
                break

        if best_slice_index == -1:
            raise RectanglePackingError
        else:
            return rect.move(
                Point(self.height_slices[best_slice_index].x, best_slice_y))

    def integrate_rectangle(self, rect):
        left, right = rect.top_left.x, rect.top_right.x
        bottom = rect.bottom_left.y

        start_slice = bisect_left(self.height_slices, Point(left, 0))

        if start_slice >= 0:
            first_slice_original_height = self.height_slices[start_slice].y
            self.height_slices[start_slice] = Point(left, bottom)
        else:
            start_slice = ~start_slice
            first_slice_original_height = self.height_slices[start_slice - 1].y
            self.height_slices.insert(start_slice, Point(left, bottom))

        start_slice += 1

        if start_slice >= len(self.height_slices):
            if right < self.packing_area.width:
                self.height_slices.append(
                    Point(right, first_slice_original_height))
        else:
            end_slice = bisect_left(
                self.height_slices, Point(right, 0), start_slice,
                len(self.height_slices))

            if end_slice > 0:
                del self.height_slices[start_slice:end_slice]
            else:
                end_slice = ~end_slice

                if end_slice == start_slice:
                    return_height = first_slice_original_height
                else:
                    return_height = self.height_slices[end_slice - 1].y

                del self.height_slices[start_slice:end_slice]
                if right < self.packing_area.width:
                    self.height_slices.insert(
                        start_slice, Point(right, return_height))


def _greedy_pack(rectangles, min_height=None):
    rects_by_height = sorted(
        rectangles, key=operator.attrgetter('height'), reverse=True)

    if min_height is None:
        min_height = min(r.height for r in rects_by_height)
    min_width = 0

    def pairwise(iterable):
        a, b = itertools.tee(iterable)
        next(b, None)
        return zip(a, b)

    rect_pairs = iter(pairwise(rects_by_height))

    r1, r2 = next(rect_pairs)
    stack_height = r1.height
    stack_width = r1.width
    min_width = 0

    while True:
        stack_height += r2.height if r2 is not None else 0
        if stack_height > min_height:
            min_width += stack_width
            stack_height = r2.height
            stack_width = r2.width
        else:
            stack_width = max(stack_width, r2.width)

        try:
            r1, r2 = next(rect_pairs)
        except StopIteration:
            min_width += stack_width
            break

    return Rectangle(min_width, min_height, children=rects_by_height)


def pack_rectangles(
        rectangles, packer_cls=CygonRectanglePacker, *, increase=0.35):
    packed_rectangles = None
    rectangles = list(rectangles)

    if not rectangles:
        raise ValueError('no rectangles to pack')
    elif len(rectangles) == 1:
        best_bounding_rect = Rectangle(
            rectangles[0].width, rectangles[0].height, children=rectangles)
    else:
        min_possible_area = sum(r.area for r in rectangles)
        best_bounding_rect = bounding_rect = _greedy_pack(rectangles)
        width, height = bounding_rect.width, bounding_rect.height
        step = math.ceil(math.sqrt(bounding_rect.area) * increase)

        while width > height:
            min_width = _greedy_pack(rectangles, height).width
            bounding_rect = Rectangle(width, height)
            if bounding_rect.area < min_possible_area or width < min_width:
                height += step
            try:
                packed_rectangles = packer_cls.pack_rectangles(
                    bounding_rect, rectangles)
            except RectanglePackingError:
                height += step
            else:
                width -= step
                best_bounding_rect = bounding_rect

        best_bounding_rect.children = packed_rectangles

    return best_bounding_rect
