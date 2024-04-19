#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""An extension to standard library module :mod:`colorsys`.

Contains additional functions, with the most notable - :func:`rgb_distance`.
"""

from __future__ import annotations

from math import sqrt as _sqrt
from colorsys import (
    rgb_to_yiq, yiq_to_rgb, rgb_to_hls, hls_to_rgb, rgb_to_hsv, hsv_to_rgb
)


__all__ = 'rgb_to_yiq', 'yiq_to_rgb', 'rgb_to_hls', 'hls_to_rgb', \
          'rgb_to_hsv', 'hsv_to_rgb', 'rgb_to_xyz', 'xyz_to_lab', \
          'rgb_distance', 'Color'


class Color:
    colors = {
        'aliceblue': '#f0f8ff',
        'antiquewhite': '#faebd7',
        'aqua': '#00ffff',
        'aquamarine': '#7fffd4',
        'azure': '#f0ffff',
        'beige': '#f5f5dc',
        'bisque': '#ffe4c4',
        'black': '#000000',
        'blanchedalmond': '#ffebcd',
        'blue': '#0000ff',
        'blueviolet': '#8a2be2',
        'brown': '#a52a2a',
        'burlywood': '#deb887',
        'cadetblue': '#5f9ea0',
        'chartreuse': '#7fff00',
        'chocolate': '#d2691e',
        'coral': '#ff7f50',
        'cornflowerblue': '#6495ed',
        'cornsilk': '#fff8dc',
        'crimson': '#dc143c',
        'cyan': '#00ffff',
        'darkblue': '#00008b',
        'darkcyan': '#008b8b',
        'darkgoldenrod': '#b8860b',
        'darkgray': '#a9a9a9',
        'darkgreen': '#006400',
        'darkkhaki': '#bdb76b',
        'darkmagenta': '#8b008b',
        'darkolivegreen': '#556b2f',
        'darkorange': '#ff8c00',
        'darkorchid': '#9932cc',
        'darkred': '#8b0000',
        'darksalmon': '#e9967a',
        'darkseagreen': '#8fbc8f',
        'darkslateblue': '#483d8b',
        'darkslategray': '#2f4f4f',
        'darkturquoise': '#00ced1',
        'darkviolet': '#9400d3',
        'deeppink': '#ff1493',
        'deepskyblue': '#00bfff',
        'dimgray': '#696969',
        'dodgerblue': '#1e90ff',
        'firebrick': '#b22222',
        'floralwhite': '#fffaf0',
        'forestgreen': '#228b22',
        'fuchsia': '#ff00ff',
        'gainsboro': '#dcdcdc',
        'ghostwhite': '#f8f8ff',
        'gold': '#ffd700',
        'goldenrod': '#daa520',
        'gray': '#808080',
        'green': '#008000',
        'greenyellow': '#adff2f',
        'honeydew': '#f0fff0',
        'hotpink': '#ff69b4',
        'indianred': '#cd5c5c',
        'indigo': '#4b0082',
        'ivory': '#fffff0',
        'khaki': '#f0e68c',
        'lavender': '#e6e6fa',
        'lavenderblush': '#fff0f5',
        'lawngreen': '#7cfc00',
        'lemonchiffon': '#fffacd',
        'lightblue': '#add8e6',
        'lightcoral': '#f08080',
        'lightcyan': '#e0ffff',
        'lightgoldenrodyellow': '#fafad2',
        'lightgreen': '#90ee90',
        'lightgrey': '#d3d3d3',
        'lightpink': '#ffb6c1',
        'lightsalmon': '#ffa07a',
        'lightseagreen': '#20b2aa',
        'lightskyblue': '#87cefa',
        'lightslategray': '#778899',
        'lightsteelblue': '#b0c4de',
        'lightyellow': '#ffffe0',
        'lime': '#00ff00',
        'limegreen': '#32cd32',
        'linen': '#faf0e6',
        'magenta': '#ff00ff',
        'maroon': '#800000',
        'mediumaquamarine': '#66cdaa',
        'mediumblue': '#0000cd',
        'mediumorchid': '#ba55d3',
        'mediumpurple': '#9370db',
        'mediumseagreen': '#3cb371',
        'mediumslateblue': '#7b68ee',
        'mediumspringgreen': '#00fa9a',
        'mediumturquoise': '#48d1cc',
        'mediumvioletred': '#c71585',
        'midnightblue': '#191970',
        'mintcream': '#f5fffa',
        'mistyrose': '#ffe4e1',
        'moccasin': '#ffe4b5',
        'navajowhite': '#ffdead',
        'navy': '#000080',
        'oldlace': '#fdf5e6',
        'olive': '#808000',
        'olivedrab': '#6b8e23',
        'orange': '#ffa500',
        'orangered': '#ff4500',
        'orchid': '#da70d6',
        'palegoldenrod': '#eee8aa',
        'palegreen': '#98fb98',
        'paleturquoise': '#afeeee',
        'palevioletred': '#db7093',
        'papayawhip': '#ffefd5',
        'peachpuff': '#ffdab9',
        'peru': '#cd853f',
        'pink': '#ffc0cb',
        'plum': '#dda0dd',
        'powderblue': '#b0e0e6',
        'purple': '#800080',
        'red': '#ff0000',
        'rosybrown': '#bc8f8f',
        'royalblue': '#4169e1',
        'saddlebrown': '#8b4513',
        'salmon': '#fa8072',
        'sandybrown': '#f4a460',
        'seagreen': '#2e8b57',
        'seashell': '#fff5ee',
        'sienna': '#a0522d',
        'silver': '#c0c0c0',
        'skyblue': '#87ceeb',
        'slateblue': '#6a5acd',
        'slategray': '#708090',
        'snow': '#fffafa',
        'springgreen': '#00ff7f',
        'steelblue': '#4682b4',
        'tan': '#d2b48c',
        'teal': '#008080',
        'thistle': '#d8bfd8',
        'tomato': '#ff6347',
        'turquoise': '#40e0d0',
        'violet': '#ee82ee',
        'wheat': '#f5deb3',
        'white': '#ffffff',
        'whitesmoke': '#f5f5f5',
        'yellow': '#ffff00',
        'yellowgreen': '#9acd32'
    }

    def __init__(self, r, g, b, a=1.0):
        r = int(r)
        g = int(g)
        b = int(b)
        a = float(a)
        if r > 255 or r < 0 or g < 0 or g > 255 or b < 0 or b > 255:
            raise ValueError('color component should belong to [0, 255]')
        if a < 0 or a > 1:
            raise ValueError('alpha component should belong to [0, 1]')
        self.r, self.g, self.b, self.a = r, g, b, a

    @classmethod
    def from_color(cls, color):
        return cls(color.r, color.g, color.b, color.a)

    @classmethod
    def from_string(cls, value, alpha=1.0):
        if not value.startswith('#'):
            if value == 'transparent':
                return cls(0, 0, 0, 0)
            else:
                try:
                    value = cls.colors[str(value)]
                except KeyError:
                    raise ValueError('Unknown color name')
        value = value[1:]
        try:
            if len(value) == 3:
                r, g, b = [int(x * 2, 16) for x in value]
            elif len(value) == 6:
                r, g, b = [int(value[i:i + 2], 16) for i in range(0, 6, 2)]
            else:
                raise ValueError
        except ValueError:
            raise ValueError('Invalid color value')
        return cls(r, g, b, a=alpha)

    @classmethod
    def from_hls(cls, h, l, s, alpha=1.0):  # NoQA: E741
        return cls(*(int(c * 255) for c in hls_to_rgb(h, l, s)), a=alpha)

    def rgb_channels(self, *, as_floats=False):
        if as_floats:
            return (self.r / 255.0, self.g / 255.0, self.b / 255.0)
        else:
            return (self.r, self.g, self.b)

    def rgba_channels(self, *, as_floats=False):
        if as_floats:
            return (self.r / 255.0, self.g / 255.0, self.b / 255.0, self.a)
        else:
            return (self.r, self.g, self.b, self.a)

    def hls_channels(self):
        return rgb_to_hls(*(c / 255 for c in self.rgb_channels()))


# Relative to RGB max white
XYZ_MAX_X = 95.047
XYZ_MAX_Y = 100.0
XYZ_MAX_Z = 108.883


def rgb_to_xyz(r, g, b):
    """Converts RGB color to XYZ

    :param float r: Red value in ``0..1`` range
    :param float g: Green value in ``0..1`` range
    :param float b: Blue value in ``0..1`` range
    :returns: ``(x, y, z)``, all values normalized to
              the ``(0..1, 0..1, 0..1)`` range
    """

    # Formulae from http://www.easyrgb.com/index.php?X=MATH

    if r > 0.04045:
        r = ((r + 0.055) / 1.055) ** 2.4
    else:
        r /= 12.92

    if g > 0.04045:
        g = ((g + 0.055) / 1.055) ** 2.4
    else:
        g /= 12.92

    if b > 0.04045:
        b = ((b + 0.055) / 1.055) ** 2.4
    else:
        b /= 12.92

    r *= 100.0
    g *= 100.0
    b *= 100.0

    x = min((r * 0.4124 + g * 0.3576 + b * 0.1805) / XYZ_MAX_X, 1.0)
    y = min((r * 0.2126 + g * 0.7152 + b * 0.0722) / XYZ_MAX_Y, 1.0)
    z = min((r * 0.0193 + g * 0.1192 + b * 0.9505) / XYZ_MAX_Z, 1.0)

    return (x, y, z)


_1_3 = 1.0 / 3.0
_16_116 = 16.0 / 116.0


def xyz_to_lab(x, y, z):
    """Converts XYZ color to LAB

    :param float x: Value from ``0..1``
    :param float y: Value from ``0..1``
    :param float z: Value from ``0..1``
    :returns: ``(L, a, b)``, values in
              range ``(0..100, -127..128, -127..128)``
    """

    # Formulae from http://www.easyrgb.com/index.php?X=MATH

    if x > 0.008856:
        x **= _1_3
    else:
        x = (7.787 * x) + _16_116

    if y > 0.008856:
        y **= _1_3
    else:
        y = (7.787 * y) + _16_116

    if z > 0.008856:
        z **= _1_3
    else:
        z = (7.787 * z) + _16_116

    lum = 116.0 * y - 16.0
    a = 500 * (x - y)
    b = 200 * (y - z)

    return (lum, a, b)


def rgb_distance(r1, g1, b1, r2, g2, b2):
    """Calculates numerical distance between two colors in RGB color space.

    The distance is calculated by CIE94 formula.

    :params: Two colors with ``r, g, b`` values in ``0..1`` range
    :returns: A number in ``0..100`` range.  The lesser - the
              closer colors are.
    """

    # Formulae from wikipedia article re CIE94

    L1, A1, B1 = xyz_to_lab(*rgb_to_xyz(r1, b1, g1))
    L2, A2, B2 = xyz_to_lab(*rgb_to_xyz(r2, b2, g2))

    dL = L1 - L2
    C1 = _sqrt(A1 * A1 + B1 * B1)
    C2 = _sqrt(A2 * A2 + B2 * B2)
    dCab = C1 - C2
    dA = A1 - A2
    dB = B1 - B2

    dEab = _sqrt(dL ** 2 + dA ** 2 + dB ** 2)

    dHab = _sqrt(max(dEab ** 2 - dL ** 2 - dCab ** 2, 0.0))

    dE = _sqrt((dL ** 2) + ((dCab / (1 + 0.045 * C1)) ** 2) + (
        dHab / (1 + 0.015 * C1)) ** 2)
    return dE
