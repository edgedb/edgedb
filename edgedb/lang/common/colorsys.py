##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""An extension to standard library module :mod:`colorsys`.

Contains additional functions, with the most notable - :func:`rgb_distance`.
"""


from math import sqrt as _sqrt
from colorsys import rgb_to_yiq, yiq_to_rgb, rgb_to_hls, hls_to_rgb, rgb_to_hsv, hsv_to_rgb


__all__ = 'rgb_to_yiq', 'yiq_to_rgb', 'rgb_to_hls', 'hls_to_rgb', 'rgb_to_hsv', 'hsv_to_rgb', \
          'rgb_to_xyz', 'xyz_to_lab', 'rgb_distance'


# Relative to RGB max white
XYZ_MAX_X = 95.047
XYZ_MAX_Y = 100.0
XYZ_MAX_Z = 108.883


def rgb_to_xyz(r, g, b):
    """Converts RGB color to XYZ

    :param float r: Red value in ``0..1`` range
    :param float g: Green value in ``0..1`` range
    :param float b: Blue value in ``0..1`` range
    :returns: ``(x, y, z)``, all values normalized to ``(0..1, 0..1, 0..1)`` range
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
    :returns: ``(L, a, b)``, values in range ``(0..100, -127..128, -127..128)``
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

    l = 116.0 * y - 16.0
    a = 500 * (x - y)
    b = 200 * (y - z)

    return (l, a, b)


def rgb_distance(r1, g1, b1, r2, g2, b2):
    """Calculates numerical distance between two colors in RGB color space.

    The distance is calculated by CIE94 formula.

    :params: Two colors with ``r, g, b`` values in ``0..1`` range
    :returns: A number in ``0..100`` range.  The lesser - the closer colors are.
    """

    # Formulae from wikipedia article re CIE94

    L1, A1, B1 = xyz_to_lab(*rgb_to_xyz(r1, b1, g1))
    L2, A2, B2 = xyz_to_lab(*rgb_to_xyz(r2, b2, g2))

    dL = L1 - L2
    C1 = _sqrt(A1*A1 + B1*B1)
    C2 = _sqrt(A2*A2 + B2*B2)
    dCab = C1 - C2
    dA = A1 - A2
    dB = B1 - B2

    dEab = _sqrt(dL ** 2 + dA ** 2 + dB ** 2)

    dHab = _sqrt(max(dEab ** 2 - dL ** 2 - dCab ** 2, 0.0))

    dE = _sqrt((dL ** 2) + ((dCab / (1 + 0.045 * C1)) ** 2) + (dHab / (1 + 0.015 * C1)) ** 2)
    return dE
