#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

import re

# Colour utils

hex_color_regexp = re.compile(r'[0-9a-fA-F]{6}')


def get_colour_vars(bg_hex: str) -> str:
    bg_rgb = hex_to_rgb(bg_hex)
    bg_hsl = rgb_to_hsl(*bg_rgb)
    luma = rgb_to_luma(*bg_rgb)
    luma_dark = luma < 0.6

    text_color = hsl_to_rgb(
        bg_hsl[0], bg_hsl[1], min(90 if luma_dark else 35, bg_hsl[2])
    )
    dark_text_color = hsl_to_rgb(bg_hsl[0], bg_hsl[1], max(60, bg_hsl[2]))

    return f'''--accent-bg-color: #{bg_hex};
        --accent-bg-text-color: #{rgb_to_hex(
            *hsl_to_rgb(
                bg_hsl[0],
                bg_hsl[1],
                95 if luma_dark else max(10, min(25, luma * 100 - 60))
            )
        )};
        --accent-bg-hover-color: #{rgb_to_hex(
            *hsl_to_rgb(
                bg_hsl[0], bg_hsl[1], bg_hsl[2] + (5 if luma_dark else -5)
            )
        )};
        --accent-text-color: #{rgb_to_hex(*text_color)};
        --accent-text-dark-color: #{rgb_to_hex(*dark_text_color)};
        --accent-focus-color: rgba({','.join(
            str(c) for c in text_color)},0.6);
        --accent-focus-dark-color: rgba({','.join(
            str(c) for c in dark_text_color)},0.6);'''


def hex_to_rgb(hex: str) -> tuple[float, float, float]:
    return (
        int(hex[0:2], base=16),
        int(hex[2:4], base=16),
        int(hex[4:6], base=16),
    )


def rgb_to_hex(r: float, g: float, b: float) -> str:
    return '%02x%02x%02x' % (int(r), int(g), int(b))


def rgb_to_luma(r: float, g: float, b: float) -> float:
    return (r * 0.299 + g * 0.587 + b * 0.114) / 255


def rgb_to_hsl(r: float, g: float, b: float) -> tuple[float, float, float]:
    r /= 255
    g /= 255
    b /= 255
    l = max(r, g, b)
    s = l - min(r, g, b)
    h = (
        (
            ((g - b) / s)
            if l == r
            else (2 + (b - r) / s) if l == g else (4 + (r - g) / s)
        )
        if s != 0
        else 0
    )
    return (
        60 * h + 360 if 60 * h < 0 else 60 * h,
        100
        * (
            (s / (2 * l - s) if l <= 0.5 else s / (2 - (2 * l - s)))
            if s != 0
            else 0
        ),
        (100 * (2 * l - s)) / 2,
    )


def hsl_to_rgb(h: float, s: float, l: float) -> tuple[float, float, float]:
    s /= 100
    l /= 100
    k = lambda n: (n + h / 30) % 12
    a = s * min(l, 1 - l)
    f = lambda n: l - a * max(-1, min(k(n) - 3, min(9 - k(n), 1)))
    return (
        round(255 * f(0)),
        round(255 * f(8)),
        round(255 * f(4)),
    )
