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


"""A collection of functions and classes to simplify output to terminal."""

from __future__ import annotations

from typing import Optional

import os
import sys
import fcntl
import termios
import struct
import functools

from edb.common.colorsys import rgb_distance as color_distance
from edb.common.colorsys import Color


def isatty(fileno):
    return os.isatty(fileno)


_COLORS: Optional[int] = None

_colorize = 'auto'


def set_colorization_option(option):
    global _colorize
    _colorize = option


def max_colors():
    """Max colors current terminal supports.

    :returns: Integer. For instance, for 'xterm' it is usually 256

    .. note:: Uses :mod:`curses`
    """
    global _COLORS

    if _COLORS is None:
        try:
            import curses
            try:
                curses.setupterm()
                _COLORS = curses.tigetnum('colors')
            except (OSError, curses.error):
                pass
        except ImportError:
            pass

    if _COLORS is None:
        _COLORS = 1

    return _COLORS


def supports_colors(fileno):
    """Check if ``fileno`` file-descriptor supports colored output.

    :params int fileno: file-descriptor
    :returns: bool
    """
    return (
        isatty(fileno) and os.getenv('TERM') != 'dumb' and
        os.getenv('ANSI_COLORS_DISABLED') is None)


def size(fileno):
    """Current terminal height and width (lines and columns).

    :params int fileno: file-descriptor
    :returns: Tuple of two integers - lines and columns respectively.
              ``(None, None)`` if ``fileno`` is not a terminal
    """
    if not isatty(fileno):
        return None, None

    try:
        size = struct.unpack(
            '2h', fcntl.ioctl(fileno, termios.TIOCGWINSZ, '    '))
    except Exception:
        size = (os.getenv('LINES', 25), os.getenv('COLUMNS', 80))

    return size


def use_colors(fileno=None):
    """Check on whether use colored output or not.

    Checks ``shell.MainCommand.colorize`` config setting and
    ``fileno`` for being capable of displaying colors.

    :param int fileno: File-descriptor. If ``None``, checks on ``sys.stdout``
    :returns bool: Whether you can or can not use color terminal output
    """
    if _colorize == 'on':
        return True

    if _colorize == 'off':
        return False

    assert _colorize == 'auto'

    if fileno is None:
        try:
            fileno = sys.stdout.fileno()
        except OSError:
            return False

    return supports_colors(fileno)


# XTerm 256 colors table.
#
_MAP256 = {
    16: '#000000',
    17: '#00005f',
    18: '#000087',
    19: '#0000af',
    20: '#0000d7',
    21: '#0000ff',
    22: '#005f00',
    23: '#005f5f',
    24: '#005f87',
    25: '#005faf',
    26: '#005fd7',
    27: '#005fff',
    28: '#008700',
    29: '#00875f',
    30: '#008787',
    31: '#0087af',
    32: '#0087d7',
    33: '#0087ff',
    34: '#00af00',
    35: '#00af5f',
    36: '#00af87',
    37: '#00afaf',
    38: '#00afd7',
    39: '#00afff',
    40: '#00d700',
    41: '#00d75f',
    42: '#00d787',
    43: '#00d7af',
    44: '#00d7d7',
    45: '#00d7ff',
    46: '#00ff00',
    47: '#00ff5f',
    48: '#00ff87',
    49: '#00ffaf',
    50: '#00ffd7',
    51: '#00ffff',
    52: '#5f0000',
    53: '#5f005f',
    54: '#5f0087',
    55: '#5f00af',
    56: '#5f00d7',
    57: '#5f00ff',
    58: '#5f5f00',
    59: '#5f5f5f',
    60: '#5f5f87',
    61: '#5f5faf',
    62: '#5f5fd7',
    63: '#5f5fff',
    64: '#5f8700',
    65: '#5f875f',
    66: '#5f8787',
    67: '#5f87af',
    68: '#5f87d7',
    69: '#5f87ff',
    70: '#5faf00',
    71: '#5faf5f',
    72: '#5faf87',
    73: '#5fafaf',
    74: '#5fafd7',
    75: '#5fafff',
    76: '#5fd700',
    77: '#5fd75f',
    78: '#5fd787',
    79: '#5fd7af',
    80: '#5fd7d7',
    81: '#5fd7ff',
    82: '#5fff00',
    83: '#5fff5f',
    84: '#5fff87',
    85: '#5fffaf',
    86: '#5fffd7',
    87: '#5fffff',
    88: '#870000',
    89: '#87005f',
    90: '#870087',
    91: '#8700af',
    92: '#8700d7',
    93: '#8700ff',
    94: '#875f00',
    95: '#875f5f',
    96: '#875f87',
    97: '#875faf',
    98: '#875fd7',
    99: '#875fff',
    100: '#878700',
    101: '#87875f',
    102: '#878787',
    103: '#8787af',
    104: '#8787d7',
    105: '#8787ff',
    106: '#87af00',
    107: '#87af5f',
    108: '#87af87',
    109: '#87afaf',
    110: '#87afd7',
    111: '#87afff',
    112: '#87d700',
    113: '#87d75f',
    114: '#87d787',
    115: '#87d7af',
    116: '#87d7d7',
    117: '#87d7ff',
    118: '#87ff00',
    119: '#87ff5f',
    120: '#87ff87',
    121: '#87ffaf',
    122: '#87ffd7',
    123: '#87ffff',
    124: '#af0000',
    125: '#af005f',
    126: '#af0087',
    127: '#af00af',
    128: '#af00d7',
    129: '#af00ff',
    130: '#af5f00',
    131: '#af5f5f',
    132: '#af5f87',
    133: '#af5faf',
    134: '#af5fd7',
    135: '#af5fff',
    136: '#af8700',
    137: '#af875f',
    138: '#af8787',
    139: '#af87af',
    140: '#af87d7',
    141: '#af87ff',
    142: '#afaf00',
    143: '#afaf5f',
    144: '#afaf87',
    145: '#afafaf',
    146: '#afafd7',
    147: '#afafff',
    148: '#afd700',
    149: '#afd75f',
    150: '#afd787',
    151: '#afd7af',
    152: '#afd7d7',
    153: '#afd7ff',
    154: '#afff00',
    155: '#afff5f',
    156: '#afff87',
    157: '#afffaf',
    158: '#afffd7',
    159: '#afffff',
    160: '#d70000',
    161: '#d7005f',
    162: '#d70087',
    163: '#d700af',
    164: '#d700d7',
    165: '#d700ff',
    166: '#d75f00',
    167: '#d75f5f',
    168: '#d75f87',
    169: '#d75faf',
    170: '#d75fd7',
    171: '#d75fff',
    172: '#d78700',
    173: '#d7875f',
    174: '#d78787',
    175: '#d787af',
    176: '#d787d7',
    177: '#d787ff',
    178: '#d7af00',
    179: '#d7af5f',
    180: '#d7af87',
    181: '#d7afaf',
    182: '#d7afd7',
    183: '#d7afff',
    184: '#d7d700',
    185: '#d7d75f',
    186: '#d7d787',
    187: '#d7d7af',
    188: '#d7d7d7',
    189: '#d7d7ff',
    190: '#d7ff00',
    191: '#d7ff5f',
    192: '#d7ff87',
    193: '#d7ffaf',
    194: '#d7ffd7',
    195: '#d7ffff',
    196: '#ff0000',
    197: '#ff005f',
    198: '#ff0087',
    199: '#ff00af',
    200: '#ff00d7',
    201: '#ff00ff',
    202: '#ff5f00',
    203: '#ff5f5f',
    204: '#ff5f87',
    205: '#ff5faf',
    206: '#ff5fd7',
    207: '#ff5fff',
    208: '#ff8700',
    209: '#ff875f',
    210: '#ff8787',
    211: '#ff87af',
    212: '#ff87d7',
    213: '#ff87ff',
    214: '#ffaf00',
    215: '#ffaf5f',
    216: '#ffaf87',
    217: '#ffafaf',
    218: '#ffafd7',
    219: '#ffafff',
    220: '#ffd700',
    221: '#ffd75f',
    222: '#ffd787',
    223: '#ffd7af',
    224: '#ffd7d7',
    225: '#ffd7ff',
    226: '#ffff00',
    227: '#ffff5f',
    228: '#ffff87',
    229: '#ffffaf',
    230: '#ffffd7',
    231: '#ffffff',
    232: '#080808',
    233: '#121212',
    234: '#1c1c1c',
    235: '#262626',
    236: '#303030',
    237: '#3a3a3a',
    238: '#444444',
    239: '#4e4e4e',
    240: '#585858',
    241: '#606060',
    242: '#666666',
    243: '#767676',
    244: '#808080',
    245: '#8a8a8a',
    246: '#949494',
    247: '#9e9e9e',
    248: '#a8a8a8',
    249: '#b2b2b2',
    250: '#bcbcbc',
    251: '#c6c6c6',
    252: '#d0d0d0',
    253: '#dadada',
    254: '#e4e4e4',
    255: '#eeeeee'
}


def _is_opt_getter(name: str):
    return lambda self: self._is_opt(name)


def _set_opt_setter(name: str):
    return lambda self, value: self._set_opt(name, value)


class AbstractStyle:
    """Encapsulates information about text-style.

    For instance, what color should text be, should it be
    underlined or bold etc.

    Use instances of :class:`Style16` or :class:`Style256`,
    this class is abstract.
    """

    __slots__ = (
        '_opts', '_color', '_bgcolor', '_term_prefix', '_term_postfix')

    _opts_table = {
        'bold': '1',
        'faint': '2',
        'italic': '3',
        'underline': '4',
        'blink': '5',
        'overline': '6',
        'reverse': '7'
    }
    _ropts_table = {v: k for k, v in _opts_table.items()}

    def __init__(
        self,
        *,
        color=None,
        bgcolor=None,
        bold=False,
        faint=False,
        italic=False,
        underline=False,
        overline=False,
        reverse=False,
    ):

        self._opts = set()
        self._color = None
        self._bgcolor = None

        self.color = color
        self.bgcolor = bgcolor

        self.bold = bold
        self.faint = faint
        self.italic = italic
        self.underline = underline
        self.overline = overline
        self.reverse = reverse

    def _filter_color(self, color):
        raise NotImplementedError

    def _get_color(self):
        return self._rcolor_table[self._color]

    def _set_color(self, color):
        self._color = self._filter_color(color)
        self._recalc()

    color = property(_get_color, _set_color)

    def _get_bgcolor(self):
        return self._rcolor_table[self._bgcolor]

    def _set_bgcolor(self, color):
        self._bgcolor = self._filter_color(color)
        self._recalc()

    bgcolor = property(_get_bgcolor, _set_bgcolor)

    @property
    def empty(self):
        return not bool(self._term_prefix)

    def _is_opt(self, name: str) -> bool:
        assert name in self._opts_table
        return self._opts_table[name] in self._opts

    def _set_opt(self, name, value):
        try:
            tr_name = self._opts_table[name]
        except KeyError:
            raise ValueError('unknown style option {!r}'.format(name))

        if value:
            self._opts.add(tr_name)
        else:
            if tr_name in self._opts:
                self._opts.discard(tr_name)
        self._recalc()

    bold = property(_is_opt_getter('bold'), _set_opt_setter('bold'))
    faint = property(_is_opt_getter('faint'), _set_opt_setter('faint'))
    italic = property(_is_opt_getter('italic'), _set_opt_setter('italic'))
    underline = property(
        _is_opt_getter('underline'), _set_opt_setter('underline'))
    blink = property(_is_opt_getter('blink'), _set_opt_setter('blink'))
    overline = property(
        _is_opt_getter('overline'), _set_opt_setter('overline'))
    reverse = property(_is_opt_getter('reverse'), _set_opt_setter('reverse'))

    def _recalc(self):
        cmd = []

        if self._color is not None:
            if self._color > 15:
                cmd.append('38;5;{}'.format(self._color))
            else:
                cmd.append('3{}'.format(self._color))

        if self._bgcolor is not None:
            if self._bgcolor > 15:
                cmd.append('48;5;{}'.format(self._bgcolor))
            else:
                cmd.append('4{}'.format(self._bgcolor))

        cmd.extend(self._opts)

        if cmd:
            self._term_prefix = '\x1B[{}m'.format(';'.join(cmd))
            self._term_postfix = '\x1B[0m'
        else:
            self._term_prefix = ''
            self._term_postfix = ''

    def apply(self, str):
        """Apply ANSI escape sequences to :param:str.

        If the result can be printed to a terminal that supports styling.
        """
        return self._term_prefix + str + self._term_postfix


class Style16(AbstractStyle):
    """16-color style."""

    _color_table = {
        'black': 0,
        'red': 1,
        'green': 2,
        'yellow': 3,
        'blue': 4,
        'magenta': 5,
        'cyan': 6,
        'white': 7
    }
    _rcolor_table = {v: k for k, v in _color_table.items()}

    def _filter_color(self, color):
        if color is None:
            return None

        try:
            return self._color_table[color]
        except KeyError as ex:
            raise ValueError('unknown color {!r}'.format(color)) from ex


class Style256(AbstractStyle):
    """256-color style.

    Accepts any rgb color in hex format, for instance:

    .. code-block:: pycon

        >>> Style256(color='#abcdef')

    Or by css name:

    .. code-block:: pycon

        >>> Style256(color='chocolate')

    In case of a color being outside of standard xterm 256 color palette,
    it'll try to locate the closest color in it.
    """

    _color_table = {v: k for k, v in _MAP256.items()}
    _rcolor_table = _MAP256

    _rgb_color_table = {
        Color.from_string(v).rgb_channels(as_floats=True): k
        for k, v in _MAP256.items()
    }

    @staticmethod
    @functools.lru_cache(500)
    def _filter_color(color):
        if color is None:
            return None

        try:
            return Style256._color_table[color]
        except KeyError:
            pass

        c = Color.from_string(color).rgb_channels(as_floats=True)
        return min(
            Style256._rgb_color_table.items(),
            key=lambda item: color_distance(item[0][0], item[0][1],
                                            item[0][2], *c))[1]


class StylesTable:
    """Base class for simple style tables."""

    def __getattr__(self, key):
        # If we're querying some non-existing style, pretend it's empty
        #
        return Style16()

    def dump(self):
        for name, style in self.__class__.__dict__.items():
            if isinstance(style, AbstractStyle):
                print(style.apply(name))
