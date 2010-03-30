##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys


from semantix.exceptions import SemantixError

from semantix.utils.io.terminal.color import colorize, colorstr, dummycolorstr


def isatty(file):
    return os.isatty(file.fileno())


class TerminalError(SemantixError):
    pass


class Terminal:
    def __init__(self, fd=None, *, colors=None):
        self.fd = fd or sys.stdout.fileno()

        self.colors = self.supports_colors() if colors is None else colors

        if self.colors:
            self.colorstr = colorstr
        else:
            self.colorstr = dummycolorstr

    def has_colors(self):
        return self.colors

    def supports_colors(self):
        return self.isatty() and os.getenv('TERM', None) != 'dumb'

    def isatty(self):
        return os.isatty(self.fd)

    @property
    def size(self):
        try:
            import fcntl, termios, struct
            size = struct.unpack('2h', fcntl.ioctl(self.fd, termios.TIOCGWINSZ, '    '))
        except:
            size = (os.getenv('LINES', 25), os.getenv('COLUMNS', 80))

        return size

    def colorize(self, string='', fg=None, bg=None, opts=()):
        if self.has_colors():
            return colorize(string, fg, bg, opts)
        else:
            return string
