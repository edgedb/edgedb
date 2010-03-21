##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys


from semantix.exceptions import SemantixError

from semantix.utils.io.terminal.color import colorize, colorstr


def isatty(file):
    return os.isatty(file.fileno())


class TerminalError(SemantixError):
    pass


class Terminal:
    def __init__(self, fd=None):
        self.fd = fd or sys.stdout.fileno()
        if not os.isatty(self.fd):
            raise TerminalError('%d is not a TTY' % self.fd)
        if self.has_colors():
            self.colorstr = colorstr
        else:
            self.colorstr = str

    def has_colors(self):
        return os.getenv('TERM', None) != 'dumb'

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
