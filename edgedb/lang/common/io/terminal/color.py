##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


def colorize(string='', fg=None, bg=None, opts=()):
    color_table = {
        'black': 0,
        'red': 1,
        'green': 2,
        'yellow': 3,
        'blue': 4,
        'magenta': 5,
        'cyan': 6,
        'white': 7,
    }

    opts_table = {
        'bold': 1,
        'faint': 2,
        'italic': 3,
        'underline': 4,
        'blink': 5,
        'overline': 6,
        'reverse': 7
    }


    cmd = []

    if fg is not None and fg in color_table:
        cmd.append('3%d' % color_table[fg])
    if bg is not None and bg in color_table:
        cmd.append('4%d' % color_table[bg])

    cmd.extend([str(opts_table[opt]) for opt in opts if opt in opts_table])

    if string:
        string += '\x1B[%sm' % '0'

    return ('\x1B[%sm' % ';'.join(cmd)) + string


class colorstr(str):
    def __new__(cls, string, color=None, opts=None):
        result = super(colorstr, cls).__new__(cls, string)
        result.color = color or 'white'
        result.opts = opts or ()
        return result

    def __init__(self, string, color=None, opts=None):
        pass

    def __str__(self):
        return colorize(self, fg=self.color, opts=self.opts)

    def __format__(self, spec):
        result = super().__format__(spec)
        return result.replace(self, colorize(self, fg=self.color, opts=self.opts))
