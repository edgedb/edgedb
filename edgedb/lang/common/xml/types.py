##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class Doctype:
    __slots__ = ('name', 'sysid', 'pubid')

    def __init__(self, name, sysid=None, pubid=None):
        self.name = name
        self.sysid = sysid
        self.pubid = pubid

    def __str__(self):
        result = '<!DOCTYPE ' + self.name

        if self.pubid:
            result += ' PUBLIC "%s"' % self.pubid

        if self.sysid:
            result += ' "%s"' % self.sysid

        return result + '>'

    def __repr__(self):
        return '"""%s"""' % self
