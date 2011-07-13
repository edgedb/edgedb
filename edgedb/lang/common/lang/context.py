##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class SourcePoint(object):
    def __init__(self, line, column, pointer):
        self.line = line
        self.column = column
        self.pointer = pointer


class SourceContext(object):
    def __init__(self, name, buffer, start, end, document=None):
        self.name = name
        self.buffer = buffer
        self.start = start
        self.end = end
        self.document = document

    def __str__(self):
        return '%s line:%d col:%d' % (self.name, self.start.line, self.start.column)


class DocumentContext(object):
    def __init__(self, module=None, import_context=None):
        self.module = module
        self.import_context = import_context
        self.imports = {}
