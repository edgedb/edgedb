##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.datastructures import registry


class SourcePoint(object):
    def __init__(self, line, column, pointer):
        self.line = line
        self.column = column
        self.pointer = pointer


class SourceContext(object):
    _object_registry = registry.WeakObjectRegistry()

    def __init__(self, name, buffer, start, end, document=None, *, filename=None):
        self.name = name
        self.buffer = buffer
        self.start = start
        self.end = end
        self.document = document
        self.filename = filename

    def __str__(self):
        return '%s line:%d col:%d' % (self.name, self.start.line, self.start.column)

    @classmethod
    def register_object(cls, object, context):
        cls._object_registry[object] = context

    @classmethod
    def from_object(cls, object):
        return cls._object_registry.get(object)


class DocumentContext(object):
    def __init__(self, module=None, import_context=None):
        self.module = module
        self.import_context = import_context
        self.imports = {}
