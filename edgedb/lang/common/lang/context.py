##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys
from metamagic.utils.datastructures import registry


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
    def from_object(cls, object, use_mro=False):
        if use_mro:
            for pcls in object.__mro__:
                context = cls.from_object(pcls)
                if context is not None:
                    return context
        else:
            return cls._object_registry.get(object)


class DocumentContext(object):
    def __init__(self, module=None, import_context=None):
        self.module = module
        self.import_context = import_context
        self.imports = {}
        self.namespace = {}

    def get_globals(self):
        _globals = {}

        for modname, modinfo in self.imports.items():
            _globals[modname] = sys.modules[modinfo.__name__]

        mymod = sys.modules[self.module.__name__]

        for attrname in dir(mymod):
            if not attrname.startswith('_'):
                _globals[attrname] = getattr(mymod, attrname)

        return _globals
