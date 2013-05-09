##
# Copyright (c) 2008-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
import sys
import types

from metamagic.utils.datastructures import registry

from .exceptions import UnresolvedError


class LazyImportsModule(types.ModuleType):
    def __sx_finalize_load__(self):
        ctx = self.__mm_module_source_context__

        for k, v in ctx.document.namespace.items():
            if isinstance(v, LazyImportAttribute):
                v = v.get()
            setattr(self, k, v)

        del self.__mm_module_source_context__


class SourcePoint(object):
    def __init__(self, line, column, pointer):
        self.line = line
        self.column = column
        self.pointer = pointer


class LazyImportAttribute:
    def __init__(self, module, attribute=None):
        self.module = module
        self.attribute = attribute

    def get(self):
        if self.attribute:
            fromlist = (self.attribute,)
        else:
            fromlist = ()

        mod = __import__(self.module, fromlist=fromlist)

        if self.attribute:
            result = getattr(mod, self.attribute)
        else:
            result = mod

        return result


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

    def resolve_name(self, name):
        namespace = self.document.namespace

        parts = name.split('.')
        part = parts[0]

        try:
            obj = namespace[part]
        except KeyError:
            raise UnresolvedError('unable to resolve {!r} name'.format(name),
                                  context=self) from None

        if isinstance(obj, LazyImportAttribute):
            try:
                self.document.lazy_import_refs.add(obj.module)
            except AttributeError:
                self.document.lazy_import_refs = {obj.module}

            obj = obj.get()

        for part in parts[1:]:
            try:
                obj = getattr(obj, part)
            except KeyError:
                raise UnresolvedError('unable to resolve {!r} name'.format(name),
                                      context=self) from None

        return obj

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

        return self.namespace


def line_col_from_char_offset(source, position):
    line = source[:position].count('\n') + 1
    col = source.rfind('\n', 0, position)
    col = position if col == -1 else position - col
    return line, col
