##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys
import os
import imp
from importlib import abc
import collections

from .meta import LanguageMeta, DocumentContext


class ImportContext(str):
    def __getitem__(self, key):
        result = super().__getitem__(key)
        return self.__class__.copy(result, self)

    @classmethod
    def copy(cls, name, other):
        return cls(name)

    @classmethod
    def from_parent(cls, name, parent):
        return cls(name)


class Importer(abc.Finder, abc.Loader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.module_file_map = dict()

    def _locate_module_file(self, fullname, path):
        basename = fullname.rpartition('.')[2]

        for p in path:
            test = os.path.join(p, basename)
            is_package = os.path.isdir(test)
            result = LanguageMeta.recognize_file(test, True, is_package)
            if result:
                return result + (is_package,)

    def find_module(self, fullname, path=None):
        if path:
            result = self._locate_module_file(fullname, path)
            if result:
                self.module_file_map[fullname] = result
                return self

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]

        language, filename, is_package = self.module_file_map[fullname]

        new_mod = imp.new_module(fullname)
        new_mod.__file__ = filename
        if is_package:
            new_mod.__path__ = [os.path.dirname(filename)]
        new_mod.__odict__ = collections.OrderedDict()
        new_mod.__loader__ = self
        new_mod._language_ = language

        sys.modules[fullname] = new_mod

        context = DocumentContext(module=new_mod, import_context=fullname)

        with open(filename) as stream:
            try:
                attributes = language.load_dict(stream, context=context)

                for attribute_name, attribute_value in attributes:
                    attribute_name = str(attribute_name)
                    new_mod.__odict__[attribute_name] = attribute_value
                    setattr(new_mod, attribute_name, attribute_value)

            except Exception as error:
                del sys.modules[fullname]
                raise ImportError('unable to import "%s" (%s: %s)' \
                                  % (fullname, type(error).__name__, error)) from error

        return new_mod
