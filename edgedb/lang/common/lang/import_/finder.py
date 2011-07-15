##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib.abc
import os
import sys

from semantix.utils.lang.meta import LanguageMeta


class Finder(importlib.abc.Finder):
    _modules_by_file = {}

    @classmethod
    def get_module_by_filename(cls, filename):
        normpath = os.path.abspath(os.path.realpath(filename))
        modname = cls._modules_by_file.get(normpath)
        if modname:
            return sys.modules.get(modname)

    def find_module(self, fullname, path=None):
        basename = fullname.rpartition('.')[2]

        if path is None:
            path = sys.path

        for p in path:
            test = os.path.join(p, basename)
            is_package = os.path.isdir(test)
            result = LanguageMeta.recognize_file(test, True, is_package)
            if result:
                language, filename = result
                loader = getattr(fullname, 'loader', None)
                if loader is None:
                    loader = language.get_loader()
                return loader(fullname, filename, language)
