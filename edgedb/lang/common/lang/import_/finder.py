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
