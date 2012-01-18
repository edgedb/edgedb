##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib.abc
from importlib import _case_ok
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
            if is_package:
                if not _case_ok(p, basename):
                    # PEP 235
                    return
            result = LanguageMeta.recognize_file(test, True, is_package)
            if result:
                language, filename = result
                if not is_package:
                    # XXX Language should return the matched extension?
                    # Or this will be buggy with extensions containing
                    # multiple dots
                    if not _case_ok(os.path.dirname(filename),
                                    basename + '.' + filename.rpartition('.')[2]):
                        # PEP 235
                        return
                loader = getattr(fullname, 'loader', None)
                if loader is None:
                    loader = language.get_loader()

                return loader(fullname, filename, language)
