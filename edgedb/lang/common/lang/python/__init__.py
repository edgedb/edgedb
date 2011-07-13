##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys
from importlib._bootstrap import _SourceFileLoader

from semantix.utils.lang import meta
from . import ast


class Loader(_SourceFileLoader):
    def __init__(self, fullname, filename, language):
        super().__init__(fullname, filename)

    def load_module(self, fullname):
        super().load_module(fullname)
        return sys.modules[fullname]


class Language(meta.Language):
    file_extensions = ('py',)
    loader = Loader
