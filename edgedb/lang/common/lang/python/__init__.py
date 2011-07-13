##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
from importlib._bootstrap import _SourceFileLoader

from semantix.utils.lang import meta
from . import ast


class Loader(_SourceFileLoader):
    pass


class Language(meta.Language):
    file_extensions = ('py',)
    loader = Loader
