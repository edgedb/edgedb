##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
from importlib._bootstrap import _PyPycFileLoader

from semantix.utils.lang import meta
from . import ast


class Loader(_PyPycFileLoader):
    pass


class Language(meta.Language):
    file_extensions = ('py',)
    loader = Loader
