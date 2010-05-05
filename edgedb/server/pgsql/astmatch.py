##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys

from semantix.utils import ast
from semantix.utils.ast import match as astmatch
from  . import ast as pgast


for name, cls in pgast.__dict__.items():
    if isinstance(cls, type) and issubclass(cls, ast.AST):
        adapter = astmatch.MatchASTMeta(name, (astmatch.MatchASTNode,), {'__module__': __name__},
                                        adapts=cls)
        setattr(sys.modules[__name__], name, adapter)
