##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys

from metamagic.utils import ast
from metamagic.utils.ast import match as astmatch
from  . import ast as caos_ast


for name, cls in caos_ast.__dict__.items():
    if isinstance(cls, type) and issubclass(cls, ast.AST):
        adapter = astmatch.MatchASTMeta(name, (astmatch.MatchASTNode,), {'__module__': __name__},
                                        adapts=cls)
        setattr(sys.modules[__name__], name, adapter)
