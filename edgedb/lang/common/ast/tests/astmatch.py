##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import sys

from edgedb.lang.common import ast
from edgedb.lang.common.ast import match as astmatch
from . import ast as tast

for name, cls in tast.__dict__.items():
    if isinstance(cls, type) and issubclass(cls, ast.AST):
        adapter = astmatch.MatchASTMeta(
            name, (astmatch.MatchASTNode, ), {'__module__': __name__},
            adapts=cls)
        setattr(sys.modules[__name__], name, adapter)
