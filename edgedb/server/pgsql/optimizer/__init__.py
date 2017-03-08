##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast
from edgedb.lang.common import debug

from . import analyzer, inliner


class Optimizer:

    def __init__(self):
        pass

    def optimize(self, pgtree):
        if debug.flags.edgeql_optimize:  # pragma: no cover
            ast.strip_meta_fields(pgtree)
            debug.header('SQL Tree before optimization')
            debug.dump(pgtree)

        rels = analyzer.Analyzer.analyze(pgtree)
        # from edgedb.lang.common import markup
        # markup.dump(rels)
        return inliner.Inliner.inline(pgtree, rels)
