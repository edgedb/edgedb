##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import analyzer, inliner


class Optimizer:

    def __init__(self):
        pass

    def optimize(self, pgtree):
        qi = analyzer.Analyzer.analyze(pgtree)
        inliner.optimize(qi)
        return pgtree
