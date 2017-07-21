##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import unittest


class TestLoader(unittest.TestLoader):
    def __init__(self, *, verbosity=1, exclude=None, include=None,
                 progress_cb=None):
        super().__init__()
        self.verbosity = verbosity

        if include:
            self.include = [re.compile(r) for r in include]
        else:
            self.include = None

        if exclude:
            self.exclude = [re.compile(r) for r in exclude]
        else:
            self.exclude = None

        self.progress_cb = progress_cb

    def getTestCaseNames(self, caseclass):
        names = super().getTestCaseNames(caseclass)
        unfiltered_len = len(names)

        if self.include or self.exclude:
            if self.include:
                names = filter(
                    lambda n: any(r.search(n) for r in self.include),
                    names)

            if self.exclude:
                names = filter(
                    lambda n: not any(r.search(n) for r in self.exclude),
                    names)

            names = list(names)

        if self.progress_cb:
            self.progress_cb(len(names), unfiltered_len)

        return names
