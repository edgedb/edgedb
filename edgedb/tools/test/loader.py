#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


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
