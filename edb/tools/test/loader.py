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


from __future__ import annotations

import re
import unittest
from typing import Callable, Optional, Sequence


class TestLoader(unittest.TestLoader):
    include: Optional[Sequence[re.Pattern]]
    exclude: Optional[Sequence[re.Pattern]]

    def __init__(
        self,
        *,
        verbosity: int = 1,
        exclude: Sequence[str] = (),
        include: Sequence[str] = (),
        progress_cb: Optional[Callable[[int, int], None]] = None,
    ):
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
        cname = caseclass.__name__

        if self.include or self.exclude:
            if self.include:
                names = filter(
                    lambda n: (
                        any(r.search(n) for r in self.include)
                        or any(r.search(f'{cname}.{n}') for r in self.include)
                    ),
                    names,
                )

            if self.exclude:
                names = filter(
                    lambda n: (
                        not any(r.search(n) for r in self.exclude)
                        and not any(
                            r.search(f'{cname}.{n}') for r in self.exclude
                        )
                    ),
                    names,
                )

            names = list(names)

        if self.progress_cb:
            self.progress_cb(len(names), unfiltered_len)

        return names
