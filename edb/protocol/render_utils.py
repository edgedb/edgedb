#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Optional, List

import contextlib
import textwrap


class RenderBuffer:

    ilevel: int
    buf: List[str]

    def __init__(self):
        self.ilevel = 0
        self.buf = []

    def write(self, line: str) -> None:
        self.buf.append(' ' * (self.ilevel * 2) + line)

    def newline(self) -> None:
        self.buf.append('')

    def lastline(self) -> Optional[str]:
        return self.buf[-1] if len(self.buf) else None

    def popline(self) -> str:
        return self.buf.pop()

    def write_comment(self, comment: str) -> None:
        lines = textwrap.wrap(comment, width=40)
        for line in lines:
            self.write(f'// {line}')

    def __str__(self):
        return '\n'.join(self.buf)

    @contextlib.contextmanager
    def indent(self):
        self.ilevel += 1
        try:
            yield
        finally:
            self.ilevel -= 1
