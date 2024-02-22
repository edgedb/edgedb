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

import collections
import time


class WindowedSum:
    """Keeps the sum of incremented values from the last minute.

    The sum is kept with second precision.

    >>> s = WindowedSum()
    >>> s += 1
    >>> s += 1
    >>> time.sleep(30)
    >>> s += 1
    >>> s += 1
    >>> int(s)
    4
    >>> time.sleep(30)
    >>> int(s)
    2
    """

    def __init__(self) -> None:
        self._maxlen = 60
        init: float = 0
        self._buckets = collections.deque([init], maxlen=self._maxlen)
        self._last_shift_at = 0.0

    def __iadd__(self, val: float) -> WindowedSum:
        self.shift()
        self._buckets[-1] += val
        return self

    def __int__(self) -> int:
        self.shift()
        return int(sum(self._buckets))

    def __float__(self) -> float:
        self.shift()
        return float(sum(self._buckets))

    def shift(self) -> None:
        now = time.monotonic()
        shift_by = int(min(now - self._last_shift_at, self._maxlen))
        if shift_by:
            self._buckets.extend(shift_by * [0])
            self._last_shift_at = now
