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


import typing


class RollingAverage:

    __slots__ = ('_hist_size', '_hist', '_pos', '_cached_avg')

    _hist_size: int
    _pos: int
    _hist: typing.List[float]
    _cached_avg: float

    def __init__(self, *, history_size: int):
        self._hist = [0] * history_size
        self._pos = 0
        self._hist_size = history_size
        self._cached_avg = 0

    def add(self, n: float) -> None:
        self._hist[self._pos % self._hist_size] = n
        self._pos += 1
        self._cached_avg = 0

    def avg(self) -> float:
        if self._cached_avg:
            return self._cached_avg

        self._cached_avg = (
            sum(self._hist) / max(min(self._pos, self._hist_size), 1)
        )

        return self._cached_avg
