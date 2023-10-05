#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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

import time


class TokenBucket:
    _capacity: float
    _token_per_sec: float
    _tokens: float
    _last_fill_time: float

    def __init__(self, capacity: float, token_per_sec: float):
        self._capacity = capacity
        self._token_per_sec = token_per_sec
        self._tokens = capacity
        self._last_fill_time = time.monotonic()

    def consume(self, tokens: int) -> float:
        if tokens <= 0:
            return True
        now = time.monotonic()
        tokens_to_add = (now - self._last_fill_time) * self._token_per_sec
        self._tokens = min(self._capacity, self._tokens + tokens_to_add)
        self._last_fill_time = now
        left = self._tokens - tokens
        if left >= 0:
            self._tokens -= tokens
            return 0
        else:
            return -left / (tokens * self._token_per_sec)
