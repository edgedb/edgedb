#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

import unittest

try:
    import async_solipsism
except ImportError:
    async_solipsism = None  # type: ignore


def with_fake_event_loop(f):
    # async_solpsism creates an event loop with, among other things,
    # a totally fake clock which starts at 0.
    def new(*args, **kwargs):
        if not async_solipsism:
            raise unittest.SkipTest('async_solipsism is missing')

        loop = async_solipsism.EventLoop()
        try:
            loop.run_until_complete(f(*args, **kwargs))
        finally:
            loop.close()

    return new
