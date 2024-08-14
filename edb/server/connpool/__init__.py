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
import os
from .pool import Pool as Pool1Impl, _NaivePool  # NoQA
from .pool2 import Pool as Pool2Impl

# During the transition period we allow for the pool to be swapped out. The
# current default is to use the old pool, however this will be switched to use
# the new pool once we've fully implemented all required features.
if os.environ.get("EDGEDB_USE_NEW_CONNPOOL", "") == "1":
    Pool = Pool2Impl
    Pool2 = Pool1Impl
else:
    # The two pools have the same effective type shape
    Pool = Pool1Impl  # type: ignore
    Pool2 = Pool2Impl  # type: ignore

__all__ = ('Pool', 'Pool2')
