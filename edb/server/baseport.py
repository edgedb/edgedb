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


from edb.common import devmode


class Port:

    def __init__(self, *, loop, pg_addr, pg_data_dir, runstate_dir, dbindex):

        self._loop = loop
        self._pg_addr = pg_addr
        self._pg_data_dir = pg_data_dir
        self._dbindex = dbindex
        self._runstate_dir = runstate_dir

        self._devmode = devmode.is_in_dev_mode()

    def in_dev_mode(self):
        return self._devmode

    def get_loop(self):
        return self._loop

    async def start(self):
        raise NotImplementedError

    async def stop(self):
        raise NotImplementedError
