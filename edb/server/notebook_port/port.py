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


from __future__ import annotations

from edb.server import compiler
from edb.server import http

from . import protocol


class NotebookPort(http.BaseHttpPort):

    def build_protocol(self):
        return protocol.Protocol(self._loop, self, self._query_cache)

    def get_compiler_worker_cls(self):
        return compiler.Compiler

    @classmethod
    def get_proto_name(cls):
        return 'notebook'
