#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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

import immutables

from edb import errors
from edb.server.config import ops

from . import github

SettingsMap = immutables.Map[str, ops.SettingValue]


def make(db_config: SettingsMap, name: str):
    match name:
        case "github":
            return github.GitHubProvider(name, db_config)
        case _:
            raise errors.BackendError(f"Unknown provider: {name}")
