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


from typing import *
from edb.server import dbview

from . import errors
from . import util


class Client:
    def __init__(self, db: dbview.Database, provider: str):
        self.db = db
        self.db_config = db.db_config
        match provider:
            case "github":
                from . import github

                self.provider = github.GitHubProvider(
                    *self._get_client_credientials("github")
                )
            case _:
                raise errors.InvalidData("Invalid provider: {provider}")

    def get_authorize_url(self, state: str, redirect_uri: str) -> str:
        return self.provider.get_code_url(
            state=state, redirect_uri=redirect_uri
        )

    def _get_client_credientials(self, client_name: str) -> tuple[str, str]:
        client_id = util.get_config(
            self.db_config, f"xxx_{client_name}_client_id"
        )
        client_secret = util.get_config(
            self.db_config, f"xxx_{client_name}_client_secret"
        )
        return client_id, client_secret
