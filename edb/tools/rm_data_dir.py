#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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


import shutil

from edb.common import devmode
from edb.tools.edb import edbcommands


@edbcommands.command("rm-data-dir")
def rm_data_dir():
    """Remove the local development data directory if present"""
    data_dir = devmode.get_dev_mode_data_dir()
    if data_dir.exists():
        shutil.rmtree(data_dir)
        print("Removed the following local dev data directory.")
        print(data_dir)
    else:
        print("The local dev data directory does not exist.")
