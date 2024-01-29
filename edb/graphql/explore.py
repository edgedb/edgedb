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


# flake8: noqa


from __future__ import annotations

import os

dirpath = os.path.dirname(__file__)

with open(os.path.join(dirpath, './explore/index.html'), 'rb') as f:
    EXPLORE_HTML = f.read()

with open(os.path.join(dirpath, './explore/graphiql.min.js'), 'rb') as f:
    EXPLORE_JS = f.read()

with open(os.path.join(dirpath, './explore/graphiql.min.css'), 'rb') as f:
    EXPLORE_CSS = f.read()
