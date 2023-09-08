#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


"""Abstractions for low-level database DDL and DML operations and data."""

from __future__ import annotations

from .base import *  # NOQA
from .config import *  # type: ignore  # NOQA
from .ddl import *  # NOQA
from .databases import *  # NOQA
from .domains import *  # NOQA
from .enums import *  # NOQA
from .extensions import *  # NOQA
from .functions import *  # NOQA
from .indexes import *  # NOQA
from .operators import *  # NOQA
from .ranges import *  # NOQA
from .roles import * # NOQA
from .schemas import *  # NOQA
from .sequences import *  # NOQA
from .tables import *  # NOQA
from .triggers import *  # NOQA
from .types import *  # NOQA
from .views import *  # NOQA
