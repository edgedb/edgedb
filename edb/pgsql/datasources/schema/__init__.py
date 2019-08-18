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


from __future__ import annotations

del annotations  # There is a conflicting `annotations` module in this package.

from . import annotations  # NOQA
from . import casts  # NOQA
from . import constraints  # NOQA
from . import functions  # NOQA
from . import indexes  # NOQA
from . import links  # NOQA
from . import modules  # NOQA
from . import objtypes  # NOQA
from . import operators  # NOQA
from . import roles  # NOQA
from . import scalars  # NOQA
