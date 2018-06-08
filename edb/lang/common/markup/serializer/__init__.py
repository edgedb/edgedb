#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
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


class settings:
    censor_sensitive_vars = True
    censor_list = ['secret', 'password']


from .base import serialize, serializer, serialize_traceback_point  # NOQA
from .base import Context  # NOQA
from .base import no_ref_detect  # NOQA
from .code import serialize_code  # NOQA
from . import logging  # NOQA
