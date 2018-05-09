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


def humanize_time_delta(t):
    f = 's'
    if t and not round(t):
        t *= 1000.0
        if round(t):
            f = 'ms'
        else:
            t *= 1000.0
            if round(t):
                f = 'us'
            else:
                t *= 1000.0
                f = 'ns'
    return f'{t:.2f}{f}'
