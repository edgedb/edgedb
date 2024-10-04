#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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
import logging

MIN_CONN_TIME_THRESHOLD = 0.01
MIN_QUERY_TIME_THRESHOLD = 0.001
MIN_LOG_TIME_THRESHOLD = 1
MIN_IDLE_TIME_BEFORE_GC = 120
CONNECT_FAILURE_RETRIES = 3
STATS_COLLECT_INTERVAL = 0.1

logger = logging.getLogger("edb.server")
