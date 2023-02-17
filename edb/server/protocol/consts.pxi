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


DEF DUMP_BLOCK_SIZE = 1024 * 1024 * 10

DEF DUMP_HEADER_BLOCK_TYPE = 101
DEF DUMP_HEADER_BLOCK_TYPE_INFO = b'I'
DEF DUMP_HEADER_BLOCK_TYPE_DATA = b'D'

DEF DUMP_HEADER_SERVER_TIME = 102
DEF DUMP_HEADER_SERVER_VER = 103
DEF DUMP_HEADER_BLOCKS_INFO = 104
DEF DUMP_HEADER_SERVER_CATALOG_VERSION = 105

DEF DUMP_HEADER_BLOCK_ID = 110
DEF DUMP_HEADER_BLOCK_NUM = 111
DEF DUMP_HEADER_BLOCK_DATA = 112
