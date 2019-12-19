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

from __future__ import annotations


# This should be a good hint that EdgeDB dumps are not text files:
#
# * "\xFF" is invalid utf-8;
# * "\xD8\x00" is invalid utf-16-le
# * "\xFF\xD8\x00\x00\xD8" is also invalid utf-16/32 both le & be
#
# Presense of "\x00" is also a hint to tools like "git" that this is
# a binary file.
HEADER_TITLE = b'\xFF\xD8\x00\x00\xD8EDGEDB\x00DUMP\x00'
HEADER_TITLE_LEN = len(HEADER_TITLE)

COPY_BUFFER_SIZE = 1024 * 1024 * 10

DUMP_PROTO_VER = 1
MAX_SUPPORTED_DUMP_VER = 1
