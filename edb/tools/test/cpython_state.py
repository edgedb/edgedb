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


from __future__ import annotations

import ctypes


class _Py_HashSecret_t(ctypes.Union):
    _fields_ = [
        ('uc', ctypes.c_ubyte * 24),
    ]


def get_py_hash_secret() -> bytes:
    hashsecret = _Py_HashSecret_t.in_dll(ctypes.pythonapi, '_Py_HashSecret')
    return bytes(hashsecret.uc)
