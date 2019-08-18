#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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


def xrepr(obj, *, max_len=None):
    """Extended ``builtins.repr`` function.

    Examples:

    .. code-block:: pycon

        >>> xrepr('1234567890', max_len=7)
        '12'...

    :param int max_len: When defined limits maximum length of the result
                        string representation.

    :returns str:
    """
    result = str(repr(obj))

    if max_len is not None and len(result) > max_len:
        ext = '...'
        if result[0] in ('"', "'"):
            ext = result[0] + ext
        elif result[0] == '<':
            ext = '>' + ext
        result = result[:(max_len - len(ext))] + ext

    return result
