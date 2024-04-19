#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

import unittest


skip = unittest.skip


def _xfail(reason, *, unless=False, allow_failure, allow_error):
    def decorator(test_item):
        if unless:
            return test_item
        else:
            test_item.__et_xfail_reason__ = reason
            test_item.__et_xfail_allow_failure__ = allow_failure
            test_item.__et_xfail_allow_error__ = allow_error
            return unittest.expectedFailure(test_item)

    return decorator


def xfail(reason, *, unless=False):
    return _xfail(reason, unless=unless, allow_failure=True, allow_error=False)


def xerror(reason, *, unless=False):
    return _xfail(reason, unless=unless, allow_failure=False, allow_error=True)


def not_implemented(reason):
    def decorator(test_item):
        test_item.__et_xfail_reason__ = reason
        test_item.__et_xfail_not_implemented__ = True
        test_item.__et_xfail_allow_failure__ = True
        test_item.__et_xfail_allow_error__ = True
        return unittest.expectedFailure(test_item)

    return decorator
