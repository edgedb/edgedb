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


import pathlib
import unittest

import edb
from edb import errors
from edb.tools import gen_errors


class TestErrorsClasses(unittest.TestCase):

    def test_api_errors_01(self):
        # Test that "edb genexc" tool generates correct
        # class hierarchy.

        self.assertTrue(
            issubclass(errors.InternalServerError, errors.EdgeDBError))

        self.assertEqual(
            errors.InternalServerError('error').get_code(), 0x_01_00_00_00)

        self.assertTrue(
            issubclass(errors.ProtocolError, errors.EdgeDBError))

        self.assertTrue(
            issubclass(errors.BinaryProtocolError, errors.ProtocolError))

        self.assertTrue(
            issubclass(errors.QueryError, errors.EdgeDBError))
        self.assertTrue(
            issubclass(errors.InvalidTypeError, errors.QueryError))
        self.assertTrue(
            issubclass(errors.InvalidTargetError, errors.InvalidTypeError))
        self.assertTrue(
            issubclass(
                errors.InvalidLinkTargetError, errors.InvalidTargetError))
        self.assertFalse(
            issubclass(
                errors.InvalidLinkTargetError,
                errors.InvalidPropertyTargetError))

    def test_api_errors_02(self):
        # Test that "edb genexc" tool doesn't generate errors
        # intended for client libraries.

        self.assertFalse(hasattr(errors, 'ClientError'))


class TestErrorsTags(unittest.TestCase):
    errors_path = pathlib.Path(edb.__path__[0]) / 'api' / 'errors.txt'

    def test_api_errors_tags_01(self):
        tree = gen_errors.ErrorsTree()
        tree.load(self.errors_path)

        for (code, desc) in tree.get_errors().items():
            parent = tree.get_parent(code)
            if parent is None:
                continue

            self.assertTrue(
                all(tag in desc.tags for tag in parent.tags),
                f"{desc.name} error doesn't inherit all {parent.name} tags")
