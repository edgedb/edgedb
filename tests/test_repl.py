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


import unittest

from edb.repl import lexutils


class TestReplLexutils(unittest.TestCase):

    def test_split_edgeql_01(self):
        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;'),
            ['select +  - 1;'])

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  '),
            ['select +  - 1;  '])

        self.assertEqual(
            lexutils.split_edgeql('  select +  - 1;  select ;;'),
            ['  select +  - 1;', '  select ;', ';'])

        self.assertEqual(
            lexutils.split_edgeql('''\
                CREATE TYPE blah {
                    set ;
                    blah ;
                };
                select 1;
                '''),
            [
                '''\
                CREATE TYPE blah {
                    set ;
                    blah ;
                };''',
                '''
                select 1;
                '''
            ])

        self.assertIsNone(lexutils.split_edgeql(''))
        self.assertIsNone(lexutils.split_edgeql(' '))
        self.assertIsNone(lexutils.split_edgeql(' \n '))
        self.assertIsNone(lexutils.split_edgeql(' \n sel \n '))

        self.assertIsNone(
            lexutils.split_edgeql('select +  - 1;  select 1'))

        self.assertIsNone(
            lexutils.split_edgeql('select +  - 1;  select {;}'))

        self.assertIsNone(
            lexutils.split_edgeql('select +  - 1;  select {;;;;}}}}'))

    def test_split_edgeql_02(self):
        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;', script_mode=True),
            ['select +  - 1;'])

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  ', script_mode=True),
            ['select +  - 1;'])

        self.assertEqual(
            lexutils.split_edgeql('  select +  - 1;  select ;;',
                                  script_mode=True),
            ['select +  - 1;', 'select ;'])

        self.assertEqual(
            lexutils.split_edgeql('''\
                CREATE TYPE blah {
                    set ;
                    blah ;
                };
                select 1;
                ''', script_mode=True),
            [
                '''CREATE TYPE blah {
                    set ;
                    blah ;
                };''',
                '''select 1;'''
            ])

        self.assertEqual(
            lexutils.split_edgeql('', script_mode=True), None)
        self.assertEqual(
            lexutils.split_edgeql(' ', script_mode=True), None)
        self.assertEqual(
            lexutils.split_edgeql(' \n ', script_mode=True), None)

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  select 1',
                                  script_mode=True),
            ['select +  - 1;', 'select 1'])

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  select {;}',
                                  script_mode=True),
            ['select +  - 1;', 'select {;}'])

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  select {;;;;}}}} select',
                                  script_mode=True),
            ['select +  - 1;', 'select {;;;;}}}} select'])

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  select {;;;;}}}}; select',
                                  script_mode=True),
            ['select +  - 1;', 'select {;;;;}}}};', 'select'])
