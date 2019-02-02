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
        # test regular complete statements
        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;', script_mode=False),
            (['select +  - 1;'], None))

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  ', script_mode=False),
            (['select +  - 1;'], None))

        self.assertEqual(
            lexutils.split_edgeql(
                '  select +  - 1;  select ;;', script_mode=False),
            (['select +  - 1;', 'select ;'], None))

        self.assertEqual(
            lexutils.split_edgeql(';;;', script_mode=False),
            ([], None))

        self.assertEqual(
            lexutils.split_edgeql('''\
                    CREATE TYPE blah {
                        set ;
                        blah ;
                    };
                    select 1;
                ''', script_mode=False),
            (
                [
                    '''CREATE TYPE blah {
                        set ;
                        blah ;
                    };''',
                    'select 1;'
                ],
                None
            ))

    def test_split_edgeql_02(self):
        # test multiline statements
        self.assertEqual(
            lexutils.split_edgeql('', script_mode=False),
            ([], ''))
        self.assertEqual(
            lexutils.split_edgeql(' ', script_mode=False),
            ([], ' '))
        self.assertEqual(
            lexutils.split_edgeql(' \n ', script_mode=False),
            ([], ' \n '))
        self.assertEqual(lexutils.split_edgeql(
            ' \n sel \n ', script_mode=False),
            ([], ' \n sel \n '))

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  select 1',
                                  script_mode=False),
            (['select +  - 1;'], '  select 1'))

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  select {;}',
                                  script_mode=False),
            (['select +  - 1;'], '  select {;}'))

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  select {;;;;}}}}',
                                  script_mode=False),
            (['select +  - 1;'], '  select {;;;;}}}}'))

    def test_split_edgeql_03(self):
        # test multiline statements where the string is unterminated
        self.assertEqual(
            lexutils.split_edgeql('SELECT "aaa', script_mode=False),
            ([], 'SELECT "aaa'))

        self.assertEqual(
            lexutils.split_edgeql('SELECT "as', script_mode=False),
            ([], 'SELECT "as'))

        self.assertEqual(
            lexutils.split_edgeql('SELECT "as\n', script_mode=False),
            ([], 'SELECT "as\n'))

    def test_split_edgeql_04(self):
        # test multiline statements where the ';' is not a separator
        self.assertEqual(
            lexutils.split_edgeql('SELECT "aaa;', script_mode=False),
            ([], 'SELECT "aaa;'))

        self.assertEqual(
            lexutils.split_edgeql('SELECT 1 #;', script_mode=False),
            ([], 'SELECT 1 #;'))

    def test_split_edgeql_05(self):
        # test invalid tokens
        self.assertEqual(
            lexutils.split_edgeql('SELECT 1 ~ 2;', script_mode=False),
            (['SELECT 1 ~ 2;'], None))

        self.assertEqual(
            lexutils.split_edgeql('SELECT 1 ~ 2', script_mode=False),
            ([], 'SELECT 1 ~ 2'))

    def test_split_edgeql_06(self):
        # test regular script mode
        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;', script_mode=True),
            (['select +  - 1;'], None))

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  ', script_mode=True),
            (['select +  - 1;'], None))

        self.assertEqual(
            lexutils.split_edgeql('  select +  - 1;  select ;;',
                                  script_mode=True),
            (['select +  - 1;', 'select ;'], None))

        self.assertEqual(
            lexutils.split_edgeql('''\
                CREATE TYPE blah {
                    set ;
                    blah ;
                };
                select 1;
                ''', script_mode=True),
            ([
                '''CREATE TYPE blah {
                    set ;
                    blah ;
                };''',
                '''select 1;'''
            ], None))

    def test_split_edgeql_07(self):
        # test script mode with various incomplete parts
        self.assertEqual(
            lexutils.split_edgeql('', script_mode=True), ([], None))
        self.assertEqual(
            lexutils.split_edgeql(' ', script_mode=True), ([], None))
        self.assertEqual(
            lexutils.split_edgeql(' \n ', script_mode=True), ([], None))

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  select 1',
                                  script_mode=True),
            (['select +  - 1;', 'select 1'], None))

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  select {;}',
                                  script_mode=True),
            (['select +  - 1;', 'select {;}'], None))

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  select {;;;;}}}} select',
                                  script_mode=True),
            (['select +  - 1;', 'select {;;;;}}}} select'], None))

        self.assertEqual(
            lexutils.split_edgeql('select +  - 1;  select {;;;;}}}}; select',
                                  script_mode=True),
            (['select +  - 1;', 'select {;;;;}}}};', 'select'], None))
