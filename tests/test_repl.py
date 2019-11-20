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

from edb.repl import utils


class TestReplUtils(unittest.TestCase):

    def test_split_edgeql_01(self):
        # test regular complete statements
        self.assertEqual(
            utils.split_edgeql('select +  - 1;', script_mode=False),
            (['select +  - 1;'], None))

        self.assertEqual(
            utils.split_edgeql('select +  - 1;  ', script_mode=False),
            (['select +  - 1;'], None))

        self.assertEqual(
            utils.split_edgeql(
                '  select +  - 1;  select ;;', script_mode=False),
            (['select +  - 1;', 'select ;'], None))

        self.assertEqual(
            utils.split_edgeql(';;;', script_mode=False),
            ([], None))

        self.assertEqual(
            utils.split_edgeql('''\
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
            utils.split_edgeql('', script_mode=False),
            ([], ''))
        self.assertEqual(
            utils.split_edgeql(' ', script_mode=False),
            ([], ' '))
        self.assertEqual(
            utils.split_edgeql(' \n ', script_mode=False),
            ([], ' \n '))
        self.assertEqual(utils.split_edgeql(
            ' \n sel \n ', script_mode=False),
            ([], ' \n sel \n '))

        self.assertEqual(
            utils.split_edgeql('select +  - 1;  select 1',
                               script_mode=False),
            (['select +  - 1;'], '  select 1'))

        self.assertEqual(
            utils.split_edgeql('select +  - 1;  select {;}',
                               script_mode=False),
            (['select +  - 1;'], '  select {;}'))

        self.assertEqual(
            utils.split_edgeql('select +  - 1;  select {;;;;}}}}',
                               script_mode=False),
            (['select +  - 1;'], '  select {;;;;}}}}'))

    def test_split_edgeql_03(self):
        # test multiline statements where the string is unterminated
        self.assertEqual(
            utils.split_edgeql('SELECT "aaa', script_mode=False),
            ([], 'SELECT "aaa'))

        self.assertEqual(
            utils.split_edgeql('SELECT "as', script_mode=False),
            ([], 'SELECT "as'))

        self.assertEqual(
            utils.split_edgeql('SELECT "as\n', script_mode=False),
            ([], 'SELECT "as\n'))

    def test_split_edgeql_04(self):
        # test multiline statements where the ';' is not a separator
        self.assertEqual(
            utils.split_edgeql('SELECT "aaa;', script_mode=False),
            ([], 'SELECT "aaa;'))

        self.assertEqual(
            utils.split_edgeql('SELECT 1 #;', script_mode=False),
            ([], 'SELECT 1 #;'))

    def test_split_edgeql_05(self):
        # test invalid tokens
        self.assertEqual(
            utils.split_edgeql('SELECT 1 ~ 2;', script_mode=False),
            (['SELECT 1 ~ 2;'], None))

        self.assertEqual(
            utils.split_edgeql('SELECT 1 ~ 2', script_mode=False),
            ([], 'SELECT 1 ~ 2'))

    def test_split_edgeql_06(self):
        # test regular script mode
        self.assertEqual(
            utils.split_edgeql('select +  - 1;', script_mode=True),
            (['select +  - 1;'], None))

        self.assertEqual(
            utils.split_edgeql('select +  - 1;  ', script_mode=True),
            (['select +  - 1;'], None))

        self.assertEqual(
            utils.split_edgeql('  select +  - 1;  select ;;',
                               script_mode=True),
            (['select +  - 1;', 'select ;'], None))

        self.assertEqual(
            utils.split_edgeql('''\
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
            utils.split_edgeql('', script_mode=True), ([], None))
        self.assertEqual(
            utils.split_edgeql(' ', script_mode=True), ([], None))
        self.assertEqual(
            utils.split_edgeql(' \n ', script_mode=True), ([], None))

        self.assertEqual(
            utils.split_edgeql('select +  - 1;  select 1',
                               script_mode=True),
            (['select +  - 1;', 'select 1'], None))

        self.assertEqual(
            utils.split_edgeql('select +  - 1;  select {;}',
                               script_mode=True),
            (['select +  - 1;', 'select {;}'], None))

        self.assertEqual(
            utils.split_edgeql('select +  - 1;  select {;;;;}}}} select',
                               script_mode=True),
            (['select +  - 1;', 'select {;;;;}}}} select'], None))

        self.assertEqual(
            utils.split_edgeql('select +  - 1;  select {;;;;}}}}; select',
                               script_mode=True),
            (['select +  - 1;', 'select {;;;;}}}};', 'select'], None))

    def test_normalize_name_01(self):
        # test REPL name normalization
        self.assertEqual(
            utils.normalize_name('Object'), 'Object')
        self.assertEqual(
            utils.normalize_name('`Object`'), 'Object')
        self.assertEqual(
            utils.normalize_name('std::Object'), 'std::Object')
        self.assertEqual(
            utils.normalize_name('`std`::`Object`'), 'std::Object')
        self.assertEqual(
            utils.normalize_name('std::Ob je`ct'), 'std::`Ob je``ct`')
        self.assertEqual(
            utils.normalize_name('std::`Ob je``ct`'), 'std::`Ob je``ct`')
        self.assertEqual(
            utils.normalize_name('foo::Group'), 'foo::`Group`')
        self.assertEqual(
            utils.normalize_name('foo::`Group`'), 'foo::`Group`')
        self.assertEqual(
            utils.normalize_name('select::Group'), '`select`::`Group`')
        self.assertEqual(
            utils.normalize_name('`select`::Group'), '`select`::`Group`')
        self.assertEqual(
            utils.normalize_name('`select`::`Group`'), '`select`::`Group`')

    def test_normalize_name_02(self):
        # The empty string quote is not a valid identifier,
        # therefore it's treated as plain text "``".
        self.assertEqual(
            utils.normalize_name('Obj``ect'), '`Obj````ect`')
        # valid, albeit odd name
        self.assertEqual(
            utils.normalize_name('`'), '````')
        self.assertEqual(
            utils.normalize_name('````'), '````')
        # empty quoted identifier is illegal, so it must be
        # interpreted literally as 2 "`"
        self.assertEqual(
            utils.normalize_name('``'), '``````')
        # "```" is not a valid quoted identifier, therefore it's
        # treated as plain text, so the quoted version consists of
        # 3*2+2=8 "`"
        self.assertEqual(
            utils.normalize_name('```'), '````````')
        self.assertEqual(
            utils.normalize_name('````````'), '````````')

    def test_normalize_name_03(self):
        # The normalization allows some unusual quoting that would not
        # be legal in EdgeQL, but since \d command is not bound by the
        # same rules for valid identifiers, these are allowed here.
        self.assertEqual(
            # quoting embedded into a name
            utils.normalize_name('Ob`je`ct'), 'Object')
        self.assertEqual(
            # "correct" identifier that actually contains 2 "`"
            utils.normalize_name('`Ob``je``ct`'), '`Ob``je``ct`')
        self.assertEqual(
            # identifier that actually contains 2 "`", where only the
            # first "`" is quoted
            utils.normalize_name('`Ob```je`ct'), '`Ob``je``ct`')
        self.assertEqual(
            # identifier that actually contains 2 "`", where only the
            # "`" are quoted
            utils.normalize_name('Ob````je````ct'), '`Ob``je``ct`')
        self.assertEqual(
            # quoting that encompasses "::", which would be illegal in EdgeQL
            utils.normalize_name('`std::Object`'), 'std::Object')

    def test_normalize_name_04(self):
        # this results in an illegal (empty) name
        self.assertEqual(
            utils.normalize_name(''), '')
        # this results in an illegal name with empty module
        self.assertEqual(
            utils.normalize_name('::Foo'), '')
        # this results in an illegal name with empty short name
        self.assertEqual(
            utils.normalize_name('foo::'), '')
        # this results in an illegal name starting with "@"
        self.assertEqual(
            utils.normalize_name('@foo'), '')
