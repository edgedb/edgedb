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


import io
import re
import textwrap
import unittest

from edb.repl import utils
from edb.repl import table


class TestReplUtils(unittest.TestCase):

    def test_repl_split_edgeql_01(self):
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

    def test_repl_split_edgeql_02(self):
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

    def test_repl_split_edgeql_03(self):
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

    def test_repl_split_edgeql_04(self):
        # test multiline statements where the ';' is not a separator
        self.assertEqual(
            utils.split_edgeql('SELECT "aaa;', script_mode=False),
            ([], 'SELECT "aaa;'))

        self.assertEqual(
            utils.split_edgeql('SELECT 1 #;', script_mode=False),
            ([], 'SELECT 1 #;'))

    def test_repl_split_edgeql_05(self):
        # test invalid tokens
        self.assertEqual(
            utils.split_edgeql('SELECT 1 ~ 2;', script_mode=False),
            (['SELECT 1 ~ 2;'], None))

        self.assertEqual(
            utils.split_edgeql('SELECT 1 ~ 2', script_mode=False),
            ([], 'SELECT 1 ~ 2'))

    def test_repl_split_edgeql_06(self):
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

    def test_repl_split_edgeql_07(self):
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

    def test_repl_normalize_name_01(self):
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

    def test_repl_normalize_name_02(self):
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

    def test_repl_normalize_name_03(self):
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

    def test_repl_normalize_name_04(self):
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

    def test_repl_filter_pattern_01(self):
        # no pattern - no filter
        clause, qkw = utils.get_filter_based_on_pattern('')
        self.assertEqual(clause, '')
        self.assertEqual(qkw, {})

        clause, qkw = utils.get_filter_based_on_pattern('', ['name'])
        self.assertEqual(clause, '')
        self.assertEqual(qkw, {})

        clause, qkw = utils.get_filter_based_on_pattern('', ['name'], 'i')
        self.assertEqual(clause, '')
        self.assertEqual(qkw, {})

        # actual filters
        clause, qkw = utils.get_filter_based_on_pattern(r'std')
        self.assertEqual(clause, r'FILTER re_test(<str>$re_filter, .name)')
        self.assertEqual(qkw, {'re_filter': 'std'})

        clause, qkw = utils.get_filter_based_on_pattern(r'std', ['foo'])
        self.assertEqual(clause, r'FILTER re_test(<str>$re_filter, foo)')
        self.assertEqual(qkw, {'re_filter': 'std'})

        clause, qkw = utils.get_filter_based_on_pattern(r'std', ['foo'], 'i')
        self.assertEqual(clause, r'FILTER re_test(<str>$re_filter, foo)')
        self.assertEqual(qkw, {'re_filter': '(?i)std'})

        clause, qkw = utils.get_filter_based_on_pattern(r'\s*\'\w+\'')
        self.assertEqual(clause, r'FILTER re_test(<str>$re_filter, .name)')
        self.assertEqual(qkw, {'re_filter': r'\s*\'\w+\''})

    def test_repl_filter_pattern_02(self):
        # no pattern - no filter
        clause, qkw = utils.get_filter_based_on_pattern(
            r'foo', ['first_name', 'last_name'])
        self.assertEqual(
            clause,
            r'FILTER re_test(<str>$re_filter, first_name) OR '
            r're_test(<str>$re_filter, last_name)')
        self.assertEqual(qkw, {'re_filter': 'foo'})

    def _compare_tables(self, tab1: str, tab2: str, max_width: int) -> None:
        # trailing whitespace for each line is ignored in this comparison
        t1 = re.sub(r' +\n', '\n', tab1.strip())
        t2 = re.sub(r' +\n', '\n', tab2.strip())
        self.assertEqual(t1, t2)

        # the table width is as specified (if applicable)
        if max_width:
            for line in tab1.split('\n'):
                if len(line) > 0:
                    # only consider non-empty lines
                    self.assertEqual(len(line), max_width,
                                     'table width is not as expected')

    def test_repl_render_table_01(self):
        output = io.StringIO()
        table.render_table(
            title='Objects',
            columns=[
                table.ColumnSpec(
                    field='foo', title='Foo', width=3, align='left'),
                table.ColumnSpec(
                    field='bar', title='Bar', width=2, align='center'),
                table.ColumnSpec(
                    field='baz', title='Baz', width=1, align='right'),
            ],
            data=[
                {'foo': 'hello', 'bar': 'green', 'baz': 1},
                {'foo': 'world', 'bar': 'red', 'baz': 9001},
                {'foo': '!', 'bar': 'magenta', 'baz': 42},
            ],
            max_width=40,
            file=output,
        )

        self._compare_tables(
            output.getvalue(),
            textwrap.dedent('''
                --------------- Objects --------+-------
                 Foo               |    Bar     |   Baz
                -------------------+------------+-------
                 hello             |   green    |     1
                 world             |    red     |  9001
                 !                 |  magenta   |    42
            '''),
            max_width=40,
        )

    def test_repl_render_table_02(self):
        output = io.StringIO()
        table.render_table(
            title='Objects',
            columns=[
                table.ColumnSpec(
                    field='foo', title='Foo', width=3, align='left'),
                table.ColumnSpec(
                    field='bar', title='Bar', width=2, align='center'),
                table.ColumnSpec(
                    field='baz', title='Baz', width=1, align='right'),
            ],
            data=[
                {'foo': 'hello', 'bar': 'green', 'baz': 1},
                {'foo': 'world', 'bar': 'red', 'baz': 9001},
                {'foo': '!', 'bar': 'magenta', 'baz': 42},
            ],
            max_width=20,
            file=output,
        )

        self._compare_tables(
            output.getvalue(),
            textwrap.dedent('''
                ----- Objects -+----
                 Foo    | Bar  | Ba+
                        |      |  z
                --------+------+----
                 hello  | gree+|  1
                        |  n   |
                 world  | red  | 90+
                        |      | 01
                 !      | mage+| 42
                        | nta  |
            '''),
            max_width=20,
        )

    def test_repl_render_table_03(self):
        output = io.StringIO()
        table.render_table(
            title='Objects',
            columns=[
                table.ColumnSpec(
                    field='foo', title='Foo', width=3, align='left'),
            ],
            data=[
                {'foo': 'hello'},
                {'foo': 'world'},
                {'foo': '!'},
            ],
            max_width=20,
            file=output,
        )

        self._compare_tables(
            output.getvalue(),
            textwrap.dedent('''
                ----- Objects ------
                 Foo
                --------------------
                 hello
                 world
                 !
            '''),
            max_width=20,
        )

    def test_repl_render_table_04(self):
        output = io.StringIO()
        table.render_table(
            title='Objects',
            columns=[
                table.ColumnSpec(
                    field='foo', title='Foo', width=3, align='left'),
                table.ColumnSpec(
                    field='bar', title='Bar', width=2, align='center'),
                table.ColumnSpec(
                    field='baz', title='Baz', width=1, align='right'),
            ],
            data=[
                {'foo': 'hello', 'bar': 'green', 'baz': 1},
                {'foo': 'world', 'bar': 'red', 'baz': 9001},
                {'foo': '!', 'bar': 'magenta', 'baz': 42},
            ],
            # minimum width for 3 columns
            max_width=11,
            file=output,
        )

        self._compare_tables(
            output.getvalue(),
            textwrap.dedent('''
                - Objects -
                 F+| B+| B+
                 o+| a+| a+
                 o | r | z
                ---+---+---
                 h+| g+| 1
                 e+| r+|
                 l+| e+|
                 l+| e+|
                 o | n |
                 w+| r+| 9+
                 o+| e+| 0+
                 r+| d | 0+
                 l+|   | 1
                 d |   |
                 ! | m+| 4+
                   | a+| 2
                   | g+|
                   | e+|
                   | n+|
                   | t+|
                   | a |
            '''),
            max_width=11,
        )

    def test_repl_render_table_05(self):
        output = io.StringIO()
        table.render_table(
            title='Extra Long Objects Title',
            columns=[
                table.ColumnSpec(
                    field='foo', title='Foo', width=3, align='left'),
                table.ColumnSpec(
                    field='bar', title='Bar', width=2, align='center'),
                table.ColumnSpec(
                    field='baz', title='Baz', width=1, align='right'),
            ],
            data=[
                {'foo': 'hello', 'bar': 'green', 'baz': 1},
                {'foo': 'world', 'bar': 'red', 'baz': 9001},
                {'foo': '!', 'bar': 'magenta', 'baz': 42},
            ],
            # width below minimum, will still render the minimum width
            # table, but the title may be longer than the table width
            max_width=1,
            file=output,
        )

        self._compare_tables(
            output.getvalue(),
            textwrap.dedent('''
                Extra Long Objects Title
                 F+| B+| B+
                 o+| a+| a+
                 o | r | z
                ---+---+---
                 h+| g+| 1
                 e+| r+|
                 l+| e+|
                 l+| e+|
                 o | n |
                 w+| r+| 9+
                 o+| e+| 0+
                 r+| d | 0+
                 l+|   | 1
                 d |   |
                 ! | m+| 4+
                   | a+| 2
                   | g+|
                   | e+|
                   | n+|
                   | t+|
                   | a |
            '''),
            # don't check width
            max_width=0,
        )

    def test_repl_render_table_06(self):
        output = io.StringIO()
        table.render_table(
            title='Objects',
            columns=[
                table.ColumnSpec(
                    field='foo', title='Foo', width=3, align='left'),
            ],
            data=[
                {'foo': 'hello'},
                {'foo': 'world'},
                {'foo': '!'},
            ],
            max_width=0,
            file=output,
        )

        self._compare_tables(
            output.getvalue(),
            textwrap.dedent(''),
            max_width=0,
        )
