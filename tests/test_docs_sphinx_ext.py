import contextlib
import os.path
import subprocess
import tempfile
import textwrap
import unittest

try:
    import requests_xml
except ModuleNotFoundError:
    requests_xml = None


class BuildFailedError(Exception):
    pass


class BaseDomainTest:

    def build(self, src, *, format='html'):
        src = textwrap.dedent(src)

        with tempfile.TemporaryDirectory() as td_in, \
                tempfile.TemporaryDirectory() as td_out:

            # Since v2.0, Sphinx uses "index" as master_doc by default.
            fn = os.path.join(td_in, 'index.rst')
            with open(fn, 'wt') as f:
                f.write(src)
                f.flush()

            args = [
                'sphinx-build',
                '-b', format,
                '-W',
                '-n',
                '-C',
                '-D', 'extensions=edb.tools.docs',
                '-q',
                td_in,
                td_out,
                fn
            ]

            try:
                subprocess.run(
                    args, check=True,
                    stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            except subprocess.CalledProcessError as ex:
                msg = [
                    'The build has failed.',
                    '',
                    'STDOUT',
                    '======',
                    ex.stdout.decode(),
                    '',
                    'STDERR',
                    '======',
                    ex.stderr.decode(),
                    '',
                    'INPUT',
                    '=====',
                    src
                ]
                new_ex = BuildFailedError('\n'.join(msg))
                new_ex.stdout = ex.stdout.decode()
                new_ex.stderr = ex.stderr.decode()
                raise new_ex from ex

            with open(os.path.join(td_out, f'index.{format}'), 'rt') as f:
                out = f.read()

            return out

    @contextlib.contextmanager
    def assert_fails(self, err):
        with self.assertRaises(BuildFailedError) as raised:
            yield

        self.assertRegex(raised.exception.stderr, err)


@unittest.skipIf(requests_xml is None, 'requests-xml package is not installed')
class TestEqlType(unittest.TestCase, BaseDomainTest):

    def test_sphinx_eql_type_01(self):
        src = '''
        .. eql:type:: int64

            descr
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('''
                //desc_signature
                    [@eql-fullname="std::int64"] /
                    desc_name / text()
            '''),
            ['int64'])

    def test_sphinx_eql_type_02(self):
        src = '''
        .. eql:type:: std::int64
        '''

        with self.assert_fails('the directive must include a description'):
            self.build(src)

    def test_sphinx_eql_type_03(self):
        src = '''
        .. eql:type:: std::int64

            aaa

        Testing refs :eql:type:`int1`
        '''

        with self.assert_fails(
                "cannot resolve :eql:type: targeting 'type::std::int1'"):
            self.build(src)

    def test_sphinx_eql_type_04(self):
        src = '''
        .. eql:type:: std::int64

            aaa

        Testing refs :eql:type:`int64`
        '''

        self.assertRegex(
            self.build(src),
            r'(?x).*<a .* href="#std::int64".*')

    def test_sphinx_eql_type_05(self):
        src = '''
        .. eql:type:: std::int64

            long text long text long text long text long text long text
            long text long text long text long text long text long text

            long text
        '''

        with self.assert_fails("shorter than 80 characters"):
            self.build(src)

    def test_sphinx_eql_type_06(self):
        src = r'''
        .. eql:type:: std::int64

            An integer.

        .. eql:type:: std::array

            Array.

        Testing :eql:type:`XXX <array<int64>>` ref.
        Testing :eql:type:`array\<int64\>` ref.
        Testing :eql:type:`array\<int64\> <array<int64>>` ref.
        Testing :eql:type:`array\<array\<int64\>\>` ref.
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('''
                //paragraph /
                reference[@eql-type="type"] /
                literal / text()
            '''),
            ['XXX', 'array<int64>', 'array<int64>', 'array<array<int64>>'])

    def test_sphinx_eql_type_07(self):
        src = '''
        .. eql:type:: int64

            An integer.

        Testing :eql:type:`OPTIONAL  int64` ref.
        Testing :eql:type:`OPTIONAL int64` ref.
        Testing :eql:type:`SET  OF  int64` ref.
        Testing :eql:type:`SET OF int64` ref.
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('''
                //paragraph /
                reference[@eql-type="type"] /
                literal / text()
            '''),
            ['OPTIONAL  int64', 'OPTIONAL int64',
             'SET  OF  int64', 'SET OF int64'])

    def test_sphinx_eql_type_08(self):
        src = '''
        .. eql:type:: SET OF

            An integer.

        Testing :eql:type:`SET OF`.
        Testing :eql:type:`XXX <SET OF>`.
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('''
                //paragraph /
                reference[@eql-type="type"] /
                literal / text()
            '''),
            ['SET OF', 'XXX'])


@unittest.skipIf(requests_xml is None, 'requests-xml package is not installed')
class TestEqlFunction(unittest.TestCase, BaseDomainTest):

    def test_sphinx_eql_func_01(self):
        src = '''
        Testing DESC !! :eql:func-desc:`test` !! >> ref.

        .. eql:type:: std::int64

            An integer.

        .. eql:type:: any

            any.

        .. eql:function:: std::test(v: any) -> any

            :index: xxx YyY

            A super function.

        Testing :eql:func:`XXX <test>` ref.
        Testing :eql:func:`test` ref.
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        func = x.xpath('//desc[@desctype="function"]')
        self.assertEqual(len(func), 1)
        func = func[0]

        self.assertEqual(func.attrs['summary'], 'A super function.')
        self.assertIn('!! A super function. !!', out)

        self.assertEqual(
            x.xpath('//desc_returns / text()'),
            ['any'])

        self.assertEqual(
            x.xpath('''
                //paragraph /
                reference[@eql-type="function" and
                    @refid="function::std::test"] /
                literal / text()
            '''),
            ['XXX', 'test()'])

        self.assertEqual(
            x.xpath('''
                //field[@eql-name="index"] / field_body / paragraph / text()
            '''),
            ['xxx YyY'])

    def test_sphinx_eql_func_02(self):
        src = '''
        .. eql:function:: std::test() -> any

            long text long text long text long text long text long text
            long text long text long text long text long text long text

            long text
        '''

        with self.assert_fails("shorter than 80 characters"):
            self.build(src)

    def test_sphinx_eql_func_03(self):
        src = '''
        .. eql:function:: std::test(v: any) -> any

            :type v: int64

            blah
        '''

        expected = r'''(?xs)
        found\sunknown\sfield\s'type' .*
        Possible\sreason:\sfield\s'type'\sis\snot\ssupported
        '''

        with self.assert_fails(expected):
            self.build(src)

    def test_sphinx_eql_func_05(self):
        src = '''
        .. eql:function:: std::test(v: any) -> any

        blah
        '''

        with self.assert_fails('the directive must include a description'):
            self.build(src)

    def test_sphinx_eql_func_06(self):
        src = '''
        .. eql:function:: std::test(v: any) -> any

            blah

            :index: foo bar

            blah
        '''

        with self.assert_fails(
                'fields must be specified before all other content'):
            self.build(src)

    def test_sphinx_eql_func_07(self):
        src = '''
        .. eql:function:: std::test(a: OPTIONAL str, b: SET OF str, \\
                            c: str) -> SET OF str

            blah

        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('//desc_returns / text()'),
            ['set of str'])

        self.assertEqual(
            x.xpath('//desc_signature/@eql-signature'),
            ['std::test(a: optional str, b: set of str, c: str) -> set of str']
        )

    def test_sphinx_eql_func_08(self):
        src = '''
        .. eql:function:: std::test(NAMED ONLY v: in64=42) -> OPTIONAL int64

            blah
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('//desc_returns / text()'),
            ['optional int64'])

        self.assertEqual(
            x.xpath('//desc_signature/@eql-signature'),
            ['std::test(named only v: in64 = 42) -> optional int64'])

    def test_sphinx_eql_func_09(self):
        src = '''
        .. eql:function:: sys::sleep(duration: duration) -> bool
                          sys::sleep(duration: float64) -> bool

            :index: sleep delay

            blah
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('//desc_signature/@eql-signature'),
            ['sys::sleep(duration: duration) ->  bool',
             'sys::sleep(duration: float64) ->  bool'])


@unittest.skipIf(requests_xml is None, 'requests-xml package is not installed')
class TestEqlConstraint(unittest.TestCase, BaseDomainTest):

    def test_sphinx_eql_constr_01(self):
        src = '''
        .. eql:type:: std::int64

            An integer.

        .. eql:type:: any

            any.

        .. eql:constraint:: std::max_len_value(v: any)

            blah

        Testing :eql:constraint:`XXX <max_len_value>` ref.
        Testing :eql:constraint:`max_len_value` ref.
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        constr = x.xpath('//desc[@desctype="constraint"]')
        self.assertEqual(len(constr), 1)
        constr = constr[0]

        self.assertEqual(constr.attrs['summary'], 'blah')

        self.assertEqual(
            x.xpath('''
                //paragraph /
                reference[@eql-type="constraint" and
                    @refid="constraint::std::max_len_value"] /
                literal / text()
            '''),
            ['XXX', 'max_len_value'])

    def test_sphinx_eql_constr_02(self):
        src = '''
        .. eql:constraint:: std::len_value on (len(<std::str>__subject__))

            blah
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        sig = x.xpath('//desc[@desctype="constraint"]/desc_signature')[0]

        self.assertEqual(
            sig.attrs['eql-signature'],
            'std::len_value on (len(<std::str>__subject__))')

        self.assertEqual(
            sig.attrs['eql-subjexpr'],
            'len(<std::str>__subject__)')


@unittest.skipIf(requests_xml is None, 'requests-xml package is not installed')
class TestEqlOperator(unittest.TestCase, BaseDomainTest):

    def test_sphinx_eql_op_01(self):
        src = '''
        Testing ?? :eql:op-desc:`PLUS` ??.

        .. eql:type:: int64

            int64

        .. eql:type:: str

            123

        .. eql:operator:: PLUS: A + B

            :optype A: int64 or str
            :optype B: int64 or str
            :resulttype: int64 or str

            Arithmetic addition.

        some text

        :eql:op:`XXX <PLUS>`
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertIn('Testing ?? Arithmetic addition. ??.', out)

        self.assertEqual(
            len(x.xpath('''
                //desc_signature[@eql-name="PLUS" and @eql-signature="A + B"] /
                *[
                    (self::desc_annotation and text()="operator") or
                    (self::desc_name and text()="A + B")
                ]
            ''')),
            2)

        self.assertEqual(len(x.xpath('//field[@eql-name="operand"]')), 2)
        self.assertEqual(len(x.xpath('//field[@eql-name="resulttype"]')), 1)

        self.assertEqual(
            x.xpath('''
                //paragraph /
                reference[@eql-type="operator" and @refid="operator::PLUS"] /
                literal / text()
            '''),
            ['XXX'])

    def test_sphinx_eql_op_02(self):
        src = '''
        .. eql:type:: any

            123

        .. eql:operator:: IS: A IS B

            :optype A: any
            :optype B: type
            :resulttype: any

            Is

        :eql:op:`XXX <IS>`
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('''
                //field[@eql-opname="B"] /
                field_body / * / literal_strong / text()
            '''),
            ['B'])


@unittest.skipIf(requests_xml is None, 'requests-xml package is not installed')
class TestEqlKeyword(unittest.TestCase, BaseDomainTest):

    def test_sphinx_eql_kw_01(self):
        src = '''
        .. eql:keyword:: SET OF

            blah

        some text

        :eql:kw:`XXX <SET OF>`
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            len(x.xpath('''
                //desc[@desctype="keyword"] /

                desc_signature[@eql-name="SET OF"] /
                *[
                    (self::desc_annotation and text()="keyword") or
                    (self::desc_name and text()="SET OF")
                ]
            ''')),
            2)

        self.assertEqual(
            x.xpath('''
                //paragraph /
                reference[@eql-type="keyword" and @refid="keyword::SET-OF"] /
                literal / text()
            '''),
            ['XXX'])


@unittest.skipIf(requests_xml is None, 'requests-xml package is not installed')
class TestEqlStatement(unittest.TestCase, BaseDomainTest):

    def test_sphinx_eql_stmt_05(self):
        src = '''

        CREATE FUNCTION
        ===============

        :eql-statement:

        ``CREATE FUNCTION``--creates a function.

        fooing and baring.

        Subhead
        -------

        asdasdas


        CREATE TYPE
        ===========

        :eql-statement:

        blah.


        Test
        ====

        A ref to :eql:stmt:`create function`

        A ref to :eql:stmt:`ttt <create type>`
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('''
                //paragraph /
                reference[@eql-type="statement" and
                          @refid="statement::create-function"] /
                literal / text()
            '''),
            ['create function'])

        self.assertEqual(
            x.xpath('''
                //paragraph /
                reference[@eql-type="statement" and
                          @refid="statement::create-type"] /
                literal / text()
            '''),
            ['ttt'])

        self.assertEqual(
            x.xpath('''
                //section[@eql-statement="true"]/@ids
            '''),
            ['create-function statement::create-function',
             'create-type statement::create-type'])

        self.assertEqual(
            x.xpath('''
                //section[@eql-statement="true"]/@summary
            '''),
            ['CREATE FUNCTION--creates a function.', 'blah.'])

    def test_sphinx_eql_stmt_06(self):
        src = '''

        AAAAAA
        ======

        :eql-statement:

        aa aaaaaa aaaaa aaaa aa aaaaaa aaaaa aaaa aa aaaaaa aaaaa aaaa aa
        aa aaaaaa aaaaa aaaa aa aaaaaa aaaaa aaaa.
        '''

        with self.assert_fails(
                'first paragraph is longer than 79 characters'):
            self.build(src)

    def test_sphinx_eql_stmt_08(self):
        src = '''

        AA AA
        =====

        :eql-statement:

        aa aaaaaa aaaaa aaaa aa.

        BB
        --

        :eql-statement:

        bbb.
        '''

        with self.assert_fails(
                ' has a nested section with a :eql-statement:'):
            self.build(src)

    def test_sphinx_eql_stmt_09(self):
        src = '''

        AA AA
        =====

        :eql-statement:

        aa aaaaaa aaaaa aaaa aa.

        AA AA
        =====

        :eql-statement:

        aa aaaaaa aaaaa aaaa aa.
        '''

        with self.assert_fails("duplicate 'AA AA' statement"):
            self.build(src)

    def test_sphinx_eql_stmt_10(self):
        src = '''
        =========
        Functions
        =========

        :edb-alt-title: Functions and Operators

        This section describes the DDL commands ...


        CREATE FUNCTION
        ===============

        :eql-statement:

        Define a new function.


        DROP FUNCTION
        =============

        :eql-statement:
        :eql-haswith:

        Remove a function.
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('''
                //section/title[text()="Functions"]/@edb-alt-title
            '''),
            ['Functions and Operators'])

        self.assertEqual(
            x.xpath('''
                //section[@eql-statement="true"]/title/text()
            '''),
            ['CREATE FUNCTION', 'DROP FUNCTION'])

        self.assertEqual(
            x.xpath('''
                //section[@eql-statement="true" and @eql-haswith="true"]
                    /title/text()
            '''),
            ['DROP FUNCTION'])

    def test_sphinx_eql_struct_01(self):
        src = '''
        .. eql:struct:: edb.protocol.AuthenticationSASLFinal

        .. eql:struct:: edb.protocol.enums.Cardinality
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('''
                literal_block/@language
            '''),
            ['c', 'c'])

        val = x.xpath('''
            literal_block/text()
        ''')
        self.assertIn('struct AuthenticationSASLFinal {', val[0])
        self.assertIn('enum Cardinality {', val[1])


@unittest.skipIf(requests_xml is None, 'requests-xml package is not installed')
class TestEqlRoles(unittest.TestCase, BaseDomainTest):

    def test_sphinx_eql_inline_role_01(self):
        src = '''
        a test of :eql:synopsis:`WITH <aaaa>`.
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('''
                //literal[@eql-lang="edgeql-synopsis"] / text()
            '''),
            ['WITH <aaaa>'])

    def test_sphinx_eql_inline_role_02(self):
        cases = [
            (
                '#123',
                'edgedb/edgedb/issues/123',
                None,
                '#123',
            ),
            (
                'magicstack/asyncpg/#227',
                'magicstack/asyncpg/issues/227',
                None,
                'magicstack/asyncpg/#227',
            ),
            (
                'ff123aaaaeeeee',
                'edgedb/edgedb/commit/ff123aaaaeeeee',
                None,
                'ff123aaa'
            ),
            (
                'magicstack/asyncpg/ff123aaaaeeeee',
                'magicstack/asyncpg/commit/ff123aaaaeeeee',
                None,
                'magicstack/asyncpg/ff123aaa'
            ),

            (
                '#123',
                'edgedb/edgedb/issues/123',
                'blah1',
                'blah1',
            ),
            (
                'magicstack/asyncpg/#227',
                'magicstack/asyncpg/issues/227',
                'blah2',
                'blah2',
            ),
            (
                'ff123aaaaeeeee',
                'edgedb/edgedb/commit/ff123aaaaeeeee',
                'blah3',
                'blah3',
            ),
            (
                'magicstack/asyncpg/ff123aaaaeeeee',
                'magicstack/asyncpg/commit/ff123aaaaeeeee',
                'blah4',
                'blah4',
            ),
        ]

        src = ''
        for (body, _, title, _) in cases:
            if title:
                src += f':eql:gh:`{title} <{body}>`\n'
            else:
                src += f':eql:gh:`{body}`\n'

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        for (_, expected_link, _, expected_title) in cases:
            self.assertEqual(
                x.xpath(f'''
                    //reference[
                        @eql-github="True" and
                        @name="{expected_title}" and
                        @refuri="https://github.com/{expected_link}"
                    ]/text()
                '''),
                [expected_title]
            )


@unittest.skipIf(requests_xml is None, 'requests-xml package is not installed')
class TestBlockquote(unittest.TestCase, BaseDomainTest):

    def test_sphinx_eql_blockquote_01(self):
        src = '''
        blah

         * list
         * item
        '''

        with self.assert_fails('blockquote found'):
            self.build(src, format='xml')

        with self.assert_fails('blockquote found'):
            self.build(src, format='html')

    def test_sphinx_eql_blockquote_02(self):
        # Test that although regular block-qoutes are blocked
        # (as their syntax is very confusing and fragile), we can
        # still use explicit block-quotes via the `.. pull-quote::`
        # directive.

        src = '''
        blah

        .. pull-quote::

            spam

        blah2
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('''
                block_quote/*/text()
            '''),
            ['spam']
        )

    def test_sphinx_eql_singlebacktick_01(self):
        src = '''
        Another use case is for giving short aliases to long module names
        (especially if module names contain `.`).
        '''

        with self.assert_fails('title reference'):
            self.build(src, format='xml')

        with self.assert_fails('title reference'):
            self.build(src, format='html')

    def test_sphinx_edb_collapsed_01(self):
        src = '''
        blah

        Foo
        ===

        bar

        .. edb:collapsed::

            spam

            ham

        blah2
        '''

        out = self.build(src, format='xml')
        x = requests_xml.XML(xml=out)

        self.assertEqual(
            x.xpath('''
                //container[@collapsed_block="True"]/paragraph/text()
            '''),
            ['spam', 'ham'])
