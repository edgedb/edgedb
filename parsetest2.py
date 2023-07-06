from typing import *

from edb.edgeql import ast as qlast
from edb.edgeql.parser import parser as qlparser
from edb.edgeql import tokenizer
from edb import _edgeql_parser as ql_parser

QS = [
    '''
        select 1
    ''',
    '''
        select User { name, email } filter .name = 'Sully'
    ''',
    '''
        SELECT {354.32,
            35400000000000.32,
            35400000000000000000.32,
            3.5432e20,
            3.5432e+20,
            3.5432e-20,
            3.543_2e-20,
            354.32e-20,
            2_354.32e-20,
            0e-999
        }
        ''',
    '''
        with module cards
        for g in (group Card by .element) union (for gi in 0 union (
            element := g.key.element,
            cst := sum(g.elements.cost + gi),
        ))
        ''',
    '''
    select '10 seconds'
    ''',
    '''SELECT (User.id, User { name := ''',
    '''SELECT (false, }]})''',
    '''
    SELECT User { name, last_name }
    WITH u := User SELECT u;
    ''',
    '''
    SELECT (false, true false])
    ''',
    '''
    for c Card union c.hello
    ''',
    '''
    SELECT User id, name }
    ''',
    '''
    CREATE TYPE cfg::TestSessionConfig EXTENDING cfg::ConfigObject {
        CREATE REQUIRED PROPERTY name -> std::str {
            CREATE CONSTRAINT std::exclusive;
        }
    };
    ''',
    '''
    CREATE FUNCTION
    std::_gen_series(
        `start`: std::int64,
        stop: std::int64
    ) -> SET OF std::int64
    {
        SET volatility := 'Immutable';
        USING SQL FUNCTION 'generate_series';
    };
    ''',
    '''
    select b"04e3b";
    ''',
    '''
    select User { intersect };
    ''',
    '''
    create module __std__;
    ''',
    '''
    create type Hello {
        create property intersect -> str;
        create property `__std__` -> str;
    };
    ''',
    '''
    SELECT
    count(
        schema::Module
        FILTER NOT .builtin AND NOT .name = "default"
    ) + count(
        schema::Object
        FILTER .name LIKE "default::%"
    ) > 0
    ''',
    '''sdl
    module test {
        function len1(a: str b: str) ->  std::str {
            using SQL function 'length1'
        }
    ''',
    '''
    SELECT len('');
    ''',
    '''
    SELECT __std__::len({'hello', 'world'});
    ''',
    '''sdl
    module test {
        alias FooBaz := [1 2];
    };
    ''',
    '''
    SEL ECT 1
    ''',
    '''
    SELECT (
        foo: 1,
        bar := 3
    );
    ''',
    '''
    SELECT (
        foo: (
            bar: 42
        )
    );
    ''',
    '''
    SELECT count(FOR X IN {Foo} UNION X);
    ''',
    '''
    SELECT some_agg(User.name) OVER (ORDER BY User.age ASC);
    SELECT some_agg(User.name) OVER (
        PARTITION BY strlen(User.name)
        ORDER BY User.age ASC);
    SELECT some_agg(User.name) OVER (
        PARTITION BY User.email, User.age
        ORDER BY User.age ASC);
    SELECT some_agg(User.name) OVER (
        PARTITION BY User.email, User.age
        ORDER BY User.age ASC THEN User.name ASC);
    ''',
    '''
    SELECT Issue{
            name,
            related_to *-1,
        };
    ''',
    '''
    SELECT __type__;
    ''',
    '''
    SELECT Issue{
        name,
        related_to *,
    };
    ''',
    '''
    SELECT Foo {(bar)};
    ''',
    '''
    SELECT Foo.__source__;
    ''',
    '''
    SELECT Foo.bar@__type__;
    ''',
    '''
    SELECT Foo {
        __type__.name
    };
    ''',
    '''
    SELECT INTROSPECT tuple<int64>;
    ''',
    '''
    CREATE FUNCTION std::strlen(string: std::str = '1', abc: std::str)
            -> std::int64 {};
    ''',
    '''
    SELECT Obj.n + random()
    ''',
    '''
    CREATE MIGRATION { ;;; CREATE TYPE Foo ;;; CREATE TYPE Bar ;;; };
    ''',
    '''
    SELECT (User IS (Named, Text));
    ''',
    '''sdl
    module test {
        scalar type foobar {
            index prop on (__source__);
        };
    };
    ''',
    '''
    INSERT Foo FILTER Foo.bar = 42;
    ''',
    '''sdl
    module test {
        function some_func($`(`: str = ) ) -> std::str {
            using edgeql function 'some_other_func';
        }
    };
    ''',
    '''
    SELECT (a := 1, foo);
    '''
]

for q in QS[-2:]:
    sdl = q.startswith('sdl')
    if sdl:
        q = q[3:]

    try:
        # s = tokenizer.NormalizedSource.from_string(q)
        source = tokenizer.Source.from_string(q)
    except BaseException as e:
        print('Error during tokenization:')
        print(e)
        continue

    if sdl:
        spec = qlparser.EdgeSDLSpec()
    else:
        spec = qlparser.EdgeQLBlockSpec()

    parser_obj = spec.get_parser()

    parser_obj.filename = None
    parser_obj.source = source

    parser_name = spec.__class__.__name__
    result, productions = ql_parser.parse(parser_name, source.tokens())

    print('-' * 30)
    print()

    for index, error in enumerate(result.errors()):
        message, span = error
        (start, end) = tokenizer.inflate_span(source.text(), span)

        print(f'Error [{index+1}/{len(result.errors())}]:')
        print(
            '\n'.join(source.text().splitlines()[(start.line - 1) : end.line])
        )
        print(
            ' ' * (start.column - 1)
            + '^'
            + '-' * (end.column - start.column - 1)
            + ' '
            + message
        )
        print()

    if result.out():
        try:
            ast = parser_obj._cst_to_ast(result.out(), productions).val
        except BaseException:
            ast = None
        if ast:
            print('Recovered AST:')
            if isinstance(ast, list):
                for x in ast:
                    x.dump_edgeql()
            elif isinstance(ast, qlast.Base):
                ast.dump_edgeql()
            else:
                print(ast)
