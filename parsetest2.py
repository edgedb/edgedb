from typing import *
import pickle

from edb.edgeql import parser
from edb.edgeql import tokenizer
from edb.edgeql import ast as qlast


def parse(querystr: str) -> qlast.Expr:
    sdl = querystr.startswith('sdl')
    if sdl:
        querystr = querystr[3:]
    
    # s = tokenizer.NormalizedSource.from_string(querystr)
    s = tokenizer.Source.from_string(querystr)
    bytes = pickle.dumps(s)
    s2 = pickle.loads(bytes)
    
    if sdl:
        return parser.parse_sdl(s2)
    else:
        return parser.parse_block(s2)


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
    SELECT (User IS (Named, Text));
    ''',
    '''
    SELECT INTROSPECT tuple<int64>;
    ''',
    '''
    CREATE FUNCTION std::strlen(string: std::str = '1', abc: std::str)
            -> std::int64 {};
    ''',
]

for q in QS[-1:]:
    
    print('-' * 30)
    print()

    # try:
    ast = parse(q)

    if not isinstance(ast, list):
        ast = [ast]

    for x in ast:
        x.dump()
        x.dump_edgeql()
    # except Exception as e:
        # print(e)
        # pass