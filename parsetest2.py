from typing import *
import pickle

from edb.edgeql import parser
from edb.edgeql import tokenizer
from edb.edgeql import ast as qlast


def parse(querystr: str) -> qlast.Expr:
    s = tokenizer.NormalizedSource.from_string(querystr)
    bytes = pickle.dumps(s)
    s2 = pickle.loads(bytes)
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
    
]

for q in QS[-1:]:
    
    print('-' * 30)
    print()

    # try:
    ast = parse(q)

    for x in ast:
        x.dump_edgeql()
    # except Exception as e:
        # print(e)
        # pass