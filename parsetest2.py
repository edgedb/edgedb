from typing import *

from edb.edgeql import parser
from edb.edgeql import ast as qlast


def parse(querystr: str) -> qlast.Expr:
    return parser.parse_block(querystr)


QS = [
    '''
        select User
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
    '''
    SELECT (false, }]})
    ''',
    '''
    SELECT 1
    SELECT 2;
    ''',
    '''
    SELECT (false, true false])
    ''',
    '''
    for c Card union c.hello
    ''',
    '''
    SELECT User name }
    ''',
]

for q in QS[:]:
    
    print('-' * 30)
    print()

    try:
        ast = parse(q)
    except:
        pass