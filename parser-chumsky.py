from edb.edgeql import qltypes as qltypes
from edb import _edgeql_rust
from edb.common import parsing

sources = [
    '''select .<deck[IS User]''',
    '''select <str>.cost ++ ' ' ++ .element''',
    '''select (SELECT .awards FILTER .name != '3rd')''',
    '''select sum(.deck.cost)''',
    '''select @count * .cost''',
    '''select .name ++ (("-" ++ @text) ?? "")''',
    'SELECT 111111111111111111111111111111111111111111111111111111;',
    'SELECT (1 + ) * 2 + (3 + (x = 4 - with))',
]

for source in sources:
    
    ast, errors = _edgeql_rust.parse_block(source)
    
    for x in ast or []:
        x.context = parsing.ParserContext('<string>', source, 3, 7)

        x.dump_edgeql()

    for index, (msg, start, end) in enumerate(errors):
        
        print(f'Error [{index+1}/{len(errors)}]:')
        print(source)
        print(' ' * start + '^' + '-' * (end - start - 1) + ' ' + msg)
        print()
