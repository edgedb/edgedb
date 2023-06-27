from typing import *

import pickle

from edb.edgeql import tokenizer

QS = [
    '''
        with module cards
        for g in (group Card by .element) union (for gi in 0 union (
            element := g.key.element,
            cst := sum(g.elements.cost + gi),
        ))
    ''',
]

for q in QS[:1]:

    tokens = tokenizer._tokenize(q)
    print(tokens)

    data = pickle.dumps(tokens)
    print(data)
    
    tokens2 = pickle.loads(data)
    print(tokens2)

    