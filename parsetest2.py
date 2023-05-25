from __future__ import annotations

from pathlib import Path
import sys
EDB_DIR = Path(__file__).parent.parent.parent.resolve()
sys.path.insert(0, str(EDB_DIR))

from typing import *

from edb import edgeql
from edb.common import parsing
from edb.edgeql import codegen as qlcodegen
from edb.edgeql import parser
from edb.edgeql import ast as qlast
from edb._edgeql_parser import parse_cheese


import json

def parse(querystr: str) -> qlast.Expr:
    source = edgeql.Source.from_string(querystr)
    # query = parser.parse_fragment_with_recovery(source)
    query = parser.parse_fragment_raw(source)
    return query

QS = [
'''
select User { name, email } filter .name = 'Sully'
''',
]

def process_spec(spec):
    # print(spec)
    actions = spec.actions()

    nonterms = sorted(set(
        x.production.lhs for xs in actions for lol in xs.values() for x in lol
        if 'ShiftAction' not in str(type(x))
    ), key=str)
    nonterm_numbers = {k: i for i, k in enumerate(nonterms)}

    rmap = {v._token: c for (_, c), v in parsing.TokenMeta.token_map.items()}

    # XXX: TOKENS
    table = []
    for st_actions in actions:
        out_st_actions = []
        for tok, act in st_actions.items():
            act = act[0]  # XXX: LR! NOT GLR??

            stok = rmap.get(str(tok), str(tok))
            if 'ShiftAction' in str(type(act)):
                oact = int(act.nextState)
            else:
                production = act.production
                oact = dict(
                    nonterm=str(production.lhs),
                    production=production.qualified.split('.')[-1],
                    cnt=len(production.rhs),
                )
            out_st_actions.append((stok, oact))

        table.append(out_st_actions)

    # goto
    goto = []
    for st_goto in spec.goto():
        out_goto = []
        for nterm, act in st_goto.items():
            out_goto.append((str(nterm), act))

        goto.append(out_goto)

    obj = dict(actions=table, goto=goto, start=str(spec.start_sym()))

    return json.dumps(obj)


def main() -> None:
    # for qry in QS:
    #     # print(qry.rstrip())
    #     # print(' =>')
    #     _, raw = parse(qry)
    #     print(json.dumps(raw, indent=2))
    #     # code = qlcodegen.generate_source(q)
    #     # print(code)
    #     # print('===')

    prs = parser.qlparser.EdgeQLExpressionParser()
    spec = prs.get_parser_spec()
    jspec = process_spec(spec)

    # parse(QS[0])

    print(parse_cheese(jspec, QS[0]))


if __name__ == '__main__':
    main()
