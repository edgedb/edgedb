#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

import parsing
import pathlib
from typing import Dict, List

class EBNF_Production:
    def __init__(self, name: str):
        self.name = name

class EBNF_Token(EBNF_Production):
    def __init__(self, name: str):
        super().__init__(name)

class EBNF_NonTerm(EBNF_Production):
    def __init__(self, name: str):
        super().__init__(name)

class EBNF_OptionalList(EBNF_NonTerm):
    def __init__(self, name: str, item_lists: List[List[str]]):
        super().__init__(name)
        self.item_lists = item_lists

class EBNF_RequiredList(EBNF_NonTerm):
    def __init__(self, name: str, item_lists: List[List[str]]):
        super().__init__(name)
        self.item_lists = item_lists

def to_iso_ebnf(productions: List[EBNF_Production]) -> List[str]:
    result = []

    for production in productions:
        if isinstance(production, EBNF_Token):
            result.append(
                production.name + ' = ' + '"' + production.name + '"' + ';'
            )
        elif isinstance(production, EBNF_OptionalList):
            result.append(
                production.name + ' = ' + ' | '.join(
                    '[' + ', '.join(item_list) + ']'
                    for item_list in production.item_lists
                ) + ';'
            )
        elif isinstance(production, EBNF_RequiredList):
            result.append(
                production.name + ' = ' + ' | '.join(
                    '(' + ', '.join(item_list) + ')'
                    for item_list in production.item_lists
                ) + ';'
            )

    return result

def to_w3c_ebnf(productions: List[EBNF_Production]) -> List[str]:
    result = []

    for production in productions:
        if isinstance(production, EBNF_Token):
            result.append(
                production.name + ' ::= ' + '"' + production.name + '"'
            )
        elif isinstance(production, EBNF_OptionalList):
            result.append(
                production.name + ' ::= ' + ' | '.join(
                    '(' + ' '.join(item_list) + ')'
                    for item_list in production.item_lists
                ) + '?'
            )
        elif isinstance(production, EBNF_RequiredList):
            result.append(
                production.name + ' ::= ' + ' | '.join(
                    '(' + ' '.join(item_list) + ')'
                    for item_list in production.item_lists
                )
            )

    return result

def to_ebnf(spec: parsing.Spec, path: pathlib.Path):
    ebnf_productions: List[EBNF_Production] = []

    # add token productions
    for token in spec._tokens:
        if token in ['<e>', '<$>']:
            continue
        ebnf_productions.append(EBNF_Token(token))

    # add nonterm productions
    nonterm_productions: Dict[str, List[List[str]]] = {}

    has_production = {
        **{
            token: True
            for token in spec._tokens
        },
        **{
            nonterm: False
            for nonterm in spec._nonterms
        },
    }

    for production in spec._productions:
        prod_name = str(production.lhs)
        if prod_name == '<S>':
            continue

        item_list = [
            item.name
            for item in production.rhs
        ]

        has_production[prod_name] = True

        if prod_name not in nonterm_productions:
            nonterm_productions[prod_name] = []
        nonterm_productions[prod_name].append(item_list)

    for prod_name, item_lists in nonterm_productions.items():
        # if a production only refers to nonterminals with no productions
        # remove it, since it does nothing

        nonterm_productions[prod_name] = [
            item_list
            for item_list in item_lists
            if
                # refers to token or nonterm with productions
                any(has_production[item] for item in item_list) or
                # keep empty reductions, handled later as an optional
                item_list == []
        ]

    for prod_name, item_lists in nonterm_productions.items():
        if [] in item_lists:
            # optional nonterm
            ebnf_productions.append(
                EBNF_OptionalList(prod_name, [
                    item_list
                    for item_list in item_lists
                    if item_list != []
                ])
            )
        else:
            ebnf_productions.append(
                EBNF_RequiredList(prod_name, item_lists)
            )

    # output
    with open(path / 'grammar.iso.ebnf', 'w') as file:
        file.write('\n'.join(to_iso_ebnf(ebnf_productions)))
    with open(path / 'grammar.w3c.ebnf', 'w') as file:
        file.write('\n'.join(to_w3c_ebnf(ebnf_productions)))
