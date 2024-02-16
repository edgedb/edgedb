#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations
from typing import *  # NoQA

import json
import logging
import os
import sys
import types

import parsing

from edb.common import context as pctx, debug

ParserContext = pctx.ParserContext

logger = logging.getLogger('edb.common.parsing')


class ParserSpecIncompatibleError(Exception):
    pass


class TokenMeta(type):
    token_map: Dict[Tuple[Any, Any], Any] = {}

    def __new__(
            mcls, name, bases, dct, *, token=None, lextoken=None,
            precedence_class=None):
        result = super().__new__(mcls, name, bases, dct)

        if precedence_class is not None:
            result._precedence_class = precedence_class

        if name == 'Token' or name == 'GrammarToken':
            return result

        if token is None:
            if not name.startswith('T_'):
                raise Exception(
                    'Token class names must either start with T_ or have '
                    'a token parameter')
            token = name[2:]

        if lextoken is None:
            lextoken = token

        result._token = token
        mcls.token_map[mcls, lextoken] = result

        if not result.__doc__:
            doc = '%%token %s' % token

            pcls = getattr(result, '_precedence_class', None)
            if pcls is None:
                try:
                    pcls = sys.modules[mcls.__module__].PrecedenceMeta
                except (KeyError, AttributeError):
                    pass

            if pcls is None:
                msg = 'Precedence class is not set for {!r}'.format(mcls)
                raise TypeError(msg)

            prec = pcls.for_token(token)
            if prec:
                doc += ' [%s]' % prec.__name__

            result.__doc__ = doc

        return result

    def __init__(
            cls, name, bases, dct, *, token=None, lextoken=None,
            precedence_class=None):
        super().__init__(name, bases, dct)

    @classmethod
    def for_lex_token(mcls, token):
        return mcls.token_map[mcls, token]


class Token(parsing.Token, metaclass=TokenMeta):
    def __init__(self, val, clean_value, context=None):
        super().__init__()
        self.val = val
        self.clean_value = clean_value
        self.context = context

    def __repr__(self):
        return '<Token %s "%s">' % (self.__class__._token, self.val)


def inline(argument_index: int):
    """
    When added to grammar productions, it makes the method equivalent to:

    self.val = kids[argument_index].val
    """

    def decorator(func: Any):
        func.inline_index = argument_index
        return func
    return decorator


class NontermMeta(type):
    def __new__(mcls, name, bases, dct):
        result = super().__new__(mcls, name, bases, dct)

        if name == 'Nonterm' or name == 'ListNonterm':
            return result

        if not result.__doc__:
            result.__doc__ = '%nonterm'

        for name, attr in result.__dict__.items():
            if (name.startswith('reduce_') and
                    isinstance(attr, types.FunctionType)):
                inline_index = getattr(attr, 'inline_index', None)

                if attr.__doc__ is None:
                    tokens = name.split('_')
                    if name == 'reduce_empty':
                        tokens = ['reduce', '<e>']

                    doc = r'%reduce {}'.format(' '.join(tokens[1:]))

                    prec = getattr(attr, '__parsing_precedence__', None)
                    if prec is not None:
                        doc += ' [{}]'.format(prec)

                    attr = lambda self, *args, meth=attr: meth(self, *args)
                    attr.__doc__ = doc

                a = pctx.has_context(attr)

                a.__doc__ = attr.__doc__
                a.inline_index = inline_index
                setattr(result, name, a)

        return result


class Nonterm(parsing.Nonterm, metaclass=NontermMeta):
    pass


class ListNontermMeta(NontermMeta):
    def __new__(mcls, name, bases, dct, *, element, separator=None):
        if name != 'ListNonterm':
            if isinstance(separator, TokenMeta):
                separator = separator._token
            elif isinstance(separator, NontermMeta):
                separator = separator.__name__

            tokens = [name]
            if separator:
                tokens.append(separator)

            if isinstance(element, TokenMeta):
                element = element._token
            elif isinstance(element, NontermMeta):
                element = element.__name__

            tokens.append(element)

            if separator:
                prod = lambda self, lst, sep, el: self._reduce_list(lst, el)
            else:
                prod = lambda self, lst, el: self._reduce_list(lst, el)
            dct['reduce_' + '_'.join(tokens)] = prod
            dct['reduce_' + element] = lambda self, el: self._reduce_el(el)

        cls = super().__new__(mcls, name, bases, dct)
        return cls

    def __init__(cls, name, bases, dct, *, element, separator=None):
        super().__init__(name, bases, dct)


class ListNonterm(Nonterm, metaclass=ListNontermMeta, element=None):

    def _reduce_list(self, lst, el):
        if el.val is None:
            tail = []
        else:
            tail = [el.val]

        self.val = lst.val + tail

    def _reduce_el(self, el):
        if el.val is None:
            tail = []
        else:
            tail = [el.val]

        self.val = tail

    def __iter__(self):
        return iter(self.val)

    def __len__(self):
        return len(self.val)


def precedence(precedence):
    """Decorator to set production precedence."""
    def decorator(func):
        func.__parsing_precedence__ = precedence.__name__
        return func

    return decorator


class PrecedenceMeta(type):
    token_prec_map: Dict[Tuple[Any, Any], Any] = {}
    last: Dict[Tuple[Any, Any], Any] = {}

    def __new__(
            mcls, name, bases, dct, *, assoc, tokens=None, prec_group=None,
            rel_to_last='>'):
        result = super().__new__(mcls, name, bases, dct)

        if name == 'Precedence':
            return result

        if not result.__doc__:
            doc = '%%%s' % assoc

            last = mcls.last.get((mcls, prec_group))
            if last:
                doc += ' %s%s' % (rel_to_last, last.__name__)

            result.__doc__ = doc

        if tokens:
            for token in tokens:
                existing = None
                try:
                    existing = mcls.token_prec_map[mcls, token]
                except KeyError:
                    mcls.token_prec_map[mcls, token] = result
                else:
                    raise Exception(
                        'token {} has already been set precedence {}'.format(
                            token, existing))

        mcls.last[mcls, prec_group] = result

        return result

    def __init__(
            cls, name, bases, dct, *, assoc, tokens=None, prec_group=None,
            rel_to_last='>'):
        super().__init__(name, bases, dct)

    @classmethod
    def for_token(mcls, token_name):
        return mcls.token_prec_map.get((mcls, token_name))


class Precedence(parsing.Precedence, assoc='fail', metaclass=PrecedenceMeta):
    pass


def load_parser_spec(
    mod: types.ModuleType
) -> parsing.Spec:
    return parsing.Spec(
        mod,
        skinny=not debug.flags.edgeql_parser,
        logFile=_localpath(mod, "log"),
        verbose=bool(debug.flags.edgeql_parser),
    )


def _localpath(mod, type):
    return os.path.join(
        os.path.dirname(mod.__file__),
        mod.__name__.rpartition('.')[2] + '.' + type)


def load_spec_productions(
    production_names: List[Tuple[str, str]],
    mod: types.ModuleType
) -> List[Tuple[Type, Callable]]:
    productions: List[Tuple[Any, Callable]] = []
    for class_name, method_name in production_names:
        cls = mod.__dict__.get(class_name, None)
        if not cls:
            # for NontermStart
            productions.append((parsing.Nonterm(), lambda *args: None))
            continue

        method = cls.__dict__[method_name]
        productions.append((cls, method))
    return productions


def spec_to_json(spec: parsing.Spec) -> str:
    # Converts a ParserSpec into JSON. Called from edgeql-parser Rust crate.
    assert spec.pureLR

    token_map: Dict[str, str] = {
        v._token: c for (_, c), v in TokenMeta.token_map.items()
    }

    # productions
    productions_all: Set[Any] = set()
    for st_actions in spec.actions():
        for _, acts in st_actions.items():
            act = cast(Any, acts[0])
            if 'ReduceAction' in str(type(act)):
                prod = act.production
                productions_all.add(prod)
    productions, production_id = sort_productions(productions_all)

    # actions
    actions = []
    for st_actions in spec.actions():
        out_st_actions = []
        for tok, acts in st_actions.items():
            act = cast(Any, acts[0])

            str_tok = token_map.get(str(tok), str(tok))
            if 'ShiftAction' in str(type(act)):
                action_obj: Any = {
                    'Shift': int(act.nextState)
                }
            else:
                prod = act.production
                action_obj = {
                    'Reduce': {
                        'production_id': production_id[prod],
                        'non_term': str(prod.lhs),
                        'cnt': len(prod.rhs),
                    }
                }

            out_st_actions.append((str_tok, action_obj))

        out_st_actions.sort(key=lambda item: item[0])
        actions.append(out_st_actions)

    # goto
    goto = []
    for st_goto in spec.goto():
        out_goto = []
        for nterm, action in st_goto.items():
            out_goto.append((str(nterm), action))

        goto.append(out_goto)

    # inlines
    inlines = []
    for prod in productions:
        id = production_id[prod]
        inline = getattr(prod.method, 'inline_index', None)
        if inline is not None:
            assert isinstance(inline, int)
            inlines.append((id, inline))

    res = {
        'actions': actions,
        'goto': goto,
        'start': str(spec.start_sym()),
        'inlines': inlines,
        'production_names': list(map(production_name, productions)),
    }
    return json.dumps(res)


def sort_productions(
    productions_all: Set[Any]
) -> Tuple[List[Any], Dict[Any, int]]:
    productions = list(productions_all)
    productions.sort(key=production_name)

    productions_id = {prod: id for id, prod in enumerate(productions)}
    return (productions, productions_id)


def production_name(prod: Any) -> Tuple[str, ...]:
    return tuple(prod.qualified.split('.')[-2:])
