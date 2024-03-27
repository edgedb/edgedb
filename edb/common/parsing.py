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
from typing import (
    Any, Callable, Tuple, Type, Dict, List, Set, Optional, cast
)

import json
import logging
import os
import sys
import types

import parsing

from edb.common import debug, span

Span = span.Span

logger = logging.getLogger('edb.common.parsing')


class ParserSpecIncompatibleError(Exception):
    pass


class Token(parsing.Token):
    token_map: Dict[Any, Any] = {}
    _token: str = ""

    def __init_subclass__(
        cls, *, token=None, lextoken=None, is_internal=False, **kwargs
    ):
        super().__init_subclass__(**kwargs)

        if is_internal:
            return

        if token is None:
            if not cls.__name__.startswith('T_'):
                raise Exception(
                    'Token class names must either start with T_ or have '
                    'a token parameter')
            token = cls.__name__[2:]

        if lextoken is None:
            lextoken = token

        cls._token = token
        Token.token_map[lextoken] = cls

        if not cls.__doc__:
            doc = '%%token %s' % token

            prec = Precedence.for_token(token)
            if prec:
                doc += ' [%s]' % prec.__name__

            cls.__doc__ = doc

    def __init__(self, val, clean_value, span=None):
        super().__init__()
        self.val = val
        self.clean_value = clean_value
        self.span = span

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


class Nonterm(parsing.Nonterm):

    def __init_subclass__(cls, *, is_internal=False, **kwargs):
        """Add docstrings to class and reduce functions

        If no class docstring is present, set it to '%nonterm'.

        If any reduce function (ie. of the form `reduce(_\\w+)+` does not
        have a docstring, a new one is generated based on the function name.

        See https://github.com/MagicStack/parsing for more information.

        Keyword arguments:
        is_internal -- internal classes do not need docstrings and processing
                       can be skipped
        """
        super().__init_subclass__(**kwargs)

        if is_internal:
            return

        if not cls.__doc__:
            cls.__doc__ = '%nonterm'

        for name, attr in cls.__dict__.items():
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

                a = span.wrap_function_to_infer_spans(attr)

                a.__doc__ = attr.__doc__
                a.inline_index = inline_index
                setattr(cls, name, a)


class ListNonterm(Nonterm, is_internal=True):
    def __init_subclass__(
        cls,
        *,
        element,
        separator=None,
        is_internal=False,
        allow_trailing_separator=False,
        **kwargs,
    ):
        """Create reductions for list classes.

        If trailing separator is not allowed, the class can handle all
        reductions directly.

            L := E
            L := L S E

        If trailing separator is allowed, create an inner class to handle
        all non-trailing reductions. Then the class handles the trailing
        separator.

            I := E
            I := I S E
            L := I
            L := I S

        The inner class is added to the same module as the class.
        """
        if not is_internal:
            if not allow_trailing_separator:
                # directly handle the list
                ListNonterm.add_list_reductions(
                    cls, element=element, separator=separator
                )

            else:
                # create inner list class and add to same module
                mod = sys.modules[cls.__module__]

                def inner_cls_exec(ns):
                    ns['__module__'] = mod.__name__
                    return ns

                inner_cls_name = cls.__name__ + 'Inner'
                inner_cls_kwds = dict(element=element, separator=separator)
                inner_cls = types.new_class(inner_cls_name, (ListNonterm,),
                                            inner_cls_kwds, inner_cls_exec)
                setattr(mod, inner_cls_name, inner_cls)

                # create reduce_inner function
                separator_name = ListNonterm.component_name(separator)

                setattr(cls,
                    'reduce_{}'.format(inner_cls_name),
                    lambda self, inner: (
                        ListNonterm._reduce_inner(self, inner)
                    ))
                setattr(cls,
                    'reduce_{}_{}'.format(inner_cls_name, separator_name),
                    lambda self, inner, sep: (
                        ListNonterm._reduce_inner(self, inner)
                    ))

        # reduce functions must be present before calling superclass
        super().__init_subclass__(is_internal=is_internal, **kwargs)

    def __iter__(self):
        return iter(self.val)

    def __len__(self):
        return len(self.val)

    @staticmethod
    def add_list_reductions(cls, *, element, separator=None):
        element_name = ListNonterm.component_name(element)
        separator_name = ListNonterm.component_name(separator)

        if separator_name:
            tail_prod = lambda self, lst, sep, el: (
                ListNonterm._reduce_list(self, lst, el)
            )
            tail_prod_name = 'reduce_{}_{}_{}'.format(
                cls.__name__, separator_name, element_name)
        else:
            tail_prod = lambda self, lst, el: (
                ListNonterm._reduce_list(self, lst, el)
            )
            tail_prod_name = 'reduce_{}_{}'.format(
                cls.__name__, element_name)
        setattr(cls, tail_prod_name, tail_prod)

        setattr(cls, 'reduce_' + element_name,
            lambda self, el: ListNonterm._reduce_el(self, el))

    @staticmethod
    def component_name(component: type) -> Optional[str]:
        if component is None:
            return None
        elif issubclass(component, Token):
            return component._token
        elif issubclass(component, Nonterm):
            return component.__name__
        else:
            raise Exception(
                'List component must be a Token or Nonterm')

    @staticmethod
    def _reduce_list(self, lst, el):
        if el.val is None:
            tail = []
        else:
            tail = [el.val]

        self.val = lst.val + tail

    @staticmethod
    def _reduce_el(self, el):
        if el.val is None:
            tail = []
        else:
            tail = [el.val]

        self.val = tail

    @staticmethod
    def _reduce_inner(self, inner):
        self.val = inner.val


def precedence(precedence):
    """Decorator to set production precedence."""
    def decorator(func):
        func.__parsing_precedence__ = precedence.__name__
        return func

    return decorator


class Precedence(parsing.Precedence):
    token_prec_map: Dict[Any, Any] = {}
    last: Dict[Any, Any] = {}

    def __init_subclass__(
        cls,
        *,
        assoc,
        tokens=None,
        prec_group=None,
        rel_to_last='>',
        is_internal=False,
        **kwargs,
    ):
        super().__init_subclass__(**kwargs)

        if is_internal:
            return

        if not cls.__doc__:
            doc = '%%%s' % assoc

            last = Precedence.last.get(prec_group)
            if last:
                doc += ' %s%s' % (rel_to_last, last.__name__)

            cls.__doc__ = doc

        if tokens:
            for token in tokens:
                existing = None
                try:
                    existing = Precedence.token_prec_map[token]
                except KeyError:
                    Precedence.token_prec_map[token] = cls
                else:
                    raise Exception(
                        'token {} has already been set precedence {}'.format(
                            token, existing))

        Precedence.last[prec_group] = cls

    @classmethod
    def for_token(cls, token_name):
        return Precedence.token_prec_map.get(token_name)


def load_parser_spec(mod: types.ModuleType) -> parsing.Spec:
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
    production_names: List[Tuple[str, str]], mod: types.ModuleType
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
        v._token: c for c, v in Token.token_map.items()
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
    productions_all: Set[Any],
) -> Tuple[List[Any], Dict[Any, int]]:
    productions = list(productions_all)
    productions.sort(key=production_name)

    productions_id = {prod: id for id, prod in enumerate(productions)}
    return (productions, productions_id)


def production_name(prod: Any) -> Tuple[str, ...]:
    return tuple(prod.qualified.split('.')[-2:])
