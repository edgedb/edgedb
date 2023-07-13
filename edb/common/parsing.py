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

import functools
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

        if name == 'Token':
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


class ParserSpec:
    parser_spec: ClassVar[parsing.Spec | None]

    def __init__(self, **parser_data):
        self.parser_data = parser_data

    def get_debug(self):
        return debug.flags.edgeql_parser

    def get_parser_spec_module(self) -> types.ModuleType:
        raise NotImplementedError

    def get_parser_spec(self, allow_rebuild: bool = True) -> parsing.Spec:
        cls = self.__class__

        try:
            spec = cls.__dict__['parser_spec']
        except KeyError:
            pass
        else:
            if spec is not None:
                return spec

        mod = self.get_parser_spec_module()
        spec = parsing.Spec(
            mod,
            pickleFile=self.localpath(mod, "pickle"),
            skinny=not self.get_debug(),
            logFile=self.localpath(mod, "log"),
            verbose=self.get_debug(),
            unpickleHook=functools.partial(
                self.on_spec_unpickle, mod, allow_rebuild)
        )

        self.__class__.parser_spec = spec
        return spec

    def on_spec_unpickle(
        self,
        mod: types.ModuleType,
        allow_rebuild: bool,
        spec: parsing.Spec,
        compatibility: str,
    ) -> None:
        if compatibility != "compatible":
            if allow_rebuild:
                logger.info(f'rebuilding grammar for {mod.__name__}...')
            else:
                raise ParserSpecIncompatibleError(
                    f'parser tables for {mod.__name__} are missing or '
                    f'incompatible with parser source'
                )

    def localpath(self, mod, type):
        return os.path.join(
            os.path.dirname(mod.__file__),
            mod.__name__.rpartition('.')[2] + '.' + type)
