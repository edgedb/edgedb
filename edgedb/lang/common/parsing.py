##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys

import Parsing
import pyggy

from semantix import SemantixError


class TokenMeta(type):
    token_map = {}

    def __new__(mcls, name, bases, dct, *, token=None, lextoken=None):
        result = super().__new__(mcls, name, bases, dct)

        if name == 'Token':
            return result

        if token is None:
            if not name.startswith('T_'):
                raise Exception('Token class names must either start with T_ or have token parameter')
            token = name[2:]

        if lextoken is None:
            lextoken = token

        result._token = token
        mcls.token_map[mcls, lextoken] = result

        if not result.__doc__:
            doc = '%%token %s' % token

            prec = sys.modules[mcls.__module__].PrecedenceMeta.for_token(token)
            if prec:
                doc += ' [%s]' % prec.__name__

            result.__doc__ = doc

        return result

    def __init__(cls, name, bases, dct, *, token=None, lextoken=None):
        super().__init__(name, bases, dct)

    @classmethod
    def for_lex_token(mcls, token):
        return mcls.token_map[mcls, token]


class Token(Parsing.Token, metaclass=TokenMeta):
    def __init__(self, parser, val):
        super().__init__(parser)
        self.val = val

    def __repr__(self):
        return '<Token %s "%s">' % (self.__class__._token, self.val)


class NontermMeta(type):
    def __new__(mcls, name, bases, dct):
        result = super().__new__(mcls, name, bases, dct)

        if name == 'Nonterm':
            return result

        if not result.__doc__:
            result.__doc__ = '%nonterm'
        return result


class Nonterm(Parsing.Nonterm, metaclass=NontermMeta):
    pass


class PrecedenceMeta(type):
    token_prec_map = {}
    last = {}

    def __new__(mcls, name, bases, dct, *, assoc, tokens=None, prec_group=None, rel_to_last='>'):
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
                    raise Exception('token %s has already been set precedence %s'\
                                    % (token, existing))

        mcls.last[mcls, prec_group] = result

        return result

    def __init__(cls, name, bases, dct, *, assoc, tokens=None, prec_group=None, rel_to_last='>'):
        super().__init__(name, bases, dct)

    @classmethod
    def for_token(mcls, token_name):
        return mcls.token_prec_map.get((mcls, token_name))


class Precedence(Parsing.Precedence, assoc='fail', metaclass=PrecedenceMeta):
    pass


class ParserError(SemantixError):
    def __init__(self, msg, *, token=None, lineno=None, expr=None):
        super().__init__(msg)

        self.token = token
        self.lineno = lineno
        self.expr = expr


    def __str__(self):
        return "unexpected `%s' on line %d" % (self.token, self.lineno)


class Parser:
    def __init__(self):
        self.lexer = None
        self.parser = None

    def cleanup(self):
        self.__class__.parser_spec = None
        self.__class__.lexer_spec = None
        self.lexer = None
        self.parser = None

    def get_debug(self):
        return False

    def get_exception(self, native_err):
        return ParserError(native_err.args[0])

    def get_specs(self):
        mod = self.get_parser_spec_module()
        self.__class__.parser_spec = Parsing.Spec(
                                                mod,
                                                pickleFile=self.localpath(mod, "pickle"),
                                                skinny=not self.get_debug(),
                                                logFile=self.localpath(mod, "log"),
                                                #graphFile=self.localpath(mod, "dot"),
                                                verbose=self.get_debug())

        _, lexer_spec = pyggy.getlexer(self.localpath(mod, "pyl"))
        self.__class__.lexer_spec = lexer_spec.lexspec

    def localpath(self, mod, type):
        return os.path.join(os.path.dirname(mod.__file__), mod.__name__.rpartition('.')[2] + '.' + type)

    def reset_parser(self, input):
        if getattr(self.__class__, 'parser_spec', None) is None:
            self.get_specs()

        if not self.parser:
            self.lexer = pyggy.lexer.lexer(self.__class__.lexer_spec)
            self.parser = Parsing.Lr(self.__class__.parser_spec)

        self.parser.reset()
        self.lexer.setinputstr(input)

    def parse(self, input):
        self.reset_parser(input)
        mod = self.get_parser_spec_module()

        try:
            tok = self.lexer.token()

            while tok:
                token = mod.TokenMeta.for_lex_token(tok)(self.parser, self.lexer.value)

                self.parser.token(token)
                tok = self.lexer.token()

            self.parser.eoi()

        except Parsing.SyntaxError as e:
            raise self.get_exception(e) from e

        return self.parser.start[0].val
