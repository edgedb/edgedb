##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


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
        mcls.token_map[lextoken] = result

        if not result.__doc__:
            doc = '%%token %s' % token

            prec = PrecedenceMeta.for_token(token)
            if prec:
                doc += ' [%s]' % prec.__name__

            result.__doc__ = doc

        return result

    def __init__(cls, name, bases, dct, *, token=None, lextoken=None):
        super().__init__(name, bases, dct)

    @classmethod
    def for_lex_token(mcls, token):
        return mcls.token_map[token]


class NontermMeta(type):
    def __new__(mcls, name, bases, dct):
        result = super().__new__(mcls, name, bases, dct)

        if name == 'Nonterm':
            return result

        if not result.__doc__:
            result.__doc__ = '%nonterm'
        return result


class PrecedenceMeta(type):
    token_prec_map = {}
    last = {}

    def __new__(mcls, name, bases, dct, *, assoc, tokens=None, prec_group=None, rel_to_last='>'):
        result = super().__new__(mcls, name, bases, dct)

        if name == 'Precedence':
            return result

        if not result.__doc__:
            doc = '%%%s' % assoc

            last = mcls.last.get(prec_group)
            if last:
                doc += ' %s%s' % (rel_to_last, last.__name__)

            result.__doc__ = doc

        if tokens:
            for token in tokens:
                existing = None
                try:
                    existing = mcls.token_prec_map[token]
                except KeyError:
                    mcls.token_prec_map[token] = result
                else:
                    raise Exception('token %s has already been set precedence %s'\
                                    % (token, existing))

        mcls.last[prec_group] = result

        return result

    def __init__(cls, name, bases, dct, *, assoc, tokens=None, prec_group=None, rel_to_last='>'):
        super().__init__(name, bases, dct)

    @classmethod
    def for_token(mcls, token_name):
        return mcls.token_prec_map.get(token_name)

