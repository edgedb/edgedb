##
# Copyright (c) 2010-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import os
import sys
import types

import parsing

from edgedb.lang.common.exceptions import EdgeDBError, add_context, get_context
from edgedb.lang.common import context as pctx
from edgedb.lang.common import lexer

ParserContext = pctx.ParserContext


class TokenMeta(type):
    token_map = {}

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
    def __init__(self, parser, val, context=None):
        super().__init__(parser)
        self.val = val
        self.context = context

    def __repr__(self):
        return '<Token %s "%s">' % (self.__class__._token, self.val)


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

            prod = (
                ListNonterm._reduce_list_separated
                if separator else ListNonterm._reduce_list)
            dct['reduce_' + '_'.join(tokens)] = prod
            dct['reduce_' + element] = ListNonterm._reduce_el

        cls = super().__new__(mcls, name, bases, dct)
        return cls

    def __init__(cls, name, bases, dct, *, element, separator=None):
        super().__init__(name, bases, dct)


class ListNonterm(Nonterm, metaclass=ListNontermMeta, element=None):
    def _reduce_list_separated(self, lst, sep, el):
        if el.val is None:
            tail = []
        else:
            tail = [el.val]

        self.val = lst.val + tail

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
    token_prec_map = {}
    last = {}

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


class ParserError(EdgeDBError):
    def __init__(
            self, msg=None, *, hint=None, details=None, token=None, line=None,
            col=None, expr=None, context=None):
        if msg is None:
            msg = 'syntax error at or near "%s"' % token
        super().__init__(msg, hint=hint, details=details)

        self.token = token
        if line is not None:
            self.line = line
        if col is not None:
            self.col = col
        self.expr = expr
        if context:
            add_context(self, context)
            if line is None and col is None:
                self.line = context.start.line
                self.col = context.start.column

    @property
    def context(self):
        try:
            return get_context(self, pctx.ParserContext)
        except LookupError:
            return None


class Parser:
    def __init__(self, **parser_data):
        self.lexer = None
        self.parser = None
        self.parser_data = parser_data

    def cleanup(self):
        self.__class__.parser_spec = None
        self.__class__.lexer_spec = None
        self.lexer = None
        self.parser = None

    def get_debug(self):
        return False

    def get_exception(self, native_err, context, token=None):
        if not isinstance(native_err, ParserError):
            return ParserError(native_err.args[0],
                               context=context, token=token)
        else:
            return native_err

    def get_parser_spec(self):
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
            mod, pickleFile=self.localpath(mod, "pickle"),
            skinny=not self.get_debug(), logFile=self.localpath(mod, "log"),
            verbose=self.get_debug())

        self.__class__.parser_spec = spec
        return spec

    def localpath(self, mod, type):
        return os.path.join(
            os.path.dirname(mod.__file__),
            mod.__name__.rpartition('.')[2] + '.' + type)

    def get_lexer(self):
        """Return an initialized lexer.

        The lexer must implement 'setinputstr' and 'token' methods.
        A lexer derived from edgedb.lang.common.lexer.Lexer will satisfy these
        criteria.
        """
        raise NotImplementedError

    def reset_parser(self, input):
        if not self.parser:
            self.lexer = self.get_lexer()
            self.parser = parsing.Lr(self.get_parser_spec())
            self.parser.parser_data = self.parser_data
            self.parser.verbose = self.get_debug()

        self.parser.reset()
        self.lexer.setinputstr(input)

    def process_lex_token(self, mod, tok):
        return mod.TokenMeta.for_lex_token(tok.type)(
            self.parser, tok.value, self.context(tok))

    def parse(self, input):
        self.reset_parser(input)
        mod = self.get_parser_spec_module()

        try:
            tok = self.lexer.token()

            while tok:
                token = self.process_lex_token(mod, tok)
                if token is not None:
                    self.parser.token(token)

                tok = self.lexer.token()

            self.parser.eoi()

        except parsing.SyntaxError as e:
            raise self.get_exception(
                e, context=self.context(tok), token=tok) from e

        except ParserError as e:
            raise self.get_exception(e, context=e.context) from e

        except lexer.UnknownTokenError as e:
            raise self.get_exception(e, context=self.context(None)) from e

        return self.parser.start[0].val

    def context(self, tok=None):
        lex = self.lexer
        name = lex.filename if lex.filename else '<string>'

        if tok is None:
            position = pctx.SourcePoint(
                line=lex.lineno, column=lex.column, pointer=lex.start)
            context = pctx.ParserContext(
                name=name, buffer=lex.inputstr, start=position, end=position)

        else:
            context = pctx.ParserContext(
                name=name, buffer=lex.inputstr, start=tok.start,
                end=tok.end)

        return context


def line_col_from_char_offset(source, position):
    line = source[:position].count('\n') + 1
    col = source.rfind('\n', 0, position)
    col = position if col == -1 else position - col
    return line, col
