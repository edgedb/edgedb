##
# Copyright (c) 2010, 2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys

import parsing

from metamagic.exceptions import MetamagicError, _add_context
from importkit import context as lang_context
from metamagic.utils.datastructures import xvalue
from metamagic.utils import markup
from metamagic.utils import lexer


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

        if name == 'Nonterm':
            return result

        if not result.__doc__:
            result.__doc__ = '%nonterm'
        return result


class Nonterm(parsing.Nonterm, metaclass=NontermMeta):
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


class Precedence(parsing.Precedence, assoc='fail', metaclass=PrecedenceMeta):
    pass


class ParserContext(lang_context.SourceContext, markup.MarkupExceptionContext):
    title = 'Parser Context'

    @classmethod
    def as_markup(cls, self, *, ctx):
        me = markup.elements

        prefix = '{} line={} col={}: '.format(self.name, self.start.line, self.start.column)
        snippet, offset = self.get_line_snippet(self.start, max_length=80 - len(prefix))
        errpos = ' ' * (len(prefix) + offset) + '^'
        prefix += snippet + '\n'

        body = []
        body.append(me.doc.Text(text=prefix))
        body.append(me.doc.Text(text=errpos))

        return me.lang.ExceptionContext(title=self.title, body=body)

    def get_line_snippet(self, point, max_length):
        if point.line > 1:
            linestart = self.buffer.rfind('\n', point.start.pointer)
        else:
            linestart = 0

        before = min(max_length // 2, point.pointer - linestart)
        after = max_length - before

        start = point.pointer - before
        end = point.pointer + after
        return self.buffer[start:end], before


class ParserError(MetamagicError):
    def __init__(self, msg=None, *, hint=None, details=None, token=None, lineno=None, expr=None,
                               context=None):
        if msg is None:
            msg = 'syntax error at or near "%s"' % token
        super().__init__(msg, hint=hint, details=details)

        self.token = token
        self.lineno = lineno
        self.expr = expr
        if context:
            _add_context(self, context)


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

    def get_exception(self, native_err, context):
        return ParserError(native_err.args[0], context=context)

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
        spec = parsing.Spec(mod,
                            pickleFile=self.localpath(mod, "pickle"),
                            skinny=not self.get_debug(),
                            logFile=self.localpath(mod, "log"),
                            verbose=self.get_debug())

        self.__class__.parser_spec = spec
        return spec

    def localpath(self, mod, type):
        return os.path.join(os.path.dirname(mod.__file__), mod.__name__.rpartition('.')[2] + '.' + type)

    def get_lexer(self):
        '''Return an initialized lexer.

        The lexer must implement 'setinputstr' and 'token' methods.
        A lexer derived from metamagic.utils.lexer.Lexer will satisfy these
        criteria.
        '''
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
        return mod.TokenMeta.for_lex_token(tok.attrs['type'])(self.parser,
                                                              tok.value,
                                                              self.context(tok))

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
            raise self.get_exception(e, context=self.context()) from e

        return self.parser.start[0].val

    def context(self, tok=None):
        lex = self.lexer
        name = lex.filename if lex.filename else '<string>'

        if tok is None:
            position = lang_context.SourcePoint(line=lex.lineno, column=lex.column, pointer=lex.start)
            context = ParserContext(name=name, buffer=lex.inputstr, start=position, end=position)

        else:
            context = ParserContext(name=name, buffer=lex.inputstr, start=tok.attrs['start'], end=tok.attrs['end'])

        return context
