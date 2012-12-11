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
import pyggy.lexer

from metamagic.exceptions import MetamagicError, _add_context
from metamagic.utils.lang import context as lang_context
from metamagic.utils.datastructures import xvalue
from metamagic.utils import markup


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


class SourcePoint(lang_context.SourcePoint):
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
        _add_context(self, context)


class Lexer(pyggy.lexer.lexer):
    def __init__(self, lexspec):
        super().__init__(lexspec)
        self.lineno = 1
        self.offset = 0
        self.column = 1
        self.lineoffset = 0

    def nextch(self):
        self.offset += 1
        return super().nextch()

    def setinputstr(self, str) :
        self.inputstr = str
        super().setinputstr(str)

    def PUSHBACK(self, backupdata):
        self.offset -= len(backupdata)
        super().PUSHBACK(backupdata)

    def newline(self):
        self.lineno += 1
        self.lineoffset = self.offset
        self.column = 1

    def context(self):
        value = self.value.value if isinstance(self.value, xvalue) else self.value
        value_len = len(str(value))

        start_offset = self.offset - value_len
        column = start_offset - self.lineoffset
        start = SourcePoint(line=self.lineno, column=column, pointer=start_offset)
        end = SourcePoint(line=self.lineno, column=self.column + value_len, pointer=self.offset)
        context = ParserContext(name='<string>', buffer=self.inputstr, start=start, end=end)
        return context


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
            self.lexer = Lexer(self.__class__.lexer_spec)
            self.parser = Parsing.Lr(self.__class__.parser_spec)
            self.parser.parser_data = self.parser_data
            self.parser.verbose = self.get_debug()

        self.parser.reset()
        self.lexer.setinputstr(input)

    def parse(self, input):
        self.reset_parser(input)
        mod = self.get_parser_spec_module()

        try:
            tok = self.lexer.token()

            while tok:
                token = mod.TokenMeta.for_lex_token(tok)(self.parser, self.lexer.value,
                                                         self.lexer.context())

                self.parser.token(token)
                tok = self.lexer.token()

            self.parser.eoi()

        except Parsing.SyntaxError as e:
            raise self.get_exception(e, context=self.lexer.context()) from e

        return self.parser.start[0].val
