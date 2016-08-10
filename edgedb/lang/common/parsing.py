##
# Copyright (c) 2010, 2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys
import types

import parsing

from edgedb.lang.common.exceptions import EdgeDBError, _add_context
from importkit import context as lang_context
from edgedb.lang.common.datastructures import xvalue
from edgedb.lang.common import markup
from edgedb.lang.common import lexer


class TokenMeta(type):
    token_map = {}

    def __new__(mcls, name, bases, dct, *, token=None, lextoken=None,
                                           precedence_class=None):
        result = super().__new__(mcls, name, bases, dct)

        if precedence_class is not None:
            result._precedence_class = precedence_class

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

    def __init__(cls, name, bases, dct, *, token=None, lextoken=None,
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
                    isinstance(attr, types.FunctionType) and
                    attr.__doc__ is None):
                tokens = name.split('_')
                if name == 'reduce_empty':
                    tokens = ['reduce', '<e>']

                doc = r'%reduce {}'.format(' '.join(tokens[1:]))

                prec = getattr(attr, '__parsing_precedence__', None)
                if prec is not None:
                    doc += ' [{}]'.format(prec)

                a = lambda self, *args, meth=attr: meth(self, *args)
                a.__doc__ = doc
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

            prod = (ListNonterm._reduce_list_separated if separator else
                    ListNonterm._reduce_list)
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
    """Decorator to set production precedence"""

    def decorator(func):
        func.__parsing_precedence__ = precedence.__name__
        return func

    return decorator


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

        body = []

        lines = []
        line_numbers = []

        if self.start.line > 1:
            ctx_line, _ = self.get_line_snippet(self.start, offset=-1)
            lines.append(ctx_line)
            line_numbers.append(self.start.line - 1)

        snippet, offset = self.get_line_snippet(self.start)
        lines.append(snippet)
        line_numbers.append(self.start.line)

        try:
            ctx_line, _ = self.get_line_snippet(self.start, offset=1)
        except ValueError:
            pass
        else:
            lines.append(ctx_line)
            line_numbers.append(self.start.line + 1)

        tbp = me.lang.TracebackPoint(
            name=self.name, filename=self.name, lineno=self.start.line,
            colno=self.start.column, lines=lines, line_numbers=line_numbers,
            context=True)

        body.append(tbp)

        return me.lang.ExceptionContext(title=self.title, body=body)

    def _find_line(self, point, offset=0):
        if point.line == 0:
            if offset < 0:
                raise ValueError('not enough lines in buffer')
            else:
                return 0, len(self.buffer)

        step = -1 if offset <= 0 else 1
        find = self.buffer.rfind if offset <= 0 else self.buffer.find

        offset += step

        ptr = point.pointer

        if offset <= 0:
            start_end = (0, ptr)
        else:
            start_end = (ptr,)

        while offset:
            offset -= step

            linestart = find('\n', *start_end)
            if linestart == -1:
                if offset:
                    raise ValueError('not enough lines in buffer')
                else:
                    lineend = self.buffer.find('\n')
                    if lineend == -1:
                        lineend = len(self.buffer)
                    return 0, lineend

            ptr = linestart + step
            if step == -1:
                start_end = (0, ptr)
            else:
                start_end = (ptr,)

        if step == -1:
            linestart += 1
            lineend = self.buffer.find('\n', linestart)
            if lineend == -1:
                lineend = len(self.buffer)
        else:
            lineend = linestart + 1
            linestart = self.buffer.rfind('\n', 0, lineend - 1)
            if linestart == -1:
                linestart = 0
            else:
                linestart += 1

        return linestart, lineend

    def get_line_snippet(self, point, max_length=120, *, offset=0):
        line_start, line_end = self._find_line(point, offset=offset)
        line_len = line_end - line_start

        if line_len > max_length:
            before = min(max_length // 2, point.pointer - line_start)
            after = max_length - before
        else:
            before = point.pointer - line_start
            after = line_len - before

        start = point.pointer - before
        end = point.pointer + after

        return self.buffer[start:end], before


class ParserError(EdgeDBError):
    def __init__(self, msg=None, *, hint=None, details=None, token=None,
                 line=None, col=None, expr=None, context=None):
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
            _add_context(self, context)
            if line is None and col is None:
                self.line = context.start.line
                self.col = context.start.column


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
        A lexer derived from edgedb.lang.common.lexer.Lexer will satisfy these
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
            raise self.get_exception(e, context=self.context(tok)) from e

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
