##
# Copyright (c) 2008-2011, 2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import parsing, debug
from .errors import WKTSyntaxError
from ..base import Serializer, SerializerError

from metamagic.utils.gis.proto.abc import GeometryContainer

from . import lexer


class WKTParser(parsing.Parser):
    def get_lexer(self):
        return lexer.WKTLexer()

    def process_lex_token(self, mod, tok):
        tok_type = tok.attrs['type']
        if tok_type in ('WS', 'NL'):
            return None

        return super().process_lex_token(mod, tok)

    def get_parser_spec_module(self):
        from . import wkt
        return wkt

    def get_debug(self):
        return 'utils.gis.parsers.wkt' in debug.channels

    def get_exception(self, native_err, context):
        return WKTSyntaxError(native_err.args[0], context=context)


class WKTSerializer(Serializer):
    def __init__(self, factory):
        self.parser = None
        self.factory = factory

    def loads(self, data):
        if self.parser is None:
            self.parser = WKTParser(factory=self.factory)

        try:
            result = self.parser.parse(data)
        except WKTSyntaxError as e:
            raise SerializerError('failed to load geography from WKT') from e

        return result

    def dumps(self, geometry):
        tag = geometry.__class__.geo_class_name.upper()
        if geometry.is_empty():
            text = ' EMPTY'
        else:
            text = '(' + self._dumps(geometry) + ')'
        return tag + text

    def _dumps(self, geometry):
        if isinstance(geometry, GeometryContainer):
            output = ', '.join(self._dumps(el) for el in geometry)
        else:
            output = ' '.join('%s' % coord for coord in geometry)

        return output
