##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import yaml
import functools
import collections


class Scanner(yaml.scanner.Scanner):
    def __init__(self):
        super().__init__()
        self.alnum_range = self.mcrange([('0', '9'), ('a', 'z'), ('A', 'Z')]) + ['_']

    def mcrange(self, ranges):
        result = []
        for c1, c2 in ranges:
            result.extend(self.crange(c1, c2))
        return result

    def crange(self, c1, c2):
        return [chr(o) for o in range(ord(c1), ord(c2) + 1)]

    def scan_directive(self):
        start_mark = self.get_mark()

        directives = {
            '%SCHEMA': functools.partial(self.scan_string, self.alnum_range + ['.']),
            '%NAME': functools.partial(self.scan_string, self.alnum_range),
            '%IMPORT': functools.partial(self.scan_string, self.alnum_range + ['.', ',', ' ']),
            '%FROM': functools.partial(self.scan_string, self.alnum_range + ['.', ',', ' '])
        }

        for directive, handler in directives.items():
            if self.prefix(len(directive)).upper() == directive:
                self.forward()
                name = self.scan_directive_name(start_mark).upper()
                value = handler(start_mark)
                end_mark = self.get_mark()
                self.scan_directive_ignored_line(start_mark)
                return yaml.tokens.DirectiveToken(name, value, start_mark, end_mark)
        else:
            return super().scan_directive()

    def scan_string(self, allowed_range, start_mark):
        while self.peek() == ' ':
            self.forward()
        length = 0
        ch = self.peek(length)
        while ch in allowed_range:
            length += 1
            ch = self.peek(length)
        if not length:
            raise yaml.scanner.ScannerError("while scanning a directive", start_mark,
                    "expected alphabetic or numeric character, but found %r"
                    % ch, self.get_mark())
        value = self.prefix(length)
        self.forward(length)
        ch = self.peek()
        if ch not in '\0 \r\n\x85\u2028\u2029':
            raise yaml.scanner.ScannerError("while scanning a directive", start_mark,
                    "expected alphabetic or numeric character, but found %r"
                    % ch, self.get_mark())
        return value

    def push_tokens(self, tokens):
        self.tokens[0:0] = tokens


class Parser(yaml.parser.Parser):
    import_re = re.compile(r"""^(?P<import>(?P<module>\w+(?:\.\w+)*)(?:\s+AS\s+(?P<alias>\w+))?)
                               (?P<tail>(?:\s*,\s*
                                  (?:(?:\w+(?:\.\w+)*)(?:\s+AS\s+(?:\w+))?)
                               )*)$""", re.X | re.I)

    importlist_re = re.compile(r"""^
                                    (?P<module>[\.\w]+)
                                    \s+ IMPORT \s+
                                       (?P<name>\w+)(?:\s+AS\s+(?P<alias>\w+))?
                                       (?P<tail>(?:\s*,\s*
                                          (?:(?:\w+)(?:\s+AS\s+(?:\w+))?)
                                       )*)
                               $""", re.X | re.I)

    name_alias_re = re.compile(r"""^
                                       (?P<name>\w+)(?:\s+AS\s+(?P<alias>\w+))?
                                       (?P<tail>(?:\s*,\s*
                                          (?:(?:\w+)(?:\s+AS\s+(?:\w+))?)
                                       )*)
                               $""", re.X | re.I)

    def __init__(self):
        super().__init__()

        self.schema = None
        self.document_name = None
        self.imports = collections.OrderedDict()

    def process_directives(self):
        self.schema = None
        self.document_name = None
        self.imports = collections.OrderedDict()

        rejected_tokens = []

        while self.check_token(yaml.tokens.DirectiveToken):
            token = self.get_token()
            if token.name == 'SCHEMA':
                if self.schema:
                    raise yaml.parser.ParserError(None, None, "duplicate SCHEMA directive", token.start_mark)
                self.schema = token.value
            elif token.name == 'NAME':
                if self.document_name:
                    raise yaml.parser.ParserError(None, None, "duplicate NAME directive", token.start_mark)
                self.document_name = token.value
            elif token.name == 'IMPORT':
                self.imports.update(self.parse_imports(token))
            elif token.name == 'FROM':
                implist = self.parse_import_list(token)
                try:
                    existing = self.imports[next(iter(implist))]
                except KeyError:
                    existing = self.imports[next(iter(implist))] = collections.OrderedDict()

                existing.update(next(iter(implist.values())))
            else:
                rejected_tokens.append(token)

        self.push_tokens(rejected_tokens)

        return super().process_directives()

    def parse_document_start(self):
        event = super().parse_document_start()
        if isinstance(event, yaml.events.DocumentStartEvent):
            event.schema = self.schema
            event.document_name = self.document_name
            event.imports = self.imports

        return event

    def parse_implicit_document_start(self):
        event = super().parse_implicit_document_start()
        if isinstance(event, yaml.events.DocumentStartEvent):
            event.schema = self.schema
            event.document_name = self.document_name
            event.imports = self.imports

        return event

    def parse_imports(self, token):
        imports = collections.OrderedDict()

        value = token.value
        match = self.import_re.match(value)

        if not match:
            raise yaml.parser.ParserError(None, None, "invalid IMPORT directive syntax", token.start_mark)

        while match:
            imports[match.group('module')] = match.group('alias')
            value = match.group('tail').strip(' ,')
            match = self.import_re.match(value)

        return imports

    def parse_import_list(self, token):
        imports = collections.OrderedDict()
        names = collections.OrderedDict()

        value = token.value
        match = self.importlist_re.match(value)

        if not match:
            raise yaml.parser.ParserError(None, None, "invalid IMPORT directive syntax", token.start_mark)

        module = match.group('module')

        while match:
            names[match.group('name')] = match.group('alias')
            value = match.group('tail').strip(' ,')
            match = self.name_alias_re.match(value)

        imports[module] = names

        return imports
