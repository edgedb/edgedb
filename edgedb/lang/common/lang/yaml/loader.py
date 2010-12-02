##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import yaml
import importlib
import functools
import collections
import copy

import semantix.utils.lang.meta


class AttributeMappingNode(yaml.nodes.MappingNode):
    """Mapping as an attribute dictionary indicator node.

    A special YAML mapping node that is used to indicate that the mapping should
    be used as an attribute dictionary.  The most common use is to turn the top-level
    document mapping into module attributes.

    """

    @classmethod
    def from_map_node(cls, node):
        """Construct an AttributeMappingNode from a regular MappingNode"""
        result = cls(tag=node.tag, value=node.value, start_mark=node.start_mark, end_mark=node.end_mark,
                     flow_style=node.flow_style)
        if hasattr(node, 'tags'):
            result.tags = node.tags
        return result


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
        }

        for directive, handler in directives.items():
            if self.prefix(len(directive)) == directive:
                self.forward()
                name = self.scan_directive_name(start_mark)
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
    import_re = re.compile("""^(?P<import>(?P<module>\w+(?:\.\w+)*)(?:\s+AS\s+(?P<alias>\w+))?)
                              (?P<tail>(?:\s*,\s*
                                  (?:(?:\w+(?:\.\w+)*)(?:\s+AS\s+(?:\w+))?)
                              )*)$""", re.X)

    def __init__(self):
        super().__init__()

    def process_directives(self):
        self.schema = None
        self.document_name = None
        self.imports = {}

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

    def parse_imports(self, token):
        imports = {}

        value = token.value
        match = self.import_re.match(value)

        if not match:
            raise yaml.parser.ParserError(None, None, "invalid IMPORT directive syntax", token.start_mark)

        while match:
            imports[match.group('module')] = match.group('alias')
            value = match.group('tail').strip(' ,')
            match = self.import_re.match(value)

        return imports


class Composer(yaml.composer.Composer):
    def compose_document(self):
        start_document = self.get_event()

        node = self.compose_node(None, None)

        schema = getattr(start_document, 'schema', None)
        if schema:
            module, obj = schema.rsplit('.', 1)
            module = importlib.import_module(module)
            schema = getattr(module, obj)
            node = schema().check(node)
            node.import_context = schema().get_import_context_class()

            if node.import_context and \
                    not isinstance(self.document_context.import_context, node.import_context):
                self.document_context.import_context = \
                            node.import_context.from_parent(self.document_context.import_context,
                                                            self.document_context.import_context)


        node.document_name = getattr(start_document, 'document_name', None)
        node.imports = getattr(start_document, 'imports', None)
        if self.document_context is not None:
            self.document_context.document_name = node.document_name

        self.get_event()
        self.anchors = {}
        return node


class Constructor(yaml.constructor.Constructor):
    def __init__(self, context=None):
        super().__init__()
        self.document_context = context
        self.documents_emitted = 0

    def _get_class_from_tag(self, clsname, node, intent='class'):
        if not clsname:
            raise yaml.constructor.ConstructorError("while constructing a Python %s" % intent, node.start_mark,
                                                    "expected non-empty class name appended to the tag", node.start_mark)

        module_name, class_name = clsname.rsplit('.', 1)

        try:
            module = importlib.import_module(module_name)
            result = getattr(module, class_name)
        except (ImportError, AttributeError) as exc:
            raise yaml.constructor.ConstructorError("while constructing a Python %s" % intent, node.start_mark,
                                                    "could not find %r (%s)" % (clsname, exc), node.start_mark)

        return result

    def _get_source_context(self, node, document_context):
        start = semantix.utils.lang.meta.SourcePoint(node.start_mark.line, node.start_mark.column,
                                               node.start_mark.pointer)
        end = semantix.utils.lang.meta.SourcePoint(node.end_mark.line, node.end_mark.column,
                                               node.end_mark.pointer)

        context = semantix.utils.lang.meta.SourceContext(node.start_mark.name, node.start_mark.buffer,
                                                   start, end, document_context)
        return context

    def construct_document(self, node):
        if node.imports:
            for module_name, alias in node.imports.items():
                try:
                    if node.import_context:
                        parent_context = self.document_context.import_context
                        module_name = node.import_context.from_parent(module_name, parent=parent_context)
                    mod = importlib.import_module(module_name)
                except ImportError as e:
                    raise yaml.constructor.ConstructorError(None, None, '%r' % e, node.start_mark)

                if not alias:
                    alias = mod.__name__
                self.document_context.imports[alias] = mod

        return super().construct_document(node)

    def construct_python_class(self, parent, node):
        cls = self._get_class_from_tag(parent, node, 'class')
        name = getattr(node, 'document_name', cls.__name__ + '_' + str(id(node)))

        # Call correct class constructor with __prepare__ method
        #
        result = type(cls)(name, (cls,), type(cls).__prepare__(name, (cls,)))

        result.__module__ = cls.__module__

        nodecopy = copy.copy(node)
        nodecopy.tags = copy.copy(nodecopy.tags)
        nodecopy.tag = nodecopy.tags.pop()

        data = self.construct_object(nodecopy)

        context = self._get_source_context(node, self.document_context)
        result.prepare_class(context, data)

        yield result

    def construct_python_object(self, classname, node):
        cls = self._get_class_from_tag(classname, node, 'object')
        if not issubclass(cls, semantix.utils.lang.meta.Object):
            raise yaml.constructor.ConstructorError(
                    "while constructing a Python object", node.start_mark,
                    "expected %s to be a subclass of semantix.utils.lang.meta.Object" % classname, node.start_mark)

        context = self._get_source_context(node, self.document_context)

        nodecopy = copy.copy(node)
        nodecopy.tags = copy.copy(nodecopy.tags)
        nodecopy.tag = nodecopy.tags.pop()

        data = self.construct_object(nodecopy, True)

        result = cls(context=context, data=data)

        yield result

        try:
            constructor = result.construct
        except AttributeError:
            pass
        else:
            constructor()

    def construct_ordered_mapping(self, node, deep=False):
        if isinstance(node, yaml.nodes.MappingNode):
            self.flatten_mapping(node)

        if not isinstance(node, yaml.nodes.MappingNode):
            raise yaml.constructor.ConstructorError(None, None,
                    "expected a mapping node, but found %s" % node.id,
                    node.start_mark)

        mapping = collections.OrderedDict()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, collections.Hashable):
                raise yaml.constructor.ConstructorError("while constructing a mapping",
                                                        node.start_mark,
                                                        "found unhashable key", key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value

        return mapping

    def construct_ordered_map(self, node):
        data = collections.OrderedDict()
        yield data
        value = self.construct_ordered_mapping(node)
        data.update(value)

    def get_dict(self, document_offset=None):
        if document_offset is not None and document_offset > self.documents_emitted:
            for i in range(self.documents_emitted, document_offset):
                if self.check_node():
                    node = self.get_node()
                else:
                    break
                self.documents_emitted += 1

        # Construct and return the next document.
        while self.check_node():
            node = self.get_node()
            data = self.construct_document(node)
            self.documents_emitted += 1

            if isinstance(node, AttributeMappingNode):
                for d in data.items():
                    yield d
            else:
                yield (node.document_name, data)


Constructor.add_multi_constructor(
    'tag:semantix.sprymix.com,2009/semantix/class/derive:',
    Constructor.construct_python_class
)

Constructor.add_multi_constructor(
    'tag:semantix.sprymix.com,2009/semantix/object/create:',
    Constructor.construct_python_object
)

Constructor.add_constructor(
    'tag:semantix.sprymix.com,2009/semantix/orderedmap',
    Constructor.construct_ordered_map
)


class Loader(yaml.reader.Reader, Scanner, Parser, Composer, Constructor, yaml.resolver.Resolver):
    def __init__(self, stream, context=None):
        yaml.reader.Reader.__init__(self, stream)
        Scanner.__init__(self)
        Parser.__init__(self)
        Composer.__init__(self)
        Constructor.__init__(self, context)
        yaml.resolver.Resolver.__init__(self)
