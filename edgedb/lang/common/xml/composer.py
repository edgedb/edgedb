##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from xml.sax.saxutils import escape
from semantix.utils.xml.types import Doctype


class Composer:
    """Small utility class aimed to simplify composing of elementary XML.

    The main method is a classmethod 'serialize' which accepts tag specification
    as its only argument.  All necessary tweaking should be done by deriving
    from Composer class and changing corresponding class attributes.

    Tag structure format: a tuple with a tag name specified as its first element.
    Tag name is the only required element.  Order of the rest tag tuple elements
    is not important.

    Dict specifies attributes.  List - children tags.  Set - set of rendering
    configurations for the particular tag.  Tuple - spec for one child tag (in
    this case there shouldn't be a list element in spec).  And everything else
    would be a tag body.

    Example:
     * ('foo',)  -> '<foo/>'
     * ('foo', 123)  -> '<foo>123</foo>'
     * ('foo, 123, {'a': 'b'})  -> '<foo a="b">123</foo>'
     * ('html', (body, '123'))  -> '<html><body>123</body></html>'
     * ('html', [('body',), ('head', {'open'})])  -> '<html><body/><head></head></html>'
    """

    version = (1, 0)
    encoding = 'UTF-8'

    append_xml_declaration = False
    doctype = None
    close_empty = True

    tag_case = 'asis'
    _allowed_tag_cases = frozenset(('asis', 'lower', 'upper'))

    _allowed_properties = frozenset(('open', 'closed'))


    @classmethod
    def _serialize_tag(cls, tag, *, _level=0, _pretty=False):
        assert isinstance(tag, tuple)

        if len(tag) > 5:
            raise ValueError('maximum 5 attributes can be specified ' \
                             'in tag definition tuple, got %d' % len(tag))

        name = str(tag[0])
        attributes, children, properties, body = None, None, None, None

        for i in range(1, len(tag)):
            if tag[i] is None:
                continue
            elif isinstance(tag[i], list):
                children = tag[i]
            elif isinstance(tag[i], tuple):
                children = [tag[i]]
            elif isinstance(tag[i], dict):
                attributes = tag[i]
            elif isinstance(tag[i], set):
                properties = tag[i]
            else:
                body = tag[i]

        if properties and properties - cls._allowed_properties:
            raise ValueError('unknown property: %r' % (properties - cls._allowed_properties))

        if ' ' in name or name != escape(name):
            raise ValueError('invalid tag name: %r' % name)

        if cls.tag_case == 'upper':
            name = name.upper()
        elif cls.tag_case == 'lower':
            name = name.lower()

        tab = '    ' * _level

        result = '<%s' % name

        if _pretty:
            result = tab + result

        open = True

        if attributes:
            for key, value in attributes.items():
                if ' ' in key or key != escape(key):
                    raise ValueError('invalid attribute name: %r' % key)

                result += ' %s="%s"' % (key, escape(str(value)))

        if body is not None:
            if open:
                result += '>'
                open = False

            result += str(body)

        if children:
            if open:
                result += '>'
                if _pretty:
                    result += '\n'
                open = False

            for i, tag in enumerate(children):
                if tag is not None:
                    result += cls._serialize_tag(tag, _level=_level+1, _pretty=_pretty)
                    if _pretty and i != len(children) - 1:
                        result += '\n'

        if open and ((properties and ('closed' in properties)) or \
                            (cls.close_empty and (not properties or 'open' not in properties))):
            result += '/>'

        else:
            if open:
                result += '>'
                open = False

            if _pretty and children:
                result += '\n' + tab
            result += '</%s>' % name

        return result


    @classmethod
    def compose(cls, tag, *, pretty=False):
        assert isinstance(tag, tuple)

        if cls.tag_case not in cls._allowed_tag_cases:
            raise ValueError('unknown tag case: %r' % cls.tag_case)

        header = ''
        body = cls._serialize_tag(tag, _pretty=pretty)

        if cls.append_xml_declaration:
            header = '<?xml version="%d.%d" encoding="%s" ?>\n' \
                                        % (cls.version[0], cls.version[1], cls.encoding)

        if cls.doctype:
            assert isinstance(cls.doctype, Doctype)
            header += str(cls.doctype) + '\n'

        return header + body
