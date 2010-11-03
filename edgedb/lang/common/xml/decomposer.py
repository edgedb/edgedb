##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import html.entities
from xml.parsers import expat

from . import composer


class Tag:
    __slots__ = ('_cnt_',)

    def __init__(self, name, case_sensitive=False):
        if not case_sensitive:
            name = name.lower()

        self._cnt_ = {'name': name, 'attributes': {}, 'children': [], 'text': None}

        self._cnt_['last_attr_id'] = id(self._cnt_['attributes'])
        self._cnt_['case_sens'] = case_sensitive

    def __getattr__(self, attr):
        if not self._cnt_['case_sens']:
            attr = attr.lower()

            if self._cnt_['last_attr_id'] != id(self._cnt_['attributes']):
                new = {}
                for key, value in self._cnt_['attributes'].items():
                    new[key.lower()] = value
                self._cnt_['attributes'] = new
                self._cnt_['last_attr_id'] = id(new)

        try:
            return self._cnt_['attributes'][attr]

        except KeyError:
            raise AttributeError('unknown tag attribute %r' % attr)

    def __getitem__(self, tag):
        if not self._cnt_['case_sens']:
            tag = tag.lower()
            tags = [child for child in self._cnt_['children'] if child._cnt_['name'].lower() == tag]
        else:
            tags = [child for child in self._cnt_['children'] if child._cnt_['name'] == tag]

        if not tags:
            raise KeyError('no child tag with name %r' % tag)

        if len(tags) == 1:
            return tags[0]

        return tags

    def __iter__(self):
        return iter(self._cnt_['children'])

    def __len__(self):
        return len(self._cnt_['children'])

    def __bool__(self):
        return True

    def __contains__(self, tag):
        try:
            self[tag]
            return True

        except KeyError:
            return False

    def __str__(self):
        return self._cnt_['text'] if self._cnt_['text'] is not None else ''

    def __int__(self):
        return int(str(self))

    def __float__(self):
        return float(str(self))

    def __repr__(self):
        return '<Tag %r children: %r attrs: %r>' % (self._cnt_['name'], self._cnt_['children'],
                                                    self._cnt_['attributes'])

    @classmethod
    def name(cls, tag):
        assert isinstance(tag, cls)
        return tag._cnt_['name']

    @classmethod
    def as_tuples(cls, tag):
        name, attrs, children, text = tag._cnt_['name'], tag._cnt_['attributes'], \
                                                    tag._cnt_['children'], tag._cnt_['text']

        child_tuples = []
        for child in children:
            child_tuples.append(cls.as_tuples(child))

        if text is not None:
            return (name, text, attrs, child_tuples)
        else:
            return (name, attrs, child_tuples)

    @classmethod
    def dump(cls, tag, *, pretty=True, composer=composer.Composer):
        return composer.compose(cls.as_tuples(tag), pretty=pretty)


class _Decomposer:
    def __init__(self, case_sensitive=False):
        self.case_sensitive = case_sensitive

        self.root = None
        self.current = None
        self.stack = collections.deque()

    def decompose(self, string):
        parser = expat.ParserCreate()
        parser.StartElementHandler = self._handle_start_element
        parser.EndElementHandler = self._handle_end_element
        parser.CharacterDataHandler = self._handle_data
        self.expat = parser
        self.expat.Parse(string)

        assert len(self.stack) == 0

        try:
            return self.root

        finally:
            self.root = None
            self.current = None

    def _handle_start_element(self, tagname, attrib):
        tag = Tag(tagname, case_sensitive=self.case_sensitive)
        tag._cnt_['attributes'] = attrib

        self.stack.append(tag)

        if not self.root:
            self.root = tag
        else:
            self.current._cnt_['children'].append(tag)

        self.current = tag

    def _handle_end_element(self, tagname):
        self.stack.pop()
        self.current = self.stack and self.stack[len(self.stack) - 1]

    def _handle_data(self, text):
        text = text.strip()
        if text:
            if self.current._cnt_['text'] is None:
                self.current._cnt_['text'] = ''
            self.current._cnt_['text'] += text


class Decomposer:
    """Utility class to simplify data extraction from XML.

    >>> tree = Decomposer.decompose('''<html>
                                            <head>foo</head>
                                            <body>
                                                <p>123</p>
                                                <p foo="bar">aaaa</p>
                                            </body>
                                       </html>''')
    ... assert str(tree['head']) == 'foo'
    ... assert int(tree['body']['p'][0]) == 123
    ... assert tree['body']['p'][1].foo == 'bar'
    """

    decomposer = _Decomposer
    case_sensitive = False

    @classmethod
    def decompose(cls, string):
        return cls.decomposer(case_sensitive=cls.case_sensitive).decompose(string)
