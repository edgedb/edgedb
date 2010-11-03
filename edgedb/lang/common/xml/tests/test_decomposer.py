##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.xml import composer, decomposer
from semantix.utils.debug import assert_raises
from semantix.utils.xml.types import Doctype


class TestXMLDecomposer:
    def test_utils_xml_decomposer(self):
        class XML(composer.Composer):
            version = (1, 0)
            encoding = 'UTF-8'
            close_empty = True
            append_xml_declaration = True

        decompose = decomposer.Decomposer.decompose

        xml = XML.compose(('html',))
        tree = decompose(xml)
        assert tree
        assert len(tree) == 0
        assert decomposer.Tag.name(tree) == 'html'
        assert not str(tree)


        xml = XML.compose(('html', ('body', {'foo': 'bar'})))
        tree = decompose(xml)
        assert len(tree) == 1
        assert 'body' in tree
        assert tree['body'].foo
        assert tree['body'].foo == 'bar'


        xml = XML.compose(('html', [('body', {'foo': 'bar'}),
                                      ('head', ('title', 'TITLE'))]
                            ))
        tree = decompose(xml)
        assert len(tree) == 2
        assert 'body' in tree
        assert tree['body'].foo
        assert tree['body'].foo == 'bar'
        assert str(tree['head']['title']) == 'TITLE'
        assert len(tree['head']) == 1
        assert 'head' in tree
        assert 'hhhh' not in tree
        assert hasattr(tree['body'], 'foo')
        assert not hasattr(tree['body'], 'bar')


        xml = XML.compose(('html', 123))
        tree = decompose(xml)
        assert str(tree) == '123'
        assert int(tree) == 123


        xml = XML.compose(('html', 1.1))
        tree = decompose(xml)
        assert float(tree) == 1.1


        xml = XML.compose(('html', [('body', {'foo': 'bar'}),
                                      ('body', ('title', 'TITLE')),
                                      ('head', ('title', 'TITLE2', {'a': 'z'}))]
                            ))
        tree = decompose(xml)
        assert len(tree['body']) == 2
        assert isinstance(tree['body'], list)
        assert tree['body'][0].foo == 'bar'
        assert str(tree['body'][1]['title']) == 'TITLE'
        assert tree['head']['title'].a == 'z'
        assert str(tree['head']['title']) == 'TITLE2'

        xml = XML.compose(('htmL', ('Body', {'AA' : 'Ff'})))
        tree = decompose(xml)
        assert 'body' in tree
        assert tree['body'].aa == 'Ff'

        class CaseSensitive(decomposer.Decomposer):
            case_sensitive = True

        xml = XML.compose(('htmL', ('Body', {'AA' : 'Ff'})))
        tree = CaseSensitive.decompose(xml)
        assert 'body' not in tree
        assert tree['Body'].AA == 'Ff'
        assert not hasattr(tree['Body'], 'aa')


        tree = decomposer.Decomposer.decompose('''<html>
                                            <head>foo</head>
                                            <body>
                                                <p>123</p>
                                                <p foo="bar">aaaa</p>
                                            </body>
                                       </html>''')
        assert str(tree['head']) == 'foo'
        assert int(tree['body']['p'][0]) == 123
        assert tree['body']['p'][1].foo == 'bar'
