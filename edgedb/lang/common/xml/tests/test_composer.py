##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from collections import OrderedDict

from metamagic.utils.xml import composer
from metamagic.utils.debug import assert_raises
from metamagic.utils.xml.types import Doctype


class TestXMLComposer:
    def test_utils_xml_composer(self):
        class XML1(composer.Composer):
            version = (1, 0)
            encoding = 'UTF-8'
            close_empty = True
            append_xml_declaration = False
            doctype = None
            tag_case = 'asis'

        tag = ('foo',)
        assert XML1.compose(tag) == '<foo/>'

        tag = ('foo', {'open'})
        assert XML1.compose(tag) == '<foo></foo>'

        tag = ('foo', {'z': 'a'})
        assert XML1.compose(tag) == '<foo z="a"/>'

        tag = ('foo', {'z': 'a'}, {'closed'})
        assert XML1.compose(tag) == '<foo z="a"/>'

        tag = ('foo', {'z': 'a'}, {'open'})
        assert XML1.compose(tag) == '<foo z="a"></foo>'

        tag = ('foo', OrderedDict((('z', 'a'), ('b', 'y&b'))))
        assert XML1.compose(tag) == '<foo z="a" b="y&amp;b"/>'

        tag = ('foo', OrderedDict((('z', 'a'), ('b', 123))))
        assert XML1.compose(tag) == '<foo z="a" b="123"/>'

        tag = ('foo', ('bar',))
        assert XML1.compose(tag) == '<foo><bar/></foo>'

        tag = ('foo', 'bar')
        assert XML1.compose(tag) == '<foo>bar</foo>'

        tag = ('foo', 'bar<p>')
        assert XML1.compose(tag) == '<foo><![CDATA[bar<p>]]></foo>'

        tag = ('foo', 'bar<p>&')
        assert XML1.compose(tag) == '<foo><![CDATA[bar<p>&]]></foo>'

        tag = ('foo', 'bar&')
        assert XML1.compose(tag) == '<foo>bar&amp;</foo>'

        tag = ('foo', 123)
        assert XML1.compose(tag) == '<foo>123</foo>'

        tag = ('foo', 123, ('bar',))
        assert XML1.compose(tag) == '<foo>123<bar/></foo>'

        tag = ('foo', [('bar',)])
        assert XML1.compose(tag) == '<foo><bar/></foo>'

        tag = ('foo', [('bar',), ('foo', {'open'})])
        assert XML1.compose(tag) == '<foo><bar/><foo></foo></foo>'

        tag = ('foo', [('bar', {'a': 'z'}), ('foo',)])
        assert XML1.compose(tag) == '<foo><bar a="z"/><foo/></foo>'

        with assert_raises(ValueError, error_re='invalid tag'):
            XML1.compose(('foo>',))

        with assert_raises(ValueError, error_re='invalid tag'):
            XML1.compose(('foo bar',))

        with assert_raises(ValueError, error_re='unknown property'):
            XML1.compose(('foo', {'fail ;)'}))

        with assert_raises(ValueError, error_re='invalid attribute'):
            XML1.compose(('foo', {'<b': 'a'}))


        class XML2(XML1):
            version = (1, 1)
            append_xml_declaration = True

        tag = ('foo',)
        assert XML2.compose(tag) == '<?xml version="1.1" encoding="UTF-8" ?>\n<foo/>'


        class XML3(XML1):
            close_empty = False

        tag = ('foo',)
        assert XML3.compose(tag) == '<foo></foo>'


        class XML4(XML1):
            tag_case = 'upper'

        tag = ('foo',)
        assert XML4.compose(tag) == '<FOO/>'


        class HTML1(XML1):
            doctype = Doctype('html', pubid='-//W3C//DTD XHTML 1.0 Transitional//EN',
                              sysid='http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd')

        tag = ('foo',)
        assert HTML1.compose(tag) == '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 ' \
                                       'Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/' \
                                       'xhtml1-transitional.dtd">\n<foo/>'

        class HTML2(XML2):
            doctype = Doctype('html', pubid='-//W3C//DTD XHTML 1.0 Transitional//EN',
                              sysid='http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd')

        tag = ('foo',)
        assert HTML2.compose(tag) == '<?xml version="1.1" encoding="UTF-8" ?>\n' \
                                       '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 ' \
                                       'Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/' \
                                       'xhtml1-transitional.dtd">\n<foo/>'
