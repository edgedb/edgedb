##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.xml.types import Doctype


class TestXMLDoctype:
    def test_utils_xml_types_doctype(self):
        assert str(Doctype('html')) == '<!DOCTYPE html>'
        assert str(Doctype('html', pubid="-//W3C//DTD HTML 4.01//EN"))\
                         == '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN">'

        assert str(Doctype('html',
                           pubid="-//W3C//DTD XHTML 1.0 Transitional//EN",
                           sysid='http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd'))\
                == '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" ' \
                   '"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">'
