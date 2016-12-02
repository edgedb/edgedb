##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import unittest

from edgedb.lang.common.nlang import morphology as m


class NLangTests(unittest.TestCase):

    def test_common_nlang_morphology_gram_numbers(self):
        w = m.WordCombination({m.Singular('test'), m.Plural('tests')})
        assert w == 'test'
        assert w.singular == 'test' and w.plural == 'tests'
        with self.assertRaises(AttributeError):
            w.dual
