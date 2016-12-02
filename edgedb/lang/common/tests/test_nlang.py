##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import unittest

from edgedb.lang.common import nlang


class NLangTests(unittest.TestCase):

    def test_common_nlang_gram_numbers(self):
        w = nlang.WordCombination({nlang.Singular('test'),
                                   nlang.Plural('tests')})
        assert w == 'test'
        assert w.singular == 'test' and w.plural == 'tests'
        with self.assertRaises(AttributeError):
            w.dual
