##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.debug import assert_raises
from semantix.utils.nlang.morphology import Singular, Plural, WordCombination


class TestNLangMorphology(object):
    def test_nlang_morphology_gram_numbers(self):
        w = WordCombination({Singular('test'), Plural('tests')})
        assert w == 'test'
        assert w.singular == 'test' and w.plural == 'tests'
        with assert_raises(AttributeError):
            w.dual
