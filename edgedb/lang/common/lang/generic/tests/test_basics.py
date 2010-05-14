##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import GenericLangTestSuite

class TestPYJSFoundation(GenericLangTestSuite):
    def test_utils_lang_generic_assignment_simple(self):
        a = True
        return a

    def test_utils_lang_generic_func_args_defaults(self):
        def test(a, b, c, d=10):
            return a + b + (c*d)
        return test(100, 200, 1)

    def test_utils_lang_generic_a_bin_op(self):
        return (10+1-3*2) + 4 / (1+1)

    def test_utils_lang_generic_types(self):
        a = list((1, 2))
        b = [1, 2]

        c = dict((('k', 'v'), ('k2', 'v2')))
        d = {'k': 'v', 'k2': 'v2'}

        return a == b and c == d

    def test_utils_lang_generic_bool_and_comp(self):
        def test(v):
            return 1 < 5 > v == 2 < 9

        return test(2) and not test(3) and 10 > 5

    def test_utils_lang_generic_getitem_index(self):
        a = [1, 2]
        return a[1]

    def test_utils_lang_generic_getitem_slice(self):
        a = [1, 2, 3, 4, 5]
        return a[1:3], a[:], a[::-1], a[:-2]

    def test_utils_lang_generic_assignment_unpack(self):
        a, b, (c, d, (e, *f)), *g = (1, 2, (3, 4, (5, 4, 5, 6)), 4)

        params = (1, 2, 3, 4, (5, 6, 7))
        h, i, j, k, (l, *m) = params

        return a, b, c, d, e, f, m, h, i, j, k, l, g

    def test_utils_lang_generic_assignment_func_unpack(self):
        def test(a, b):
            return (1, 2, a, [3, b, 0])
        a, b, c, (d, *e) = test(100, 200)
        return a, b, c, d, e
