##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common.algos import boolean


def test_common_algos_boolean_minimize():
    ones = boolean.ints_to_terms(0, 1, 2, 4, 5, 6, 9, 12, 13)
    assert boolean.minimize(ones) == {(1, 0, None, None), (None, 0, 1, None), (
        0, None, None, 0)}

    ones = boolean.ints_to_terms(0, 1, 2, 5, 6, 7)
    assert boolean.minimize(ones) in [
        {(None, 0, 0), (None, 1, 1), (0, 1, None)},
        {(None, 1, 1), (0, None, 0), (1, 0, None)},
        {(None, 0, 0), (1, None, 1), (0, 1, None)}
    ]

    ones = boolean.ints_to_terms(4, 8, 10, 11, 12, 15)
    dcs = boolean.ints_to_terms(9, 14)

    assert boolean.minimize(ones, dcs) in [
        {(None, None, 0, 1), (0, 0, 1, None), (None, 1, None, 1)},
        {(0, None, None, 1), (0, 0, 1, None), (None, 1, None, 1)}
    ]

    ones = boolean.ints_to_terms(4, 5, 6, 8, 9, 10, 13)
    dcs = boolean.ints_to_terms(0, 7, 15)

    assert boolean.minimize(
        ones, dcs) == {(0, None, 0, 1), (None, None, 1, 0), (1, 0, None, 1)}

    ones = boolean.ints_to_terms(0, 1, 2, 3, 4, 6, 7, 8, 9, 11, 15)
    assert boolean.minimize(ones) == {(0, None, None, 0), (None, 0, 0, None), (
        1, 1, None, None)}

    ones = boolean.ints_to_terms(9, 25, 13, 29, 15, 31, 3, 11, 10, 26)
    dcs = boolean.ints_to_terms(8, 24, 12, 7, 5, 4, 17, 19)
    assert boolean.minimize(ones, dcs) in [{
        (0, None, 0, 1, None), (1, None, 1, 1, None), (1, 1, None, None, 0),
        (1, 0, None, 1, None)
    }, {(0, None, 0, 1, None), (1, None, 1, 1, None), (None, 0, 0, 1, None),
        (1, 1, None, None, 0)}]

    ones = boolean.ints_to_terms(*range(40))
    assert boolean.minimize(ones) == {(None, None, None, 0, 0, None), (
        None, None, None, None, None, 0)}


if __name__ == '__main__':
    test_algos_boolean_minimize()
