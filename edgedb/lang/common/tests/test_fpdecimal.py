##
# Copyright (c) 2010-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import decimal
import unittest

from edgedb.lang.common import fpdecimal


class FPDecimalTests(unittest.TestCase):

    def test_utils_fpdecimal(self):
        dc = fpdecimal.FPDecimal

        with fpdecimal.CascadedContext(prec=4, scale=2):
            with self.assertRaises(decimal.Overflow):
                # decimal_t is declared as 6, 2
                # yielding a maximum exponent of 3
                dc('100.50')

        with fpdecimal.CascadedContext(prec=4, scale=1):
            assert str(dc('100.45') + dc('100.45')) == '200.8'
            assert str(dc('100.45') + dc('100.46')) == '200.9'

            with fpdecimal.CascadedContext(prec=5, scale=2):
                sum_ = dc('100.45') + dc('100.46')
                assert str(sum_) == '200.91'

                with fpdecimal.CascadedContext(traps=[decimal.Rounded]):
                    # Rounding traps
                    with self.assertRaises(decimal.Rounded):
                        dc('100.451')

                    with fpdecimal.CascadedContext(
                            traps={decimal.DivisionByZero: False}):
                        # Zero division does not trap
                        result = dc('100') / dc('0')
                        assert result == decimal.Decimal('Infinity')

                        # Rounding still traps
                        with self.assertRaises(decimal.Rounded):
                            dc('100.451')

                # Rounding no longer traps
                dc('100.452')

                with fpdecimal.CascadedContext(rounding=decimal.ROUND_UP):
                    assert str(dc('100.452')) == '100.46'

                    with fpdecimal.CascadedContext(
                            rounding=decimal.ROUND_HALF_DOWN):
                        assert str(dc('100.455')) == '100.45'

                    assert str(dc('100.452')) == '100.46'

        with self.assertRaises(ValueError):
            fpdecimal.CascadedContext(prec=1, scale=2)

        with fpdecimal.CascadedContext(prec=4, scale=3):
            with self.assertRaises(ValueError):
                with fpdecimal.CascadedContext(prec=3):
                    pass

        with fpdecimal.CascadedContext(prec=4, scale=0):
            assert str(dc('1000.600')) == '1001'

            with self.assertRaises(decimal.Overflow):
                dc('10000')

            with self.assertRaises(decimal.Overflow):
                # Regular decimal is affected too
                decimal.Decimal('10000') + 0

        with fpdecimal.CascadedContext(prec=6, scale=2):
            assert str(dc('1000') / dc('3')) == '333.333'
