##
# Copyright (c) 20017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import unittest

from edgedb.lang.common import debug


class DebugTests(unittest.TestCase):

    def test_common_debug_flags(self):
        flags = dict(debug.flags.items())
        self.assertIn('edgeql_compile', flags)
        self.assertIn('EdgeQL', flags['edgeql_compile'].doc)
        self.assertIs(debug.flags.edgeql_compile, False)
