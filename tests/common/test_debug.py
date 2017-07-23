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
        flags = {flag.name: flag for flag in debug.flags}
        self.assertIn('edgeql_compile', flags)
        self.assertIn('EdgeQL', flags['edgeql_compile'].doc)
        self.assertIsInstance(debug.flags.edgeql_compile, bool)
