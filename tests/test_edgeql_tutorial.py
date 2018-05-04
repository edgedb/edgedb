##
# Copyright (c) 2018-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest  # NOQA

from edgedb.server import _testbase as tb
from edgedb.client import exceptions as exc  # NOQA


class TestEdgeQLTutorial(tb.QueryTestCase):
    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'tutorial.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'tutorial.eql')

    async def test_edgeql_tutorial_query_01(self):
        await self.assert_query_result('''
            SELECT User.login;
        ''', [
            {'alice', 'bob', 'carol', 'dave'}
        ])
