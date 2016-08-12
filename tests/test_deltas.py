##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid

from edgedb.lang.common import datetime

from edgedb.server import _testbase as tb


class TestDeltas(tb.QueryTestCase):
    async def test_delta_simple(self, input="""
        # setup delta
        #
        CREATE DELTA [test.d1] TO $$
            concept NamedObject:
                required link name -> str
        $$;

        COMMIT DELTA [test.d1];

        # test updated schema
        #
        INSERT [test.NamedObject] {
            name := 'Test'
        };

        SELECT
            [test.NamedObject] {
                name
            }
        WHERE
            [test.NamedObject].name = 'Test';

        """) -> [

        None,

        None,

        [],

        [{
            'std.id': uuid.UUID,
            'std.ctime': datetime.DateTime,
            'std.mtime': datetime.DateTime,
            'test.name': 'Test',
        }]
    ]:
        pass
