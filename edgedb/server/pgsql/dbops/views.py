##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import textwrap

from .. import common
from . import base
from . import ddl


class View(base.DBObject):
    def __init__(self, name, query):
        super().__init__()
        self.name = name
        self.query = query


class CreateView(ddl.SchemaObjectOperation):
    def __init__(self, view, *,
                 conditions=None, neg_conditions=None, priority=0):
        super().__init__(view.name, conditions=conditions,
                         neg_conditions=neg_conditions, priority=priority)
        self.view = view

    async def code(self, context):
        code = (
            'CREATE VIEW {name} AS\n{query}'
        ).format(
            name=common.qname(*self.view.name),
            query=textwrap.indent(textwrap.dedent(self.view.query), '    ')
        )

        return code
