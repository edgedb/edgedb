##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class SchemaValidationError(Exception):
    def __init__(self, value, node=None):
        if node is not None and node.start_mark:
            mark = node.start_mark
            value = ('in %s line %d col %d\n%s\n' % (mark.name, mark.line, mark.column, mark.get_snippet())) + value

        super(SchemaValidationError, self).__init__(value)

class SchemaError(Exception): pass
