##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.caosql.errors import CaosQLError


class CaosQLSyntaxError(CaosQLError):
    def __init__(self, token, lineno, expr=None):
        self.token = token
        self.expr = expr
        self.lineno = lineno

    def __str__(self):
        return "unexpected `%s' (%s) on line %d" % (self.token, self.expr, self.lineno)
