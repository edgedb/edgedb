##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import SemantixError


class PgSQLParserError(SemantixError):
    def __init__(self, msg, *, token, lineno, expr=None):
        super().__init__(msg)

        self.token = token
        self.lineno = lineno
        self.expr = expr


    def __str__(self):
        return "unexpected `%s' on line %d" % (self.token, self.lineno)
