##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


def quote_literal(text):
    return "'" + text.replace("'", "''") + "'"
