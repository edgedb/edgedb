##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import named
from . import primary


class NodeCommandContext:
    # context mixin
    pass


class NodeCommand(named.NamedClassCommand):
    pass


class Node(primary.PrimaryClass):
    pass
