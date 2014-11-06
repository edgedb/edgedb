##
# Copyright (c) 2012, 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""This package defines necessary abstractions to simplify work with resources,
such as defining them, managing, publishing etc.
"""


from .resource import Resource, AbstractFileResource, VirtualFile, \
                      AbstractFileSystemResource, \
                      ResourceContainer, File, Directory, EmptyResource, \
                      add_publication_hook, mark_standalone

from .exceptions import ResourceError
