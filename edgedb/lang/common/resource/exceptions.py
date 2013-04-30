##
# Copyright (c) 2012, 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.exceptions import MetamagicError


class ResourceError(MetamagicError):
    """Any error occurred during resource creation or manipulation"""


class ResourcePublisherError(MetamagicError):
    """An error occurred during resource publishing"""
