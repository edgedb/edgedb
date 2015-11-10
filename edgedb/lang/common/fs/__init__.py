##
# Copyright (c) 2012, 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .bucket import BaseBucket, Bucket, BucketMeta
from .exceptions import FSError
from . import backends


__all__ = 'Bucket', 'backends', 'FSError'


from .implementation import DefaultImplementation
BaseBucket.set_implementation(DefaultImplementation)

# Register JS hook
from .frontends import javascript
from .frontends import jplus
