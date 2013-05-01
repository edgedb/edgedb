##
# Copyright (c) 2012, 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .bucket import Bucket

from .implementation import DefaultImplementation
Bucket.set_implementation(DefaultImplementation)
