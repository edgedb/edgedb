##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from metamagic import node


class FSSystem(node.System):
    def __init__(self):
        self.buckets = collections.OrderedDict()

    def add_bucket(self, bucket_cls, backends):
        self.buckets[bucket_cls] = backends

    def configure(self):
        for bucket_cls, backends in self.buckets.items():
            bucket_cls.set_backends(*backends)
            bucket_cls.configure()

    def build(self):
        for bucket_cls in self.buckets.keys():
            bucket_cls.build()
