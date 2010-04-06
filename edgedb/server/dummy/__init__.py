##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos.proto import RealmMeta


class Backend(object):
    def __init__(self):
        self.meta = RealmMeta()

    def apply_delta(self, delta):
        delta.apply(self.meta)

    def getmeta(self):
        return self.meta
