##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic import node


class FSSystem(node.System):
    class_buckets = None

    def configure(self):
        assert self.class_buckets

        for bucket_cls, backend_classes in self.class_buckets.items():
            #backends = [ctr.cls(**(ctr.args or {})) for ctr in backend_classes]
            backends = []
            for ctr in backend_classes:
                backends.append(ctr.cls(**(ctr.args or {})))

            bucket_cls.set_backends(*backends)
            bucket_cls.configure()

    def build(self):
        for bucket_cls in self.class_buckets.keys():
            bucket_cls.build()
