##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections

from metamagic.node.schema._objects import ClassObject
from metamagic.node.exceptions import NodeSchemaError
from metamagic.utils.fs import nodesystem, bucket, backends


class FSSystem(nodesystem.FSSystem):
    class_buckets = None

    def __init__(self):
        super().__init__()

        if self.class_buckets:
            for bucket_cls, backend_ctrs in self.class_buckets.items():
                backends = []
                for backend_ctr in backend_ctrs:
                    args = backend_ctr.args or {}
                    if args:
                        args = dict(args.items())
                    backends.append(backend_ctr.cls(**args))

                self.add_bucket(bucket_cls, backends)


class FS(metaclass=ClassObject, baseclass=FSSystem):
    @classmethod
    def _resolve_bucket(cls, name, context):
        buck_cls = context.resolve_name(name)
        if not issubclass(buck_cls, bucket.BaseBucket):
            raise NodeSchemaError('invalid bucket: subclass of fs.bucket.BaseBucket expected, '
                                  'got {!r}'.format(buck_cls), context=context)
        return buck_cls

    @classmethod
    def _check_backend_ctr(cls, ctr, context):
        if not issubclass(ctr.cls, backends.Backend):
            raise NodeSchemaError('invalid backend: subclass of fs.backends.Backend expected, '
                                  'got {!r}'.format(ctr.cls), context=context)
        return ctr

    @classmethod
    def _apply_data(cls, dct, *, data, context):
        buckets = collections.OrderedDict()

        for bucket, spec in data.items():
            bucket_cls = cls._resolve_bucket(bucket, context)
            buckets[bucket_cls] = [cls._check_backend_ctr(bn, context) for bn in spec['backends']]

        dct['class_buckets'] = buckets
