##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import weakref

from metamagic import node as node_module
from ..exceptions import CacheError
from ..bucket import Bucket


class CacheSystem(node_module.System):
    class_buckets = None
    node_buckets_cache = weakref.WeakKeyDictionary()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._backends = []

    def configure(self):
        assert self.class_buckets

        for bucket_cls, backend_classes in self.class_buckets.items():
            backends = [ctr.cls(**(ctr.args or {})) for ctr in backend_classes]
            bucket_cls.set_backends(*backends)
            self._backends.extend(backends)

    def freeze(self):
        for backend in self._backends:
            backend.freeze()

    def thaw(self):
        for backend in self._backends:
            backend.thaw()

    @classmethod
    def get_bucket(cls, bucket_cls=None):
        active_node = node_module.Node.active
        if not active_node:
            raise CacheError('An active node is required to get cache bucket {}'.format(bucket_cls))

        try:
            desc = cls.node_buckets_cache[active_node]
        except KeyError:
            desc = cls.node_buckets_cache[active_node] = _NodeCacheDescriptor(active_node)

        return desc.get(bucket_cls)


class NodeCache(Bucket):
    pass


class NodeDeploymentCache(NodeCache):
    pass


class PersistentNodeDeploymentCache(NodeDeploymentCache):
    pass


class _NodeCacheDescriptor:
    __slots__ = ('root', 'depl', 'buckets')

    def __init__(self, node):
        try:
            node.get_system(CacheSystem)
        except node_module.NodeError as ex:
            raise CacheError('Node does not have a configured cache system.') from ex

        self.root = NodeCache('{}.{}'.format(node.__class__.__module__,
                                             node.__class__.__name__))

        self.depl = NodeDeploymentCache(node.deployment_name,
                                        parent=self.root)

        self.buckets = {NodeCache: self.root,
                        NodeDeploymentCache: self.depl}

    def get(self, bucket_cls):
        if bucket_cls is None:
            return self.depl

        try:
            return self.buckets[bucket_cls]
        except KeyError:
            bucket = self.buckets[bucket_cls] = bucket_cls(parent=self.depl)
            return bucket
