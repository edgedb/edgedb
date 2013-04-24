##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.cache.nodesystem import CacheSystem


class TestNodeCacheSystem:
    def test_utils_cache_node_1(self):
        from . import node
        from metamagic.utils.cache import Bucket, MemoryBackend

        assert issubclass(node.Cache, CacheSystem)
        assert node.Cache.class_buckets[Bucket][0].cls == MemoryBackend

        c = node.Cache()
        c.configure()
        assert len(Bucket.get_backends()) == 1
