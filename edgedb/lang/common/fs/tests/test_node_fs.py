##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os

from metamagic.utils.fs.nodesystem import FSSystem


class TestNodeFSSystem:
    def test_utils_fs_node_1(self):
        from . import node
        from metamagic.utils.fs import Bucket, backends

        assert issubclass(node.FS, FSSystem)
        assert node.FS.class_buckets[Bucket][0].cls is backends.FSBackend

        c = node.FS()
        c.configure()
        assert len(Bucket.get_backends()) == 1

        b = Bucket.get_backends()[0]
        assert b.path == os.path.join(os.path.dirname(os.path.abspath(__file__)), 'foo')
        assert b.pub_path == '/a'
