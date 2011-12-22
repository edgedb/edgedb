##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import pickle

from semantix.utils.datastructures import Void
from semantix.utils.debug import assert_raises


class TestUtilsDSAll:
    def test_utils_ds_markers_pickle(self):
        assert pickle.loads(pickle.dumps(Void)) is Void
        assert not Void
        with assert_raises(TypeError, error_re='instantiated'):
            Void()
