##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import tags


class TestPackage:
    def test_package_tags(self):
        from . import package1
        from metamagic.utils.lang.import_ import cache

        assert cache.deptracked_modules.get(package1.__name__) == True
        assert package1.__name__ in cache.package_tag_maps

        from .package1.foopkg import spam

        assert hasattr(spam, '__mm_module_tags__')
        assert tags.TestTag in spam.__mm_module_tags__

        from .package1.foopkg import py

        assert hasattr(py, '__mm_module_tags__')
        assert tags.TestTag2 not in py.__mm_module_tags__

        assert tags.TestTag3 in py.__mm_module_tags__

        from .package1.foopkg import ham
        assert not hasattr(ham, '__mm_module_tags__')
