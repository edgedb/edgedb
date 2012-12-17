##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from importlib import machinery, _bootstrap
import os
import sys


class FileFinder(machinery.FileFinder):
    def __init__(self, path, *details):
        super().__init__(path, *details)

    @classmethod
    def update_loaders(cls, finder, loader_details, replace=False):
        if replace:
            loaders = []

            for loader, suffixes in loader_details:
                loaders.extend((s, loader) for s in suffixes)

            finder._loaders[:] = loaders
        else:
            for loader, suffixes in loader_details:
                for suffix in suffixes:
                    if (suffix, loader) not in finder._loaders:
                        finder._loaders.append((suffix, loader))

        finder.invalidate_caches()

    @classmethod
    def path_hook(cls):
        from metamagic.utils.lang.meta import LanguageMeta

        def path_hook_for_FileFinder(path):
            loader_details = list(_bootstrap._get_supported_file_loaders())
            loader_details.extend(LanguageMeta.get_loaders())
            return cls(path, *loader_details)

        return path_hook_for_FileFinder


def install():
    sys.path_hooks.insert(0, FileFinder.path_hook())


def update_finders():
    import metamagic
    from metamagic.utils.lang.meta import LanguageMeta

    rpath = os.path.realpath

    loader_details = list(_bootstrap._get_supported_file_loaders())
    loader_details.extend(LanguageMeta.get_loaders())

    for path, finder in list(sys.path_importer_cache.items()):
        if isinstance(finder, FileFinder) or any(rpath(path) == rpath(nspath) for nspath in list(metamagic.__path__)):
            FileFinder.update_loaders(finder, loader_details, isinstance(finder, FileFinder))
