##
# Copyright (c) 2008-2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from importlib import machinery, _bootstrap
import os
import sys


def _get_file_loaders():
    from metamagic.utils.lang.meta import LanguageMeta

    loader_details = list(_bootstrap._get_supported_file_loaders())

    lang_loaders = list(LanguageMeta.get_loaders())
    ext_map = {}

    for loader, extensions in lang_loaders:
        for extension in extensions:
            ext_map[extension] = loader

    for i, (loader, extensions) in enumerate(loader_details):
        untouched_exts = set(extensions) - set(ext_map)
        loader_details[i] = (loader, list(untouched_exts))

    loader_details.extend(lang_loaders)

    return loader_details


class FileFinder(machinery.FileFinder):
    def __init__(self, path, *details):
        super().__init__(path, *details)

    @classmethod
    def update_loaders(cls, finder, loader_details, replace=False):
        loaders = []

        for loader, suffixes in loader_details:
            loaders.extend((s, loader) for s in suffixes)

        finder._loaders[:] = loaders

        finder.invalidate_caches()

    @classmethod
    def path_hook(cls):
        def path_hook_for_FileFinder(path):
            return cls(path, *_get_file_loaders())
        return path_hook_for_FileFinder

    def __repr__(self):
        return 'mm.FileFinder({!r})'.format(self.path)


def install():
    for i, hook in enumerate(sys.path_hooks):
        hook_mod = getattr(hook, '__module__', '')
        if (hook_mod.startswith('importlib')
                or hook_mod.startswith('_frozen_importlib')):
            sys.path_hooks.insert(i, FileFinder.path_hook())
            break
    else:
        sys.path_hooks.insert(0, FileFinder.path_hook())


def update_finders():
    import metamagic

    rpath = os.path.realpath
    loader_details = _get_file_loaders()
    for path, finder in list(sys.path_importer_cache.items()):
        if (isinstance(finder, FileFinder)
                or any(rpath(path) == rpath(nspath) for nspath in list(metamagic.__path__))):
            FileFinder.update_loaders(finder, loader_details, isinstance(finder, FileFinder))
