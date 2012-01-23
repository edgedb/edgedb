##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import shutil

from semantix.exceptions import SemantixError
from semantix.utils.algos import topological
from semantix.utils.datastructures import OrderedSet
from semantix.utils.debug import timeit


class ResourceError(SemantixError):
    pass


class Resource:
    def __init__(self):
        self.__sx_resource_deps__ = []
        self.__sx_resource_parent__ = None

    @classmethod
    def add_required_resource(cls, resource, dependency, weak=False):
        if not isinstance(dependency, Resource):
            raise ResourceError('an instance of Resource expected, got {!r}'.format(dependency))

        resource.__sx_resource_deps__.append((dependency, weak))

    @classmethod
    def _list_resources(cls, resource):
        """Builds the full list of resources that current resource depends on.
        The list includes the resource itself."""

        def _collect_deps(resource, collected, visited, to_import):
            visited.add(resource)

            parent = resource.__sx_resource_parent__
            if parent is not None and parent not in visited:
                _collect_deps(parent, collected, visited, to_import)

            for mod, weak in resource.__sx_resource_deps__:
                if weak:
                    to_import.add(mod)
                else:
                    if mod not in visited:
                        _collect_deps(mod, collected, visited, to_import)

            collected.add(resource)

        visited = set()
        collected = OrderedSet()

        to_import = OrderedSet((resource,))
        while to_import:
            mod = to_import.pop()
            _collect_deps(mod, collected, visited, to_import)
            to_import -= collected

        return tuple(collected)


class VirtualFile(Resource):
    def __init__(self, source, public_path):
        super().__init__()

        self.__sx_resource_source__ = source
        self.__sx_resource_public_path__ = public_path

    def __sx_resource_get_source__(self):
        src = self.__sx_resource_source__
        if src is None:
            raise ResourceError('no source for VirtualFile resource')
        return src


class AbstractFileSystemResource(Resource):
    def __init__(self, path, public_path=None):
        super().__init__()

        if not os.path.exists(path):
            raise ResourceError('path {!r} does not exist'.format(path))

        self.__sx_resource_path__ = path

        if public_path is None:
            public_path = os.path.basename(path)
        self.__sx_resource_public_path__ = public_path


class File(AbstractFileSystemResource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        path = self.__sx_resource_path__
        if not os.path.isfile(path):
            raise ResourceError('{!r} is not a file'.format(path))


class Directory(AbstractFileSystemResource):
    def __init__(self, path):
        super().__init__(path)

        path = self.__sx_resource_path__
        if not os.path.isdir(path):
            raise ResourceError('{!r} is not a directory'.format(path))


class Publisher:
    can_publish = ()

    def __init__(self):
        self.resources = []

    def add_resource(self, resource):
        if not self.can_publish or not isinstance(resource, self.can_publish):
            raise ResourceError('publisher {!r} can\'t publish resource {!r}'. \
                                format(self, resource))

        self.resources.append(resource)

    def _collect_deps(self):
        collected = OrderedSet()

        for resource in self.resources:
            collected.update(Resource._list_resources(resource))

        return tuple(collected)


class StaticPublisher(Publisher):
    """This published should be used to consolidate certain resource in one
    file-system location.  Creates all necessary directories and symlinks all
    ``VirtualFile``, ``File`` and ``Directory`` resources."""

    can_publish = (AbstractFileSystemResource, VirtualFile)

    def __init__(self, pubdir, *, autocreate=True):
        """
        :param str pubdir: Directory to publish all resources in
        :param bool autocreate: If ``pubdir`` path does not exist, and this flag is set,
                                the publisher will create it automatically.
        """

        super().__init__()

        if not os.path.isabs(pubdir):
            pubdir = os.path.abspath(pubdir)

        if not os.path.isdir(pubdir):
            if not autocreate:
                raise ResourcePublisherError('missing directory {!r}'.format(pubdir))

            try:
                os.makedirs(pubdir, exist_ok=True)
            except OSError as ex:
                raise ResourcePublisherError('an error occurred during {!r} directory ' \
                                             'autocreation'.format(pubdir)) from ex

        if not os.access(pubdir, os.R_OK | os.W_OK):
            raise ResourcePublisherError('directory {!r} is not writable and readable')

        self.pubdir = pubdir
        self._published = None

    published = property(lambda self: self._published)

    def publish_all(self):
        """Publish all resources in the ``pubdir``"""

        self._published = deps = self._collect_deps()

        for resource in deps:
            if isinstance(resource, AbstractFileSystemResource):
                src_path = resource.__sx_resource_path__
                dest_path = os.path.abspath(os.path.join(self.pubdir,
                                                         resource.__sx_resource_public_path__))

                if os.path.exists(dest_path):
                    if os.path.islink(dest_path):
                        ex_dest = os.readlink(dest_path)
                        if not os.path.isabs(ex_dest):
                            ex_dest = os.path.abspath(os.path.join(os.path.dirname(dest_path),
                                                                   ex_dest))

                        if ex_dest != dest_path:
                            os.unlink(dest_path)
                        else:
                            continue

                    else:
                        if os.path.isfile(dest_path):
                            os.remove(dest_path)
                        else:
                            os.rmdir(dest_path)

                os.symlink(src_path, dest_path)

            elif isinstance(resource, VirtualFile):
                dest_path = os.path.abspath(os.path.join(self.pubdir,
                                                         resource.__sx_resource_public_path__))

                if os.path.exists(dest_path):
                    os.remove(dest_path)

                with open(dest_path, 'wt+') as dest:
                    dest.write(resource.__sx_resource_get_source__())
