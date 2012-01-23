##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os

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
    can_publish = (AbstractFileSystemResource,)
