##
# Copyright (c) 2012, 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import collections

from metamagic.utils.datastructures import OrderedSet
from .exceptions import ResourceError


class Resource:
    """Resource is an abstract concept of a piece of information.  It may be
    some code, some image of video file etc."""

    def __init__(self):
        self.__sx_resource_deps__ = collections.OrderedDict()
        self.__sx_resource_parent__ = None

    def __sx_add_required_resource__(self, dependency, weak=False):
        """Make ``resource`` dependent on ``dependency``.  ``weak`` means that the
        ``dependency`` must be loaded with ``resource`` but the loading order is
        not important.

        :param Resource resource: Resource to add dependency to.
        :param Resource dependency: A resource required for ``resource`` to work.
        :param bool weak:
        """

        if not isinstance(dependency, Resource):
            raise ResourceError('an instance of Resource expected, got {!r}'.format(dependency))

        try:
            cur = self.__sx_resource_deps__[dependency]
        except KeyError:
            self.__sx_resource_deps__[dependency] = weak
        else:
            if cur and not weak:
                self.__sx_resource_deps__[dependency] = False

    @classmethod
    def _list_resources(cls, resource):
        """Builds the full list of resources that current resource depends on.
        The list includes the resource itself."""

        def _collect_deps(resource, collected, visited, to_import, *, level, child=None):
            visited.add(resource)

            parent = resource.__sx_resource_parent__
            if parent is not None and parent not in visited:
                _collect_deps(parent, collected, visited, to_import, level=level+1, child=resource)

            for mod, weak in resource.__sx_resource_deps__.items():
                if weak:
                    to_import.add(mod)
                else:
                    if mod not in visited:
                        _collect_deps(mod, collected, visited, to_import, level=level+1)
                    else:
                        if child is not None and mod is child:
                            # If we were called from _collect_deps for a child's
                            # __sx_resource_parent__, and the parent module imports that child,
                            # then we add child to the "collected" list, to preserve the order
                            # of imports in __init__.js files
                            collected.add(mod)

            collected.add(resource)

        visited = set()
        collected = OrderedSet()

        to_import = OrderedSet((resource,))
        while to_import:
            mod = to_import.pop()
            _collect_deps(mod, collected, visited, to_import, level=0)
            to_import -= collected

        return tuple(collected)


class ResourceContainer(Resource):
    pass


class AbstractFileResource(Resource):
    def __init__(self, public_path):
        super().__init__()
        self.__sx_resource_public_path_value__ = public_path

    def __sx_resource_get_public_path__(self):
        return self.__sx_resource_public_path_value__

    __sx_resource_public_path__ = property(__sx_resource_get_public_path__)


class VirtualFile(AbstractFileResource):
    """A resource that encapsulates some information and stores it in memory, but
    later, in order to be published, will be dumped to a file"""

    def __init__(self, source, public_path):
        assert source is None or isinstance(source, bytes)
        super().__init__(public_path)
        self.__sx_resource_source_value__ = source if source is not None else b''

    def __sx_resource_set_source__(self, source):
        assert isinstance(source, bytes)
        self.__sx_resource_source_value__ = source

    def __sx_resource_append_source__(self, src):
        assert isinstance(src, bytes)
        self.__sx_resource_source_value__ += src

    def __sx_resource_get_source__(self):
        src = self.__sx_resource_source_value__
        if not src:
            raise ResourceError('no source for {} resource {}'.
                                format(type(self).__name__, self.__sx_resource_public_path__))
        return src


class AbstractFileSystemResource(AbstractFileResource):
    """Abstract file system resource.

    .. note:: Don't use it directly, it's a base class for ``File``
              and ``Directory`` resources."""

    def __init__(self, path, public_path=None):
        if public_path is None:
            public_path = os.path.basename(path)

        super().__init__(public_path)

        if not os.path.exists(path):
            raise ResourceError('path {!r} does not exist'.format(path))

        self.__sx_resource_path__ = path


class File(AbstractFileSystemResource):
    """A file.  For instance, a stylesheet file required for application front-end."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        path = self.__sx_resource_path__
        if not os.path.isfile(path):
            raise ResourceError('{!r} is not a file'.format(path))


class Directory(AbstractFileSystemResource):
    """A directory.  For instance, a directory of graphical assets required for
    the user interface."""

    def __init__(self, path):
        super().__init__(path)

        path = self.__sx_resource_path__
        if not os.path.isdir(path):
            raise ResourceError('{!r} is not a directory'.format(path))
