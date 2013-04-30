##
# Copyright (c) 2012, 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import collections

from metamagic.utils import config
from metamagic.utils.datastructures import OrderedSet

from .exceptions import ResourceError, ResourcePublisherError
from .resource import Resource, VirtualFile, AbstractFileSystemResource


class Publisher(config.Configurable):
    """Publishers are used to make resources accessible.

    .. note:: This is a base class, don't use it directly"""

    can_publish = ()

    def __init__(self):
        self.resources = []

    def add_resource(self, resource):
        """Add a root resource to be published"""

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
    """This publisher should be used to consolidate certain resources in one
    file-system location.  Creates all necessary directories, and symlinks all
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
        self._published = OrderedSet()

    published = property(lambda self: self._published)

    def _publish_fs_resource(self, resource):
        src_path = resource.__sx_resource_path__
        dest_path = os.path.abspath(os.path.join(self.pubdir,
                                                 resource.__sx_resource_public_path__))

        if os.path.exists(dest_path):
            if os.path.islink(dest_path):
                if os.stat(dest_path).st_ino == os.stat(src_path).st_ino:
                    # same file
                    return
                else:
                    os.unlink(dest_path)

            else:
                # not a symlink, let's just remove it
                if os.path.isfile(dest_path):
                    os.remove(dest_path)
                else:
                    os.rmdir(dest_path)

        elif os.path.islink(dest_path):
            # broken symlink
            os.unlink(dest_path)

        os.symlink(src_path, dest_path)

    def _publish_virtual_resource(self, resource):
        dest_path = os.path.abspath(os.path.join(self.pubdir,
                                                 resource.__sx_resource_public_path__))

        if os.path.exists(dest_path):
            os.remove(dest_path)

        with open(dest_path, 'wb+') as dest:
            dest.write(resource.__sx_resource_get_source__())

    def publish(self, resources):
        for resource in resources:
            if isinstance(resource, AbstractFileSystemResource):
                self._published.add(resource)
                self._publish_fs_resource(resource)

            elif isinstance(resource, VirtualFile):
                self._published.add(resource)
                self._publish_virtual_resource(resource)

    def publish_all(self):
        """Publish all resources in the ``pubdir``"""
        self.publish(self._collect_deps())
