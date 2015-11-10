##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types

from importkit.context import LazyImportsModule
from importkit.yaml.schema import LazyImportsSchema

from . import _schema

__all__ = ('Package',)


class PackageModule(LazyImportsModule):
    pass


class Package(_schema.Package, LazyImportsSchema):
    @classmethod
    def get_module_class(cls):
        return PackageModule
