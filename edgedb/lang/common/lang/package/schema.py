##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types

from metamagic.utils.lang.context import LazyImportsModule
from metamagic.utils.lang.yaml.schema import LazyImportsSchema

from . import _schema

__all__ = ('Package',)


class PackageModule(LazyImportsModule):
    pass


class Package(_schema.Package, LazyImportsSchema):
    @classmethod
    def get_module_class(cls):
        return PackageModule
