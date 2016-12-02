##
# Copyright (c) 2011-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from .meta import GeometryMeta


class GeometryFactory:
    def __init__(self, geometry_meta=GeometryMeta):
        self.geometry_meta = geometry_meta

    def new_node(self, type, dimensions, values, srid=0):
        cls = self.geometry_meta.class_from_name(type)
        return cls(values, srid=srid, dimensions=dimensions)
