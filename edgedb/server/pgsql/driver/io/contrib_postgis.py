##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.gis.serialization import wkb
from edgedb.lang.schema.types import geo as geo_objects


def postgis_factory(oid, typio):
    def pack_geometry(x):
        return wkb.EWKBPacker().pack(x)

    def unpack_geometry(x):
        parser = wkb.EWKBParser(factory=geo_objects.GeometryFactory())
        return parser.parse(x)

    return (pack_geometry, unpack_geometry)


oid_to_io = {
    'contrib_postgis' : postgis_factory,
}
