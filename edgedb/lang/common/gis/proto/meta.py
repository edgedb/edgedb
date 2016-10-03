##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class GeometryMeta(type):
    _index_by_class_id = {}
    _index_by_classname = {}

    def __new__(mcls, name, bases, dct):
        result = super().__new__(mcls, name, bases, dct)

        geo_class_id = getattr(result, 'geo_class_id', None)
        geo_classname = getattr(result, 'geo_classname', name)
        if (geo_class_id is not None and
                (mcls, geo_class_id) not in mcls._index_by_class_id):
            if geo_classname is None:
                geo_classname = name

            mcls._index_by_class_id[mcls, geo_class_id] = result
            mcls._index_by_classname[mcls, geo_classname] = result
            mcls._index_by_classname[mcls, geo_classname.upper()] = result

        return result

    @classmethod
    def class_from_id(cls, geo_class_id):
        return cls._index_by_class_id.get((cls, geo_class_id))

    @classmethod
    def class_from_name(cls, geo_classname):
        return cls._index_by_classname.get((cls, geo_classname.upper()))
