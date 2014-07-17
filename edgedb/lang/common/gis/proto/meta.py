##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class GeometryMeta(type):
    _index_by_class_id = {}
    _index_by_class_name = {}

    def __new__(mcls, name, bases, dct):
        result = super().__new__(mcls, name, bases, dct)

        geo_class_id = getattr(result, 'geo_class_id', None)
        geo_class_name = getattr(result, 'geo_class_name', name)
        if geo_class_id is not None:
            if geo_class_name is None:
                geo_class_name = name

            mcls._index_by_class_id[mcls, geo_class_id] = result
            mcls._index_by_class_name[mcls, geo_class_name] = result
            mcls._index_by_class_name[mcls, geo_class_name.upper()] = result

        return result

    @classmethod
    def class_from_id(cls, geo_class_id):
        return cls._index_by_class_id.get((cls, geo_class_id))

    @classmethod
    def class_from_name(cls, geo_class_name):
        return cls._index_by_class_name.get((cls, geo_class_name))
