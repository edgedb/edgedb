##
# Copyright (c) 2010, 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class Sequence(str):
    def __new__(cls, value):
        if value == 'next':
            if hasattr(cls, '_class_metadata'):
                value = cls._fetch_next()

        return super().__new__(cls, value)

    @classmethod
    def default(cls):
        result = cls.get_raw_default()

        if result is not None:
            result = cls._fetch_next()

        return result

    @classmethod
    def _fetch_next(cls):  # XXX
        raise NotImplementedError


_add_impl('std::sequence', Sequence)
_add_map(Sequence, 'std::sequence')


class SequenceTypeInfo(s_types.TypeInfo, type=Sequence):
    def strop(self, other: (str, Sequence)) -> 'std::sequence':
        pass

    __add__ = strop
    __radd__ = strop
