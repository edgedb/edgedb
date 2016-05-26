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
        from metamagic.caos import session as caos_session

        session = caos_session.Session.from_object(cls)
        value = session.sequence_next(cls)

        pattern = cls.__sx_prototype__.get_attribute(
            'metamagic.caos.builtins.pattern')
        if pattern:
            value = pattern.value.format(value)

        return value


_add_impl('metamagic.caos.builtins.sequence', Sequence)
_add_map(Sequence, 'metamagic.caos.builtins.sequence')
