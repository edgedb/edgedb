##
# Copyright (c) 2014 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import types

from setuptools.command import build_ext as _build_ext


class cython_build_ext(_build_ext.build_ext):
    def __new__(cls, *args, **kwargs):
        # Since Cython availability is not guaranteed at top level,
        # we have to resort to injecting cython build_ext into
        # our mro here.
        #
        from Cython.Distutils import build_ext as _cython_build_ext

        _ns = cls.__dict__

        _cls = types.new_class(
            'build_ext', bases=(_cython_build_ext, ), exec_body=lambda ns: _ns)

        result = _cls.__new__(_cls, *args, **kwargs)
        result.__init__(*args, **kwargs)

        return result
