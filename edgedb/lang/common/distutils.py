##
# Copyright (c) 2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from distutils.command import build_ext as _build_ext


class cython_build_ext(_build_ext.build_ext):
    def __init__(self, *args, **kwargs):
        self._ctor_args = args
        self._ctor_kwargs = kwargs
        self._cython = None

    def __getattribute__(self, name):
        cython = object.__getattribute__(self, '_cython')
        if cython is None:
            from Cython.Distutils import build_ext

            _ctor_args = object.__getattribute__(self, '_ctor_args')
            _ctor_kwargs = object.__getattribute__(self, '_ctor_kwargs')
            cython = build_ext(*_ctor_args, **_ctor_kwargs)
            object.__setattr__(self, '_cython', cython)
        return getattr(cython, name)
