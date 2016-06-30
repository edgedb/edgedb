##
# Copyright (c) 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import ctypes
import ctypes.util
import os
import sys

from edgedb.lang.common.exceptions import EdgeDBError


class FileMagicError(EdgeDBError):
    pass


MAGIC_MIME = 0x000010


def _find_libmagic():
    lib = ctypes.util.find_library('magic') or ctypes.util.find_library('magic1')

    if lib:
        return ctypes.cdll.LoadLibrary(lib)

    else:
        raise ImportError('could not find libmagic')


class _MagicSet(ctypes.Structure):
    pass
_MagicSet._fields_ = []
_magic_t = ctypes.POINTER(_MagicSet)


_libmagic = _find_libmagic()


_magic_error = _libmagic.magic_error
_magic_error.argtypes = [_magic_t]
_magic_error.restype = ctypes.c_char_p


def _check_error_null(result, func, args):
    if result is None:
        error = _magic_error(args[0])
        raise FileMagicError(error)
    else:
        return result


def _check_error_negative(result, func, args):
    if result < 0:
        error = _magic_error(args[0])
        raise FileMagicError(error)
    else:
        return result


_magic_open = _libmagic.magic_open
_magic_open.argtypes = [ctypes.c_int]
_magic_open.restype = _magic_t

_magic_close = _libmagic.magic_close
_magic_close.argtypes = [_magic_t]
_magic_close.restype = None

_magic_load = _libmagic.magic_load
_magic_load.argtypes = [_magic_t, ctypes.c_char_p]
_magic_load.restype = ctypes.c_int
_magic_load.errcheck = _check_error_negative

_magic_buffer = _libmagic.magic_buffer
_magic_buffer.argtypes = [_magic_t, ctypes.c_void_p, ctypes.c_size_t]
_magic_buffer.restype = ctypes.c_char_p
_magic_buffer.errcheck = _check_error_null

_magic_file = _libmagic.magic_file
_magic_file.argtypes = [_magic_t, ctypes.c_char_p]
_magic_file.restype = ctypes.c_char_p
_magic_file.errcheck = _check_error_null

_magic_descriptor = _libmagic.magic_descriptor
_magic_descriptor.argtypes = [_magic_t, ctypes.c_int]
_magic_descriptor.restype = ctypes.c_char_p
_magic_descriptor.errcheck = _check_error_null


def _init():
    flags = 0

    flags |= MAGIC_MIME

    cookie = _magic_open(flags)
    _magic_load(cookie, None)

    return cookie


def _fini(cookie):
    _magic_close(cookie)


def _massage_result(result):
    if result == 'binary':
        return 'application/octet-stream'
    else:
        return result


def get_mime_from_buffer(buffer):
    cookie = _init()

    try:
        result = _magic_buffer(_init(), buffer, len(buffer)).decode('latin-1')
    finally:
        _fini(cookie)

    return _massage_result(result)


def get_mime_from_path(path):
    cookie = _init()

    try:
        result = _magic_file(cookie, path.encode(sys.getfilesystemencoding())).decode('latin-1')
    finally:
        _fini(cookie)

    return _massage_result(result)


def get_mime_from_fileno(fileno):
    cookie = _init()

    fd = os.dup(fileno)

    try:
        result = _magic_descriptor(cookie, fd).decode('latin-1')
    finally:
        _fini(cookie)

    return _massage_result(result)
