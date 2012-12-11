##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import ctypes
import ctypes.util
import os
import sys


from metamagic.exceptions import MetamagicError


class FileMagicError(MetamagicError):
    pass


MAGIC_MIME = 0x000010


def _find_libmagic():
    lib = ctypes.util.find_library('magic') or ctypes.util.find_library('magic1')

    if lib:
        return ctypes.CDLL(lib)

    else:
        raise ImportError('could not find libmagic')


_magic_t = ctypes.c_void_p

_libmagic = _find_libmagic()


_magic_error = _libmagic.magic_error
_magic_error.argtypes = [_magic_t]
_magic_error.restype = ctypes.c_char_p


def _check_error(result, func, args):
    error = _magic_error(args[0])
    if error is not None:
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
_magic_load.errcheck = _check_error

_magic_buffer = _libmagic.magic_buffer
_magic_buffer.argtypes = [_magic_t, ctypes.c_void_p, ctypes.c_size_t]
_magic_buffer.restype = ctypes.c_char_p
_magic_buffer.errcheck = _check_error

_magic_file = _libmagic.magic_file
_magic_file.argtypes = [_magic_t, ctypes.c_char_p]
_magic_file.restype = ctypes.c_char_p
_magic_file.errcheck = _check_error

_magic_descriptor = _libmagic.magic_descriptor
_magic_descriptor.argtypes = [_magic_t, ctypes.c_int]
_magic_descriptor.restype = ctypes.c_char_p
_magic_descriptor.errcheck = _check_error


def _init():
    flags = 0

    flags |= MAGIC_MIME

    cookie = _magic_open(flags)
    _magic_load(cookie, None)

    return cookie


def _fini(cookie):
    _magic_close(cookie)


def get_mime_from_buffer(buffer):
    result = _magic_buffer(_init(), buffer, len(buffer))
    return result.decode('latin-1')


def get_mime_from_path(path):
    cookie = None

    try:
        cookie = _init()
        return _magic_file(cookie, path.encode(sys.getfilesystemencoding())).decode('latin-1')
    finally:
        _fini(cookie)


def get_mime_from_fileno(fileno):
    cookie = None

    fd = os.dup(fileno)

    try:
        cookie = _init()
        return _magic_descriptor(cookie, fd).decode('latin-1')
    finally:
        _fini(cookie)
