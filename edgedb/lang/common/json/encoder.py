##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types
from re import compile as re_compile
from numbers import Number
from decimal import Decimal
from math import isnan, isinf
from collections import OrderedDict, Set, Sequence, Mapping
from uuid import UUID
from datetime import date, time


JAVASCRIPT_MAXINT = 9007199254740992  # see http://ecma262-5.com/ELS5_HTML.htm#Section_8.5

ESCAPE_ASCII = re_compile(r'([\\"/]|[^\ -~])')

ESCAPE_DCT = {
    '\\': '\\\\',
    '/':  '\/',
    '"': '\\"',
    '\b': '\\b',
    '\f': '\\f',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
}
for i in range(0x20):
    ESCAPE_DCT.setdefault(chr(i), '\\u{0:04x}'.format(i))


class Encoder:
    """A Python implementation of a JSON encoder for Python objects designed
       to be compatible with native JSON decoders in various web browsers.

       Can either encode to a python string (see ``dumps``) or a sequence
       of bytes (see ``dumpb``). The string returned by dumps() is guaranteed
       to have only 7-bit ASCII characters [#f1]_ and ``dumps(obj).encode('ascii') = dumpb(obj)``.

       Supports a special encoder class method `encode_hook(obj)` which, if present, is applied to
       the input object and the rest of the processing is applied to the output of encode_hook().
       Note: encode_hook() should always return an object; for objects which should not be
       specially encoded encode_hook() should return the original object.

       Supports custom encoders by using objects' ``__sx_serialize__()``
       method, if available. It is guaranteed that for all non-native types __sx_serialize__
       will be tried before any other attempt to encode the object [#f2]_. The output
       of __sx_serialize__ is in turn encoded as any other object (and may in turn have
       an __sx_serialize__ method or not be supported).

       Natively supports strings, integers, floats, True, False, None, lists, tuples,
       dicts, sets, frozensets, collections.OrderedDicts, colections.Set,
       collections.Sequence [#f3]_, collections.Mapping, uuid.UUIDs [#f4]_, decimal.Decimals,
       datetime.datetime and objects derived form all listed objects.

       For all objects which could not be encoded in any other way an
       attempt is made to convert an object to an encodeable one using ``self.default(obj)``
       method (which can be overwrite in derived classes). If self.default succeeds,
       the output is again encoded as any other object.


       Exceptions raised:

       * Both ``dumps()`` and ``dumpb()`` raise a TypeError for unsupported objects and
         for all dictionary keys which are not strings (or UUIDs [#f5]_) and
         which are not representable as strings (or UUIDs) by their __sx_serialize__ method.

       * ``default()`` raises a TypeError for all unsupported objects, and overwritten ``default()``
         is also expected to raise a TypeError for all objects it does not support.

       * When encoding integers, ``dumps()`` and ``dumpb()`` raise a ValueError if integer
         value is greater than the maximum integer value supported by JavaScript
         (``9007199254740992``, see http://ecma262-5.com/ELS5_HTML.htm#Section_8.5).

       * When encoding a nested object a ValueError is raised when going deeper than
         the allowed nesting level (100 by default, can be overwritten by passing the
         desired value as the second argument to ``dumps()`` and ``dumpb()`` methods)


       .. [#f1] All characters required to be escaped by the JSON spec @ http://json.org are escaped
       .. [#f2] If present, encode_hook() is applied before and independently of all other encoders
       .. [#f3] To avoid errors in the semantix framework ``bytes()``, ``bytearray()`` and derived
                classes are deliberately not encoded using the built-in sequence encoder;
                the only way to encode these objects is to either overwrite the encoders' default()
                method or to provide __sx_serialize__ method in the object being serialized.
       .. [#f4] UUIDs and Decimals are encoded as strings.
       .. [#f5] JSON specification only supports string dictionary keys; since UUIDs
                are also encoded to strings and are a common key in the semantix framework,
                this encoder also supports UUIDs as dictionary keys.
    """

    _nested_level     = 0            # current recursion level
    _max_nested_level = 100          # max allowed level
    _use_hook        = False

    def __init__(self):
        # If 'Encoder.encode_hook' wasn't overridden then don't call it.
        #
        func = self.encode_hook
        if isinstance(func, types.MethodType):
            func = func.__func__
        self._use_hook = func is not Encoder.encode_hook

    def encode_hook(self, obj):
        """Override this method to hook in the encoding process.  Should either
        return a modified/coerced or the same ``obj`` argument.
        """
        return obj

    def default(self, obj):
        """In this implementation always raises a TypeError.

           In order to support new object types can be overwritten to return an
           object natively supported by the encoder (see class description for
           the list of supported objects).

           Example::

            # try to import the C version, if available
            try:
                from semantix.utils.json._encoder import Encoder
            except ImportError:
                # C version is unavailable - import Python version
                from semantix.utils.json.encoder import Encoder

            class Bar:
                pass

            class MyEncoder(Encoder):
                def default(self, obj):
                    if isinstance(obj, Bar):
                        return ['Bar']
                    return super().default(obj)

            MyEncoder().dumps([Bar()]) == '[["Bar"]]'
        """
        raise TypeError('{!r} is not JSON serializable by this encoder'.format(obj))

    def _increment_nested_level(self):
        self._nested_level += 1
        if (self._nested_level > self._max_nested_level):
            raise ValueError('Exceeded maximum allowed recursion level ({}), ' \
                             'possibly circular reference detected'.format(self._max_nested_level))

    def _decrement_nested_level(self):
        self._nested_level -= 1

    def _encode_str(self, obj):
        """Return an ASCII-only JSON representation of a Python string"""
        def replace(match):
            s = match.group(0)
            try:
                return ESCAPE_DCT[s]
            except KeyError:
                n = ord(s)
                if n < 0x10000:
                    return '\\u{0:04x}'.format(n)
                else:
                    # surrogate pair
                    n -= 0x10000
                    s1 = 0xd800 | ((n >> 10) & 0x3ff)
                    s2 = 0xdc00 | (n & 0x3ff)
                    return '\\u{0:04x}\\u{1:04x}'.format(s1, s2)
        return '"' + ESCAPE_ASCII.sub(replace, obj) + '"'

    def _encode_numbers(self, obj):
        """Returns a JSON representation of a Python number (int, float or Decimal)"""

        # strict checks first - for speed
        if obj.__class__ is int:
            if abs(obj)>JAVASCRIPT_MAXINT:
                raise ValueError('Number out of range: {!r}'.format(obj))
            return str(obj)

        if obj.__class__ is float:
            if isnan(obj):
                raise ValueError('NaN is not supported')
            if isinf(obj):
                raise ValueError('Infinity is not supported')
            return repr(obj)

        # more in-depth class analysys last
        if isinstance(obj,int):
            if abs(obj)>JAVASCRIPT_MAXINT:
                raise ValueError('Number out of range: {!r}'.format(obj))
            return str(obj)

        if isinstance(obj,float):
            if isnan(obj):
                raise ValueError('NaN is not supported')
            if isinf(obj):
                raise ValueError('Infinity is not supported')
            return repr(obj)

        if isinstance(obj, Decimal):
            return '"' + str(obj) + '"'

        # for complex and other Numbers
        return self._encode(self.default(obj))

    def _encode_list(self, obj):# do
        """Returns a JSON representation of a Python list"""

        self._increment_nested_level()

        buffer = []
        for element in obj:
            buffer.append(self._encode(element))

        self._decrement_nested_level()

        return '['+ ','.join(buffer) + ']'

    def _encode_dict(self, obj):
        """Returns a JSON representation of a Python dict"""

        self._increment_nested_level()

        buffer = []
        for key, value in obj.items():
            buffer.append(self._encode_key(key) + ':' + self._encode(value))

        self._decrement_nested_level()

        return '{'+ ','.join(buffer) + '}'

    def _encode_key(self, obj):
        """Encodes a dictionary key - a key can only be a string in std JSON"""

        if obj.__class__ is str:
            return self._encode_str(obj)

        if obj.__class__ is UUID:
            return str(obj)

        # __sx_serialize__ is called before any isinstance checks (but after exact type checks)
        try:
            sx_encoder = obj.__sx_serialize__
        except AttributeError:

            if isinstance(obj, UUID):
                return str(obj)

            if isinstance(obj, str):
                return self._encode_str(obj)

            # if everything else failed try the default() method and re-raise any TypeError
            # exceptions as more specific "not a valid dict key" TypeErrors
            try:
                value = self.default(obj)
            except TypeError:
                raise TypeError('{!r} is not a valid dictionary key'.format(obj))
            return self._encode_key(value)

        else:
            return self._encode_key(sx_encoder())

    def _encode(self, obj):
        """Returns a JSON representation of a Python object - see dumps.
        Accepts objects of any type, calls the appropriate type-specific encoder.
        """

        if self._use_hook:
            obj = self.encode_hook(obj)

        # first try simple strict checks

        _objtype = obj.__class__

        if _objtype is str:
            return self._encode_str(obj)

        if _objtype is bool:
            if obj:
                return 'true'
            else:
                return 'false'

        if _objtype is int or _objtype is float:
            return self._encode_numbers(obj)

        if _objtype is list or _objtype is tuple:
            return self._encode_list(obj)

        if obj is None:
            return 'null'

        if _objtype is dict or obj is OrderedDict:
            return self._encode_dict(obj)

        if _objtype is UUID:
            return '"' + str(obj) + '"'

        if _objtype is Decimal:
            return '"' + str(obj) + '"'

        # for all non-std types try __sx_serialize__ before any isinstance checks

        try:
            sx_encoder = obj.__sx_serialize__
        except AttributeError:
            pass
        else:
            return self._encode(sx_encoder())

        # do more in-depth class analysis

        if isinstance(obj, UUID):
            return '"' + str(obj) + '"'

        if isinstance(obj, str):
            return self._encode_str(obj)

        if isinstance(obj, (list, tuple, set, frozenset, Set)):
            return self._encode_list(obj)

        if isinstance(obj, Sequence) and not isinstance(obj, (bytes, bytearray)):
            return self._encode_list(obj)

        if isinstance(obj, (dict, OrderedDict, Mapping)):
            return self._encode_dict(obj)

        # note: number checks using isinstance should come after True/False checks
        if isinstance(obj, Number):
            return self._encode_numbers(obj)

        if isinstance(obj, (date, time)):
            return '"' + obj.isoformat() + '"'

        return self._encode(self.default(obj))

    def dumps(self, obj, *, max_nested_level=100):
        """Returns a string representing a JSON-encoding of ``obj``.

           The second optional ``max_nested_level`` argument controls the maximum
           allowed recursion/nesting level.

           See class description for details.
        """
        self._max_nested_level = max_nested_level
        return self._encode(obj)

    def dumpb(self, obj, *, max_nested_level=100):
        """Similar to ``dumps()``, but returns ``bytes`` instead of a ``string``"""
        self._max_nested_level = max_nested_level
        return self._encode(obj).encode('ascii')
