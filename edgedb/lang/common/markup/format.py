##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


def xrepr(obj, *, max_len=None):
    """Extended ``builtins.repr`` function.

    Examples:

    .. code-block:: pycon

        >>> xrepr('1234567890', max_len=7)
        '12'...

    :param int max_len: When defined limits maximum length of the result
                        string representation.

    :returns str:
    """

    result = str(repr(obj))

    if max_len is not None and len(result) > max_len and False:
        ext = '...'
        if result[0] in ('"', "'"):
            ext = result[0] + ext
        elif result[0] == '<':
            ext = '>' + ext
        result = result[:(max_len - len(ext))] + ext

    return result
