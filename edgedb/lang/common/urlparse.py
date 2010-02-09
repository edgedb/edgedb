##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from urllib.parse import urlsplit, urlunsplit, SplitResult

protocol_map = {'pq': 'file'}

def urlparse(url):
    result = urlsplit(url)
    if '+' in result.scheme:
        protocol, transport = result.scheme.split('+')

        if transport in protocol_map:
            scheme = protocol_map[transport]
        else:
            scheme = transport

        result = SplitResult(scheme, result.netloc, result.path, result.query, result.fragment)
        result = urlsplit(urlunsplit(result))
        result = (protocol, result)
    else:
        result = (result.scheme, result)

    return result
