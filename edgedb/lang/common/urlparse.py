##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from urllib.parse import urlsplit, urlunsplit, SplitResult, parse_qs


protocol_map = {'pymod': 'http'}

def urlparse(url):
    result = urlsplit(url)
    if '+' in result.scheme:
        protocol, transport = result.scheme.split('+')
    else:
        protocol = transport = result.scheme

    if transport in protocol_map:
        scheme = protocol_map[transport]
    else:
        scheme = transport

    result = SplitResult(scheme, result.netloc, result.path, result.query, result.fragment)
    result = urlsplit(urlunsplit(result))

    scheme, netloc, path, query, fragment = result

    if protocol == 'pymod':
        path = result.path.lstrip('/')

    if query:
        query = parse_qs(query)

    result = SplitResult(protocol, netloc, path, query, fragment)

    result = (protocol, result)

    return result
