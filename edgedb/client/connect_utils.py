##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import urllib.parse


def render_dsn(scheme, params):
    params = dict(params)
    dsn = params.pop('dsn', '')
    if dsn:
        return dsn

    user = params.pop('user', '')
    if user:
        password = params.pop('password', '')
        if password:
            user += f':{password}'

    if user:
        user += '@'

    host = params.pop('host', 'localhost')
    if '/' in host:
        # Put host back, it's a UNIX socket path, needs to be
        # in query part.
        params['host'] = host
        host = ''
        port = ''
    else:
        port = params.pop('port')
        if port:
            port = f':{port}'

    if params:
        query = '?' + urllib.parse.urlencode(params)
    else:
        query = ''

    return f'{scheme}://{user}{host}{port}{query}'
