#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

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
