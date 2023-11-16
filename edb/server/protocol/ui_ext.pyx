#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

import base64
import http
import json
import urllib.parse
import os
import mimetypes

import immutables

from edb import buildmeta
from edb import errors

from edb.common import debug
from edb.common import markup


STATIC_FILES_DIR = str(buildmeta.get_shared_data_dir_path() / 'ui')

static_files = dict()

def cache_assets():
    for dirpath, _, filenames in os.walk(STATIC_FILES_DIR):
        for filename in filenames:
            fullpath = os.path.join(dirpath, filename)

            mimetype = mimetypes.guess_type(filename)[0]
            if mimetype is None:
                mimetype = 'application/octet-stream'

            with open(fullpath, 'rb') as f:
                static_files[os.path.relpath(fullpath, STATIC_FILES_DIR)] = (
                    f.read(),
                    mimetype.encode()
                )

async def handle_request(
    request,
    response,
    path_parts,
    server,
):
    try:
        if path_parts == []:
            path_parts = ['index.html']

        data, content_type = static_files.get(
                os.path.join(*path_parts),
                static_files['index.html']
            )
        response.status = http.HTTPStatus.OK
        response.content_type = content_type
        response.body = data
        return
    except Exception as ex:
        return handle_error(request, response, ex)


def handle_error(
    request,
    response,
    error
):
    if debug.flags.server:
        markup.dump(error)

    response.body = b'Internal Server Error'
    response.status = http.HTTPStatus.INTERNAL_SERVER_ERROR
    response.close_connection = True
