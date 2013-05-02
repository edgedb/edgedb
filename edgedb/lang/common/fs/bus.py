##
# Copyright (c) 2012-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types

from metamagic.spin.node.dispatch.bus import public
from metamagic.spin.node.dispatch.http import http as http_base
from metamagic.spin.protocols.http import headers as http_headers, statuses as http_statuses
from metamagic.spin.node.extras.middleware import CaosSessionMiddleware

from metamagic.utils.lang.import_ import get_object

from .bucket import BucketMeta, Bucket
from .backends import FSBackend


class files(public):
    pass

files.add_middleware(CaosSessionMiddleware)


class http(http_base):
    pass

http.add_middleware(CaosSessionMiddleware)


@files
def upload(context, bucket:str, concept:str=None, fieldcls=None, config=None):
    bucket_cls = BucketMeta.get_bucket_class(bucket)

    session = context.session
    bucket_entity = bucket_cls.get_bucket_entity(session)

    form = yield context.multipart.parse()
    files = form.getlist('files')

    if fieldcls:
        fieldcls = get_object(fieldcls)

    config = types.SimpleNamespace(**(config or {}))

    if concept is None:
        Concept = session.schema.metamagic.utils.fs.file.File
    else:
        Concept = session.schema.get(concept)

    with session.transaction():
        file_entities = []
        for file in files:
            file_entities.append((file, Concept(name=file.filename,
                                                hash=file.md5,
                                                mimetype=str(file.content_type),
                                                bucket=bucket_entity),
                                  file.filename))
        session.sync()

        file_ids = {}
        for file, file_entity, filename in file_entities:
            file_id = file_entity.id
            bucket_cls.store_http_file(id=file_id, file=file)

            if fieldcls and callable(getattr(fieldcls, 'validate', None)):
                filepath = bucket_cls.get_file_path(file_id, file_entity.name)
                fieldcls.validate(filepath, config)

            file_ids[file_id] = [filename]

    return file_ids


@http
def download(context, id):
    # NOTE: Right now, this method just redirects to the public URL of
    # the file.  Later, it should read settings from file's bucket
    # and decide either to redirect, or to validate user session and
    # transfer file manually

    session = context.session
    File = session.schema.metamagic.utils.fs.file.File

    file = File.get(File.id == id)
    bucket_cls = BucketMeta.get_bucket_class(file.bucket.id)

    url = bucket_cls.get_file_pub_url(file.id, file.name)

    context.response.status = http_statuses.found
    context.response.headers.add(http_headers.Location(url))

    return ''
