##
# Copyright (c) 2012-2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import mimetypes
import types

from metamagic.spin import coroutine
from metamagic.spin.node.dispatch.bus import public
from metamagic.spin.node.dispatch.http import http as http_base
from metamagic.spin.protocols.http import headers as http_headers, statuses as http_statuses
from metamagic.caos.nodesystem.middleware import CaosSessionMiddleware

from metamagic.utils import config
from metamagic.utils.lang.import_ import get_object

from .bucket import BucketMeta, Bucket
from .backends import FSBackend


class files(public):
    pass

files.add_middleware(CaosSessionMiddleware)


class http(http_base):
    pass

http.add_middleware(CaosSessionMiddleware)


class settings(config.Configurable):
    use_x_sendfile = config.cvalue(False, type=bool,
                          doc=('whether default fs download method shoule use X-SendFile '
                               '(or comparable) header.  Use if your frontend server supports '
                               'X-SendFile or X-Accel-Redirect'))


class XAccelRedirect(http_headers.ValueHeader):
    __slots__ = ()
    pattern = 'x-accel-redirect'
    quote_value_on_dump = False


class XSendFile(http_headers.ValueHeader):
    __slots__ = ()
    pattern = 'x-send-file'
    quote_value_on_dump = False


@coroutine
def _do_upload(session, fileobj, bucket_cls, concept, fieldcls=None, config=None):
    with session.transaction():
        with session.transaction():
            bucket_entity = bucket_cls.get_bucket_entity(session)
            concept = concept.set_session(session)
            file_entity = concept(name=fileobj.filename, hash=fileobj.md5,
                                  mimetype=str(fileobj.content_type),
                                  bucket=bucket_entity)

        file_id = file_entity.id
        bucket_cls.store_http_file(id=file_id, file=fileobj)

        if fieldcls and callable(getattr(fieldcls, 'validate', None)):
            filepath = bucket_cls.get_file_path(file_id, file_entity.name)
            fieldcls.validate(filepath, config)

    return file_entity


@files
def upload(context, bucket:str, concept:str=None, fieldcls=None, config=None):
    bucket_cls = BucketMeta.get_bucket_class(bucket)

    session = context.session

    form = yield context.multipart.parse()
    files = form.getlist('files')

    if fieldcls:
        fieldcls = get_object(fieldcls)

    config = types.SimpleNamespace(**(config or {}))

    if concept is None:
        Concept = session.schema.metamagic.utils.fs.file.File
    else:
        Concept = session.schema.get(concept)

    file_ids = {}

    with session.transaction():
        for file in files:
            file_entity = yield _do_upload(session, file, bucket_cls, Concept, fieldcls, config)
            file_ids[file_entity.id] = [file_entity.name]

    return file_ids


@http('download/<id>')
def download(context, id):
    session = context.session
    response = context.response

    File = session.schema.metamagic.utils.fs.file.File

    file = File.get(File.id == id)
    bucket_cls = BucketMeta.get_bucket_class(file.bucket.id)

    url = bucket_cls.get_file_pub_url(file.id, file.name)


    if settings.use_x_sendfile:
        response.headers.add(XSendFile(url))
        response.headers.add(XAccelRedirect(url))
        response.content_type = http_headers.ContentType(*(file.mimetype.split('/')))

        return 'Redirecting...'
    else:
        response.status = http_statuses.found
        response.headers.add(http_headers.Location(url))

        return ''


@coroutine
def http_upload(session, request, bucket_cls, *, concept=None, fieldcls=None, config=None,
                                                 basename='file'):
    """Store a file represented by an HTTP request body.

    Note that this function does not handle multipart uploads.  For
    that see :method:`upload`
    """

    content_type = request.headers.get('content-type')
    if not content_type:
        raise ValueError('missing required Content-Type header')

    content_type = '{}/{}'.format(content_type.type, content_type.subtype)
    extension = mimetypes.guess_extension(content_type)
    filename = '{}{}'.format(basename, extension)

    fileobj = yield request.get_file(filename)

    file_entity = yield _do_upload(session, fileobj, bucket_cls, concept, fieldcls, config)

    return file_entity
