##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import shell
from . import error


class DataResolver(shell.BackendResolverHelper):
    def resolve(self, url):
        schema, data = url.path.split(',', 1)
        schema = schema.split(';')
        mime_type = schema[0]

        handler = shell.BackendResolverHelperMeta.get('data_mime_types', mime_type)

        if handler:
            return handler().resolve_data(data)
        else:
            err = 'unsupported Caos data MIME type: %s' % mime_type
            raise error.BackendResolverError(err)
