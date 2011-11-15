##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import errno
import marshal
import pickle
import os
import struct
import sys
import logging


from semantix.utils.lang.loader import LanguageSourceFileLoader
from semantix.utils.lang.import_ import utils as imp_utils


class ProtoSchemaModuleLoader(LanguageSourceFileLoader):
    _schema_cache_struct = struct.Struct('!I')
    logger = logging.getLogger('semantix.lang.import.loader')

    def execute_module_code(self, module, code):
        cache_path = None
        cached_data = None

        if getattr(self._context, 'toplevel', True):
            modpath = module.__file__

            if modpath:
                cache_path = imp_utils.cache_from_source(modpath, cache_ext='.schema')

                try:
                    with open(cache_path, 'rb') as f:
                        cache_bytes = f.read()
                except IOError:
                    cache_bytes = None

                if cache_bytes is not None:
                    cm_size, = self._schema_cache_struct.unpack_from(cache_bytes)
                    cm_offset = self._schema_cache_struct.size
                    cache_manifest = marshal.loads(cache_bytes[cm_offset:cm_offset+cm_size])

                    try:
                        for file, mtime in cache_manifest.items():
                            if os.stat(file).st_mtime != mtime:
                                raise ImportError('stale cache')
                    except (IOError, ImportError):
                        pass
                    else:
                        try:
                            cached_data = pickle.loads(cache_bytes[cm_offset+cm_size:])
                        except Exception:
                            self.logger.warning('exception caught while unpacking schema cache',
                                                exc_info=True)

        if cached_data is not None:
            self.set_module_attributes(module, cached_data.items())
        else:
            super().execute_module_code(module, code)

        if cache_path is not None and cached_data is None:
            files = {}
            for m in module._index_.iter_modules():
                m = sys.modules[m]
                f = m.__file__
                files[f] = os.stat(f).st_mtime

            cache_manifest = marshal.dumps(files)
            cache_manifest_size = len(cache_manifest)

            cache = bytearray(self._schema_cache_struct.pack(cache_manifest_size))
            cache.extend(cache_manifest)
            cache.extend(pickle.dumps({'_index_': module._index_, '_module_': module._module_}))

            if not os.path.exists(os.path.dirname(cache_path)):
                try:
                    os.makedirs(os.path.dirname(cache_path))
                except IOError as e:
                    if e.errno == errno.EACCES:
                        return
                    else:
                        raise

            try:
                with open(cache_path, 'wb') as f:
                    f.write(cache)
            except IOError as e:
                if e.errno == errno.EACCES:
                    pass
                else:
                    raise
