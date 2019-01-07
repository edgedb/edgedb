#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
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


import hashlib
import logging
import os
import pathlib
import pickle
import tempfile
import typing


logger = logging.getLogger('edb.devmode.cache')


def enable_dev_mode():
    os.environ['__EDGEDB_DEVMODE'] = '1'


def is_in_dev_mode() -> bool:
    devmode = os.environ.get('__EDGEDB_DEVMODE', '0')
    return devmode.lower() not in ('0', '', 'false')


def get_dev_mode_cache_dir() -> os.PathLike:
    if is_in_dev_mode():
        root = pathlib.Path(__file__).parent.parent.parent
        cache_dir = (root / 'build' / 'cache')
        cache_dir.mkdir(exist_ok=True)
        return cache_dir
    else:
        raise RuntimeError('server is not running in dev mode')


def read_dev_mode_cache(cache_key, path):
    full_path = get_dev_mode_cache_dir() / path

    if full_path.exists():
        with open(full_path, 'rb') as f:
            src_hash = f.read(16)
            if src_hash == cache_key:
                try:
                    return pickle.load(f)
                except Exception:
                    logging.exception(f'could not unpickle {path}')


def write_dev_mode_cache(obj, cache_key, path):
    full_path = get_dev_mode_cache_dir() / path

    try:
        with tempfile.NamedTemporaryFile(
                mode='wb', dir=full_path.parent, delete=False) as f:
            f.write(cache_key)
            pickle.dump(obj, file=f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        try:
            os.unlink(f.name)
        except OSError:
            pass
        finally:
            raise
    else:
        os.rename(f.name, full_path)


def hash_dirs(dirs: typing.Tuple[str, str]) -> bytes:
    def hash_dir(dirname, ext, paths):
        with os.scandir(dirname) as it:
            for entry in it:
                if entry.is_file() and entry.name.endswith(ext):
                    paths.append(entry.path)
                elif entry.is_dir():
                    hash_dir(entry.path, ext, paths)

    paths = []
    for dirname, ext in dirs:
        hash_dir(dirname, ext, paths)

    h = hashlib.md5()
    for path in sorted(paths):
        with open(path, 'rb') as f:
            h.update(f.read())

    return h.digest()
