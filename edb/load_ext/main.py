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

"""Command to load an extension into an edgedb installation."""

from __future__ import annotations


import pathlib
import shutil
import subprocess
import sys
import tomllib
import zipfile

import click

from edb import buildmeta
from edb.common import typeutils

# Directories that we map to config values in pg_config.
CONFIG_PATHS = {
    'share': 'sharedir',
    'lib': 'libdir',
    'include': 'includedir',
}


def install(
    pkg: pathlib.Path,
    ext_dir: pathlib.Path,
    pg_config: dict[str, str],
) -> None:
    print("Installing", ext_dir / pkg.name)
    shutil.copyfile(pkg, ext_dir / pkg.name)

    with zipfile.ZipFile(pkg) as z:
        with z.open('MANIFEST.toml') as m:
            manifest = tomllib.load(m)

        if 'postgres_files' in manifest:
            dir = manifest['postgres_files']
            pdir = pathlib.Path(dir)

            for entry in z.infolist():
                if entry.is_dir():
                    continue
                fpath = pathlib.Path(entry.filename)

                if fpath.parts[0] != dir:
                    continue
                # If the path is too short or isn't one of the
                # directories we know about, skip it.
                if (
                    len(fpath.parts) < 2
                    or not (config_field := CONFIG_PATHS.get(fpath.parts[1]))
                ):
                    print("Skipping", fpath)
                    continue

                config_dir = pg_config[config_field]
                fpath = fpath.relative_to(pdir / fpath.parts[1])

                target_file = config_dir / fpath
                print("Installing", target_file)
                with z.open(entry) as f:
                    data = f.read()
                with open(target_file, "wb") as f:
                    f.write(data)


def get_pg_config() -> dict[str, str]:
    output = subprocess.run(
        buildmeta.get_pg_config_path(),
        capture_output=True,
        text=True,
        check=True,
    )
    stdout_lines = output.stdout.split('\n')

    config = {}
    for line in stdout_lines:
        k, eq, v = line.partition('=')
        if eq:
            config[k.strip().lower()] = v.strip()

    return config


def load_ext_main(package: pathlib.Path):
    ext_dir = buildmeta.get_extension_dir_path()
    pg_config = get_pg_config()

    install(package, ext_dir, pg_config)


# Option is pulled out like this so that an edb tool can reuse it.
options = typeutils.chain_decorators([
    click.argument('package', type=pathlib.Path),
])


@click.command()
@options
def main(**kwargs):
    load_ext_main(**kwargs)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
