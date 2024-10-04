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

"""Command to load an extension into an edgedb installation.

It is a command distributed with the server, but it is designed so
that it has no dependencies and does not import any server code
if it is *only* installing the postgres part of an extension with
a specified pg_config, so it *can* be pulled out and used standalone.
(It requires Python 3.11 for tomllib.)
"""

from __future__ import annotations


import argparse
import os
import pathlib
import shutil
import subprocess
import sys
import tomllib
import zipfile

# Directories that we map to config values in pg_config.
CONFIG_PATHS = {
    'share': 'sharedir',
    'lib': 'pkglibdir',
    'include': 'pkgincludedir-server',
}


def install_pg_extension(
    pkg: pathlib.Path,
    pg_config: dict[str, str],
) -> None:
    with zipfile.ZipFile(pkg) as z:
        with z.open('MANIFEST.toml') as m:
            manifest = tomllib.load(m)

        if 'postgres_files' in manifest:
            dir = manifest['postgres_files']
            pdir = pathlib.Path(dir)

            for entry in z.infolist():
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

                if entry.is_dir():
                    print("Creating directory", f'{target_file}/')
                    os.makedirs(target_file, exist_ok=True)
                else:
                    with z.open(entry) as src:
                        with open(target_file, "wb") as dst:
                            print("Installing", target_file)
                            shutil.copyfileobj(src, dst)


def get_pg_config(pg_config_path: pathlib.Path) -> dict[str, str]:
    output = subprocess.run(
        pg_config_path,
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


def load_ext_main(
    package: pathlib.Path,
    skip_edgedb: bool,
    skip_postgres: bool,
    with_pg_config: pathlib.Path | None,
) -> None:
    if not skip_edgedb:
        from edb import buildmeta

        ext_dir = buildmeta.get_extension_dir_path()
        os.makedirs(ext_dir, exist_ok=True)
        print("Installing", ext_dir / package.name)
        shutil.copyfile(package, ext_dir / package.name)

    if not skip_postgres:
        if with_pg_config is None:
            from edb import buildmeta
            with_pg_config = buildmeta.get_pg_config_path()

        pg_config = get_pg_config(with_pg_config)
        install_pg_extension(package, pg_config)


parser = argparse.ArgumentParser(description='Install an extension package')
parser.add_argument(
    '--skip-edgedb', action='store_true',
    help="Skip installing the extension package into the EdgeDB "
          "installation",
)
parser.add_argument(
    '--skip-postgres', action='store_true',
    help="Skip installing the extension package into the "
         "Postgres installation",
)
parser.add_argument(
    '--with-pg-config', metavar='PATH',
    help="Use the specified pg_config binary to find the Postgres "
         "to install into (instead of using the bundled one)"
)
parser.add_argument('package', type=pathlib.Path)


def main(argv: tuple[str, ...] | None = None):
    argv = argv if argv is not None else tuple(sys.argv[1:])
    args = parser.parse_args(argv)
    load_ext_main(**vars(args))


if __name__ == '__main__':
    main()
