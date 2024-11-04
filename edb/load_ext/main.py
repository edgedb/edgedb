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
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
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
    manifest_target: pathlib.Path | None,
) -> None:

    to_install = []
    with zipfile.ZipFile(pkg) as z:
        base = get_dir(z)

        # Compute what files to install
        for entry in z.infolist():
            fpath = pathlib.Path(entry.filename)

            if entry.is_dir():
                continue
            if fpath.parts[0] != str(base):
                continue
            # If the path is too short or isn't one of the
            # directories we know about, skip it.
            if (
                len(fpath.parts) < 3
                or not (config_field := CONFIG_PATHS.get(fpath.parts[1]))
                or fpath.parts[2] != 'postgresql'
            ):
                # print("Skipping", fpath)
                continue

            fpath = fpath.relative_to(
                pathlib.Path(fpath.parts[0])
                / fpath.parts[1]
                / 'postgresql'
            )
            to_install.append((entry.filename, config_field, fpath))

        # Write a manifest out of all the files installed into the
        # postgres installation.
        if manifest_target:
            manifest_contents = [
                {'postgres_dir': config_field, 'path': str(fpath)}
                for _, config_field, fpath in to_install
            ]
            with open(manifest_target, "w") as f:
                json.dump(manifest_contents, f)

        # Install them
        for zip_name, config_field, fpath in to_install:
            config_dir = pg_config[config_field]
            target_file = config_dir / fpath

            os.makedirs(target_file.parent, exist_ok=True)
            with z.open(zip_name) as src:
                with open(target_file, "wb") as dst:
                    print("Installing", target_file)
                    shutil.copyfileobj(src, dst)


def uninstall_pg_extension(
    pg_manifest: list[dict[str, str]],
    pg_config: dict[str, str],
) -> None:
    for entry in pg_manifest:
        config_field = entry['postgres_dir']
        fpath = entry['path']

        full_path = pathlib.Path(pg_config[config_field]) / fpath
        print("Removing", full_path)
        try:
            os.remove(full_path)
        except FileNotFoundError:
            print("Could not remove missing", full_path)


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


def get_dir(z: zipfile.ZipFile) -> pathlib.Path:
    files = z.infolist()
    if not (files and files[0].is_dir()):
        print('ERROR: Extension package must contain one top-level dir')
        sys.exit(1)
    dirname = pathlib.Path(files[0].filename)

    return dirname


def install_edgedb_extension(
    pkg: pathlib.Path,
    ext_dir: pathlib.Path,
) -> pathlib.Path:
    with tempfile.TemporaryDirectory() as tdir, \
         zipfile.ZipFile(pkg) as z:

        dirname = get_dir(z)

        target = ext_dir / dirname
        if target.exists():
            print(
                f'ERROR: Extension {dirname} is already installed at {target}'
            )
            sys.exit(1)

        print("Installing", target)

        ttarget = pathlib.Path(tdir) / dirname
        os.mkdir(ttarget)

        with z.open(str(dirname / 'MANIFEST.toml')) as m:
            manifest = tomllib.load(m)

        files = ['MANIFEST.toml'] + manifest['files']

        for f in files:
            target_file = target / f
            ttarget_file = ttarget / f

            with z.open(str(dirname / f)) as src:
                with open(ttarget_file, "wb") as dst:
                    print("Installing", target_file)
                    shutil.copyfileobj(src, dst)

        os.makedirs(ext_dir, exist_ok=True)
        # If there was a race and the file was created between the
        # earlier check and now, we'll produce a worse error
        # message. Oh well.
        shutil.move(ttarget, ext_dir)

    return target


def load_ext_install(
    package: pathlib.Path,
    skip_edgedb: bool,
    skip_gel: bool,
    skip_postgres: bool,
    with_pg_config: pathlib.Path | None,
) -> None:
    target_dir = None
    if not skip_edgedb and not skip_gel:
        from edb import buildmeta

        ext_dir = buildmeta.get_extension_dir_path()
        target_dir = install_edgedb_extension(package, ext_dir)

    if not skip_postgres:
        if with_pg_config is None:
            from edb import buildmeta
            with_pg_config = buildmeta.get_pg_config_path()

        pg_config = get_pg_config(with_pg_config)
        pg_manifest = target_dir / "PG_MANIFEST.json" if target_dir else None
        install_pg_extension(package, pg_config, pg_manifest)


def load_ext_uninstall(
    package: pathlib.Path,
    skip_edgedb: bool,
    skip_gel: bool,
    skip_postgres: bool,
    with_pg_config: pathlib.Path | None,
) -> None:
    from edb import buildmeta
    target_dir = None
    if len(package.parts) != 1:
        print(
            f'ERROR: {package} is not a valid extension name'
        )
        sys.exit(1)

    ext_dir = buildmeta.get_extension_dir_path()
    target_dir = ext_dir / package

    if not target_dir.exists():
        print(
            f'ERROR: Extension {package} is not currently '
            f'installed at {target_dir}'
        )
        sys.exit(1)

    if not skip_postgres:
        try:
            with open(target_dir / "PG_MANIFEST.json") as f:
                pg_manifest = json.load(f)
        except FileNotFoundError:
            pg_manifest = []

        if with_pg_config is None:
            with_pg_config = buildmeta.get_pg_config_path()

        pg_config = get_pg_config(with_pg_config)
        uninstall_pg_extension(pg_manifest, pg_config)

    if not skip_edgedb and not skip_gel:
        print("Removing", target_dir)
        shutil.rmtree(target_dir)


def load_ext_list_packages() -> None:
    from edb import buildmeta

    ext_dir = buildmeta.get_extension_dir_path()

    exts = []
    try:
        with os.scandir(ext_dir) as it:
            for entry in it:
                entry_path = pathlib.Path(entry)
                manifest_path = entry_path / 'MANIFEST.toml'
                if (
                    entry.is_dir()
                    and manifest_path.exists()
                ):
                    with open(manifest_path, 'rb') as m:
                        manifest = tomllib.load(m)

                    info = dict(
                        key=entry_path.name,
                        extension_name=manifest['name'],
                        extension_version=manifest['version'],
                        path=str(entry_path.absolute()),
                    )

                    exts.append(info)
    except FileNotFoundError:
        pass

    print(json.dumps(exts, indent=4))


def load_ext_main(
    *,
    package: pathlib.Path | None,
    uninstall: pathlib.Path | None,
    list_packages: bool,
    **kwargs,
) -> None:
    if uninstall:
        load_ext_uninstall(uninstall, **kwargs)
    elif package:
        load_ext_install(package, **kwargs)
    elif list_packages:
        load_ext_list_packages()
    else:
        raise AssertionError('No command specified?')


parser = argparse.ArgumentParser(description='Install an extension package')
parser.add_argument(
    '--skip-gel', action='store_true',
    help="Skip installing the extension package into the Gel "
          "installation",
)
parser.add_argument(
    '--skip-edgedb', action='store_true', help=argparse.SUPPRESS,
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
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    '--list-packages', action='store_true',
    help="List the extension packages that are installed (in JSON)"
)
group.add_argument(
    '--uninstall', metavar='NAME',
    type=pathlib.Path,
    help="Uninstall a package (by package directory name) instead of "
         "installing it"
)
group.add_argument('package', nargs='?', type=pathlib.Path)


def main(argv: tuple[str, ...] | None = None):
    argv = argv if argv is not None else tuple(sys.argv[1:])
    args = parser.parse_args(argv)
    load_ext_main(**vars(args))


if __name__ == '__main__':
    main()
