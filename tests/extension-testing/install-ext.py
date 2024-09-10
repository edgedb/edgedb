#!/usr/bin/env python3

# TODO: Rewrite this in rust as part of the CLI.

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tomllib
import zipfile

parser = argparse.ArgumentParser(description='Install an extension package')
parser.add_argument('--dev', '-d', action='store_true',
                    help='Install into the current dev environment')
parser.add_argument('--installed', '-i', metavar='PATH',
                    type=pathlib.Path,
                    help='Install into an installed EdgeDB')
parser.add_argument('package', type=pathlib.Path)


def install(
    pkg: pathlib.Path,
    ext_dir: pathlib.Path,
    pg_dir: pathlib.Path,
) -> None:
    print("Installing", ext_dir / pkg.name)
    shutil.copyfile(pkg, ext_dir / pkg.name)

    with zipfile.ZipFile(pkg) as z:
        with z.open('MANIFEST.toml') as m:
            manifest = tomllib.load(m)

        if 'postgres_files' in manifest:
            dir = manifest['postgres_files']

            for entry in z.infolist():
                # Dirs should already exist
                if entry.is_dir():
                    continue
                fpath = pathlib.Path(entry.filename)
                if fpath.parts[0] != dir:
                    continue
                fpath = fpath.relative_to(dir)

                print("Installing", pg_dir / fpath)
                with z.open(entry) as f:
                    data = f.read()
                with open(pg_dir / fpath, "wb") as f:
                    f.write(data)


def get_configs(py: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    # TODO: Call get_extension_dir_path instead, but we don't have that yet!!
    command = [
        str(py),
        '-c',
        'import json; '
        'import edb.buildmeta; '
        'print(json.dumps([str(edb.buildmeta.get_pg_config_path()), '
        'str(edb.buildmeta.get_shared_data_dir_path())]))'
    ]

    output = subprocess.run(
        command,
        capture_output=True,
        cwd="/",
        check=True,
    )
    pg_config_s, data_dir_s = json.loads(output.stdout)
    pg_config = pathlib.Path(pg_config_s)
    ext_dir = pathlib.Path(data_dir_s) / "extensions"

    return ext_dir, pg_config


def main(argv):
    args = parser.parse_args()

    if not args.dev and not args.installed:
        print("Must specify an installation target.")
        return 2
    if args.dev and args.installed:
        print("--dev and --installed are mutually exclusive")
        return 2

    if args.dev:
        os.environ['__EDGEDB_DEVMODE'] = '1'
        ext_dir, pg_config = get_configs(pathlib.Path(sys.executable))
    elif args.installed:
        py = args.installed / "bin/python3"
        ext_dir, pg_config = get_configs(py)

    pg_dir = pg_config.parent.parent

    install(args.package, ext_dir, pg_dir)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
