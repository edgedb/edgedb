#!/usr/bin/env python3

import argparse
import pathlib
import sys

import jinja2
import yaml


env = jinja2.Environment(
    variable_start_string='<<',
    variable_end_string='>>',
    block_start_string='<%',
    block_end_string='%>',
    loader=jinja2.FileSystemLoader(pathlib.Path(__file__).parent),
)


def die(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('template')
    parser.add_argument('datafile')

    args = parser.parse_args()

    tplfile = f'{args.template}.tpl.yml'
    path = pathlib.Path(__file__).parent / tplfile

    if not path.exists():
        die(f'template does not exist: {tplfile}')

    with open(path) as f:
        tpl = env.from_string(f.read())

    datapath = pathlib.Path(__file__).parent / args.datafile

    if datapath.exists():
        with open(datapath) as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
    else:
        data = {}

    output = tpl.render(**data)

    target = (
        pathlib.Path(__file__).parent.parent
        / 'workflows'
        / f'{args.template}.yml'
    )
    with open(target, 'w') as f:
        print(output, file=f)


if __name__ == '__main__':
    main()
