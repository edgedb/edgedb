#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations

import re
import sys
import textwrap

import click

import edgedb

from edb.cli import cli
from edb.cli import utils
from edb.edgeql.quote import quote_literal as ql, quote_ident as qi


@cli.group()
@utils.connect_command
def configure(ctx):
    utils.connect(ctx)


@configure.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.pass_context
@click.argument('parameter', type=str)
@click.argument('values', nargs=-1, type=click.UNPROCESSED)
def insert(ctx, parameter: str, values):
    if not values:
        raise click.UsageError(
            'missing configuration value properties', ctx=ctx)

    try:
        cfg_obj_name, props = _process_configure_composite_options(
            ctx, parameter, values)
    except NotAnObjectError as e:
        raise click.UsageError(str(e), ctx=ctx) from None

    attrs = []

    for pn, (pval, ptype) in props.items():
        if ptype.__type__.name == 'schema::ObjectType':
            pval = f'(INSERT {pval})'
        else:
            pval = f'<{ptype.name}>{ql(pval)}'

        attrs.append(f'{qi(pn)} := {pval}')

    attrs = ',\n'.join(attrs)

    qry = textwrap.dedent(f'''\
        CONFIGURE SYSTEM INSERT {cfg_obj_name} {{
            {textwrap.indent(attrs, ' ' * 12).strip()}
        }}
    ''')

    try:
        ctx.obj['conn'].execute(qry)
    except edgedb.EdgeDBError as e:
        raise click.ClickException(str(e)) from e
    else:
        click.echo(ctx.obj['conn']._get_last_status())


@configure.command()
@click.pass_context
@click.argument('parameter', type=str)
@click.argument('value', nargs=-1, type=str)
def set(ctx, parameter: str, value):
    cfg_obj_name, cfg_type, cfg_card = _process_configure_scalar(
        ctx, parameter, [])

    if cfg_card == 'ONE':
        if len(value) > 1:
            raise click.ClickException('too many values', ctx=ctx)
        value = value[0]
        val_expr = ql(value)
    else:
        val_expr = f'{{{", ".join(ql(v) for v in value)}}}'

    # Canonicalize the values by casting them.
    vals = ctx.obj['conn'].fetchall(f'''
        SELECT <str><{cfg_type.name}>{val_expr}
    ''')

    args = []
    for val in vals:
        if cfg_type.is_numeric or cfg_type.is_bool:
            args.append(val)
        elif cfg_type.is_str:
            args.append(ql(val))
        else:
            raise click.ClickException(
                f'cannot set {parameter}: it is not a string, numeric or bool'
            )

    args_list = ', '.join(args)
    args_expr = f'{{{args_list}}}'

    qry = textwrap.dedent(f'''
        CONFIGURE SYSTEM SET {cfg_obj_name} := {args_expr}
    ''')

    try:
        ctx.obj['conn'].execute(qry)
    except edgedb.EdgeDBError as e:
        raise click.ClickException(str(e)) from e
    else:
        click.echo(ctx.obj['conn']._get_last_status())


@configure.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.pass_context
@click.argument('parameter', type=str)
@click.argument('values', nargs=-1, type=click.UNPROCESSED)
def reset(ctx, parameter: str, values):
    try:
        cfg_obj_name, props = _process_configure_composite_options(
            ctx, parameter, values)
    except NotAnObjectError:
        is_scalar = True
    else:
        is_scalar = False

    if is_scalar:
        cfg_obj_name, cfg_type, cfg_card = _process_configure_scalar(
            ctx, parameter, values)

        qry = f'CONFIGURE SYSTEM RESET {cfg_obj_name}'
    else:
        attrs = []

        for pn, (pval, ptype) in props.items():
            if ptype.__type__.name == 'schema::ObjectType':
                pval = f'.{pn} IS {pval}'
            else:
                pval = f'.{pn} = <{ptype.name}>{ql(pval)}'

            attrs.append(pval)

        if attrs:
            flt = f"FILTER {' AND '.join(attrs)}"
        else:
            flt = ''

        qry = textwrap.dedent(f'''
            CONFIGURE SYSTEM RESET {cfg_obj_name} {flt}
        ''')

    try:
        ctx.obj['conn'].execute(qry)
    except edgedb.EdgeDBError as e:
        raise click.ClickException(str(e)) from e
    else:
        click.echo(ctx.obj['conn']._get_last_status())


class NotAnObjectError(Exception):
    pass


def _process_configure_composite_options(ctx, parameter, values):
    props = {}

    cfg_objects = ctx.obj['conn'].fetchall('''
        WITH MODULE schema
        SELECT ObjectType {
            name
        } FILTER .name LIKE 'cfg::%'
    ''')

    cfg_objmap = {}

    for obj in cfg_objects:
        _, _, obj_name = obj.name.partition('::')
        cfg_objmap[obj_name] = obj_name
        cfg_objmap[obj_name.lower()] = obj_name

    cfg_obj_name = cfg_objmap.get(parameter)
    if not cfg_obj_name:
        raise NotAnObjectError(
            f'{parameter} is not a valid configuration object')

    cfg_props = ctx.obj['conn'].fetchall('''
        WITH
            MODULE schema,
            Cfg := (SELECT ObjectType FILTER .name = <str>$typename)
        SELECT Cfg.pointers {
            name,
            target: {name, __type__: {name}}
        };
    ''', typename=f'cfg::{cfg_obj_name}')

    cfg_prop_map = {p.name: p.target for p in cfg_props}

    for value in values:
        v = re.match(r'--(\w+)(?: |=)(.*)', value)
        if not v:
            raise click.UsageError(f'unrecognized option: {value}', ctx=ctx)

        propname = v.group(1)
        propval = v.group(2)

        proptype = cfg_prop_map.get(propname)
        if proptype is None:
            raise click.UsageError(
                f'{propname!r} is not a valid {cfg_obj_name} property',
                ctx=ctx)

        if propval in cfg_objmap:
            propval = cfg_objmap[propval]

        props[propname] = (propval, proptype)

    return cfg_obj_name, props


def _process_configure_scalar(ctx, parameter, values):
    if values:
        raise click.UsageError(f'unexpected option: {next(iter(values))}')

    cfg_props = ctx.obj['conn'].fetchall('''
        WITH
            MODULE schema,
            Cfg := (SELECT ObjectType FILTER .name = <str>$typename)
        SELECT Cfg.pointers {
            name,
            cardinality,
            target: {
                name,
                __type__: {name},
                is_numeric := (
                    'std::anyreal' IN
                        Cfg.pointers.target[IS ScalarType].ancestors.name),
                is_bool := (
                    Cfg.pointers.target.name = 'std::bool'
                    OR 'std::bool' IN
                        Cfg.pointers.target[IS ScalarType].ancestors.name),
                is_str := (
                    Cfg.pointers.target.name = 'std::str'
                    OR any({'std::str', 'std::anyenum'} IN
                        Cfg.pointers.target[IS ScalarType].ancestors.name)),
            }
        } FILTER .name = <str>$propname;
    ''', typename=f'cfg::Config', propname=parameter)

    if len(cfg_props) == 0:
        raise click.UsageError(
            f'{parameter!r} is not a valid configuration parameter',
            ctx=ctx)

    return parameter, cfg_props[0].target, cfg_props[0].cardinality


@cli.group()
@utils.connect_command
def create(ctx):
    utils.connect(ctx)


@cli.group()
@utils.connect_command
def alter(ctx):
    utils.connect(ctx)


@cli.group()
@utils.connect_command
def drop(ctx):
    utils.connect(ctx)


def options(options):
    def _decorator(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _decorator


_role_options = [
    click.option('--password/--no-password', default=None),
    click.option('--password-from-stdin', is_flag=True, default=False),
    click.option('--allow-login/--no-allow-login', default=None),
]


def _process_role_options(ctx, password, password_from_stdin, allow_login):
    if password is None and password_from_stdin:
        password = True

    if password is not None:
        if password:
            if password_from_stdin:
                password_value = ql(sys.stdin.readline().strip('\r\n'))
            elif sys.stdin.isatty():
                password_value = ql(click.prompt(
                    'Password',
                    hide_input=True,
                    confirmation_prompt=True,
                    type=str,
                ))
            else:
                raise click.UsageError(
                    'input is not a TTY, please use --password-from-stdin '
                    'to provide the password value'
                )
        else:
            password_value = '{}'
    else:
        password_value = None

    if allow_login is not None:
        allow_login_value = 'true' if allow_login else 'false'
    else:
        allow_login_value = None

    alters = []
    if password_value is not None:
        alters.append(f'SET password := {password_value}')
    if allow_login_value is not None:
        alters.append(f'SET allow_login := {allow_login_value}')

    if not alters:
        raise click.UsageError(
            'please specify an attribute to alter', ctx=ctx,
        )

    return alters


@create.command(name='role')
@click.argument('role-name', type=str)
@options(_role_options)
@click.pass_context
def create_role(ctx, role_name, **kwargs):
    attrs = ";\n".join(_process_role_options(ctx, **kwargs))

    qry = f'''
        CREATE ROLE {qi(role_name)} {{
            {attrs}
        }}
    '''

    try:
        ctx.obj['conn'].execute(qry)
    except edgedb.EdgeDBError as e:
        raise click.ClickException(str(e)) from e


@alter.command(name='role')
@click.argument('role-name', type=str)
@options(_role_options)
@click.pass_context
def alter_role(ctx, role_name, **kwargs):

    attrs = ";\n".join(_process_role_options(ctx, **kwargs))

    qry = f'''
        ALTER ROLE {qi(role_name)} {{
            {attrs}
        }}
    '''

    try:
        ctx.obj['conn'].execute(qry)
    except edgedb.EdgeDBError as e:
        raise click.ClickException(str(e)) from e


@drop.command(name='role')
@click.argument('role-name', type=str)
@click.pass_context
def drop_role(ctx, role_name, **kwargs):
    qry = f'''
        DROP ROLE {qi(role_name)};
    '''

    try:
        ctx.obj['conn'].execute(qry)
    except edgedb.EdgeDBError as e:
        raise click.ClickException(str(e)) from e
