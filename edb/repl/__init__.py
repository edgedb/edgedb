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


import argparse
import atexit
import getpass
import json
import os
import pathlib
import select
import subprocess
import sys

import edgedb

from prompt_toolkit import application as pt_app
from prompt_toolkit import buffer as pt_buffer
from prompt_toolkit import enums as pt_enums
from prompt_toolkit import filters as pt_filters
from prompt_toolkit import history as pt_history
from prompt_toolkit import interface as pt_interface
from prompt_toolkit.key_binding import manager as pt_keymanager
from prompt_toolkit import keys as pt_keys
from prompt_toolkit import shortcuts as pt_shortcuts
from prompt_toolkit import styles as pt_styles
from prompt_toolkit import token as pt_token
from prompt_toolkit.layout import lexers as pt_lexers

from edb.common import devmode
from edb.common import term
from edb.edgeql import pygments as eql_pygments

from edb.server import cluster as edgedb_cluster

from . import context
from . import lexutils
from . import render


class InputBuffer(pt_buffer.Buffer):

    def is_multiline_impl(self):
        if (self.document.cursor_position and
                self.document.text[self.document.cursor_position:].strip()):
            return True

        text = self.document.text.strip()

        if text in Cli.exit_commands:
            return False

        if not text:
            return False

        if text.startswith('\\'):
            return False

        if text.endswith(';'):
            return not lexutils.split_edgeql(text)

        return True

    def __init__(self, *args, **kwargs):
        is_multiline = pt_filters.Condition(self.is_multiline_impl)
        super().__init__(*args, is_multiline=is_multiline, **kwargs)


class Cli:

    style = pt_styles.style_from_dict({
        pt_token.Token.Prompt: '#aaa',
        pt_token.Token.PromptCont: '#888',
        pt_token.Token.Toolbar: 'bg:#222222 #aaaaaa',
        pt_token.Token.Toolbar.On: 'bg:#222222 #fff',

        # Syntax
        pt_token.Token.Keyword: '#e8364f',
        pt_token.Token.Keyword.Reserved: '#e8364f',

        pt_token.Token.Operator: '#e8364f',

        pt_token.Token.String: '#d3c970',
        pt_token.Token.String.Other: '#d3c970',
        pt_token.Token.String.Backtick: '#d3c970',

        pt_token.Token.Number: '#9a79d7',
        pt_token.Token.Number.Integer: '#9a79d7',
        pt_token.Token.Number.Float: '#9a79d7',

        pt_token.Token.Timing.Key: '#555',
        pt_token.Token.Timing.Value: '#888',
    })

    TOOLBAR_SEP = '   '

    exit_commands = {'exit', 'quit', R'\q', ':q'}

    def _command(prefix, title, desc, *, _all_commands={}):
        def wrap(func):
            _all_commands[prefix] = title, desc, func
            return func
        return wrap

    def __init__(self, conn_args):
        self.connection = None

        self.eventloop = pt_shortcuts.create_eventloop()
        self.cli = None
        self.conn_args = conn_args
        self.cur_db = None
        self.context = context.ReplContext()
        self.commands = type(self)._command.__kwdefaults__['_all_commands']

    def get_prompt(self):
        return '{}>'.format(self.cur_db)

    def get_prompt_tokens(self, cli):
        return [
            (pt_token.Token.Prompt, '{} '.format(self.get_prompt())),
        ]

    def get_continuation_tokens(self, cli, width):
        return [
            (pt_token.Token.PromptCont, '.' * len(self.get_prompt())),
        ]

    def get_toolbar_tokens(self, cli):
        toolbar = [
            (pt_token.Token.Toolbar, '[F3] Mode: '),
            (pt_token.Token.Toolbar, self.context.query_mode._name_),
        ]

        if self.context.query_mode is context.QueryMode.Normal:
            toolbar.extend([
                (pt_token.Token.Toolbar, self.TOOLBAR_SEP),

                (pt_token.Token.Toolbar, '[F4] Implicit Properties: '),
                (pt_token.Token.Toolbar,
                    'On' if self.context.show_implicit_fields else 'Off'),
            ])

            toolbar.extend([
                (pt_token.Token.Toolbar, self.TOOLBAR_SEP),

                (pt_token.Token.Toolbar, '[F5] Introspect Types: '),
                (pt_token.Token.Toolbar,
                    'On' if self.context.introspect_types else 'Off'),
            ])

        return toolbar

    def build_cli(self):
        history = pt_history.FileHistory(
            os.path.expanduser('~/.edgedbhistory'))

        key_binding_manager = pt_keymanager.KeyBindingManager(
            enable_system_bindings=True,
            enable_search=True,
            enable_abort_and_exit_bindings=True)

        @key_binding_manager.registry.add_binding(pt_keys.Keys.F3)
        def _mode_toggle(event):
            self.context.toggle_query_mode()

        @key_binding_manager.registry.add_binding(pt_keys.Keys.F4)
        def _implicit_toggle(event):
            self.context.toggle_implicit()

        @key_binding_manager.registry.add_binding(pt_keys.Keys.F5)
        def _introspect_toggle(event):
            self.context.toggle_introspect_types()

            if self.context.introspect_types:
                self.ensure_connection(self.conn_args)
                names = self.connection.fetch(
                    'SELECT schema::ObjectType { name }')
                self.context.typenames = {n.id: n.name for n in names}
            else:
                self.context.typenames = None

        @key_binding_manager.registry.add_binding(pt_keys.Keys.Tab)
        def _tab(event):
            b = cli.current_buffer
            before_cursor = b.document.current_line_before_cursor
            if b.text and (not before_cursor or before_cursor.isspace()):
                b.insert_text('    ')

        layout = pt_shortcuts.create_prompt_layout(
            lexer=pt_lexers.PygmentsLexer(eql_pygments.EdgeQLLexer),
            reserve_space_for_menu=4,
            get_prompt_tokens=self.get_prompt_tokens,
            get_continuation_tokens=self.get_continuation_tokens,
            get_bottom_toolbar_tokens=self.get_toolbar_tokens,
            multiline=True)

        buf = InputBuffer(
            history=history,

            # to make reserve_space_for_menu work:
            complete_while_typing=pt_filters.Always(),

            accept_action=pt_app.AcceptAction.RETURN_DOCUMENT)

        app = pt_app.Application(
            style=self.style,
            layout=layout,
            buffer=buf,
            ignore_case=True,
            key_bindings_registry=key_binding_manager.registry,
            on_exit=pt_app.AbortAction.RAISE_EXCEPTION,
            on_abort=pt_app.AbortAction.RETRY)

        cli = pt_interface.CommandLineInterface(
            application=app,
            eventloop=self.eventloop)
        cli.editing_mode = pt_enums.EditingMode.VI

        return cli

    def connect(self, args):
        try:
            con = edgedb.connect(**args)
        except Exception:
            return None
        else:
            self.cur_db = con.dbname
            return con

    def ensure_connection(self, args):
        if self.connection is None:
            self.connection = self.connect(args)

        if self.connection is None or self.connection.is_closed():
            self.connection = self.connect(args)

        if self.connection is None:
            print('Could not establish connection', file=sys.stderr)
            exit(1)

    @_command('c', R'\c DBNAME', 'connect to database DBNAME')
    def command_connect(self, args):
        new_db = args.strip()
        new_args = {**self.conn_args,
                    'database': new_db}
        self.connection.close()
        self.connection = None
        self.ensure_connection(new_args)
        self.conn_args = new_args
        self.context.on_new_connection()

    @_command('l', R'\l', 'list databases')
    def command_list_dbs(self, args):
        result = self.connection.fetch('SELECT schema::Database.name')

        print('List of databases:')
        for dbn in result:
            print(f'  {dbn}')

    @_command('psql', R'\psql', 'open psql to the current postgres process')
    def command_psql(self, args):
        settings = self.connection.get_settings()
        pgaddr = settings.get('pgaddr')
        if not pgaddr:
            print('\\psql requires EdgeDB to run in DEV mode')
            return

        host = os.path.dirname(pgaddr)
        port = pgaddr.rpartition('.')[2]

        pg_config = edgedb_cluster.get_pg_config_path()
        psql = pg_config.parent / 'psql'

        cmd = [
            str(psql),
            '-h', host,
            '-p', port,
            '-d', self.cur_db,
            '-U', 'postgres'
        ]

        def _psql(cmd):
            proc = subprocess.Popen(cmd)
            while proc.returncode is None:
                try:
                    proc.wait()
                except KeyboardInterrupt:
                    pass

            return proc.returncode

        self.cli.run_in_terminal(
            lambda: _psql(cmd) == 0)

        self.cli.current_buffer.reset()
        print('\r                ')

    def run(self):
        self.cli = self.build_cli()
        self.ensure_connection(self.conn_args)
        use_colors = term.use_colors(sys.stdout.fileno())

        try:
            while True:
                document = self.cli.run(True)
                command = document.text.strip()

                if not command:
                    continue

                if command in self.exit_commands:
                    raise EOFError

                if command == R'\?':
                    for title, desc, _ in self.commands.values():
                        print(f'  {title:<20} {desc}')
                    continue

                elif command.startswith('\\'):
                    prefix, _, args = command.partition(' ')
                    prefix = prefix[1:]
                    if prefix in self.commands:
                        self.ensure_connection(self.conn_args)
                        self.commands[prefix][2](self, args)
                    else:
                        print(f'No command {command} is found.')
                        print(R'Try \? to see the list of supported commands.')
                    continue

                self.ensure_connection(self.conn_args)
                qm = self.context.query_mode
                results = []
                try:
                    if qm is context.QueryMode.GraphQL:
                        command = command.rstrip(';')
                        results.append(
                            self.connection._execute_graphql(command))
                    elif qm is context.QueryMode.Normal:
                        for query in lexutils.split_edgeql(command):
                            results.append(self.connection.fetch(query))
                    else:
                        for query in lexutils.split_edgeql(command):
                            results.append(self.connection.fetch_json(query))

                except KeyboardInterrupt:
                    self.connection.close()
                    self.connection = None
                    print('\r             ')
                    continue
                except Exception as ex:
                    self.connection.close()
                    self.connection = None
                    print('{}: {}'.format(type(ex).__name__, str(ex)))
                    continue

                max_width = term.size(sys.stdout.fileno())[1]
                for result in results:
                    if qm is context.QueryMode.GraphQL:
                        out = render.render_json(
                            self.context,
                            json.dumps(result),
                            max_width=min(max_width, 120),
                            use_colors=use_colors)

                    elif qm is context.QueryMode.JSON:
                        out = render.render_json(
                            self.context,
                            result,
                            max_width=min(max_width, 120),
                            use_colors=use_colors)
                    else:
                        out = render.render_binary(
                            self.context,
                            result,
                            max_width=min(max_width, 120),
                            use_colors=use_colors)

                    print(out)

        except EOFError:
            return


def execute_script(conn_args, data):
    con = edgedb.connect(**conn_args)
    queries = lexutils.split_edgeql(data, script_mode=True)
    ctx = context.ReplContext()
    for query in queries:
        ret = con.fetch(query)
        out = render.render_binary(
            ctx,
            ret,
            max_width=80,
            use_colors=False)
        print(out)


def parse_connect_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default=None)
    parser.add_argument('-P', '--port', type=int, default=None)
    parser.add_argument('-u', '--user', default=None)
    parser.add_argument('-d', '--database', default=None)
    parser.add_argument('-p', '--password', default=False, action='store_true')
    parser.add_argument('-t', '--timeout', default=60)
    parser.add_argument('--start-server', action='store_true', default=False,
                        help='Start EdgeDB server')
    parser.add_argument('--server-dir', type=str, metavar='DIR',
                        default=pathlib.Path.home() / '.edgedb',
                        help='Start EdgeDB server in the data directory DIR')

    args = parser.parse_args()
    if args.password:
        args.password = getpass.getpass()
    else:
        args.password = None

    if args.start_server:
        args.host = '127.0.0.1'
        args.retry_conn = True

    return args


def main():
    args = parse_connect_args()

    if args.start_server:
        cluster = edgedb_cluster.Cluster(args.server_dir)
        if cluster.get_status() == 'not-initialized':
            cluster.init()
        cluster.start(port=args.port, timezone='UTC')
        atexit.register(cluster.stop)

    connect_kwargs = {
        'user': args.user,
        'password': args.password,
        'database': args.database,
        'host': args.host,
        'port': args.port,
        'timeout': args.timeout,
    }

    if select.select([sys.stdin], [], [], 0.0)[0]:
        data = sys.stdin.read()
        execute_script(connect_kwargs, data)
        return

    Cli(connect_kwargs).run()


def main_dev():
    devmode.enable_dev_mode()
    main()
