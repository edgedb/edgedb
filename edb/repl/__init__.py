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
import functools
import getpass
import os
import pathlib
import select
import subprocess
import sys

import edgedb
import immutables

from prompt_toolkit import application as pt_app
from prompt_toolkit import completion as pt_complete
from prompt_toolkit import enums as pt_enums
from prompt_toolkit import filters as pt_filters
from prompt_toolkit import history as pt_history
from prompt_toolkit import key_binding as pt_key_binding
from prompt_toolkit import shortcuts as pt_shortcuts
from prompt_toolkit import styles as pt_styles
from prompt_toolkit import lexers as pt_lexers

from edb.common import devmode
from edb.common import term
from edb.edgeql import pygments as eql_pygments

from edb.server import cluster as edgedb_cluster

from . import context
from . import lexutils
from . import render


STATUSES_WITH_OUTPUT = frozenset({
    'SELECT', 'INSERT', 'DELETE', 'UPDATE',
    'GET MIGRATION',
})


@functools.lru_cache(100)
def is_multiline_text(text):
    text = text.strip()

    if text in Cli.exit_commands:
        return False

    if not text:
        return False

    if text.startswith('\\'):
        return False

    if text.endswith(';'):
        _, incomplete = lexutils.split_edgeql(text, script_mode=False)
        return incomplete is not None

    return True


@pt_filters.Condition
def is_multiline():
    doc = pt_app.get_app().layout.get_buffer_by_name(
        pt_enums.DEFAULT_BUFFER).document

    if (doc.cursor_position and doc.text[doc.cursor_position:].strip()):
        return True

    return is_multiline_text(doc.text)


class Cli:

    style = pt_styles.Style.from_dict({
        'prompt': '#aaa',
        'continuation': '#888',

        'bottom-toolbar': 'bg:#222222 #aaaaaa noreverse',
        'bottom-toolbar.on': 'bg:#222222 #ffffff',

        # See prompt_tookit/styles/defaults.py for the reference.
        'pygments.comment': '#555',
        'pygments.keyword': '#e8364f',
        'pygments.keyword.constant': 'green',
        'pygments.operator': '#e8364f',
        'pygments.literal.string': '#d3c970',
        'pygments.literal.number': '#9a79d7',
        'pygments.key': '#555',
        'pygments.value': '#888',
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

        self.prompt = None
        self.conn_args = immutables.Map(conn_args)
        self.context = context.ReplContext()
        self.commands = type(self)._command.__kwdefaults__['_all_commands']

    def get_prompt(self):
        return '{}>'.format(self.connection.dbname)

    def get_prompt_tokens(self):
        return [
            ('class:prompt', '{} '.format(self.get_prompt())),
        ]

    def get_continuation_tokens(self, width, line_number, wrap_count):
        return [
            ('class:continuation', '.' * (width - 1) + ' '),
        ]

    def get_toolbar_tokens(self):
        toolbar = [
            ('class:bottom-toolbar', '[F3] Mode: '),
            ('class:bottom-toolbar', self.context.query_mode._name_),
        ]

        if self.context.query_mode is context.QueryMode.Normal:
            toolbar.extend([
                ('class:bottom-toolbar', self.TOOLBAR_SEP),

                ('class:bottom-toolbar', '[F4] Implicit Properties: '),
                ('class:bottom-toolbar',
                    'On' if self.context.show_implicit_fields else 'Off'),
            ])

            toolbar.extend([
                ('class:bottom-toolbar', self.TOOLBAR_SEP),

                ('class:bottom-toolbar', '[F5] Introspect Types: '),
                ('class:bottom-toolbar',
                    'On' if self.context.introspect_types else 'Off'),
            ])

        return toolbar

    def introspect_db(self, con):
        names = con.fetch('SELECT schema::ObjectType { name }')
        self.context.typenames = {n.id: n.name for n in names}

    def build_propmpt(self):
        history = pt_history.FileHistory(
            os.path.expanduser('~/.edgedbhistory'))

        bindings = pt_key_binding.KeyBindings()
        handle = bindings.add

        @handle('f3')
        def _mode_toggle(event):
            self.context.toggle_query_mode()

        @handle('f4')
        def _implicit_toggle(event):
            self.context.toggle_implicit()

        @handle('f5')
        def _introspect_toggle(event):
            self.context.toggle_introspect_types()

            if self.context.introspect_types:
                self.ensure_connection()
                self.introspect_db(self.connection)
            else:
                self.context.typenames = None

        @handle('tab')
        def _tab(event):
            b = prompt.app.current_buffer
            before_cursor = b.document.current_line_before_cursor
            if b.text and (not before_cursor or before_cursor.isspace()):
                b.insert_text('    ')

        prompt = pt_shortcuts.PromptSession(
            lexer=pt_lexers.PygmentsLexer(eql_pygments.EdgeQLLexer),
            include_default_pygments_style=False,

            completer=pt_complete.DummyCompleter(),
            reserve_space_for_menu=6,

            message=self.get_prompt_tokens,
            prompt_continuation=self.get_continuation_tokens,
            bottom_toolbar=self.get_toolbar_tokens,
            multiline=is_multiline,
            history=history,
            complete_while_typing=pt_filters.Always(),
            key_bindings=bindings,
            style=self.style,
            editing_mode=pt_enums.EditingMode.VI,
            search_ignore_case=True,
        )

        return prompt

    def ensure_connection(self):
        try:
            if self.connection is None:
                self.connection = edgedb.connect(**self.conn_args)
            elif self.connection.is_closed():
                self.connection = edgedb.connect(**self.conn_args)
        except Exception:
            self.connection = None

        if self.connection is None:
            dbname = self.conn_args.get("database")
            if not dbname:
                dbname = 'EdgeDB'
            print(f'Could not establish connection to {dbname}')
            exit(1)

    @_command('c', R'\c DBNAME', 'connect to database DBNAME')
    def command_connect(self, args):
        new_db = args.strip()
        new_args = self.conn_args.set('database', new_db)
        try:
            new_connection = edgedb.connect(**new_args)
            if self.context.introspect_types:
                self.introspect_db(new_connection)
        except Exception:
            print(f'Could not establish connection to {new_db!r}', flush=True)
            return

        self.connection.close()
        self.connection = new_connection
        self.conn_args = new_args

    @_command('l', R'\l', 'list databases')
    def command_list_dbs(self, args):
        result, _ = self.fetch(
            '''
                SELECT name := sys::Database.name
                ORDER BY name ASC
            ''',
            json=False
        )

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
            '-d', self.connection.dbname,
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

        pt_app.run_in_terminal(
            lambda: _psql(cmd) == 0)

        self.prompt.app.current_buffer.reset()
        print('\r                ')

    def fetch(self, query: str, *, json: bool, retry: bool=True):
        self.ensure_connection()

        if json:
            meth = self.connection.fetch_json
        else:
            meth = self.connection.fetch

        try:
            result = meth(query)
        except (ConnectionAbortedError, BrokenPipeError):
            # The connection is closed; try again with a new one.
            if retry:
                self.connection.close()
                self.connection = None

                render.render_error(
                    self.context,
                    '== connection is closed; attempting to open a new one ==')

                return self.fetch(query, json=json, retry=False)
            else:
                raise

        return result, self.connection._get_last_status()

    def run(self):
        self.prompt = self.build_propmpt()
        self.ensure_connection()
        self.context.use_colors = term.use_colors(sys.stdout.fileno())

        try:
            while True:
                self.ensure_connection()

                try:
                    text = self.prompt.prompt()
                except KeyboardInterrupt:
                    continue

                command = text.strip()
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
                        self.ensure_connection()
                        self.commands[prefix][2](self, args)
                    else:
                        print(f'No command {command} is found.')
                        print(R'Try \? to see the list of supported commands.')
                    continue

                qm = self.context.query_mode
                results = []
                try:
                    if qm is context.QueryMode.Normal:
                        for query in lexutils.split_edgeql(command)[0]:
                            results.append(self.fetch(query, json=False))
                    else:
                        for query in lexutils.split_edgeql(command)[0]:
                            results.append(self.fetch(query, json=True))

                except KeyboardInterrupt:
                    self.connection.close()
                    self.connection = None
                    print('\r', end='')
                    render.render_error(
                        self.context,
                        '== aborting query and closing the connection ==')
                    continue
                except Exception as ex:
                    render.render_error(
                        self.context, f'{type(ex).__name__}: {ex}')
                    continue

                max_width = self.prompt.output.get_size().columns
                try:
                    for result, status in results:
                        if status in STATUSES_WITH_OUTPUT:
                            if qm is context.QueryMode.JSON:
                                render.render_json(
                                    self.context,
                                    result,
                                    max_width=min(max_width, 120))
                            else:
                                render.render_binary(
                                    self.context,
                                    result,
                                    max_width=min(max_width, 120))
                        else:
                            render.render_status(self.context, status)
                except KeyboardInterrupt:
                    print('\r', end='')
                    render.render_error(
                        self.context,
                        '== aborting rendering of the result ==')
                    continue

        except EOFError:
            return


def execute_script(conn_args, data):
    con = edgedb.connect(**conn_args)
    queries = lexutils.split_edgeql(data)[0]
    ctx = context.ReplContext()
    for query in queries:
        ret = con.fetch(query)
        render.render_binary(
            ctx,
            ret,
            max_width=80)


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
