##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import argparse
import asyncio
import getpass
import os
import select
import sys

from prompt_toolkit import application as pt_app
from prompt_toolkit import buffer as pt_buffer
from prompt_toolkit import filters as pt_filters
from prompt_toolkit import history as pt_history
from prompt_toolkit import interface as pt_interface
from prompt_toolkit.key_binding import manager as pt_keymanager
from prompt_toolkit import keys as pt_keys
from prompt_toolkit import shortcuts as pt_shortcuts
from prompt_toolkit import styles as pt_styles
from prompt_toolkit import token as pt_token
from prompt_toolkit import enums as pt_enums
from prompt_toolkit.layout import lexers as pt_lexers

from edgedb import client
from edgedb.lang.common import lexer as core_lexer
from edgedb.lang.common import markup
from edgedb.lang.edgeql.parser.grammar import lexer as edgeql_lexer
from edgedb.lang.edgeql import pygments as eql_pygments


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
            lexer = edgeql_lexer.EdgeQLLexer()
            lexer.setinputstr(text)
            try:
                toks = list(lexer.lex())
            except core_lexer.UnknownTokenError as ex:
                return True

            if toks[-1].type == ';':
                return False

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
    })

    exit_commands = {'exit', 'quit', '\q', ':q'}

    def __init__(self, conn_args):
        self.connection = None

        self.eventloop = pt_shortcuts.create_eventloop()
        self.aioloop = None
        self.cli = None
        self.conn_args = conn_args
        self.cur_db = None
        self.graphql = False
        self.optimize = False

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
        return [
            (pt_token.Token.Toolbar, '[F3] GraphQL: '),
            (pt_token.Token.Toolbar.On, 'On') if self.graphql else
            (pt_token.Token.Toolbar, 'Off'),
            (pt_token.Token.Toolbar, '  '),
            (pt_token.Token.Toolbar, '[F4] Optimize: '),
            (pt_token.Token.Toolbar.On, 'On') if self.optimize else
            (pt_token.Token.Toolbar, 'Off'),
        ]

    def build_cli(self):
        history = pt_history.FileHistory(
            os.path.expanduser('~/.edgedbhistory'))

        key_binding_manager = pt_keymanager.KeyBindingManager(
            enable_system_bindings=True,
            enable_search=True,
            enable_abort_and_exit_bindings=True)

        @key_binding_manager.registry.add_binding(pt_keys.Keys.F3)
        def _(event):
            self.graphql = not self.graphql

        @key_binding_manager.registry.add_binding(pt_keys.Keys.F4)
        def _(event):
            self.optimize = not self.optimize

        @key_binding_manager.registry.add_binding(pt_keys.Keys.Tab)
        def _(event):
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

    def run_coroutine(self, coro):
        if self.aioloop is None:
            self.aioloop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.aioloop)

        try:
            return self.aioloop.run_until_complete(coro)
        except KeyboardInterrupt:
            self.aioloop.close()
            self.aioloop = None
            asyncio.set_event_loop(None)
            raise

    async def connect(self, args):
        try:
            con = await client.connect(**args)
        except:
            return None
        else:
            self.cur_db = con._dbname
            return con

    def ensure_connection(self, args):
        async def dummy():
            pass
        self.run_coroutine(dummy())

        if self.connection is None:
            self.connection = self.run_coroutine(self.connect(args))

        if self.connection is not None and \
                self.connection._transport.is_closing():
            self.connection = self.run_coroutine(self.connect(args))

        if self.connection is None:
            print('Could not establish connection', file=sys.stderr)
            exit(1)

    def run(self):
        self.cli = self.build_cli()
        self.ensure_connection(self.conn_args)

        try:
            while True:
                document = self.cli.run(True)
                command = document.text.strip()

                if not command:
                    continue

                if command in self.exit_commands:
                    raise EOFError

                if command.startswith('\\'):
                    if command.startswith('\\c '):
                        new_db = command.split(' ', 1)
                        new_args = {**self.conn_args,
                                    'database': new_db[1].strip()}
                        self.connection._transport.abort()
                        self.connection = None
                        self.ensure_connection(new_args)
                        self.conn_args = new_args
                        continue

                    print('Unknown command')
                    continue

                self.ensure_connection(self.conn_args)
                try:
                    if self.graphql:
                        command = command.rstrip(';')
                    self.connection.set_optimize(self.optimize)
                    result = self.run_coroutine(
                        self.connection.execute(
                            command, graphql=self.graphql))
                except KeyboardInterrupt:
                    continue
                except Exception as ex:
                    print('{}: {}'.format(type(ex).__name__, str(ex)))
                    continue

                markup.dump(result)

        except EOFError:
            return


async def execute(conn_args, data):
    con = await client.connect(**conn_args)
    print(await con.execute(data))


def parse_connect_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', default=None)
    parser.add_argument('-P', '--port', type=int, default=None)
    parser.add_argument('-u', '--user', default=None)
    parser.add_argument('-d', '--database', default=None)
    parser.add_argument('-p', '--password', default=False, action='store_true')

    args = parser.parse_args()
    if args.password:
        args.password = getpass.getpass()
    else:
        args.password = None

    return {
        'user': args.user,
        'password': args.password,
        'database': args.database,
        'host': args.host,
        'port': args.port,
    }


def main():
    args = parse_connect_args()

    if select.select([sys.stdin], [], [], 0.0)[0]:
        data = sys.stdin.read()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(execute(args, data))
        finally:
            loop.close()
            asyncio.set_event_loop(None)

        return

    Cli(args).run()
