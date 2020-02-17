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


from __future__ import annotations
from typing import *

import json
import functools
import os
import subprocess
import sys
import uuid

import edgedb

from prompt_toolkit import application as pt_app
from prompt_toolkit import completion as pt_complete
from prompt_toolkit import document as pt_document
from prompt_toolkit import enums as pt_enums
from prompt_toolkit import filters as pt_filters
from prompt_toolkit import formatted_text as pt_formatted_text
from prompt_toolkit import history as pt_history
from prompt_toolkit import key_binding as pt_key_binding
from prompt_toolkit import shortcuts as pt_shortcuts
from prompt_toolkit import styles as pt_styles
from prompt_toolkit import lexers as pt_lexers

from edb.errors import base as base_errors

from edb.common import term
from edb.edgeql import pygments as eql_pygments
from edb.edgeql import quote as eql_quote
from edb.schema import schema

from edb.server import buildmeta
from edb.cli import utils as cli_utils

from . import cmd
from . import context
from . import render
from . import table
from . import utils


STD_RE = '|'.join(schema.STD_MODULES)

STATUSES_WITH_OUTPUT = frozenset({
    'SELECT', 'INSERT', 'DELETE', 'UPDATE',
    'GET MIGRATION', 'DESCRIBE',
})

# Maximum width for rendering output and tables.
MAX_WIDTH = 120


@functools.lru_cache(100)
def is_multiline_text(text: str) -> bool:
    text = text.strip()

    if text in Cli.exit_commands:
        return False

    if not text:
        return False

    if text.startswith('\\'):
        return False

    if text.endswith(';'):
        _, incomplete = utils.split_edgeql(text, script_mode=False)
        return incomplete is not None

    return True


@pt_filters.Condition  # type: ignore
def is_multiline() -> bool:
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
        'pygments.name.builtin': '#A6E22E',
        'pygments.punctuation.navigation': '#e8364f',
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

    _connection: Optional[edgedb.BlockingIOConnection]
    cargs: cli_utils.ConnectionArgs
    database: Optional[str]

    _prompt: Optional[pt_shortcuts.PromptSession]

    def __init__(self, cargs: cli_utils.ConnectionArgs) -> None:
        self._connection = None

        self._prompt = None
        self.cargs = cargs
        self.database = None
        self.context = context.ReplContext()

        self._parser = cmd.Parser(
            commands=[
                # Introspection

                cmd.Command(
                    trigger='d',
                    desc='describe schema object',
                    group='Introspection',
                    arg_name='NAME',
                    arg_optional=False,
                    flags={'+': 'verbose'},
                    callback=self._command_describe_object,
                ),

                cmd.Command(
                    trigger='l',
                    desc='list databases',
                    group='Introspection',
                    callback=self._command_list_dbs,
                ),

                cmd.Command(
                    trigger='lr',
                    desc='list roles',
                    group='Introspection',
                    arg_name='PATTERN',
                    arg_optional=True,
                    flags={'I': 'case_sensitive'},
                    callback=self._command_list_roles,
                ),

                cmd.Command(
                    trigger='lm',
                    desc='list modules',
                    group='Introspection',
                    arg_name='PATTERN',
                    arg_optional=True,
                    flags={'I': 'case_sensitive'},
                    callback=self._command_list_modules,
                ),

                cmd.Command(
                    trigger='lT',
                    desc='list scalar types',
                    group='Introspection',
                    arg_name='PATTERN',
                    arg_optional=True,
                    flags={
                        'I': 'case_sensitive',
                        'S': 'system',
                    },
                    callback=self._command_list_scalar_types,
                ),

                cmd.Command(
                    trigger='lt',
                    desc='list object types',
                    group='Introspection',
                    arg_name='PATTERN',
                    arg_optional=True,
                    flags={
                        'I': 'case_sensitive',
                        'S': 'system',
                    },
                    callback=self._command_list_object_types,
                ),

                cmd.Command(
                    trigger='la',
                    desc='list expression aliases',
                    group='Introspection',
                    arg_name='PATTERN',
                    arg_optional=True,
                    flags={
                        'I': 'case_sensitive',
                        'S': 'system',
                        '+': 'verbose',
                    },
                    callback=self._command_list_expression_aliases,
                ),

                cmd.Command(
                    trigger='lc',
                    desc='list casts',
                    group='Introspection',
                    arg_name='PATTERN',
                    arg_optional=True,
                    flags={
                        'I': 'case_sensitive',
                    },
                    callback=self._command_list_casts,
                ),

                cmd.Command(
                    trigger='li',
                    desc='list indexes',
                    group='Introspection',
                    arg_name='PATTERN',
                    arg_optional=True,
                    flags={
                        'I': 'case_sensitive',
                        'S': 'system',
                        '+': 'verbose',
                    },
                    callback=self._command_list_indexes,
                ),

                # REPL setting

                cmd.Command(
                    trigger='limit',
                    arg_name='LIMIT',
                    desc=(
                        'Set implicit LIMIT. '
                        'Defaults to 100, specify 0 to disable.'
                    ),
                    group='Variables',
                    callback=self._command_set_limit,
                ),

                # Help

                cmd.Command(
                    trigger='?',
                    desc='Show help on backslash commands',
                    group='Help',
                    callback=self._command_help,
                ),

                # Connection

                cmd.Command(
                    trigger='c',
                    desc='Connect to database DBNAME',
                    group='Connection',
                    callback=self._command_connect,
                    arg_name='DBNAME',
                ),

                # Development

                cmd.Command(
                    trigger='E',
                    desc='show most recent error message at maximum verbosity',
                    group='Development',
                    callback=self._command_errverbose,
                ),

                cmd.Command(
                    trigger='psql',
                    desc='open psql to the current postgres process',
                    group='Development',
                    callback=self._command_psql,
                    devonly=True,
                ),

                cmd.Command(
                    trigger='pgaddr',
                    desc='show the network addr of the postgres server',
                    group='Development',
                    callback=self._command_pgaddr,
                    devonly=True,
                ),
            ]
        )

    def _new_connection(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> edgedb.BlockingIOConnection:
        con = self.cargs.new_connection(*args, **kwargs)
        con._set_type_codec(
            uuid.UUID('00000000-0000-0000-0000-000000000110'),
            encoder=lambda obj: obj,
            decoder=lambda obj: utils.BigInt(obj),
            format='python',
        )
        return con

    @property
    def connection(self) -> edgedb.BlockingIOConnection:
        if self._connection is None:
            # If this exception is thrown it means that the calling
            # code did not call `self.ensure_connection()`.
            raise RuntimeError('connection is not available')
        return self._connection

    @property
    def prompt(self) -> pt_shortcuts.PromptSession:
        if self._prompt is None:
            # If this exception is thrown it means that the calling
            # code did not call inside the `self.run()` method.
            raise RuntimeError('prompt is not available')
        return self._prompt

    def get_server_pgaddr(self) -> Optional[Mapping[str, str]]:
        settings = self.connection.get_settings()
        pgaddr = settings.get('pgaddr')
        if pgaddr is not None:
            return cast(Mapping[str, str], json.loads(pgaddr))
        else:
            return None

    def get_prompt(self) -> str:
        return f'{self.connection.dbname}>'

    def get_prompt_tokens(self) -> List[Tuple[str, str]]:
        return [
            ('class:prompt', f'{self.get_prompt()} '),
        ]

    def get_continuation_tokens(
        self,
        width: int,
        line_number: int,
        wrap_count: int,
    ) -> List[Tuple[str, str]]:
        return [
            ('class:continuation', '.' * (width - 1) + ' '),
        ]

    def get_toolbar_tokens(self) -> List[Tuple[str, str]]:
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

    def introspect_db(self, con: edgedb.BlockingIOConnection) -> None:
        names = con.fetchall('''
            WITH MODULE schema
            SELECT Type { name }
            FILTER Type IS (ObjectType | ScalarType);
        ''')
        self.context.typenames = {n.id: n.name for n in names}

    def build_propmpt(self) -> pt_shortcuts.PromptSession:
        history = pt_history.FileHistory(
            os.path.expanduser('~/.edgedbhistory'))

        bindings = pt_key_binding.KeyBindings()
        handle = bindings.add

        @handle('f3')  # type: ignore
        def _mode_toggle(event: Any) -> None:
            self.context.toggle_query_mode()

        @handle('f4')  # type: ignore
        def _implicit_toggle(event: Any) -> None:
            self.context.toggle_implicit()

        @handle('f5')  # type: ignore
        def _introspect_toggle(event: Any) -> None:
            self.context.toggle_introspect_types()

            if self.context.introspect_types:
                self.ensure_connection()
                self.introspect_db(self.connection)
            else:
                self.context.typenames = None

        @handle('tab')  # type: ignore
        def _tab(event: Any) -> None:
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

    def on_edgedb_log_message(
        self,
        connection: edgedb.BlockingIOConnection,
        msg: edgedb.EdgeDBMessage,
    ) -> None:
        render.render_status(
            self.context,
            f'{msg.get_severity_name()}: {msg}')

    def ensure_connection(self) -> None:
        try:
            if self._connection is None or self._connection.is_closed():
                self._connection = self._new_connection(
                    database=self.database,
                    timeout=60,
                )
        except edgedb.AuthenticationError:
            self._connection = None
            reason = 'could not authenticate'

        except Exception as e:
            self._connection = None
            reason = str(e)

        if self._connection is None:
            dbname = self.database or self.cargs.database
            if not dbname:
                dbname = 'EdgeDB'
            print(f'Could not establish connection to {dbname}: {reason}')
            exit(1)

        self._connection.add_log_listener(self.on_edgedb_log_message)

    def _command_simplelist(self, pattern: str, query: str,
                            name_field: str, item_name: str,
                            case_sensitive: bool = False) -> None:
        if case_sensitive:
            flag = ''
        else:
            flag = 'i'

        filter_clause, qkw = utils.get_filter_based_on_pattern(
            pattern, [name_field], flag)

        try:
            result, _ = self.fetch(
                f'''
                    {query}
                    {filter_clause}
                    ORDER BY {name_field} ASC
                ''',
                json=False,
                kwargs=qkw
            )
        except edgedb.EdgeDBError as exc:
            render.render_exception(self.context, exc)
        else:
            if result:
                print(f'List of {item_name}:')
                for item in result:
                    print(f'  {item}')
            else:
                print(f'No {item_name} found matching '
                      f'{eql_quote.quote_literal(pattern)}')
                self._command_semicolon_hint(pattern)

    def _render_sdl(self, sdl: str) -> None:
        desc_doc = pt_document.Document(sdl)
        lexer = pt_lexers.PygmentsLexer(eql_pygments.EdgeQLLexer)
        formatter = lexer.lex_document(desc_doc)

        for line in range(desc_doc.line_count):
            pt_shortcuts.print_formatted_text(
                pt_formatted_text.FormattedText(formatter(line)),
                style=self.style
            )
        print()

    def _command_help(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        in_devmode = self.get_server_pgaddr() is not None
        out = self._parser.render(
            show_devonly=in_devmode,
            group_annos={
                'Introspection':
                    '(options: S = show system objects, ' +
                    'I = case-sensitive match)'
            }
        )
        print(out)

    def _command_set_limit(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str],
    ) -> None:
        if not arg:
            print(self.context.implicit_limit)
            return

        try:
            limit = int(arg)
            if limit < 0:
                raise ValueError
        except (ValueError, TypeError):
            print('Invalid value for limit, expecting non-negative integer')
        else:
            self.context.implicit_limit = limit

    def _command_connect(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        new_db = arg
        # No need to prompt for a password if a connect attempt failed.
        self.cargs.allow_password_request = False
        try:
            new_connection = self._new_connection(
                database=new_db,
                timeout=60,
            )
            if self.context.introspect_types:
                self.introspect_db(new_connection)
        except Exception:
            print(f'Could not establish connection to {new_db!r}.', flush=True)
            return

        self.database = new_db

        if self._connection:
            self._connection.close()
        self._connection = new_connection

    def _command_list_dbs(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        self._command_simplelist(
            '',
            query='SELECT name := sys::Database.name',
            name_field='name',
            item_name='databases',
        )

    def _command_describe_object(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        verbose = 'verbose' in flags

        # normalize name
        name = utils.normalize_name(arg or '')

        if not name:
            print(f'The name {arg!r} is not valid', flush=True)
            return

        try:
            result, _ = self.fetch(
                f'''
                    DESCRIBE OBJECT {name} AS TEXT
                        {'VERBOSE' if verbose else ''}
                ''',
                json=False
            )
        except edgedb.InvalidReferenceError as exc:
            render.render_exception(self.context, exc)
            # if the error itself doesn't have a hint, render our
            # generic hint
            if not exc._hint:
                self._command_semicolon_hint(arg)

        except edgedb.EdgeDBError as exc:
            render.render_exception(self.context, exc)
        else:
            self._render_sdl(result[0])

    def _command_list_roles(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        self._command_simplelist(
            arg or '',
            query='SELECT name := sys::Role.name',
            name_field='name',
            item_name='roles',
            case_sensitive='case_sensitive' in flags,
        )

    def _command_list_modules(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        self._command_simplelist(
            arg or '',
            query='SELECT name := schema::Module.name',
            name_field='name',
            item_name='modules',
            case_sensitive='case_sensitive' in flags,
        )

    def _command_list_scalar_types(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:

        flag = '' if 'case_sensitive' in flags else 'i'
        pattern = arg or ''
        filter_and = 'NOT .is_from_alias'
        if 'system' not in flags:
            filter_and = f'''
                {filter_and} AND NOT (re_test("^({STD_RE})::", .name))
            '''

        filter_clause, qkw = utils.get_filter_based_on_pattern(
            pattern, flag=flag, filter_and=filter_and)

        base_query = r'''
            WITH MODULE schema
            SELECT ScalarType {
                name,
                `extending` := to_str(array_agg(.bases.name), ', '),
                kind := (
                    'enum' IF 'std::anyenum' IN .ancestors.name ELSE
                    'sequence' IF 'std::sequence' IN .ancestors.name ELSE
                    'normal'
                ),
            }
        '''

        query = f'''
            {base_query}
            {filter_clause}
            ORDER BY .name;
        '''

        try:
            result, _ = self.fetch(
                query,
                json=False,
                kwargs=qkw
            )
        except edgedb.EdgeDBError as exc:
            render.render_exception(self.context, exc)
        else:
            if result:
                assert self.prompt
                max_width = self.prompt.output.get_size().columns
                render.render_table(
                    self.context,
                    title='Scalar Types',
                    columns=[
                        table.ColumnSpec(field='name', title='Name',
                                         width=2, align='left'),
                        table.ColumnSpec(field='extending', title='Extending',
                                         width=2, align='left'),
                        table.ColumnSpec(field='kind', title='Kind',
                                         width=1, align='left'),
                    ],
                    data=result,
                    max_width=min(max_width, MAX_WIDTH),
                )

            else:
                if pattern:
                    print(f'No scalar types found matching '
                          f'{eql_quote.quote_literal(pattern)}.')
                    self._command_semicolon_hint(arg)
                elif 'system' not in flags:
                    print(R'No user-defined scalar types found. Try \lTS.')

    def _command_list_object_types(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        flag = '' if 'case_sensitive' in flags else 'i'
        pattern = arg or ''
        filter_and = 'NOT .is_from_alias'
        if 'system' not in flags:
            filter_and += f'''
                AND NOT (re_test("^({STD_RE})::", .name))
            '''

        filter_clause, qkw = utils.get_filter_based_on_pattern(
            pattern, flag=flag, filter_and=filter_and)

        base_query = r'''
            WITH MODULE schema
            SELECT ObjectType {
                name,
                `extending` := to_str(array_agg(.ancestors.name), ', '),
            }
        '''

        query = f'''
            {base_query}
            {filter_clause}
            ORDER BY .name;
        '''

        try:
            result, _ = self.fetch(
                query,
                json=False,
                kwargs=qkw
            )
        except edgedb.EdgeDBError as exc:
            render.render_exception(self.context, exc)
        else:
            if result:
                max_width = self.prompt.output.get_size().columns
                render.render_table(
                    self.context,
                    title='Object Types',
                    columns=[
                        table.ColumnSpec(field='name', title='Name',
                                         width=2, align='left'),
                        table.ColumnSpec(field='extending', title='Extending',
                                         width=3, align='left'),
                    ],
                    data=result,
                    max_width=min(max_width, MAX_WIDTH),
                )

            else:
                if pattern:
                    print(f'No object types found matching '
                          f'{eql_quote.quote_literal(pattern)}.')
                    self._command_semicolon_hint(arg)
                elif 'system' not in flags:
                    print(R'No user-defined object types found. Try \ltS.')

    def _command_list_expression_aliases(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        flag = '' if 'case_sensitive' in flags else 'i'
        pattern = arg or ''
        filter_and = '.is_from_alias'
        if 'system' not in flags:
            filter_and += f'''
                AND NOT (re_test("^({STD_RE})::", .name))
            '''

        filter_clause, qkw = utils.get_filter_based_on_pattern(
            pattern, flag=flag, filter_and=filter_and)

        base_query = r'''
            WITH MODULE schema
            SELECT Type {
                name,
                expr,
                class := (
                    'object' IF Type IS ObjectType ELSE
                    'scalar' IF Type IS ScalarType ELSE
                    'tuple' IF Type IS Tuple ELSE
                    'array' IF Type IS Array ELSE
                    'unknown'
                ),
            }
        '''

        query = f'''
            {base_query}
            {filter_clause}
            ORDER BY .name;
        '''

        try:
            result, _ = self.fetch(
                query,
                json=False,
                kwargs=qkw
            )
        except edgedb.EdgeDBError as exc:
            render.render_exception(self.context, exc)
        else:
            if result:
                columns = [
                    table.ColumnSpec(field='name', title='Name',
                                     width=2, align='left'),
                    table.ColumnSpec(field='class', title='Class',
                                     width=3, align='left'),
                ]
                if 'verbose' in flags:
                    columns.append(
                        table.ColumnSpec(
                            field='expr',
                            title='Expression',
                            width=4,
                            align='left',
                        )
                    )

                max_width = self.prompt.output.get_size().columns
                render.render_table(
                    self.context,
                    title='Expression Aliases',
                    data=result,
                    columns=columns,
                    max_width=min(max_width, MAX_WIDTH),
                )

            else:
                if pattern:
                    print(f'No expression aliases found matching '
                          f'{eql_quote.quote_literal(pattern)}.')
                    self._command_semicolon_hint(arg)
                elif 'system' not in flags:
                    print(
                        R'No user-defined expression aliases found. Try \laS.')

    def _command_list_casts(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        flag = '' if 'case_sensitive' in flags else 'i'
        pattern = arg or ''
        filter_clause, qkw = utils.get_filter_based_on_pattern(
            pattern, ['.from_type_name', '.to_type_name'], flag=flag)

        base_query = r'''
            WITH MODULE schema
            SELECT Cast {
                from_type_name := .from_type.name,
                to_type_name := .to_type.name,
                kind := (
                    'implicit' IF .allow_implicit ELSE
                    'assignment' IF .allow_assignment ELSE
                    'regular'
                ),
                volatility,
            }
        '''

        try:
            result, _ = self.fetch(
                f'''
                    {base_query}
                    {filter_clause}
                    ORDER BY .kind THEN .from_type.name THEN .to_type.name;
                ''',
                json=False,
                kwargs=qkw
            )
        except edgedb.EdgeDBError as exc:
            render.render_exception(self.context, exc)
        else:
            if result:
                max_width = self.prompt.output.get_size().columns
                render.render_table(
                    self.context,
                    title='Casts',
                    columns=[
                        table.ColumnSpec(
                            field='from_type_name', title='From Type',
                            width=1, align='left'),
                        table.ColumnSpec(
                            field='to_type_name', title='To Type',
                            width=1, align='left'),
                        table.ColumnSpec(
                            field='kind', title='Type of Cast',
                            width=1, align='left'),
                        table.ColumnSpec(
                            field='volatility', title='Volatility',
                            width=1, align='left'),
                    ],
                    data=result,
                    max_width=min(max_width, MAX_WIDTH),
                )

            else:
                if pattern:
                    print(f'No casts found matching '
                          f'{eql_quote.quote_literal(pattern)}.')
                    self._command_semicolon_hint(arg)
                else:
                    print('No casts found.')

    def _command_list_indexes(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        flag = '' if 'case_sensitive' in flags else 'i'
        pattern = arg or ''

        filter_and = []
        if 'system' not in flags:
            filter_and.append(f'''
                NOT (re_test("^({STD_RE})::", .subject_name))
            ''')
        if 'verbose' not in flags:
            filter_and.append('NOT .is_implicit')

        filter_clause, qkw = utils.get_filter_based_on_pattern(
            pattern, ['.subject_name'], flag=flag,
            filter_and=' AND '.join(filter_and))

        base_query = r'''
            WITH
                MODULE schema,
                I := {
                    Index,
                    (
                        SELECT Constraint
                        FILTER .name = 'std::exclusive' AND NOT .is_abstract
                    )
                }
            SELECT I {
                expr,
                subject_name := I[IS Index].<indexes[IS Source].name,
                cons_on := '.' ++ I[IS Constraint].subject.name,
                cons_of := I[Is Constraint].subject[IS Pointer]
                    .<pointers[IS Source].name,
                cons_of_of := I[Is Constraint].subject[IS Pointer]
                    .<properties[IS Source].<links[IS Source].name,
            } {
                expr := .cons_on ?? .expr,
                is_implicit := EXISTS .cons_on,
                subject_name :=
                    (.cons_of_of ++ '.' ++ .cons_of) ??
                    (.cons_of) ??
                    (.subject_name)
            }
        '''

        try:
            result, _ = self.fetch(
                f'''
                    {base_query}
                    {filter_clause}
                    ORDER BY .subject_name THEN .is_implicit THEN .expr;
                ''',
                json=False,
                kwargs=qkw
            )
        except edgedb.EdgeDBError as exc:
            render.render_exception(self.context, exc)
        else:
            if result:
                max_width = self.prompt.output.get_size().columns
                render.render_table(
                    self.context,
                    title='Indexes',
                    columns=[
                        table.ColumnSpec(
                            field='expr', title='Index on',
                            width=3, align='left'),
                        table.ColumnSpec(
                            field='is_implicit', title='Implicit',
                            width=1, align='left'),
                        table.ColumnSpec(
                            field='subject_name', title='Subject',
                            width=3, align='left'),
                    ],
                    data=result,
                    max_width=min(max_width, MAX_WIDTH),
                )

            else:
                if pattern:
                    print(f'No indexes found matching '
                          f'{eql_quote.quote_literal(pattern)}.')
                    self._command_semicolon_hint(arg)
                else:
                    print('No indexes found.')

    def _command_psql(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        pgaddr = self.get_server_pgaddr()
        if not pgaddr:
            print('\\psql requires EdgeDB to run in DEV mode')
            return

        pg_config = buildmeta.get_pg_config_path()
        psql = pg_config.parent / 'psql'

        cmd = [
            str(psql),
            '-h', pgaddr['host'],
            '-p', str(pgaddr['port']),
            '-d', self.connection.dbname,
            '-U', pgaddr['user']
        ]

        def _psql(cmd: List[str]) -> int:
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
        # Fix 'psql' command stdout artefacts:
        print('\r                ')

    def _command_pgaddr(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        pgaddr = self.get_server_pgaddr()
        if not pgaddr:
            print('\\psqlport requires EdgeDB to run in DEV mode')
            return
        print(pgaddr)

    def _command_errverbose(
        self,
        *,
        flags: AbstractSet[str],
        arg: Optional[str]
    ) -> None:
        exc = self.context.last_exception

        if exc is None:
            render.render_error(
                self.context,
                '== there is no previous error ==')
            return

        if not isinstance(exc, edgedb.EdgeDBError):
            # shouldn't ever happen
            render.render_error(
                self.context,
                '== previous error is not an EdgeDB error ==')
            return

        attrs = exc._attrs

        print(f'CODE: {hex(exc.get_code())}')

        hint = attrs.get(base_errors.FIELD_HINT)
        if hint:
            hint = hint.decode('utf-8')
            print(f'HINT: {hint}')

        srv_tb = exc.get_server_context()
        if srv_tb:
            print('SERVER TRACEBACK:')
            print('> ' + '\n> '.join(srv_tb.strip().split('\n')))
            print()

    def _command_semicolon_hint(self, arg: Optional[str]) -> None:
        arg = (arg or '').strip()
        if arg and arg[-1] == ';':
            print("Consider removing the trailing ';' from the command.")

    def fetch(
        self,
        query: str,
        *,
        json: bool,
        retry: bool=True,
        implicit_limit: int=0,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Tuple[Any, Optional[str]]:
        self.ensure_connection()
        self.context.last_exception = None

        if json:
            meth = self.connection._fetchall_json
        else:
            meth = self.connection._fetchall

        if kwargs is None:
            kwargs = {}

        try:
            result = meth(query, __limit__=implicit_limit, **kwargs)
        except edgedb.EdgeDBError as ex:
            self.context.last_exception = ex
            raise
        except (ConnectionAbortedError, BrokenPipeError):
            # The connection is closed; try again with a new one.
            if retry:
                assert self._connection
                self._connection.close()
                self._connection = None

                render.render_error(
                    self.context,
                    '== connection is closed; attempting to open a new one ==')

                return self.fetch(
                    query,
                    json=json,
                    implicit_limit=implicit_limit,
                    retry=False,
                )
            else:
                raise

        return result, self.connection._get_last_status()

    def show_banner(self) -> None:
        version = self.connection.fetchone('SELECT sys::get_version_as_str()')
        render.render_status(self.context, f'EdgeDB {version}')
        render.render_status(self.context, R'Type "\?" for help.')
        print()

    def run(self) -> None:
        self._prompt = self.build_propmpt()
        self.ensure_connection()
        self.context.use_colors = term.use_colors(sys.stdout.fileno())
        banner_shown = False

        try:
            while True:
                self.ensure_connection()
                if not banner_shown:
                    self.show_banner()
                    banner_shown = True

                try:
                    text = self.prompt.prompt()
                except KeyboardInterrupt:
                    continue

                command = text.strip()
                if not command:
                    continue

                if command in self.exit_commands:
                    raise EOFError

                elif command.startswith('\\'):
                    try:
                        self._parser.run(command)
                        print()
                    except LookupError as e:
                        render.render_error(self.context, str(e))
                    except Exception as ex:
                        render.render_exception(self.context, ex)
                    continue

                qm = self.context.query_mode
                results = []
                last_query = None
                if self.context.implicit_limit:
                    limit = self.context.implicit_limit + 1
                else:
                    limit = 0
                json_mode = qm is context.QueryMode.JSON
                try:
                    for query in utils.split_edgeql(command)[0]:
                        last_query = query
                        results.append(self.fetch(
                            query,
                            json=json_mode,
                            implicit_limit=limit,
                        ))

                except KeyboardInterrupt:
                    self.connection.close()
                    self._connection = None
                    print('\r', end='')
                    render.render_error(
                        self.context,
                        '== aborting query and closing the connection ==')
                    continue
                except Exception as ex:
                    render.render_exception(self.context, ex, query=last_query)
                    continue

                max_width = self.prompt.output.get_size().columns
                try:
                    for result, status in results:
                        if status in STATUSES_WITH_OUTPUT:
                            if qm is context.QueryMode.JSON:
                                render.render_json(
                                    self.context,
                                    result,
                                    max_width=min(max_width, MAX_WIDTH))
                            else:
                                render.render_binary(
                                    self.context,
                                    result,
                                    max_width=min(max_width, MAX_WIDTH))
                        elif status:
                            render.render_status(self.context, status)
                except KeyboardInterrupt:
                    print('\r', end='')
                    render.render_error(
                        self.context,
                        '== aborting rendering of the result ==')
                    continue
                except Exception as ex:
                    render.render_error(
                        self.context,
                        '== an exception while rendering the result ==')
                    render.render_exception(self.context, ex)

        except EOFError:
            return


def execute_script(cargs: cli_utils.ConnectionArgs, data: str) -> int:
    con = cargs.new_connection()

    try:
        queries = utils.split_edgeql(data)[0]
        ctx = context.ReplContext()
        for query in queries:
            try:
                ret = con.fetchall(query)
            except Exception as ex:
                render.render_exception(
                    ctx,
                    ex,
                    query=query)
                return 1
            else:
                render.render_binary(
                    ctx,
                    ret,
                    max_width=80)
        return 0
    finally:
        con.close()


def main(cargs: cli_utils.ConnectionArgs) -> int:
    try:
        interactive = sys.stdin.isatty()
    except AttributeError:
        # mock streams are always non-interactive
        interactive = False

    if interactive:
        Cli(cargs).run()
        return 0
    else:
        return execute_script(cargs, sys.stdin.read())
