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

from typing import Optional, List
import dataclasses
import pathlib
import os

from pygls.server import LanguageServer
from pygls import uris as pygls_uris
from lsprotocol import types as lsp_types


from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler

from edb.schema import schema as s_schema
from edb.schema import std as s_std
from edb.schema import ddl as s_ddl

from . import parsing as ls_parsing


@dataclasses.dataclass(kw_only=True, slots=True)
class State:
    schema: Optional[s_schema.Schema] = None

    std_schema: Optional[s_schema.Schema] = None


class EdgeDBLanguageServer(LanguageServer):
    state: State

    def __init__(self):
        super().__init__('Gel Language Server', 'v0.1')
        self.state = State()


def compile(
    ls: EdgeDBLanguageServer, stmts: List[qlast.Base]
) -> List[lsp_types.Diagnostic]:
    diagnostics: List[lsp_types.Diagnostic] = []

    if not stmts:
        return diagnostics

    schema = _get_schema(ls)
    if not schema:
        return diagnostics

    for ql_stmt in stmts:

        try:
            if isinstance(ql_stmt, qlast.DDLCommand):
                schema, _delta = s_ddl.delta_and_schema_from_ddl(
                    ql_stmt, schema=schema, modaliases={None: 'default'}
                )

            elif isinstance(ql_stmt, (qlast.Command, qlast.Query)):
                ir_stmt = qlcompiler.compile_ast_to_ir(ql_stmt, schema)
                ls.show_message_log(f'IR: {ir_stmt}')

            else:
                ls.show_message_log(f'skip compile of {ql_stmt}')
        except errors.EdgeDBError as error:
            diagnostics.append(_convert_error(error))
    return diagnostics


def _convert_error(error: errors.EdgeDBError) -> lsp_types.Diagnostic:
    return lsp_types.Diagnostic(
        range=lsp_types.Range(
            start=lsp_types.Position(
                line=error.line - 1,
                character=error.col - 1,
            ),
            end=lsp_types.Position(
                line=error.line_end - 1,
                character=error.col_end - 1,
            ),
        ),
        severity=lsp_types.DiagnosticSeverity.Error,
        message=error.args[0],
    )


def _get_schema(ls: EdgeDBLanguageServer) -> Optional[s_schema.Schema]:

    if ls.state.schema:
        return ls.state.schema

    # discover dbschema/ folders
    if len(ls.workspace.folders) != 1:

        if len(ls.workspace.folders) > 1:
            ls.show_message_log(
                "WARNING: workspaces with multiple root folders "
                "are not supported"
            )
        return None

    workspace: lsp_types.WorkspaceFolder = next(
        iter(ls.workspace.folders.values())
    )
    workspace_path = pathlib.Path(pygls_uris.to_fs_path(workspace.uri))

    dbschema = workspace_path / 'dbschema'

    # read and parse .esdl files
    sdl = qlast.Schema(declarations=[])
    for entry in os.listdir(dbschema):
        if not entry.endswith('.esdl'):
            continue
        doc = ls.workspace.get_text_document(f'dbschema/{entry}')

        res = ls_parsing.parse(doc, ls)
        if diagnostics := res.error:
            ls.publish_diagnostics(doc.uri, diagnostics, doc.version)
        else:
            if isinstance(res.ok, qlast.Schema):
                sdl.declarations.extend(res.ok.declarations)
            else:
                # TODO: complain that .esdl contains non-SDL syntax
                pass

    # apply SDL to std schema
    std_schema = _load_std_schema(ls.state)
    schema, _warnings = s_ddl.apply_sdl(
        sdl,
        base_schema=std_schema,
        current_schema=std_schema,
    )

    ls.state.schema = schema
    return ls.state.schema


def _load_std_schema(state: State) -> s_schema.Schema:
    if state.std_schema is not None:
        return state.std_schema

    schema: s_schema.Schema = s_schema.EMPTY_SCHEMA
    for modname in [*s_schema.STD_SOURCES, *s_schema.TESTMODE_SOURCES]:
        schema = s_std.load_std_module(schema, modname)
    schema, _ = s_std.make_schema_version(schema)
    schema, _ = s_std.make_global_schema_version(schema)

    state.std_schema = schema
    return state.std_schema
