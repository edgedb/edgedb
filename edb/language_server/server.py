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

from typing import Dict, Mapping, Optional, List, Tuple
import dataclasses
import pathlib
import os

from pygls.server import LanguageServer
from pygls import uris as pygls_uris
import pygls
from lsprotocol import types as lsp_types


from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler

from edb.schema import schema as s_schema
from edb.schema import std as s_std
from edb.schema import ddl as s_ddl
import pygls.workspace

from . import parsing as ls_parsing
from . import is_schema_file


@dataclasses.dataclass(kw_only=True, slots=True)
class State:
    schema_docs: List[pygls.workspace.TextDocument] = dataclasses.field(
        default_factory=lambda: []
    )

    schema: Optional[s_schema.Schema] = None

    std_schema: Optional[s_schema.Schema] = None


class GelLanguageServer(LanguageServer):
    state: State

    def __init__(self):
        super().__init__('Gel Language Server', 'v0.1')
        self.state = State()


@dataclasses.dataclass(kw_only=True, slots=True)
class DiagnosticsSet:
    by_doc: Dict[pygls.workspace.TextDocument, List[lsp_types.Diagnostic]] = (
        dataclasses.field(default_factory=lambda: {})
    )


def compile(
    ls: GelLanguageServer,
    doc: pygls.workspace.TextDocument,
    stmts: List[qlast.Base],
) -> DiagnosticsSet:
    if not stmts:
        return DiagnosticsSet(by_doc={doc: []})

    schema, diagnostics_set = get_schema(ls)
    if not schema:
        return diagnostics_set

    diagnostics: List[lsp_types.Diagnostic] = []
    modaliases: Mapping[Optional[str], str] = {None: 'default'}
    for ql_stmt in stmts:

        try:
            if isinstance(ql_stmt, qlast.DDLCommand):
                schema, _delta = s_ddl.delta_and_schema_from_ddl(
                    ql_stmt, schema=schema, modaliases=modaliases
                )

            elif isinstance(ql_stmt, (qlast.Command, qlast.Expr)):
                options = qlcompiler.CompilerOptions(modaliases=modaliases)
                ir_stmt = qlcompiler.compile_ast_to_ir(
                    ql_stmt, schema, options=options
                )
                ls.show_message_log(
                    f'IR: {ir_stmt}', msg_type=lsp_types.MessageType.Debug
                )
            else:
                ls.show_message_log(f'skip compile of {ql_stmt}')
        except errors.EdgeDBError as error:
            diagnostics.append(_convert_error(error))

    diagnostics_set.by_doc[doc] = diagnostics
    return diagnostics_set


def _convert_error(error: errors.EdgeDBError) -> lsp_types.Diagnostic:
    return lsp_types.Diagnostic(
        range=(
            lsp_types.Range(
                start=lsp_types.Position(
                    line=error.line - 1,
                    character=error.col - 1,
                ),
                end=lsp_types.Position(
                    line=error.line_end - 1,
                    character=error.col_end - 1,
                ),
            )
            if error.line >= 0
            else lsp_types.Range(
                start=lsp_types.Position(line=0, character=0),
                end=lsp_types.Position(line=0, character=0),
            )
        ),
        severity=lsp_types.DiagnosticSeverity.Error,
        message=error.args[0],
    )


def get_schema(
    ls: GelLanguageServer,
) -> Tuple[Optional[s_schema.Schema], DiagnosticsSet]:
    if ls.state.schema:
        return (ls.state.schema, DiagnosticsSet())

    if len(ls.state.schema_docs) == 0:
        _load_schema_docs(ls)

    return _compile_schema(ls)


def update_schema_doc(ls: GelLanguageServer, doc: pygls.workspace.TextDocument):
    # TODO: check that this doc in actually in dbschema dir

    if len(ls.state.schema_docs) == 0:
        _load_schema_docs(ls)

    existing = next(
        (i for i, d in enumerate(ls.state.schema_docs) if d.path == doc.path),
        None,
    )
    if existing is not None:
        # update
        ls.state.schema_docs[existing] = doc
    else:
        # insert
        ls.show_message_log("new schema file added: " + doc.path)
        ls.show_message_log("existing files: ")
        for d in ls.state.schema_docs:
            ls.show_message_log("- " + d.path)

        ls.state.schema_docs.append(doc)


def _load_schema_docs(ls: GelLanguageServer):
    # discover dbschema/ folders
    if len(ls.workspace.folders) != 1:

        if len(ls.workspace.folders) > 1:
            ls.show_message_log(
                "WARNING: workspaces with multiple root folders "
                "are not supported"
            )
        return None

    # discard all existing docs
    ls.state.schema_docs.clear()

    workspace: lsp_types.WorkspaceFolder = next(
        iter(ls.workspace.folders.values())
    )
    workspace_path = pathlib.Path(pygls_uris.to_fs_path(workspace.uri))

    # TODO: read gel.toml and use [project.schema-dir]
    schema_dir = 'dbschema'

    # read .esdl files
    for entry in os.listdir(workspace_path / schema_dir):
        if not is_schema_file(entry):
            continue
        doc = ls.workspace.get_text_document(
            str(workspace_path / schema_dir / entry)
        )
        ls.state.schema_docs.append(doc)


def _compile_schema(
    ls: GelLanguageServer,
) -> Tuple[Optional[s_schema.Schema], DiagnosticsSet]:
    # parse
    sdl = qlast.Schema(declarations=[])
    diagnostics = DiagnosticsSet()
    for doc in ls.state.schema_docs:
        res = ls_parsing.parse(doc, ls)
        if d := res.err:
            diagnostics.by_doc[doc] = d
        else:
            diagnostics.by_doc[doc] = []
            if isinstance(res.ok, qlast.Schema):
                sdl.declarations.extend(res.ok.declarations)
            else:
                # TODO: complain that .esdl contains non-SDL syntax
                pass

    std_schema = _load_std_schema(ls.state)

    # apply SDL to std schema
    ls.show_message_log('compiling schema ..')
    try:
        schema, _warnings = s_ddl.apply_sdl(
            sdl,
            base_schema=std_schema,
            current_schema=std_schema,
        )
        ls.show_message_log('.. done')
    except errors.EdgeDBError as error:
        schema = None

        # find doc
        do = next(
            (d for d in ls.state.schema_docs if error.filename == d.filename),
            None,
        )
        if do is None:
            ls.show_message_log(
                f'cannot find original doc of the error ({error.filename}), '
                'using first schema file'
            )
            do = ls.state.schema_docs[0]

        # convert error
        diagnostics.by_doc[do].append(_convert_error(error))

    ls.state.schema = schema
    return (schema, diagnostics)


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
