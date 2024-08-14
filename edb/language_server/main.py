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

from lsprotocol import types as lsp_types
import click
import sys

from edb import buildmeta
from edb.common import traceback as edb_traceback
from edb.edgeql import parser as qlparser

from . import parsing as ls_parsing
from . import server as ls_server


@click.command()
@click.option('--version', is_flag=True, help="Show the version and exit.")
@click.option(
    '--stdio',
    is_flag=True,
    help="Use stdio for LSP. This is currently the only transport.",
)
def main(*, version: bool, stdio: bool):
    if version:
        print(f"edgedb-ls, version {buildmeta.get_version()}")
        sys.exit(0)

    ls = init()

    if stdio:
        ls.start_io()
    else:
        print("Error: no LSP transport enabled. Use --stdio.")


def init() -> ls_server.EdgeDBLanguageServer:
    ls = ls_server.EdgeDBLanguageServer()

    @ls.feature(
        lsp_types.INITIALIZE,
    )
    def init(_params: lsp_types.InitializeParams):
        ls.show_message_log('Starting')
        qlparser.preload_spec()
        ls.show_message_log('Started')

    @ls.feature(lsp_types.TEXT_DOCUMENT_DID_OPEN)
    def text_document_did_open(params: lsp_types.DidOpenTextDocumentParams):
        document_updated(ls, params.text_document.uri)

    @ls.feature(lsp_types.TEXT_DOCUMENT_DID_CHANGE)
    def text_document_did_change(params: lsp_types.DidChangeTextDocumentParams):
        document_updated(ls, params.text_document.uri)

    @ls.feature(
        lsp_types.TEXT_DOCUMENT_COMPLETION,
        lsp_types.CompletionOptions(trigger_characters=[',']),
    )
    def completions(params: lsp_types.CompletionParams):
        items = []

        document = ls.workspace.get_text_document(params.text_document.uri)

        if item := ls_parsing.parse_and_suggest(document, params.position):
            items.append(item)

        return lsp_types.CompletionList(is_incomplete=False, items=items)

    return ls


def document_updated(ls: ls_server.EdgeDBLanguageServer, doc_uri: str):
    # each call to this function should yield in exactly one publish_diagnostics
    # for this document

    document = ls.workspace.get_text_document(doc_uri)
    ql_ast = ls_parsing.parse(document, ls)
    if diagnostics := ql_ast.error:
        ls.publish_diagnostics(document.uri, diagnostics, document.version)
        return
    assert ql_ast.ok

    try:
        if isinstance(ql_ast.ok, list):
            diagnostics = ls_server.compile(ls, ql_ast.ok)
            ls.publish_diagnostics(document.uri, diagnostics, document.version)
        else:
            ls.publish_diagnostics(document.uri, [], document.version)
    except BaseException as e:
        send_internal_error(ls, e)
        ls.publish_diagnostics(document.uri, [], document.version)


def send_internal_error(ls: ls_server.EdgeDBLanguageServer, e: BaseException):
    text = edb_traceback.format_exception(e)
    ls.show_message_log(f'Internal error: {text}')


if __name__ == '__main__':
    main()
