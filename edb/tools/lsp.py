from typing import Any, List, Tuple
import click
from pygls.server import LanguageServer
from pygls.workspace import TextDocument
from lsprotocol import types as lsp_types

from edb.tools.edb import edbcommands

from edb.edgeql import tokenizer
from edb.edgeql import parser as qlparser
from edb.edgeql.parser.grammar import tokens as qltokens

import edb._edgeql_parser as rust_parser


@edbcommands.command("lsp")
@click.option('--stdio', default=False, is_flag=True)
def main(stdio: bool):
    ls = LanguageServer('EdgeDB Language Server', 'v0.1')

    @ls.feature(
        lsp_types.INITIALIZE,
    )
    def init(_params: lsp_types.InitializeParams):
        ls.show_message_log('Starting')
        qlparser.preload_spec()
        ls.show_message_log('Started')

    @ls.feature(lsp_types.TEXT_DOCUMENT_DID_OPEN)
    def text_document_did_open(params: lsp_types.DidOpenTextDocumentParams):
        ls.show_message_log(f'did open: {params.text_document.uri}')

        document = ls.workspace.get_text_document(params.text_document.uri)
        parse_and_report_diagnostics(document, ls)

    @ls.feature(lsp_types.TEXT_DOCUMENT_DID_CHANGE)
    def text_document_did_change(params: lsp_types.DidChangeTextDocumentParams):
        ls.show_message_log(f'did change: {params.text_document.uri}')

        document = ls.workspace.get_text_document(params.text_document.uri)
        parse_and_report_diagnostics(document, ls)

    @ls.feature(
        lsp_types.TEXT_DOCUMENT_COMPLETION,
        lsp_types.CompletionOptions(trigger_characters=[',']),
    )
    def completions(params: lsp_types.CompletionParams):
        items = []

        document = ls.workspace.get_text_document(params.text_document.uri)

        if item := parse_and_suggest_keyword(document, params.position):
            items.append(item)

        return lsp_types.CompletionList(is_incomplete=False, items=items)

    if stdio:
        ls.start_io()


def position_in_span(pos: lsp_types.Position, span: Tuple[Any, Any]):
    start, end = span

    if pos.line < start.line - 1:
        return False
    if pos.line > end.line - 1:
        return False
    if pos.line == start.line - 1 and pos.character < start.column - 1:
        return False
    if pos.line == end.line - 1 and pos.character > end.column - 1:
        return False
    return True


def parse(doc: TextDocument) -> Tuple[tokenizer.Source, List[Any], Any]:
    sdl = False

    try:
        source = tokenizer.Source.from_string(doc.source)
    except Exception as e:
        # TODO
        print(e)
        return

    start_t = qltokens.T_STARTSDLDOCUMENT if sdl else qltokens.T_STARTBLOCK
    start_t_name = start_t.__name__[2:]
    tokens = source.tokens()

    result, productions = rust_parser.parse(start_t_name, tokens)
    return source, result, productions


def parse_and_report_diagnostics(doc: TextDocument, ls: LanguageServer) -> None:
    source, result, _productions = parse(doc)

    if result.errors:
        diagnostics = []
        for error in result.errors:
            message, span, hint, details = error

            if details:
                message += f"\n{details}"
            if hint:
                message += f"\nHint: {hint}"
            (start, end) = tokenizer.inflate_span(source.text(), span)

            diagnostics.append(
                lsp_types.Diagnostic(
                    range=lsp_types.Range(
                        start=lsp_types.Position(
                            line=start.line - 1,
                            character=start.column - 1,
                        ),
                        end=lsp_types.Position(
                            line=end.line - 1,
                            character=end.column - 1,
                        ),
                    ),
                    severity=lsp_types.DiagnosticSeverity.Error,
                    message=message,
                )
            )

        ls.publish_diagnostics(doc.uri, diagnostics, doc.version)
        return

    ls.publish_diagnostics(doc.uri, [], doc.version)
    # parsing successful


def parse_and_suggest_keyword(document, position) -> lsp_types.CompletionItem:
    source, result, _productions = parse(document)
    for error in result.errors:
        message: str
        message, span, hint, details = error
        if not message.startswith('Missing keyword '):
            continue
        (start, end) = tokenizer.inflate_span(source.text(), span)

        if not position_in_span(position, (start, end)):
            continue

        keyword = message.removeprefix('Missing keyword \'')[:-1]

        return lsp_types.CompletionItem(
            label=keyword,
            kind=lsp_types.CompletionItemKind.Keyword,
        )
    return None
