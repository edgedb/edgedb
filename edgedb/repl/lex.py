##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import lexer as core_lexer
from edgedb.lang.edgeql.parser.grammar import lexer as edgeql_lexer
from edgedb.lang.edgeql.parser.grammar.keywords import edgeql_keywords

from prompt_toolkit import token as pt_token
from prompt_toolkit.layout import lexers as pt_lexers


class EdgeQLLexer(pt_lexers.Lexer):
    """Prompt toolkit compatible lexer for EdgeQL."""

    def lex_document(self, cli, document):
        """Return a lexer function mapping line numbers to tokens."""
        def translate_token_type(tok_type):
            prompt_tok = pt_token.Token

            if tok_type.lower() in edgeql_keywords:
                prompt_tok = pt_token.Token.Keyword
            elif tok_type == 'SCONST':
                prompt_tok = pt_token.Token.String
            elif tok_type in {'ICONST', 'FCONST'}:
                prompt_tok = pt_token.Token.Number
            elif tok_type in {'OP', 'PLUS', 'STAR', 'MINUS', 'AT',
                              'LANGBRACKET', 'RANGBRACKET',
                              'EQUALS', 'DOUBLECOLON', 'TURNSTILE',
                              '+', '::', '-', '=', '*', '/', ':='}:
                prompt_tok = pt_token.Token.Operator

            return prompt_tok

        def get_line_gen(i):
            text = '\n'.join(document.lines)

            lexer = edgeql_lexer.EdgeQLLexer()
            lexer.setinputstr(text)

            tok_stream = lexer.lex_highlight()

            line = 0
            try:
                for tok in tok_stream:
                    tok_type = tok.type

                    if tok_type == 'NL':
                        line += 1
                        if line > i:
                            return
                        else:
                            continue

                    if line == i:
                        txt = tok.text
                        pt_tok = translate_token_type(tok_type)

                        if '\n' in txt:
                            # multi-line token
                            yield pt_tok, txt.split('\n')[0]
                            return
                        else:
                            yield pt_tok, txt

                    elif '\n' in tok.text:
                        # multi-line token
                        lines = tok.text.split('\n')
                        if len(lines) + line > i:
                            yield (translate_token_type(tok_type),
                                   lines[i - line])

                        line += len(lines) - 1
                        if line > i:
                            return

            except core_lexer.UnknownTokenError as ex:
                if line == i:
                    nontokenized = document.lines[ex.line - 1][ex.col - 1:]
                    yield pt_token.Token, nontokenized
                else:
                    yield pt_token.Token, document.lines[i]

        def get_line(i):
            return list(get_line_gen(i))

        return get_line
