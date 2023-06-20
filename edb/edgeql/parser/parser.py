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

from typing import *

from edb import errors
from edb.common import parsing
from edb.common import context as pctx
from edb.common.english import add_a as a

from .grammar import tokens
from .grammar import expressions as gr_exprs
from .grammar import commondl as gr_commondl
from .grammar import keywords as gr_keywords
from .. import tokenizer

from edb import _edgeql_parser as eql_parser


class EdgeQLParserBase(parsing.ParserSpec):
    def get_exception(self, native_err, context, token=None):
        msg = native_err.args[0]
        details = None
        hint = None

        if isinstance(native_err, errors.EdgeQLSyntaxError):
            return native_err
        else:
            if msg.startswith('Unexpected token: '):
                token = token or getattr(native_err, 'token', None)
                token_kind = token.kind()
                ltok = self.parser._stack[-1][0]

                is_reserved = (
                    token.text().lower()
                    in gr_keywords.by_type[gr_keywords.RESERVED_KEYWORD]
                )

                # Look at the parsing stack and use tokens and
                # non-terminals to infer the parser rule when the
                # error occurred.
                i, rule = self._get_rule()

                if (
                    rule == 'shape' and
                    token_kind == 'IDENT' and
                    isinstance(ltok, parsing.Nonterm)
                ):
                    # Make sure that the previous element in the stack
                    # is some kind of Nonterminal, because if it's
                    # not, this is probably not an issue of a missing
                    # COMMA.
                    hint = (f"It appears that a ',' is missing in {a(rule)} "
                            f"before {token.text()!r}")
                elif (
                    rule == 'list of arguments' and
                    # The stack is like <NodeName> LPAREN <AnyIdentifier>
                    i == 1 and
                    isinstance(ltok, (gr_exprs.AnyIdentifier,
                                      tokens.T_WITH,
                                      tokens.T_SELECT,
                                      tokens.T_FOR,
                                      tokens.T_INSERT,
                                      tokens.T_UPDATE,
                                      tokens.T_DELETE))
                ):
                    hint = ("Missing parentheses around statement used "
                            "as an expression")
                    # We want the error context correspond to the
                    # statement keyword
                    context = ltok.context
                    token = None
                elif (
                    rule == 'array slice' and
                    # The offending token was something that could
                    # make an expression
                    token_kind in {'IDENT', 'ICONST'} and
                    not isinstance(ltok, tokens.T_COLON)
                ):
                    hint = (f"It appears that a ':' is missing in {a(rule)} "
                            f"before {token.text()!r}")
                elif (
                    rule in {'list of arguments', 'tuple', 'array'} and
                    # The offending token was something that could
                    # make an expression
                    token_kind in {
                        'IDENT', 'TRUE', 'FALSE',
                        'ICONST', 'FCONST', 'NICONST', 'NFCONST',
                        'BCONST', 'SCONST',
                    } and
                    not isinstance(ltok, tokens.T_COMMA)
                ):
                    hint = (f"It appears that a ',' is missing in {a(rule)} "
                            f"before {token.text()!r}")
                elif (
                    rule == 'definition' and
                    token_kind == 'IDENT'
                ):
                    # Something went wrong in a definition, so check
                    # if the last successful token is a keyword.
                    if (
                        isinstance(ltok, gr_exprs.Identifier) and
                        ltok.val.upper() == 'INDEX'
                    ):
                        msg = (f"Expected 'ON', but got {token.text()!r} "
                               f"instead")
                    else:
                        msg = f'Unexpected {token.text()!r}'
                elif rule == 'for iterator':
                    msg = ("Missing parentheses around complex expression in "
                           "a FOR iterator clause")

                    if i > 0:
                        context = pctx.merge_context([
                            self.parser._stack[-i][0].context, context,
                        ])
                    token = None
                elif hasattr(token, 'val'):
                    msg = f'Unexpected {token.val!r}'
                elif token_kind == 'NL':
                    msg = 'Unexpected end of line'
                elif token.text() == "explain":
                    msg = f'Unexpected keyword {token.text()!r}'
                    hint = f'Use `analyze` to show query performance details'
                elif is_reserved and not isinstance(ltok, gr_exprs.Expr):
                    # Another token followed by a reserved keyword:
                    # likely an attempt to use keyword as identifier
                    msg = f'Unexpected keyword {token.text()!r}'
                    details = (
                        f'Token {token.text()!r} is a reserved keyword and'
                        f' cannot be used as an identifier'
                    )
                    hint = (
                        f'Use a different identifier or quote the name with'
                        f' backticks: `{token.text()}`'
                    )
                else:
                    msg = f'Unexpected {token.text()!r}'

        return errors.EdgeQLSyntaxError(
            msg, details=details, hint=hint, context=context, token=token)

    def _get_rule(self):
        ltok = self.parser._stack[-1][0]
        # Look at the parsing stack and use tokens and non-terminals
        # to infer the parser rule when the error occurred.
        rule = ''

        def _matches_for(i):
            return (
                len(self.parser._stack) >= i + 3
                and isinstance(self.parser._stack[-3 - i][0], tokens.T_FOR)
                and isinstance(
                    self.parser._stack[-2 - i][0], gr_exprs.Identifier)
                and isinstance(self.parser._stack[-1 - i][0], tokens.T_IN)
            )

        # Check if we're in the `FOR x IN <bad_token>` situation
        if (
            len(self.parser._stack) >= 4
            and isinstance(self.parser._stack[-2][0], tokens.T_RANGBRACKET)
            and isinstance(self.parser._stack[-3][0], gr_exprs.FullTypeExpr)
            and isinstance(self.parser._stack[-4][0], tokens.T_LANGBRACKET)
            and _matches_for(4)
        ):
            return 4, 'for iterator'

        if (
            len(self.parser._stack) >= 2
            and isinstance(self.parser._stack[-2][0], gr_exprs.AtomicExpr)
            and _matches_for(2)
        ):
            return 2, 'for iterator'

        if (
            len(self.parser._stack) >= 1
            and isinstance(self.parser._stack[-1][0], gr_exprs.BaseAtomicExpr)
            and _matches_for(1)
        ):
            return 1, 'for iterator'

        if _matches_for(0):
            return 0, 'for iterator'

        # If the last valid token was a closing brace/parent/bracket,
        # so we need to find a match for it before deciding what rule
        # context we're in.
        need_match = isinstance(ltok, (tokens.T_RBRACE,
                                       tokens.T_RPAREN,
                                       tokens.T_RBRACKET))
        nextel = None
        for i, (el, _) in enumerate(reversed(self.parser._stack)):
            if isinstance(el, tokens.Token):
                # We'll need the element right before "{", "[", or "(".
                prevel = self.parser._stack[-2 - i][0]

                if isinstance(el, tokens.T_LBRACE):
                    if need_match and isinstance(ltok,
                                                 tokens.T_RBRACE):
                        # This is matched, while we're looking
                        # for unmatched braces.
                        need_match = False
                        continue

                    elif isinstance(prevel, gr_commondl.OptExtending):
                        # This is some SDL/DDL
                        rule = 'definition'
                    elif (
                        isinstance(prevel, gr_exprs.Expr) or
                        (
                            isinstance(prevel, tokens.T_COLON) and
                            isinstance(self.parser._stack[-3 - i][0],
                                       gr_exprs.ShapePointer)
                        )
                    ):
                        # This is some kind of shape.
                        rule = 'shape'
                    break
                elif isinstance(el, tokens.T_LPAREN):
                    if need_match and isinstance(ltok,
                                                 tokens.T_RPAREN):
                        # This is matched, while we're looking
                        # for unmatched parentheses.
                        need_match = False
                        continue
                    elif isinstance(prevel, gr_exprs.NodeName):
                        rule = 'list of arguments'
                    elif isinstance(nextel, (tokens.T_FOR,
                                             tokens.T_SELECT,
                                             tokens.T_UPDATE,
                                             tokens.T_DELETE,
                                             tokens.T_INSERT,
                                             tokens.T_FOR)):
                        # A parenthesized subquery expression,
                        # we should leave the error as is.
                        break
                    else:
                        rule = 'tuple'
                    break
                elif isinstance(el, tokens.T_LBRACKET):
                    if need_match and isinstance(ltok,
                                                 tokens.T_RBRACKET):
                        # This is matched, while we're looking
                        # for unmatched brackets.
                        need_match = False
                        continue
                    # This is either an array literal or
                    # array index.
                    elif isinstance(prevel, gr_exprs.Expr):
                        rule = 'array slice'
                    else:
                        rule = 'array'
                    break

            # Also keep track of the element right after current.
            nextel = el

        return i, rule

    def get_parser(self):
        return CheeseParser(self)


class CheeseParser():
    parser: EdgeQLParserBase

    filename: Optional[str]
    source: tokenizer.Source

    def __init__(self, p: EdgeQLParserBase):
        self.parser = p
        self.filename = None

        mod = self.parser.get_parser_spec_module()
        self.token_map = {}
        for (_, token), cls in mod.TokenMeta.token_map.items():
            self.token_map[token] = cls

    def parse(
        self,
        source: Union[str, tokenizer.Source],
        filename: Optional[str] = None
    ):
        if isinstance(source, str):
            source = tokenizer.Source.from_string(source)

        self.filename = filename
        self.source = source

        try:
            parser_name = self.parser.__class__.__name__
            cst = eql_parser.parse(parser_name, source.tokens())

            return self._cst_to_ast(cst).val

        except eql_parser.TokenizerError as e:
            message, position = e.args
            raise errors.EdgeQLSyntaxError(
                message, position=position) from e


    def get_parser_spec(self, allow_rebuild=False):
        return self.parser.get_parser_spec(allow_rebuild=allow_rebuild)


    def _cst_to_ast(self, cst: eql_parser.CSTNode):
        stack: List[eql_parser.CSTNode | eql_parser.Production] = [cst]
        result: List[Any] = []

        while len(stack) > 0:
            node = stack.pop()

            if isinstance(node, eql_parser.CSTNode):

                if terminal := node.terminal():

                    context = parsing.ParserContext(
                        name=self.filename,
                        buffer=self.source.text(),
                        start=terminal.start(),
                        end=terminal.end()
                    )
                    result.append(parsing.Token(
                        terminal.text(), terminal.value(), context
                    ))

                elif production := node.production():
                    stack.append(production)
                    args = list(production.args())
                    args.reverse()
                    stack.extend(args)
                else:
                    assert False, node

            elif isinstance(node, eql_parser.Production):
                len_args = len(node.args())
                split_at = len(result)-len_args
                args = result[split_at:]
                result = result[0:split_at]

                mod = self.parser.get_parser_spec_module()
                cls = mod.__dict__[node.non_term()]
                obj = cls()
                method = cls.__dict__[node.production()]
                method(obj, *args)
                result.append(obj)
        return result.pop()



class EdgeQLSingleParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import single
        return single


class EdgeQLExpressionParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import fragment
        return fragment


class EdgeQLBlockParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import block
        return block


class EdgeQLMigrationBodyParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import migration_body
        return migration_body


class EdgeQLExtensionPackageBodyParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import extension_package_body
        return extension_package_body


class EdgeSDLParser(EdgeQLParserBase):
    def get_parser_spec_module(self):
        from .grammar import sdldocument
        return sdldocument
