##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import asyncio
import traceback

from edgedb import client

from prompt_toolkit import shortcuts as pt_shortcuts
from prompt_toolkit import token as pt_token
from prompt_toolkit import styles as pt_styles

from . import lex


style = pt_styles.style_from_dict({
    pt_token.Token.Prompt: '#aaa',
    pt_token.Token.PromptCont: '#888',

    pt_token.Token.RED: '#900',
    pt_token.Token.GREEN: '#090',

    # Syntax
    pt_token.Token.Keyword: '#e8364f',
    pt_token.Token.Operator: '#e8364f',
    pt_token.Token.String: '#d3c970',
    pt_token.Token.Number: '#9a79d7'
})


def get_prompt_tokens(cli):
    return [
        (pt_token.Token.Prompt, '>>> '),
    ]


def get_continuation_tokens(cli, width):
    return [
        (pt_token.Token.PromptCont, '...'),
    ]


async def repl():
    c = await client.connect()

    while True:
        query = await pt_shortcuts.prompt_async(
            patch_stdout=True,
            multiline=True,
            get_prompt_tokens=get_prompt_tokens,
            get_continuation_tokens=get_continuation_tokens,
            style=style,
            lexer=lex.EdgeQLLexer())

        if not query.strip():
            continue

        if c._transport.is_closing():
            print('Connection lost: reconnecting')
            c = await client.connect()

        try:
            result = await c.execute(query)
        except Exception as ex:
            traceback.print_exception(type(ex), ex, ex.__traceback__)
        else:
            print(result)


def main():
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(repl())
    finally:
        loop.close()
