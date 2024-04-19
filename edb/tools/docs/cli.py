#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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

from typing import Any, Dict

from edb.tools.pygments.edgeql import EdgeQLLexer

import pygments.lexers

from sphinx import domains as s_domains
from docutils.parsers.rst import directives as d_directives  # type: ignore

from . import shared


class CLISynopsisDirective(shared.CodeBlock):

    has_content = True
    optional_arguments = 0
    required_arguments = 0
    option_spec: Dict[str, Any] = {
        'version-lt': d_directives.unchanged_required
    }

    def run(self):
        self.arguments = ['cli-synopsis']
        return super().run()


class CLIDomain(s_domains.Domain):

    name = "cli"
    label = "Command Line Interface"

    directives = {
        'synopsis': CLISynopsisDirective,
    }


def setup_domain(app):
    # Dummy lexers; the actual highlighting is implemented
    # in the edgedb.com website code.
    app.add_lexer("txt", pygments.lexers.TextLexer)
    app.add_lexer("bash", pygments.lexers.TextLexer)

    app.add_lexer("cli", EdgeQLLexer)
    app.add_lexer("cli-synopsis", EdgeQLLexer)

    app.add_role(
        'cli:synopsis',
        shared.InlineCodeRole('cli-synopsis'))

    app.add_domain(CLIDomain)
