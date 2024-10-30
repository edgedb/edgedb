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

from sphinx import domains as s_domains
from docutils.parsers.rst import directives as d_directives  # type: ignore

from . import shared


class SDLSynopsisDirective(shared.CodeBlock):

    has_content = True
    optional_arguments = 0
    required_arguments = 0
    option_spec: Dict[str, Any] = {
        'version-lt': d_directives.unchanged_required
    }

    def run(self):
        self.arguments = ['sdl-synopsis']
        return super().run()


class SDLDomain(s_domains.Domain):

    name = "sdl"
    label = "Gel Schema Definition Language"

    directives = {
        'synopsis': SDLSynopsisDirective,
    }


def setup_domain(app):
    app.add_lexer("sdl", EdgeQLLexer)
    app.add_lexer("sdl-synopsis", EdgeQLLexer)

    app.add_role(
        'sdl:synopsis',
        shared.InlineCodeRole('sdl-synopsis'))

    app.add_domain(SDLDomain)
