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

from sphinx import domains as s_domains
from docutils import nodes as d_nodes
from docutils.parsers import rst as d_rst
from docutils.parsers.rst import directives as d_directives  # type: ignore


class EDBYoutubeEmbed(d_rst.Directive):

    has_content = True
    optional_arguments = 0
    required_arguments = 1

    def run(self):
        node = d_nodes.container()
        node['youtube-video-id'] = self.arguments[0]
        self.state.nested_parse(self.content, self.content_offset, node)
        return [node]


class EDBCollapsed(d_rst.Directive):

    has_content = True
    optional_arguments = 0
    required_arguments = 0
    option_spec = {
        'summary': d_directives.unchanged_required,
    }

    def run(self):
        node = d_nodes.container()
        node['collapsed_block'] = True
        if 'summary' in self.options:
            node['summary'] = self.options['summary']
        self.state.nested_parse(self.content, self.content_offset, node)
        return [node]


class GelDomain(s_domains.Domain):
    name = "edb"
    label = "Gel"

    directives = {
        'collapsed': EDBCollapsed,
        'youtube-embed': EDBYoutubeEmbed
    }


def setup_domain(app):
    app.add_domain(GelDomain)


def setup(app):
    setup_domain(app)
