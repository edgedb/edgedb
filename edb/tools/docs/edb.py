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

from sphinx_code_tabs import TabsNode


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


class EDBEnvironmentSwitcher(d_rst.Directive):

    has_content = False
    optional_arguments = 0
    required_arguments = 0

    def run(self):
        node = d_nodes.container()
        node['env-switcher'] = True
        return [node]


class EDBSplitSection(d_rst.Directive):

    has_content = True
    optional_arguments = 0
    required_arguments = 0

    def run(self):
        node = d_nodes.container()
        node['split-section'] = True
        self.state.nested_parse(self.content, self.content_offset, node)
        
        split_indexes = [
            index for index,child in enumerate(node.children)
            if isinstance(child, d_nodes.container)
                and child.get('split-point') == True
        ]
        if len(split_indexes) > 1:
            raise Exception(
                f'cannot have multiple edb:split-point\'s in edb:split-section'
            )
        blocks = (
            node.children[:split_indexes[0]] if
            node.children[split_indexes[0]].get('code-above') == True
            else node.children[split_indexes[0]+1:]
        ) if len(split_indexes) == 1 else [node.children[-1]]
        if len(blocks) < 1:
            raise Exception(
                f'no content found at end of edb:split-section block, '
                f'or before/after the edb:split-point in the edb:split-section'
            )
        for block in blocks:
            if (
                not isinstance(block, d_nodes.literal_block)
                and not isinstance(block, TabsNode)
                and not isinstance(block, d_nodes.image)
                and not isinstance(block, d_nodes.figure)
            ):
                raise Exception(
                    f'expected all content before/after the edb:split-point or '
                    f'at the end of the edb:split-section to be either a '
                    f'code block, code tabs, or image/figure'
                )
        return [node]
    

class EDBSplitPoint(d_rst.Directive):

    has_content = False
    optional_arguments = 1
    required_arguments = 0

    def run(self):
        node = d_nodes.container()
        node['split-point'] = True
        if len(self.arguments) > 0:
            if self.arguments[0] not in ['above', 'below']:
                raise Exception(
                    f"expected edb:split-point arg to be 'above', 'below' "
                    f"or empty (defaults to 'below')"
                )
            if self.arguments[0] == 'above':
                node['code-above'] = True
        return [node]


class GelDomain(s_domains.Domain):
    name = "edb"
    label = "Gel"

    directives = {
        'collapsed': EDBCollapsed,
        'youtube-embed': EDBYoutubeEmbed,
        'env-switcher': EDBEnvironmentSwitcher,
        'split-section': EDBSplitSection,
        'split-point': EDBSplitPoint
    }


def setup_domain(app):
    app.add_domain(GelDomain)


def setup(app):
    setup_domain(app)
