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

from docutils import nodes as d_nodes
from docutils.parsers import rst as d_rst
from sphinx import addnodes as s_nodes
from sphinx import transforms as s_transforms

from . import edb
from . import cli
from . import eql
from . import js
from . import sdl
from . import graphql
from . import shared


class ProhibitedNodeTransform(s_transforms.SphinxTransform):

    default_priority = 1  # before ReferencesResolver

    def apply(self):
        for bq in list(self.document.traverse(d_nodes.block_quote)):
            if not bq['classes'] or 'pull-quote' not in bq['classes']:
                raise shared.EdgeSphinxExtensionError(
                    f'blockquote found: {bq.asdom().toxml()!r} in {bq.source};'
                    f' Try using the "pull-quote" directive')
            else:
                bq.get('classes').remove('pull-quote')

        trs = list(self.document.traverse(d_nodes.title_reference))
        if trs:
            raise shared.EdgeSphinxExtensionError(
                f'title reference (single backticks quote) found: '
                f'{trs[0].asdom().toxml()!r} in {trs[0].source}; '
                f'perhaps you wanted to use double backticks?')


class VersionAdded(d_rst.Directive):

    has_content = True
    optional_arguments = 0
    required_arguments = 1

    def run(self):
        node = s_nodes.versionmodified()
        node['type'] = 'versionadded'
        node['version'] = self.arguments[0]
        self.state.nested_parse(self.content, self.content_offset, node)
        return [node]


class VersionChanged(d_rst.Directive):

    has_content = True
    optional_arguments = 0
    required_arguments = 1

    def run(self):
        node = s_nodes.versionmodified()
        node['type'] = 'versionchanged'
        node['version'] = self.arguments[0]
        self.state.nested_parse(self.content, self.content_offset, node)
        return [node]


class VersionedSection(d_rst.Directive):

    has_content = False
    optional_arguments = 0
    required_arguments = 0

    def run(self):
        node = d_nodes.container()
        node['versioned-section'] = True
        return [node]


class VersionedReplaceRole:

    def __call__(
        self, role, rawtext, text, lineno, inliner, options=None, content=None
    ):
        nodes = []
        if not text.startswith('_default:'):
            text = '_default:' + text
        for section in text.split(';'):
            parts = section.split(':', maxsplit=1)
            node = s_nodes.versionmodified()
            node['type'] = 'versionchanged'
            node['version'] = parts[0].strip()
            node += d_nodes.Text(parts[1].strip())
            nodes.append(node)
        return nodes, []


def setup(app):
    edb.setup_domain(app)
    cli.setup_domain(app)
    eql.setup_domain(app)
    js.setup_domain(app)
    sdl.setup_domain(app)
    graphql.setup_domain(app)

    app.add_directive('versionadded', VersionAdded, True)
    app.add_directive('versionchanged', VersionChanged, True)
    app.add_directive('code-block', shared.CodeBlock, True)
    app.add_directive('versioned-section', VersionedSection)
    app.add_role('versionreplace', VersionedReplaceRole())

    app.add_transform(ProhibitedNodeTransform)
