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

import re

from sphinx import domains as s_domains
from docutils import nodes as d_nodes
from docutils.parsers import rst as d_rst
from docutils.parsers.rst import directives as d_directives  # type: ignore

from sphinx import transforms


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
            index for index, child in enumerate(node.children)
            if isinstance(child, d_nodes.container) and child.get('split-point')
        ]
        if len(split_indexes) > 1:
            raise Exception(
                f'cannot have multiple edb:split-point\'s in edb:split-section'
            )
        blocks = (
            node.children[:split_indexes[0]] if
            node.children[split_indexes[0]].get('code-above')
            else node.children[split_indexes[0] + 1:]
        ) if len(split_indexes) == 1 else [node.children[-1]]
        if len(blocks) < 1:
            raise Exception(
                f'no content found at end of edb:split-section block, '
                f'or before/after the edb:split-point in the edb:split-section'
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


class GelSubstitutionTransform(transforms.SphinxTransform):
    default_priority = 0

    def apply(self):
        builder_name = "html"
        if hasattr(self.document.settings, 'env'):
            env = self.document.settings.env
            if env and hasattr(env, "app"):
                builder_name = env.app.builder.name

        # Traverse all substitution_reference nodes.
        for node in self.document.traverse(d_nodes.substitution_reference):
            nt = node.astext()
            if nt.lower() in {
                "gel", "gel's", "edgedb", "gelcmd", ".gel", "gel.toml",
                "gel-server", "geluri", "admin", "main",
                "branch", "branches"
            }:
                if builder_name in {"xml", "edge-xml"}:
                    if nt == "gelcmd":
                        sub = d_nodes.literal(
                            'gel', 'gel',
                            **{
                                "edb-gelcmd": "true",
                                "edb-gelcmd-top": "true",
                                "edb-substitution": "true",
                            }
                        )
                    elif nt == "geluri":
                        sub = d_nodes.literal(
                            'gel', 'gel://',
                            **{
                                "edb-geluri": "true",
                                "edb-geluri-top": "true",
                                "edb-substitution": "true",
                            }
                        )
                    else:
                        sub = d_nodes.inline(
                            nt, nt, **{"edb-substitution": "true"}
                        )
                    node.replace_self(sub)
                else:
                    node.replace_self(d_nodes.Text(nt))


class GelCmdRole:

    def __call__(
        self, role, rawtext, text, lineno, inliner, options=None, content=None
    ):
        text = text.strip()
        text = re.sub(r'(\n\s*)+', " ", text)
        if text.startswith("edgedb"):
            fn = inliner.document.current_source
            raise Exception(
                f"{fn}:{lineno} - :gelcmd:`{text}` - can't start with 'edgedb'"
            )
        if text.startswith("gel ") or text == "gel":
            fn = inliner.document.current_source
            raise Exception(
                f"{fn}:{lineno} - :gelcmd:`{text}` - can't start with 'gel'"
            )
        text = f'gel {text}'
        node = d_nodes.literal(text, text)
        node["edb-gelcmd"] = "true"
        node["edb-gelcmd-top"] = "false"
        node["edb-substitution"] = "true"
        return [node], []


class GelUriRole:

    def __call__(
        self, role, rawtext, text, lineno, inliner, options=None, content=None
    ):
        if text.startswith("edgedb://"):
            fn = inliner.document.current_source
            raise Exception(
                f"{fn}:{lineno} - :geluri:`{text}`"
                f" - can't start with 'edgedb://'"
            )
        if text.startswith("gel://"):
            fn = inliner.document.current_source
            raise Exception(
                f"{fn}:{lineno} - :geluri:`{text}` - can't start with 'gel://'"
            )
        text = f'gel://{text}'
        node = d_nodes.literal(text, text)
        node["edb-geluri"] = "true"
        node["edb-geluri-top"] = "false"
        node["edb-substitution"] = "true"
        return [node], []


class DotGelRole:

    def __call__(
        self, role, rawtext, text, lineno, inliner, options=None, content=None
    ):
        if text.endswith(".gel") or text.endswith(".esdl"):
            fn = inliner.document.current_source
            raise Exception(
                f"{fn}:{lineno} - :dotgel:`{text}`"
                f" - can't end with '.esdl' or '.gel'"
            )
        text = f'{text}.gel'
        node = d_nodes.literal(text, text)
        node["edb-dotgel"] = "true"
        node["edb-substitution"] = "true"
        return [node], []


class GelEnvRole:

    def __call__(
        self, role, rawtext, text, lineno, inliner, options=None, content=None
    ):
        if (
            text.startswith("EDGEDB_") or
            text.startswith("GEL_") or
            text.startswith("_")
        ):
            fn = inliner.document.current_source
            raise Exception(
                f"{fn}:{lineno} - :gelenv:`{text}`"
                f" - can't start with 'EDGEDB_', 'GEL_', or '_'"
            )
        text = f'GEL_{text}'
        node = d_nodes.literal(text, text)
        node["edb-gelenv"] = "true"
        node["edb-substitution"] = "true"
        return [node], []


def setup_domain(app):

    app.add_role('gelcmd', GelCmdRole())
    app.add_role('geluri', GelUriRole())
    app.add_role('dotgel', DotGelRole())
    app.add_role('gelenv', GelEnvRole())
    app.add_domain(GelDomain)
    app.add_transform(GelSubstitutionTransform)


def setup(app):
    setup_domain(app)
