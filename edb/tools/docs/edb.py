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
from docutils import utils as d_utils

from . import shared


class EDBReactElement(d_rst.Directive):

    has_content = False
    optional_arguments = 0
    required_arguments = 1

    def run(self):
        node = d_nodes.container()
        node['react-element'] = self.arguments[0]
        return [node]


class EDBSectionIntroPage(d_rst.Directive):

    has_content = False
    optional_arguments = 0
    required_arguments = 1

    def run(self):
        node = d_nodes.container()
        node['section-intro-page'] = self.arguments[0]
        return [node]


class EDBYoutubeEmbed(d_rst.Directive):

    has_content = True
    optional_arguments = 0
    required_arguments = 1

    def run(self):
        node = d_nodes.container()
        node['youtube-video-id'] = self.arguments[0]
        self.state.nested_parse(self.content, self.content_offset, node)
        return [node]


class GitHubLinkRole:

    DEFAULT_REPO = 'edgedb/edgedb'
    BASE_URL = 'https://github.com/'

    # \x00 means the "<" was backslash-escaped
    explicit_title_re = re.compile(r'^(.+?)\s*(?<!\x00)<(.*?)>$', re.DOTALL)

    link_re = re.compile(
        r'''
            (?:
                (?P<repo>(?:[\w\d\-_]+)/(?:[\w\d\-_]+))
                /
            )?
            (?:
                (?:\#(?P<issue>\d+))
                |
                (?P<commit>[A-Fa-f\d]{8,40})
            )
        ''',
        re.X)

    def __call__(self, role, rawtext, text, lineno, inliner,
                 options=None, content=None):
        if options is None:
            options = {}
        if content is None:
            content = []

        matched = self.explicit_title_re.match(text)
        if matched:
            has_explicit_title = True
            title = d_utils.unescape(matched.group(1))
            target = d_utils.unescape(matched.group(2))
        else:
            has_explicit_title = False
            title = d_utils.unescape(text)
            target = d_utils.unescape(text)

        matched = self.link_re.match(target)
        if not matched:
            raise shared.EdgeSphinxExtensionError(f'cannot parse {rawtext}')

        repo = matched.group('repo')
        explicit_repo = True
        if not repo:
            repo = self.DEFAULT_REPO
            explicit_repo = False

        issue = matched.group('issue')
        commit = matched.group('commit')
        if issue:
            postfix = f'issues/{issue}'
        elif commit:
            postfix = f'commit/{commit}'
            if not has_explicit_title:
                if explicit_repo:
                    title = f'{repo}/{commit[:8]}'
                else:
                    title = f'{commit[:8]}'
        else:
            raise shared.EdgeSphinxExtensionError(f'cannot parse {rawtext}')

        url = f'{self.BASE_URL}{repo}/{postfix}'

        node = d_nodes.reference(refuri=url, name=title)
        node['eql-github'] = True
        node += d_nodes.Text(title)
        return [node], []


class EdgeDBDomain(s_domains.Domain):
    name = "edb"
    label = "EdgeDB"

    directives = {
        'react-element': EDBReactElement,
        'section-intro-page': EDBSectionIntroPage,
        'youtube-embed': EDBYoutubeEmbed
    }

    roles = {
        'gh': GitHubLinkRole(),
    }


def setup_domain(app):
    app.add_domain(EdgeDBDomain)


def setup(app):
    setup_domain(app)
