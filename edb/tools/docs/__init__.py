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
from sphinx import transforms as s_transforms

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


def setup(app):
    cli.setup_domain(app)
    eql.setup_domain(app)
    js.setup_domain(app)
    sdl.setup_domain(app)
    graphql.setup_domain(app)

    app.add_transform(ProhibitedNodeTransform)
