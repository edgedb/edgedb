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
from docutils import utils as d_utils
from docutils.parsers.rst import roles as d_roles

from sphinx import errors as s_errors


class EdgeSphinxExtensionError(s_errors.ExtensionError):
    pass


class DomainError(EdgeSphinxExtensionError):
    pass


class InlineCodeRole:

    def __init__(self, lang):
        self.lang = lang

    def __call__(self, role, rawtext, text, lineno, inliner,
                 options=None, content=None):
        if options is None:
            options = {}
        if content is None:
            content = []
        d_roles.set_classes(options)
        node = d_nodes.literal(rawtext, d_utils.unescape(text), **options)
        node['eql-lang'] = self.lang
        return [node], []
