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

from typing import Any, Dict

from sphinx import addnodes as s_nodes
from sphinx import directives as s_directives
from sphinx import domains as s_domains


class BaseGoDirective(s_directives.ObjectDescription):

    def get_signatures(self):
        return [re.compile(r'\\\s*\n').sub('\n', self.arguments[0])]

    def add_target_and_index(self, name, sig, signode):
        target = name.replace(' ', '-')

        if target in self.state.document.ids:
            raise self.error(
                f'duplicate {self.objtype} {name} description')

        signode['names'].append(target)
        signode['ids'].append(target)
        self.state.document.note_explicit_target(signode)

        objects = self.env.domaindata['go']['objects']

        if target in objects:
            raise self.error(
                f'duplicate {self.objtype} {name} description')
        objects[target] = (self.env.docname, self.objtype)


class GoTypeDirective(BaseGoDirective):

    def handle_signature(self, sig, signode):
        name = re.split(r'\s+', sig)[1].strip()

        signode['name'] = name
        signode['fullname'] = name

        signode['is_multiline'] = True
        signode += [
            s_nodes.desc_signature_line(sig, line)
            for line in sig.split('\n')
          ]

        return name

    def add_target_and_index(self, name, sig, signode):
        return super().add_target_and_index(name, sig, signode)


goFuncRegex = re.compile(
    r"func\s+(?:\(.+?\s+\*?(?P<receiver>.+?)\)\s+)?(?P<name>.+?)\s*\(")


class GoFunctionDirective(BaseGoDirective):

    def handle_signature(self, sig, signode):
        match = goFuncRegex.match(sig)
        if match is None:
            raise self.error(f'could not parse go func signature: {sig!r}')

        signode['fullname'] = fullname = (
            f"{match.group('receiver')}.{match.group('name')}"
            if match.group('receiver')
            else match.group('name')
        )
        signode['name'] = match.group('name')

        signode['is_multiline'] = True
        signode += [
            s_nodes.desc_signature_line(sig, line)
            for line in sig.split('\n')
          ]

        return fullname

    def add_target_and_index(self, name, sig, signode):
        return super().add_target_and_index(name, sig, signode)


class GoMethodDirective(GoFunctionDirective):
    pass


class GolangDomain(s_domains.Domain):

    name = "go"
    label = "Golang"

    object_types = {
        'function': s_domains.ObjType('function'),
        'type': s_domains.ObjType('type'),
    }

    directives = {
        'function': GoFunctionDirective,
        'type': GoTypeDirective,
        'method': GoMethodDirective,
    }

    initial_data: Dict[str, Dict[str, Any]] = {
        'objects': {}  # fullname -> docname, objtype
    }

    def clear_doc(self, docname):
        for fullname, (fn, _l) in list(self.data['objects'].items()):
            if fn == docname:
                del self.data['objects'][fullname]

    def merge_domaindata(self, docnames, otherdata):
        for fullname, (fn, objtype) in otherdata['objects'].items():
            if fn in docnames:
                self.data['objects'][fullname] = (fn, objtype)

    def get_objects(self):
        for refname, (docname, type) in self.data['objects'].items():
            yield (refname, refname, type, docname, refname, 1)

    def get_full_qualified_name(self, node):
        fn = node.get('fullname')
        if not fn:
            raise self.error('no fullname attribute')
        return fn


def setup_domain(app):
    app.add_domain(GolangDomain)


def setup(app):
    setup_domain(app)
