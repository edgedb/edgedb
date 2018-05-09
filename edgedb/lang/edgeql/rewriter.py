#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2015-present MagicStack Inc. and the EdgeDB authors.
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


from edgedb.lang.common import ast

from edgedb.lang.schema import name as sn

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import codegen as qlcodegen
from edgedb.lang.edgeql import parser as qlparser


def rewrite_refs(expr, callback):
    """Rewrite class references in EdgeQL expression."""

    tree = qlparser.parse_fragment(expr)

    def _cb(node):
        if isinstance(node, qlast.ObjectRef):
            name = sn.Name(name=node.name, module=node.module)
            upd = callback(name)
            if name != upd:
                node.name = upd.name
                node.module = upd.module

    ast.find_children(tree, _cb)

    return qlcodegen.generate_source(tree, pretty=False)
