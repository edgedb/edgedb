#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

from edb.common import typeutils

from . import base
from . import visitor


class NodeTransformer(visitor.NodeVisitor):
    """Walks the abstract syntax tree and allows modification of nodes.

    The `NodeTransformer` will walk the AST and use the return value of the
    visitor methods to replace or remove the old node.  If the return value of
    the visitor method is ``None``, the node will be removed from its location,
    otherwise it is replaced with the return value.  The return value may be
    the original node in which case no replacement takes place.

    Here is an example transformer that rewrites all occurrences of name
    lookups (``foo``) to ``data['foo']``::

       class RewriteName(NodeTransformer):

           def visit_Name(self, node):
               return copy_location(Subscript(
                   value=Name(id='data', ctx=Load()),
                   slice=Index(value=Str(s=node.id)),
                   ctx=node.ctx
               ), node)

    Keep in mind that if the node you're operating on has child nodes you must
    either transform the child nodes yourself or call the :meth:`generic_visit`
    method for the node first.

    For nodes that were part of a collection of statements (that applies to all
    statement nodes), the visitor may also return a list of nodes rather than
    just a single node.

    Usually you use the transformer like this::

       node = YourTransformer().visit(node)
    """

    def generic_visit(self, node):
        if isinstance(node, base.ImmutableASTMixin):
            changes = {}

            for field, old_value in base.iter_fields(node, include_meta=False):
                old_value = getattr(node, field, None)

                if typeutils.is_container(old_value):
                    new_values = old_value.__class__(self.visit(old_value))
                    changes[field] = old_value.__class__(new_values)

                elif isinstance(old_value, base.AST):
                    new_node = self.visit(old_value)
                    if new_node is not old_value:
                        changes[field] = new_node

            node = node.replace(**changes)

        else:
            for field, old_value in base.iter_fields(node, include_meta=False):
                old_value = getattr(node, field, None)

                if typeutils.is_container(old_value):
                    new_values = old_value.__class__(self.visit(old_value))
                    setattr(node, field, old_value.__class__(new_values))

                elif isinstance(old_value, base.AST):
                    new_node = self.visit(old_value)
                    if new_node is not old_value:
                        setattr(node, field, new_node)

        return node
