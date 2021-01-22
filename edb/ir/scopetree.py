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


"""Query scope tree implementation."""

from __future__ import annotations
from typing import *

import textwrap
import weakref

from edb import errors
from edb.common import context as pctx
from . import pathid


class FenceInfo(NamedTuple):
    unnest_fence: bool
    factoring_fence: bool

    def __or__(self, other: FenceInfo) -> FenceInfo:
        return FenceInfo(
            unnest_fence=self.unnest_fence or other.unnest_fence,
            factoring_fence=self.factoring_fence or other.factoring_fence,
        )


class ScopeTreeNode:
    unique_id: Optional[int]
    """A unique identifier used to map scopes on sets."""

    path_id: Optional[pathid.PathId]
    """Node path id, or None for branch nodes."""

    fenced: bool
    """Whether the subtree represents a SET OF argument."""

    protect_parent: bool
    """Whether the subtree represents a scope that must not affect parents."""

    unnest_fence: bool
    """Prevent unnesting in parents."""

    factoring_fence: bool
    """Prevent prefix factoring across this node."""

    factoring_allowlist: Set[pathid.PathId]
    """A list of prefixes that are always allowed to be factored."""

    optional_count: Optional[int]
    """Whether this node represents an optional path.

    If None, the node is not an optional path. If 0, it is.
    If positive, the node *was* an optional path but has had non-optional
    nodes merged into it, but could become optional again.
    """

    children: Set[ScopeTreeNode]
    """A set of child nodes."""

    namespaces: Set[pathid.AnyNamespace]
    """A set of namespaces used by paths in this branch.

    When a path node is pulled up from this branch,
    and its namespace matches anything in `namespaces`,
    the namespace will be stripped.  This is used to
    implement "semi-detached" semantics used by
    aliases declared in a WITH block."""

    def __init__(
        self,
        *,
        path_id: Optional[pathid.PathId]=None,
        fenced: bool=False,
        unique_id: Optional[int]=None,
        optional: bool=False,
    ) -> None:
        self.unique_id = unique_id
        self.path_id = path_id
        self.fenced = fenced
        self.protect_parent = False
        self.unnest_fence = False
        self.factoring_fence = False
        self.factoring_allowlist = set()
        self.optional_count = 0 if optional else None
        self.children = set()
        self.namespaces = set()
        self._parent: Optional[weakref.ReferenceType[ScopeTreeNode]] = None

    def __repr__(self) -> str:
        name = 'ScopeFenceNode' if self.fenced else 'ScopeTreeNode'
        return (f'<{name} {self.path_id!r} at {id(self):0x}>')

    def _copy(self, parent: Optional[ScopeTreeNode]) -> ScopeTreeNode:
        cp = self.__class__(
            path_id=self.path_id,
            fenced=self.fenced)
        cp.optional_count = self.optional_count
        cp.unnest_fence = self.unnest_fence
        cp.factoring_fence = self.factoring_fence
        cp.namespaces = set(self.namespaces)
        cp.unique_id = self.unique_id
        cp.factoring_allowlist = set(self.factoring_allowlist)
        cp.protect_parent = self.protect_parent
        cp._set_parent(parent)

        for child in self.children:
            child._copy(parent=cp)

        return cp

    def find_dupe_unique_ids(self) -> Set[int]:
        seen = set()
        dupes = set()
        for node in self.root.descendants:
            if node.unique_id is not None:
                if node.unique_id in seen:
                    dupes.add(node.unique_id)
                seen.add(node.unique_id)
        return dupes

    def validate_unique_ids(self) -> None:
        dupes = self.find_dupe_unique_ids()
        assert not dupes, f'Duplicate "unique" ids seen {dupes}'

    @property
    def name(self) -> str:
        return self._name(debug=False)

    def _name(self, debug: bool) -> str:
        if self.path_id is None:
            name = f'FENCE' if self.fenced else f'BRANCH'
        else:
            name = self.path_id.pformat_internal(debug=debug)
        ocount = self.optional_count
        return (
            f'{name}{" [OPT]" if self.optional else ""}'
            f'{" ["+str(ocount)+"]" if ocount else ""}'
        )

    def debugname(self, fuller: bool=False) -> str:
        parts = [f'{self._name(debug=fuller)}']
        if self.unique_id:
            parts.append(f'uid:{self.unique_id}')
        if self.namespaces:
            parts.append(','.join(self.namespaces))
        if self.unnest_fence:
            parts.append('no-unnest')
        if self.factoring_fence:
            parts.append('no-factor')
        parts.append(f'0x{id(self):0x}')
        return ' '.join(parts)

    @property
    def optional(self) -> bool:
        return self.optional_count == 0

    @property
    def fence_info(self) -> FenceInfo:
        return FenceInfo(
            unnest_fence=self.unnest_fence,
            factoring_fence=self.factoring_fence,
        )

    @property
    def ancestors(self) -> Iterator[ScopeTreeNode]:
        """An iterator of node's ancestors, including self."""
        node: Optional[ScopeTreeNode] = self
        while node is not None:
            yield node
            node = node.parent

    @property
    def strict_ancestors(self) -> Iterator[ScopeTreeNode]:
        """An iterator of node's ancestors, not including self."""
        node: Optional[ScopeTreeNode] = self.parent
        while node is not None:
            yield node
            node = node.parent

    @property
    def ancestors_and_namespaces(self) \
            -> Iterator[Tuple[ScopeTreeNode, FrozenSet[pathid.AnyNamespace]]]:
        """An iterator of node's ancestors and namespaces, including self."""
        namespaces: FrozenSet[str] = frozenset()
        node: Optional[ScopeTreeNode] = self
        while node is not None:
            namespaces |= node.namespaces
            yield node, namespaces
            node = node.parent

    @property
    def path_children(self) -> Iterator[ScopeTreeNodeWithPathId]:
        """An iterator of node's children that have path ids."""
        return filter(
            lambda p: p.path_id is not None,
            self.children,  # type: ignore
        )

    @property
    def path_descendants(self) -> Iterator[ScopeTreeNodeWithPathId]:
        """An iterator of node's descendants that have path ids."""
        return filter(
            lambda p: p.path_id is not None,
            self.descendants,  # type: ignore
        )

    def get_all_paths(self) -> Set[pathid.PathId]:
        paths = set()

        if self.path_id:
            paths.add(self.path_id)
        else:
            paths.update(p.path_id for p in self.path_children)

        return paths

    @property
    def descendants(self) -> Iterator[ScopeTreeNode]:
        """An iterator of node's descendants including self top-first."""
        yield self
        yield from self.strict_descendants

    @property
    def strict_descendants(self) -> Iterator[ScopeTreeNode]:
        """An iterator of node's descendants not including self top-first."""
        for child in tuple(self.children):
            yield child
            yield from child.strict_descendants

    @property
    def strict_descendants_and_namespaces(
        self,
    ) -> Iterator[
        Tuple[
            ScopeTreeNode,
            AbstractSet[pathid.AnyNamespace],
            FenceInfo
        ]
    ]:
        """An iterator of node's descendants and namespaces.

        Does not include self. Top-first.
        """
        for child in tuple(self.children):
            finfo = child.fence_info
            yield child, child.namespaces, finfo
            desc_ns = child.strict_descendants_and_namespaces
            for desc, desc_namespaces, desc_finfo in desc_ns:
                yield (
                    desc,
                    child.namespaces | desc_namespaces,
                    finfo | desc_finfo,
                )

    @property
    def descendant_namespaces(self) -> Set[pathid.AnyNamespace]:
        """An set of namespaces declared by descendants."""
        namespaces = set()
        for child in self.descendants:
            namespaces.update(child.namespaces)

        return namespaces

    @property
    def unfenced_descendants(self) -> Iterator[ScopeTreeNode]:
        """An iterator of node's unfenced descendants including self."""
        yield self
        for child in tuple(self.children):
            if not child.fenced:
                yield from child.unfenced_descendants

    @property
    def strict_unfenced_descendants(self) -> Iterator[ScopeTreeNode]:
        """An iterator of node's unfenced descendants."""
        for child in tuple(self.children):
            if not child.fenced:
                yield from child.unfenced_descendants

    @property
    def fence(self) -> ScopeTreeNode:
        """The nearest ancestor fence (or self, if fence)."""
        if self.fenced:
            return self
        else:
            return cast(ScopeTreeNode, self.parent_fence)

    @property
    def parent(self) -> Optional[ScopeTreeNode]:
        """The parent node."""
        if self._parent is None:
            return None
        else:
            return self._parent()

    @property
    def path_ancestor(self) -> Optional[ScopeTreeNodeWithPathId]:
        for ancestor in self.strict_ancestors:
            if ancestor.path_id is not None:
                return cast(ScopeTreeNodeWithPathId, ancestor)

        return None

    @property
    def parent_fence(self) -> Optional[ScopeTreeNode]:
        """The nearest strict ancestor fence."""
        for ancestor in self.strict_ancestors:
            if ancestor.fenced:
                return ancestor

        return None

    @property
    def root(self) -> ScopeTreeNode:
        """The root of this tree."""
        node = self
        while node.parent is not None:
            node = node.parent
        return node

    def attach_child(self, node: ScopeTreeNode,
                     context: Optional[pctx.ParserContext]=None) -> None:
        """Attach a child node to this node.

        This is a low-level operation, no tree validation is
        performed.  For safe tree modification, use attach_subtree()""
        """
        if node.path_id is not None:
            for child in self.children:
                if child.path_id == node.path_id:
                    raise errors.InvalidReferenceError(
                        f'{node.path_id} is already present in {self!r}',
                        context=context,
                    )

        if node.unique_id is not None:
            for child in self.children:
                if child.unique_id == node.unique_id:
                    return

        node._set_parent(self)

    def attach_fence(self) -> ScopeTreeNode:
        """Create and attach an empty fenced node."""
        fence = ScopeTreeNode(fenced=True)
        self.attach_child(fence)
        return fence

    def attach_branch(self) -> ScopeTreeNode:
        """Create and attach an empty branch node."""
        fence = ScopeTreeNode()
        self.attach_child(fence)
        return fence

    def attach_path(
        self,
        path_id: pathid.PathId,
        *,
        optional: bool=False,
        context: Optional[pctx.ParserContext],
        fence_points: FrozenSet[pathid.PathId] = frozenset(),
    ) -> List[ScopeTreeNode]:
        """Attach a scope subtree representing *path_id*."""

        subtree = parent = ScopeTreeNode(fenced=True)
        is_lprop = False
        fences = []

        for prefix in reversed(list(path_id.iter_prefixes(include_ptr=True))):
            if prefix.is_ptr_path():
                is_lprop = True
                continue

            new_child = ScopeTreeNode(path_id=prefix,
                                      optional=optional and parent is subtree)
            parent.attach_child(new_child)

            # If the path is a link property, or a tuple
            # indirection, then its prefix is added at
            # the *same* scope level, otherwise, the prefix
            # is nested.
            #
            # For example, Foo.bar.baz, where Foo is an object type,
            # forms this scope shape:
            #   Foo.bar.baz
            #    |-Foo.bar
            #       |-Foo
            #
            # Whereas, <tuple>.bar.baz results in this:
            #   <tuple>
            #   <tuple>.bar
            #   <tuple>.bar.baz
            #
            # This is because both link properties and tuples are
            # *always* singletons, and so there is no semantic ambiguity
            # as to the cardinality of the path prefix in different
            # contexts.
            #
            # We could include other cases with invariant cardinality here,
            # like type intersection, but we want to preserve the prefix
            # visibility information for the sake of possible optimizations.
            if (
                not (is_lprop or prefix.is_linkprop_path())
                and not prefix.is_tuple_indirection_path()
            ):
                parent = new_child

            # Skip through type intersections (i.e [IS Foo]) until
            # we actually get to the link.
            if not prefix.is_type_intersection_path():
                is_lprop = False

            if prefix in fence_points:
                fence = ScopeTreeNode()
                fences.append(fence)
                parent.attach_child(fence)
                parent = fence

        self.attach_subtree(subtree, context=context)
        return fences

    def attach_subtree(self, node: ScopeTreeNode,
                       was_fenced: bool=False,
                       context: Optional[pctx.ParserContext]=None) -> None:
        """Attach a subtree to this node.

        *node* is expected to be a balanced scope tree and may be modified
        by this function.

        If *node* is not a path node (path_id is None), it is discarded,
        and it's descendants are attached directly.  The tree balance is
        maintained.
        """
        if node.path_id is not None:
            # Wrap path node
            wrapper_node = ScopeTreeNode(fenced=True)
            wrapper_node.attach_child(node)
            node = wrapper_node

        dns = node.descendant_namespaces

        for descendant in node.path_descendants:
            path_id = descendant.path_id.strip_namespace(dns)
            visible, visible_finfo = self.find_visible_ex(path_id)
            desc_optional = descendant.is_optional_upto(node.parent)
            if visible is not None:
                if visible_finfo is not None and visible_finfo.factoring_fence:
                    # This node is already present in the surrounding
                    # scope and cannot be factored out, such as
                    # a reference to a correlated set inside a DML
                    # statement.
                    raise errors.InvalidReferenceError(
                        f'cannot reference correlated set '
                        f'{path_id.pformat()!r} here',
                        context=context,
                    )

                # This path is already present in the tree, discard,
                # but merge its OPTIONAL status, if any.
                desc_fenced = descendant.fence is not node.fence or was_fenced
                descendant.remove()
                keep_optional = desc_optional or desc_fenced
                if (
                    visible.optional_count is not None
                    and not keep_optional
                ):
                    visible.optional_count += 1

            elif descendant.parent_fence is node:
                # Unfenced path.
                # First, find any existing descendant with the same path_id.
                # If not found, find any _unfenced_ node that is a child of
                # any of our ancestors.
                # If found, attach the node directly to its parent fence
                # and remove all other occurrences.
                existing, existing_ns, existing_finfo = (
                    self.find_descendant_and_ns(path_id))
                if (existing is not None and existing_finfo is not None
                        and existing_finfo.factoring_fence):
                    # This node is already present in the surrounding
                    # scope and cannot be factored out, such as
                    # a reference to a correlated set inside a DML
                    # statement.
                    raise errors.InvalidReferenceError(
                        f'cannot reference correlated set '
                        f'{path_id.pformat()!r} here',
                        context=context,
                    )

                unnest_fence = False
                parent_fence = None
                if existing is None:
                    existing, unnest_fence = self.find_unfenced(path_id)
                    if existing is not None:
                        parent_fence = existing.parent_fence
                    existing_fenced = False
                else:
                    existing_fenced = (
                        existing.parent_fence is not self.fence)
                    parent_fence = self.fence

                if existing is not None and parent_fence is not None:
                    if existing.is_optional_upto(parent_fence):
                        existing.mark_as_optional()
                    if desc_optional:
                        descendant.mark_as_optional()

                    if parent_fence.find_child(path_id) is None:
                        assert existing.path_id is not None

                        if (
                            unnest_fence
                            and (
                                parent_fence.find_child(
                                    path_id,
                                    in_branches=True,
                                    pfx_with_invariant_card=True,
                                ) is None
                            )
                            and (
                                not path_id.is_type_intersection_path()
                                or (
                                    (src_path := path_id.src_path())
                                    and src_path is not None
                                    and not self.is_visible(src_path)
                                )
                            )
                        ):
                            path_ancestor = descendant.path_ancestor
                            if path_ancestor is not None:
                                offending_node = path_ancestor
                            else:
                                offending_node = descendant

                            assert offending_node.path_id is not None

                            raise errors.InvalidReferenceError(
                                f'reference to '
                                f'{offending_node.path_id.pformat()!r} '
                                f'changes the interpretation of '
                                f'{existing.path_id.pformat()!r} '
                                f'elsewhere in the query',
                                context=context,
                            )

                        parent_fence.remove_descendants(path_id)
                        node.remove_descendants(path_id)
                        existing.path_id = existing.path_id.strip_namespace(
                            existing_ns)
                        parent_fence.attach_child(existing)

                    # Discard the node from the subtree being attached.
                    existing.fuse_subtree(descendant,
                                          self_fenced=existing_fenced)

        for child in tuple(node.children):
            # Attach whatever is remaining in the subtree.
            for pd in child.path_descendants:
                if pd.path_id.namespace:
                    to_strip = set(pd.path_id.namespace) & node.namespaces
                    pd.path_id = pd.path_id.strip_namespace(to_strip)

            self.attach_child(child)

    def fuse_subtree(
        self,
        node: ScopeTreeNode,
        self_fenced: bool=False,
    ) -> None:
        node.remove()

        if (
            self.optional_count is not None
            and not node.optional
        ):
            self.optional_count += 1
        if node.optional and self_fenced:
            self.mark_as_optional()

        if node.path_id is not None:
            subtree = ScopeTreeNode(fenced=True)
            subtree.optional_count = node.optional_count
            for child in tuple(node.children):
                subtree.attach_child(child)
        else:
            subtree = node

        self.attach_subtree(subtree, was_fenced=self_fenced)

    def remove_subtree(self, node: ScopeTreeNode) -> None:
        """Remove the given subtree from this node."""
        if node not in self.children:
            raise KeyError(f'{node} is not a child of {self}')

        node._set_parent(None)

    def remove_descendants(self, path_id: pathid.PathId) -> None:
        """Remove all descendant nodes matching *path_id*."""

        matching = set()

        for node in self.descendants:
            if (node.path_id is not None
                    and _paths_equal(node.path_id, path_id, set())):
                matching.add(node)

        for node in matching:
            node.remove()

    def mark_as_optional(self) -> None:
        """Indicate that this scope is used as an OPTIONAL argument."""
        self.optional_count = 0

    def is_optional(self, path_id: pathid.PathId) -> bool:
        node = self.find_visible(path_id)
        if node is not None:
            return node.optional
        else:
            return False

    def add_namespaces(
        self,
        namespaces: AbstractSet[pathid.AnyNamespace],
    ) -> None:
        # Make sure we don't add namespaces that already appear
        # in on of the ancestors.
        namespaces = frozenset(namespaces) - self.get_effective_namespaces()
        self.namespaces.update(namespaces)

    def get_effective_namespaces(self) -> AbstractSet[pathid.AnyNamespace]:
        namespaces: Set[pathid.AnyNamespace] = set()

        for _node, ans in self.ancestors_and_namespaces:
            namespaces |= ans

        return namespaces

    def remove(self) -> None:
        """Remove this node from the tree (subtree becomes independent)."""
        parent = self.parent
        if parent is not None:
            parent.remove_subtree(self)

    def collapse(self) -> None:
        """Remove the node, reattaching the children to the parent."""
        parent = self.parent
        if parent is None:
            raise ValueError('cannot collapse the root node')

        if self.path_id is not None:
            subtree = ScopeTreeNode()

            for child in self.children:
                subtree.attach_child(child)
        else:
            subtree = self

        self.remove()
        parent.attach_subtree(subtree)

    def unfence(self) -> ScopeTreeNode:
        """Remove the node, reattaching the children as an unfenced branch."""
        parent = self.parent
        if parent is None:
            raise ValueError('cannot unfence the root node')

        subtree = ScopeTreeNode(optional=self.optional)

        for child in list(self.children):
            subtree.attach_child(child)

        self.remove()

        parent_subtree = ScopeTreeNode(fenced=True)
        parent_subtree.attach_child(subtree)

        parent.attach_subtree(parent_subtree)

        return subtree

    def is_empty(self) -> bool:
        if self.path_id is not None:
            return False
        else:
            return (
                not self.children or
                all(c.is_empty() for c in self.children)
            )

    def get_all_visible(self) -> Set[pathid.PathId]:
        paths = set()

        for node in self.ancestors:
            if node.path_id:
                paths.add(node.path_id)
            else:
                for c in node.children:
                    if c.path_id:
                        paths.add(c.path_id)

        return paths

    def find_visible_ex(
        self,
        path_id: pathid.PathId,
    ) -> Tuple[Optional[ScopeTreeNode], Optional[FenceInfo]]:
        """Find the visible node with the given *path_id*."""
        namespaces: Set[pathid.AnyNamespace] = set()
        finfo = None
        found = None

        for node, ans in self.ancestors_and_namespaces:
            if (node.path_id is not None
                    and _paths_equal(node.path_id, path_id, namespaces)):
                found = node
                break

            for child in node.children:
                if (child.path_id is not None
                        and _paths_equal(child.path_id, path_id, namespaces)):
                    found = child
                    break

            if found is not None:
                break

            namespaces |= ans

            if node is not self:
                ans_finfo = node.fence_info
                parent_fence = node.parent_fence
                if (parent_fence is not None
                        and any(_paths_equal(path_id, wl, namespaces)
                                for wl in parent_fence.factoring_allowlist)):
                    ans_finfo = FenceInfo(
                        unnest_fence=ans_finfo.unnest_fence,
                        factoring_fence=False,
                    )

                if finfo is None:
                    finfo = ans_finfo
                else:
                    finfo = finfo | ans_finfo

        return found, finfo

    def find_visible(self, path_id: pathid.PathId) -> Optional[ScopeTreeNode]:
        node, _ = self.find_visible_ex(path_id)
        return node

    def is_visible(self, path_id: pathid.PathId) -> bool:
        return self.find_visible(path_id) is not None

    def is_any_prefix_visible(self, path_id: pathid.PathId) -> bool:
        for prefix in reversed(list(path_id.iter_prefixes())):
            if self.find_visible(prefix) is not None:
                return True

        return False

    def find_child(
        self,
        path_id: pathid.PathId,
        *,
        in_branches: bool = False,
        pfx_with_invariant_card: bool = False,
    ) -> Optional[ScopeTreeNode]:
        for child in self.children:
            if child.path_id == path_id:
                return child
            if (
                in_branches and child.path_id is None and not child.fenced
                or (
                    pfx_with_invariant_card
                    and child.path_id is not None
                    # Type intersections have invariant cardinality
                    # regardless of prefix visiblity.
                    and child.path_id.is_type_intersection_path()
                )
            ):
                desc = child.find_child(
                    path_id,
                    in_branches=True,
                    pfx_with_invariant_card=pfx_with_invariant_card,
                )
                if desc is not None:
                    return desc

        return None

    def find_descendant(
        self,
        path_id: pathid.PathId,
    ) -> Optional[ScopeTreeNode]:
        for descendant, dns, _ in self.strict_descendants_and_namespaces:
            if (descendant.path_id is not None
                    and _paths_equal(descendant.path_id, path_id, dns)):
                return descendant

        return None

    def find_descendants(
        self,
        path_id: pathid.PathId,
    ) -> List[ScopeTreeNodeWithPathId]:
        matched = []
        for descendant, dns, _ in self.strict_descendants_and_namespaces:
            if (descendant.path_id is not None
                    and _paths_equal(descendant.path_id, path_id, dns)):
                matched.append(cast(ScopeTreeNodeWithPathId, descendant))

        return matched

    def find_descendant_and_ns(
        self,
        path_id: pathid.PathId
    ) -> Tuple[
        Optional[ScopeTreeNode],
        AbstractSet[pathid.AnyNamespace],
        Optional[FenceInfo],
    ]:
        for descendant, dns, finfo in self.strict_descendants_and_namespaces:
            if (descendant.path_id is not None
                    and _paths_equal(descendant.path_id, path_id, dns)):
                return descendant, dns, finfo

        return None, frozenset(), None

    def is_optional_upto(self, ancestor: Optional[ScopeTreeNode]) -> bool:
        node: Optional[ScopeTreeNode] = self
        while node and node is not ancestor:
            if node.optional_count is not None:
                return True
            node = node.parent
        return False

    def find_unfenced(self, path_id: pathid.PathId) \
            -> Tuple[Optional[ScopeTreeNode], bool]:
        """Find the unfenced node with the given *path_id*."""
        namespaces: Set[str] = set()
        unnest_fence_seen = False

        for node, ans in self.ancestors_and_namespaces:
            for descendant in node.unfenced_descendants:
                if (descendant.path_id is not None
                        and _paths_equal(descendant.path_id,
                                         path_id, namespaces)):
                    return descendant, unnest_fence_seen

            namespaces |= ans
            unnest_fence_seen = unnest_fence_seen or node.unnest_fence

        return None, unnest_fence_seen

    def find_by_unique_id(self, unique_id: int) -> Optional[ScopeTreeNode]:
        for node in self.descendants:
            if node.unique_id == unique_id:
                return node

        return None

    def copy(self) -> ScopeTreeNode:
        """Return a complete copy of this subtree."""
        return self._copy(parent=None)

    def copy_all(self) -> Tuple[ScopeTreeNode, ScopeTreeNode]:
        """Make a copy the entire tree and return the copy of this node."""
        if self.unique_id is None:
            self.unique_id = -1
        new = self.root.copy()
        me = new.find_by_unique_id(self.unique_id)
        assert me
        if self.unique_id == -1:
            self.unique_id = None
        return new, me

    def pformat(self) -> str:
        if self.children:
            child_formats = []
            for c in self.children:
                cf = c.pformat()
                if cf:
                    child_formats.append(cf)

            if child_formats:
                child_formats = sorted(child_formats)
                children = textwrap.indent(',\n'.join(child_formats), '    ')
                return f'"{self.name}": {{\n{children}\n}}'

        if self.path_id is not None:
            return f'"{self.name}"'
        else:
            return ''

    def pdebugformat(self, fuller: bool=False) -> str:
        if self.children:
            child_formats = []
            for c in self.children:
                cf = c.pdebugformat()
                if cf:
                    child_formats.append(cf)

            child_formats = sorted(child_formats)
            children = textwrap.indent(',\n'.join(child_formats), '    ')
            return f'"{self.debugname(fuller=fuller)}": {{\n{children}\n}}'
        else:
            return f'"{self.debugname(fuller=fuller)}"'

    def _set_parent(self, parent: Optional[ScopeTreeNode]) -> None:
        current_parent = self.parent
        if parent is current_parent:
            return

        if current_parent is not None:
            # Make sure no other node refers to us.
            current_parent.children.remove(self)

        if parent is not None:
            self._parent = weakref.ref(parent)
            parent.children.add(self)
        else:
            self._parent = None


class ScopeTreeNodeWithPathId(ScopeTreeNode):

    path_id: pathid.PathId


def _paths_equal(path_id_1: pathid.PathId, path_id_2: pathid.PathId,
                 namespaces: AbstractSet[str]) -> bool:
    if namespaces:
        path_id_1 = path_id_1.strip_namespace(namespaces)
        path_id_2 = path_id_2.strip_namespace(namespaces)

    return path_id_1 == path_id_2
