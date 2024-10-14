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
from typing import (
    Any,
    Optional,
    Tuple,
    AbstractSet,
    Iterator,
    Mapping,
    Collection,
    List,
    Set,
    FrozenSet,
    NamedTuple,
    Protocol,
    cast,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from typing_extensions import TypeGuard

import sys
import textwrap
import weakref

from edb import errors
from edb.common import span
from edb.common import term
from edb.common.typeutils import not_none

from . import pathid
from . import ast as irast


class WarningContext(Protocol):
    def log_warning(self, warning: errors.EdgeDBError) -> None:
        ...


class FenceInfo(NamedTuple):
    unnest_fence: bool
    factoring_fence: bool

    def __or__(self, other: FenceInfo) -> FenceInfo:
        return FenceInfo(
            unnest_fence=self.unnest_fence or other.unnest_fence,
            factoring_fence=self.factoring_fence or other.factoring_fence,
        )


def has_path_id(nobe: ScopeTreeNode) -> TypeGuard[ScopeTreeNodeWithPathId]:
    return nobe.path_id is not None


class ScopeTreeNode:
    unique_id: Optional[int]
    """A unique identifier used to map scopes on sets."""

    path_id: Optional[pathid.PathId]
    """Node path id, or None for branch nodes."""

    fenced: bool
    """Whether the subtree represents a SET OF argument."""

    warn: bool
    """Whether to warn when paths are factored from beneath two warns."""

    is_group: bool
    """Whether the node reprents a GROUP binding (and so *is* multi...)."""

    unnest_fence: bool
    """Prevent unnesting in parents."""

    factoring_fence: bool
    """Prevent prefix factoring across this node."""

    factoring_allowlist: Set[pathid.PathId]
    """A list of prefixes that are always allowed to be factored."""

    optional: bool
    """Whether this node represents an optional path."""

    children: List[ScopeTreeNode]
    """A set of child nodes."""

    namespaces: Set[pathid.Namespace]
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
        self.unnest_fence = False
        self.factoring_fence = False
        self.warn = False
        self.factoring_allowlist = set()
        self.optional = optional
        self.children = []
        self.namespaces = set()
        self.is_group = False
        self._parent: Optional[weakref.ReferenceType[ScopeTreeNode]] = None

    FIELDS = (
        'unique_id', 'path_id', 'fenced', 'unnest_fence', 'factoring_fence',
        'factoring_allowlist', 'optional', 'children', 'namespaces',
        'is_group',
    )

    def __getstate__(self) -> Any:
        res = self.__dict__.copy()
        del res['_parent']
        return res

    def __setstate__(self, state: Any) -> None:
        for f, val in state.items():
            setattr(self, f, val)
        self._parent = None
        for child in self.children:
            child._parent = weakref.ref(self)

    def __repr__(self) -> str:
        name = 'ScopeFenceNode' if self.fenced else 'ScopeTreeNode'
        return (f'<{name} {self.path_id!r} at {id(self):0x}>')

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
            name = (
                ('FENCE' if self.fenced else 'BRANCH')
            )
        else:
            name = self.path_id.pformat_internal(debug=debug)
        return f'{name}{" [OPT]" if self.optional else ""}'

    def debugname(self, fuller: bool = False) -> str:
        parts = [f'{self._name(debug=fuller)}']
        if self.unique_id:
            parts.append(f'uid:{self.unique_id}')
        if self.namespaces:
            parts.append(','.join(self.namespaces))
        if self.unnest_fence:
            parts.append('no-unnest')
        if self.factoring_fence:
            parts.append('no-factor')
        if self.is_group:
            parts.append('group')
        if self.warn:
            parts.append('warn')
        return ' '.join(parts)

    @property
    def fence_info(self) -> FenceInfo:
        return FenceInfo(
            unnest_fence=self.unnest_fence,
            factoring_fence=self.factoring_fence,
        )

    def fence_info_ex(
        self, path_id: pathid.PathId, namespaces: AbstractSet[str]
    ) -> FenceInfo:
        finfo = self.fence_info
        if any(
            _paths_equal(path_id, wl, namespaces)
            for wl in self.factoring_allowlist
        ):
            finfo = finfo._replace(factoring_fence=False)
        return finfo

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
    def ancestors_and_namespaces(
        self,
    ) -> Iterator[Tuple[ScopeTreeNode, FrozenSet[pathid.Namespace]]]:
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
        return (
            p for p in self.children
            if has_path_id(p)
        )

    @property
    def path_descendants(self) -> Iterator[ScopeTreeNodeWithPathId]:
        """An iterator of node's descendants that have path ids."""
        return (
            p for p in self.descendants
            if has_path_id(p)
        )

    def get_all_paths(self) -> Set[pathid.PathId]:
        return {pd.path_id for pd in self.path_descendants}

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
            if child.parent is self:
                yield from child.strict_descendants

    def descendants_and_namespaces_ex(
        self,
        *,
        unfenced_only: bool=False,
        strict: bool=False,
        skip: Optional[ScopeTreeNode]=None,
    ) -> Iterator[
        Tuple[
            ScopeTreeNode,
            AbstractSet[pathid.Namespace],
            FenceInfo
        ]
    ]:
        """An iterator of node's descendants and namespaces.

        Args:
            unfenced_only:
                Whether to skip traversing through fenced nodes
            strict:
                Whether to skip the node itself
            skip:
                An optional child to skip during the traversal. This
                is useful for avoiding performance pathologies when
                repeatedly searching descendants while climbing the
                tree (see find_factorable_nodes).

        Top-first.
        """
        if not strict:
            yield self, frozenset(), FenceInfo(
                unnest_fence=False, factoring_fence=False)
        for child in tuple(self.children):
            if unfenced_only and child.fenced:
                continue
            if child is skip:
                continue
            finfo = child.fence_info
            yield child, child.namespaces, finfo
            if child.parent is not self:
                continue
            desc_ns = child.descendants_and_namespaces_ex(
                unfenced_only=unfenced_only, strict=True)
            for desc, desc_namespaces, desc_finfo in desc_ns:
                yield (
                    desc,
                    child.namespaces | desc_namespaces,
                    finfo | desc_finfo,
                )

    @property
    def strict_descendants_and_namespaces(
        self,
    ) -> Iterator[
        Tuple[
            ScopeTreeNode,
            AbstractSet[pathid.Namespace],
            FenceInfo
        ]
    ]:
        """An iterator of node's descendants and namespaces.

        Does not include self. Top-first.
        """
        return self.descendants_and_namespaces_ex(strict=True)

    @property
    def descendant_namespaces(self) -> Set[pathid.Namespace]:
        """An set of namespaces declared by descendants."""
        namespaces = set()
        for child in self.descendants:
            namespaces.update(child.namespaces)

        return namespaces

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
            if has_path_id(ancestor):
                return ancestor

        return None

    @property
    def parent_fence(self) -> Optional[ScopeTreeNode]:
        """The nearest strict ancestor fence."""
        for ancestor in self.strict_ancestors:
            if ancestor.fenced:
                return ancestor

        return None

    @property
    def parent_branch(self) -> Optional[ScopeTreeNode]:
        """The nearest strict ancestor branch or fence."""
        for ancestor in self.strict_ancestors:
            if ancestor.path_id is None:
                return ancestor

        return None

    @property
    def root(self) -> ScopeTreeNode:
        """The root of this tree."""
        node = self
        while node.parent is not None:
            node = node.parent
        return node

    def strip_path_namespace(self, ns: AbstractSet[str]) -> None:
        if not ns:
            return
        for pd in self.path_descendants:
            pd.path_id = pd.path_id.strip_namespace(ns)

    def attach_child(
        self, node: ScopeTreeNode, span: Optional[span.Span] = None
    ) -> None:
        """Attach a child node to this node.

        This is a low-level operation, no tree validation is
        performed.  For safe tree modification, use attach_subtree()""
        """
        if node.path_id is not None:
            for child in self.children:
                if child.path_id == node.path_id:
                    raise errors.InvalidReferenceError(
                        f'{node.path_id} is already present in {self!r}',
                        span=span,
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
        span: Optional[span.Span],
        ctx: WarningContext,
    ) -> None:
        """Attach a scope subtree representing *path_id*."""

        subtree = parent = ScopeTreeNode(fenced=True)
        is_lprop = False
        lprop_base = None
        for prefix in reversed(list(path_id.iter_prefixes())):
            new_child = ScopeTreeNode(path_id=prefix,
                                      optional=optional and parent is subtree)

            # Normally the prefix is nested, except that tuple
            # indirection prefixes and the *object* prefixes of link
            # properties are are at the same level.
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
            # And Foo.bar[is Typ]@baz results in:
            #   Foo.bar[is Typ]@baz
            #    |-Foo.bar[is Typ]
            #       |-Foo.bar
            #   Foo
            #
            # For tuples, this is permissable because their fields are always
            # singletons.
            # FIXME: I think that it should not be *necessary* for tuples,
            # but test_edgeql_volatility_select_tuples_* fail if it is changed,
            # I think for incidental reasons.
            #
            # For link properties, this is necessary because referring
            # to a link property at the end of a path suppresses
            # deduplication of the link, which is realized by forcing
            # the link source to be visible. We avoid making the rest of
            # the path visible, to preserve prefix visibility information
            # for certain optimizations. (Foo.bar[is Typ] can be compiled
            # such that it joins directly on Typ (instead of on Bar first),
            # but *only* if Foo.bar isn't visible without the type intersection.
            if prefix.is_linkprop_path():
                assert lprop_base is None
                # If we just saw a linkprop, track where, since we'll
                # need to come back to this level in the tree once we
                # reach the "object prefix" of it.
                lprop_base = parent
                is_lprop = True
            elif is_lprop:
                # Skip through type intersections (i.e [IS Foo]) until
                # we actually get to the link.
                if not prefix.is_type_intersection_path():
                    is_lprop = False
            else:
                # If we've reached the "object prefix" of a path
                # referencing a linkprop, pop back up to the level the
                # linkprop was attached to.
                if lprop_base is not None:
                    parent = lprop_base
                    lprop_base = None

            parent.attach_child(new_child)
            if not prefix.is_tuple_indirection_path():
                parent = new_child

        self.attach_subtree(subtree, span=span, ctx=ctx)

    def attach_subtree(
        self,
        node: ScopeTreeNode,
        was_fenced: bool = False,
        span: Optional[span.Span] = None,
        fusing: bool = False,
        *,
        ctx: WarningContext,
    ) -> None:
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

        for descendant, dns, _ in node.descendants_and_namespaces_ex():
            if not has_path_id(descendant):
                continue

            path_id = descendant.path_id.strip_namespace(dns)
            if descendant.parent_fence is node:
                # Unfenced path.

                # Search for occurences elsewhere in the tree that
                # can be factored with this one.
                # If found, attach that node directly to the factoring point
                # and fuse our node onto it.
                # If there are multiple factorable occurences, we do
                # this iteratively, from closest to furthest away.
                factorable_nodes = self.find_factorable_nodes(path_id)

                current = descendant
                if factorable_nodes:
                    descendant.strip_path_namespace(dns)
                    desc_optional = (
                        descendant.is_optional_upto(node.parent)
                        # Check if there is an optional branch between here
                        # and the *highest* factoring point.
                        or self.is_optional_upto(factorable_nodes[-1][1])
                    )
                    if desc_optional:
                        descendant.mark_as_optional()

                moved = False
                for factorable in factorable_nodes:
                    (
                        existing,
                        factor_point,
                        current_ns,
                        existing_ns,
                        existing_finfo,
                        unnest_fence,
                        node_fenced,
                    ) = factorable

                    self._check_factoring_errors(
                        path_id, descendant, factor_point, existing,
                        unnest_fence, existing_finfo, span,
                    )

                    existing_fenced = existing.parent_fence is not None and (
                        factor_point in existing.parent_fence.strict_ancestors
                    )
                    if existing.is_optional_upto(factor_point):
                        existing.mark_as_optional()

                    current_warn = (
                        current.is_warn_upto(factor_point)
                        or (not moved and self.is_warn_upto(factor_point))
                    )
                    existing_warn = existing.is_warn_upto(factor_point)
                    if current_warn and existing_warn:
                        # Allow factoring single pointers when the src
                        # is visible.
                        #
                        # TODO: If we want this to work on computeds,
                        # we need to we need to register the problem
                        # somewhere and check their cardinality at the
                        # end.
                        if (
                            (src := path_id.src_path())
                            and self.is_visible(src)
                            and (
                                dir := not_none(path_id.rptr()).dir_cardinality(
                                    not_none(path_id.rptr_dir()))
                            )
                            and dir.is_single()
                        ):
                            pass
                        else:
                            ex = errors.DeprecatedScopingError(
                                f'attempting to factor out '
                                f'{path_id.pformat()!r} here',
                                span=span,
                            )
                            ctx.log_warning(ex)
                    if existing_warn:
                        existing.warn = True

                    # Strip the namespaces of everything in the lifted nodes
                    # based on what they have been lifted through.
                    existing.strip_path_namespace(existing_ns)
                    current.strip_path_namespace(current_ns)

                    current.remove()
                    if (
                        factor_point is not existing.parent
                        and factor_point is not existing
                    ):
                        existing.remove()
                        factor_point.attach_child(existing)

                    # Discard the node from the subtree being attached.
                    existing.fuse_subtree(
                        current,
                        self_fenced=existing_fenced,
                        node_fenced=node_fenced,
                        span=span,
                        ctx=ctx,
                    )

                    current = existing
                    moved = True

                    # HACK: If we are being called from fuse_subtree,
                    # skip all but the first. This is because we don't
                    # want to merge any children before the parent
                    # fully finishes all of its factoring.
                    if fusing:
                        break

        for child in tuple(node.children):
            # Attach whatever is remaining in the subtree.
            for pd in child.path_descendants:
                if pd.path_id.namespace:
                    to_strip = set(pd.path_id.namespace) & node.namespaces
                    pd.path_id = pd.path_id.strip_namespace(to_strip)

            self.attach_child(child)

    def _check_factoring_errors(
        self,
        path_id: pathid.PathId,
        descendant: ScopeTreeNodeWithPathId,
        factor_point: ScopeTreeNode,
        existing: ScopeTreeNodeWithPathId,
        unnest_fence: bool,
        existing_finfo: FenceInfo,
        span: Optional[span.Span],
    ) -> None:
        if existing_finfo.factoring_fence:
            # This node is already present in the surrounding
            # scope and cannot be factored out, such as
            # a reference to a correlated set inside a DML
            # statement.
            raise errors.InvalidReferenceError(
                f'cannot reference correlated set '
                f'{path_id.pformat()!r} here',
                span=span,
            )

        if (
            unnest_fence
            and (
                factor_point.find_child(
                    path_id,
                    in_branches=True,
                    pfx_with_invariant_card=True,
                ) is None
            )
            and (
                not (src_path := path_id.src_path())
                or not self.is_visible(src_path)
            )
            and not existing._node_paths_are_not_links()
        ):
            path_ancestor = descendant.path_ancestor
            if path_ancestor is not None:
                offending_node = path_ancestor
            else:
                offending_node = descendant

            assert offending_node.path_id is not None

            imp = ''
            offending_id = f'{offending_node.path_id.pformat()!r}'
            existing_id = f'{existing.path_id.pformat()!r}'
            # If the id is generated, don't leak meaningless info
            # and try to explain that the reference is implicit.
            if '~' in offending_id:
                imp = 'implicit '
                offending_id = 'an object'
                existing_id = 'it'

            raise errors.InvalidReferenceError(
                f'{imp}reference to {offending_id} '
                f'changes the interpretation of {existing_id} '
                f'elsewhere in the query',
                span=span,
            )

    def _node_paths_are_not_links(self) -> bool:
        """
        Check if all the pointers a path might be hoisted past are not links

        If the node is a path_id node, return true if the rptrs on
        all of the chain of parent nodes with path_ids are not links.

        This is in support of allowing queries like
          select Card.element filter Card.name = 'Imp'

        No real change in interpretation happens here, since element
        is a property and so doesn't get deduplicated.
        """

        node: ScopeTreeNode | None = self
        while node and node.path_id:
            if (
                isinstance(node.path_id.rptr(), irast.PointerRef)
                and node.path_id.is_objtype_path()
            ):
                return False
            node = node.parent
        return True

    def fuse_subtree(
        self,
        node: ScopeTreeNode,
        self_fenced: bool=False,
        node_fenced: bool=False,
        span: Optional[span.Span]=None,
        *,
        ctx: WarningContext,
    ) -> None:
        node.remove()

        if not node.optional and not node_fenced:
            self.optional = False
        if node.optional and self_fenced:
            self.optional = True

        if node.path_id is not None:
            subtree = ScopeTreeNode(fenced=True)
            subtree.optional = node.optional
            for child in tuple(node.children):
                subtree.attach_child(child)
        else:
            subtree = node

        self.attach_subtree(
            subtree, was_fenced=self_fenced, span=span, fusing=True, ctx=ctx
        )

    def remove_subtree(self, node: ScopeTreeNode) -> None:
        """Remove the given subtree from this node."""
        if node not in self.children:
            raise KeyError(f'{node} is not a child of {self}')

        node._set_parent(None)

    def remove_descendants(
        self, path_id: pathid.PathId, new: ScopeTreeNode
    ) -> None:
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
        self.optional = True

    def is_optional(self, path_id: pathid.PathId) -> bool:
        node = self.find_visible(path_id)
        if node is not None:
            return node.optional
        else:
            return False

    def add_namespaces(
        self,
        namespaces: AbstractSet[pathid.Namespace],
    ) -> None:
        # Make sure we don't add namespaces that already appear
        # in on of the ancestors.
        namespaces = frozenset(namespaces) - self.get_effective_namespaces()
        self.namespaces.update(namespaces)

    def get_effective_namespaces(self) -> AbstractSet[pathid.Namespace]:
        namespaces: Set[pathid.Namespace] = set()

        for _node, ans in self.ancestors_and_namespaces:
            namespaces |= ans

        return namespaces

    def remove(self) -> None:
        """Remove this node from the tree (subtree becomes independent)."""
        parent = self.parent
        if parent is not None:
            parent.remove_subtree(self)

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
        *,
        allow_group: bool=False,
    ) -> Tuple[
        Optional[ScopeTreeNode],
        FenceInfo,
        AbstractSet[pathid.Namespace],
    ]:
        """Find the visible node with the given *path_id*."""
        namespaces: Set[pathid.Namespace] = set()
        found = None
        nodes: List[ScopeTreeNode] = []
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
                nodes.append(node)

        finfo = FenceInfo(False, False)
        for node in nodes:
            finfo |= node.fence_info_ex(path_id, namespaces)

        if found and found.is_group and not allow_group:
            found = None
        return found, finfo, namespaces

    def find_visible(
        self, path_id: pathid.PathId, *, allow_group: bool = False
    ) -> Optional[ScopeTreeNode]:
        node, _, _ = self.find_visible_ex(path_id, allow_group=allow_group)
        return node

    def is_visible(
        self, path_id: pathid.PathId, *, allow_group: bool = False
    ) -> bool:
        return self.find_visible(path_id, allow_group=allow_group) is not None

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
                (
                    in_branches
                    and child.path_id is None
                    and not child.fenced
                ) or (
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
            if (has_path_id(descendant)
                    and _paths_equal(descendant.path_id, path_id, dns)):
                matched.append(descendant)

        return matched

    def find_descendant_and_ns(self, path_id: pathid.PathId) -> Tuple[
        Optional[ScopeTreeNode],
        AbstractSet[pathid.Namespace],
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
            if node.optional:
                return True
            node = node.parent
        return False

    def is_warn_upto(self, ancestor: Optional[ScopeTreeNode]) -> bool:
        node: Optional[ScopeTreeNode] = self
        while node and node is not ancestor:
            if node.warn:
                return True
            node = node.parent
        return False

    def find_factorable_nodes(
        self,
        path_id: pathid.PathId,
    ) -> List[
        Tuple[
            ScopeTreeNodeWithPathId,
            ScopeTreeNode,
            AbstractSet[pathid.Namespace],
            AbstractSet[pathid.Namespace],
            FenceInfo,
            bool,
            bool,
        ]
    ]:
        """Find nodes factorable with path_id (if attaching path_id to self)

        This is done by searching up the tree looking for an ancestor
        node that has path_id as a descendant such that *at most one*
        of self and the path_id descendant are fenced.

        That descendant, then, is a factorable node, and the ancestor
        is its factoring point.

        We do this by tracking whether we have passed a fence on our
        way up the tree, and only looking for unfenced descendants if
        so.

        We find all such factorable nodes and return them sorted by
        factoring point, from closest to furthest up.
        """
        namespaces: AbstractSet[str] = frozenset()
        unnest_fence_seen = False
        fence_seen = False
        points = []
        up_finfo = FenceInfo(False, False)

        # Track the last seen node so that we can skip it while looking
        # for descendants, to avoid performance pathologies, but also
        # to avoid rediscovering the same nodes when searching higher
        # in the tree.
        last = None

        # Search up the tree
        for node, ans in self.ancestors_and_namespaces:

            # For each ancestor, search its descendants for path_id.
            # If we have passed a fence on the way up, only look for
            # unfenced descendants.
            for descendant, dns, finfo in (
                node.descendants_and_namespaces_ex(
                    unfenced_only=fence_seen, skip=last)
            ):
                cns = namespaces | dns
                if (has_path_id(descendant)
                        and not descendant.is_group
                        and _paths_equal(descendant.path_id, path_id, cns)):
                    points.append((
                        descendant, node, namespaces, dns, finfo | up_finfo,
                        unnest_fence_seen, fence_seen,
                    ))

            namespaces |= ans
            unnest_fence_seen |= node.unnest_fence
            fence_seen |= node.fenced

            if node is not self:
                up_finfo |= node.fence_info_ex(path_id, namespaces)

            last = node

        return points

    def pformat(self) -> str:
        if self.children:
            child_formats = []
            for c in self.children:
                cf = c.pformat()
                if cf:
                    child_formats.append(cf)

            if child_formats:
                children = textwrap.indent(',\n'.join(child_formats), '    ')
                return f'"{self.name}": {{\n{children}\n}}'

        if self.path_id is not None:
            return f'"{self.name}"'
        else:
            return ''

    def pdebugformat(
        self,
        fuller: bool=False,
        styles: Optional[Mapping[ScopeTreeNode, term.AbstractStyle]]=None,
    ) -> str:
        name = f'"{self.debugname(fuller=fuller)}"'
        if styles and self in styles:
            name = styles[self].apply(name)

        if self.children:
            child_formats = []
            for c in self.children:
                cf = c.pdebugformat(fuller=fuller, styles=styles)
                if cf:
                    child_formats.append(cf)

            children = textwrap.indent(',\n'.join(child_formats), '    ')
            return f'{name}: {{\n{children}\n}}'
        else:
            return name

    def dump(self) -> None:
        print(self.pdebugformat())

    def dump_full(self, others: Collection[ScopeTreeNode] = ()) -> None:
        """Do a debug dump of the root but hilight the current node."""
        styles = {}
        if term.supports_colors(sys.stdout.fileno()):
            styles[self] = term.Style16(color='magenta', bold=True)
            for other in others:
                styles[other] = term.Style16(color='blue', bold=True)
        print(self.root.pdebugformat(styles=styles))

    def _set_parent(self, parent: Optional[ScopeTreeNode]) -> None:
        assert self is not parent
        current_parent = self.parent
        if parent is current_parent:
            return

        if current_parent is not None:
            # Make sure no other node refers to us.
            current_parent.children.remove(self)

        if parent is not None:
            self._parent = weakref.ref(parent)
            parent.children.append(self)
        else:
            self._parent = None


class ScopeTreeNodeWithPathId(ScopeTreeNode):

    path_id: pathid.PathId


def _paths_equal(
    path_id_1: pathid.PathId,
    path_id_2: pathid.PathId,
    namespaces: AbstractSet[str],
) -> bool:
    if namespaces:
        path_id_1 = path_id_1.strip_namespace(namespaces)
        path_id_2 = path_id_2.strip_namespace(namespaces)

    return path_id_1 == path_id_2
