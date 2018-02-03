##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import textwrap
import typing
import weakref

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import types as s_types


class PathId:
    """Unique identifier of a path in an expression."""

    __slots__ = ('_path', '_norm_path', '_namespace', '_is_ptr')

    def __init__(self, initializer=None, *, namespace=None):
        if isinstance(initializer, PathId):
            self._path = initializer._path
            self._norm_path = initializer._norm_path
            if namespace is not None:
                self._namespace = namespace
            else:
                self._namespace = initializer._namespace
            self._is_ptr = initializer._is_ptr
        elif initializer is not None:
            if not isinstance(initializer, s_types.Type):
                raise ValueError(
                    f'invalid PathId: bad source: {initializer!r}')
            self._path = (initializer,)
            if (initializer.is_view() and
                    initializer.peel_view().name == initializer.name):
                # The initializer is a view that aliases its base type.
                self._norm_path = (initializer.peel_view(),)
            else:
                self._norm_path = (initializer,)
            self._namespace = namespace
            self._is_ptr = False
        else:
            self._path = ()
            self._norm_path = ()
            self._namespace = namespace
            self._is_ptr = False

    def __hash__(self):
        return hash((self.__class__, self._norm_path, self._namespace,
                     self._is_ptr))

    def __eq__(self, other):
        if not isinstance(other, PathId):
            return NotImplemented

        return (
            self._norm_path == other._norm_path and
            self._namespace == other._namespace and
            self._is_ptr == other._is_ptr
        )

    def __len__(self):
        return len(self._path)

    def __getitem__(self, n):
        if not isinstance(n, slice):
            return self._path[n]
        else:
            # Validate that slicing results in a
            # valid PathId, it must not produce a path ending
            # with a pointer spec.  Slicing off the start
            # is not allowed either.
            if (n.start != 0 and n.start is not None or
                    (n.step is not None and n.step != 1)):
                raise KeyError(f'invalid PathId slice: {n!r}')

            stop_point = self._path[n.stop - 1:n.stop]
            if stop_point and isinstance(stop_point[0], tuple):
                raise KeyError(f'invalid PathId slice: {n!r}')

            result = self.__class__()
            result._path = self._path[n]
            result._norm_path = self._norm_path[n]
            result._namespace = self._namespace

            if n.stop < len(self) and self._norm_path[n.stop][2]:
                # A link property ref has been chopped off.
                result._is_ptr = True

            return result

    def __str__(self):
        result = ''

        if not self._path:
            return ''

        if self._namespace:
            result += f'{self._namespace}@@'

        path = self._norm_path

        result += f'({path[0].name})'

        for i in range(1, len(path) - 1, 2):
            ptr = path[i][0]
            ptrdir = path[i][1]
            is_lprop = path[i][2]
            tgt = path[i + 1]

            if tgt:
                lexpr = f'({ptr})[IS {tgt.name}]'
            else:
                lexpr = f'({ptr})'

            if is_lprop:
                step = '@'
            else:
                step = f'.{ptrdir}'

            result += f'{step}{lexpr}'

        if self._is_ptr:
            result += '@'

        return result

    __repr__ = __str__

    def pformat(self):
        """Pretty PathId format for user-visible messages."""
        result = ''

        if not self._path:
            return ''

        path = self._path

        result += f'{path[0].shortname.name}'

        for i in range(1, len(path) - 1, 2):
            ptr = path[i][0]
            ptrdir = path[i][1]
            is_lprop = isinstance(ptr, s_lprops.LinkProperty)

            lexpr = f'{ptr.shortname.name}'

            if is_lprop:
                step = '@'
            else:
                step = '.'
                if ptrdir == s_pointers.PointerDirection.Inbound:
                    step += ptrdir

            result += f'{step}{lexpr}'

        if self._is_ptr:
            result += '@'

        return result

    def rptr(self):
        if len(self) > 1:
            return self[-2][0]
        else:
            return None

    def rptr_dir(self):
        if len(self) > 1:
            return self[-2][1]
        else:
            return None

    def rptr_name(self):
        rptr = self.rptr()
        if rptr is not None:
            return rptr.shortname
        else:
            return None

    def src_path(self):
        if len(self) > 1:
            return self[:-2]
        else:
            return None

    def tgt_path(self):
        if not self._is_ptr:
            return self
        else:
            result = self.__class__(self)
            result._is_ptr = False
            return result

    def iter_prefixes(self, include_ptr=False):
        yield self[:1]

        for i in range(1, len(self) - 1, 2):
            path_id = self[:i + 2]
            if path_id.is_ptr_path():
                yield path_id.tgt_path()
                if include_ptr:
                    yield path_id
            else:
                yield path_id

    def startswith(self, path_id):
        return self[:len(path_id)] == path_id

    def replace_prefix(self, prefix, replacement):
        if self.startswith(prefix):
            prefix_len = len(prefix)
            if prefix_len < len(self):
                result = self.__class__()
                result._path = replacement._path + self._path[prefix_len:]
                result._norm_path = \
                    replacement._norm_path + self._norm_path[prefix_len:]
                result._namespace = replacement._namespace
                return result
            else:
                return replacement
        else:
            return self

    def extend(self, link, direction=None, target=None):
        if not self:
            raise ValueError('cannot extend empty PathId')

        if link.generic():
            raise ValueError('path id must contain specialized links')

        if direction is None:
            direction = s_pointers.PointerDirection.Outbound

        if target is None:
            target = link.get_far_endpoint(direction)

        is_linkprop = isinstance(link, s_lprops.LinkProperty)
        if is_linkprop and not self._is_ptr:
            raise ValueError('link property path extension on a non-link path')

        result = self.__class__()
        result._path = self._path + ((link, direction), target)
        lnk = (link.shortname, direction, is_linkprop)
        norm_target = target.material_type()
        result._norm_path = self._norm_path + (lnk, norm_target)
        result._namespace = self._namespace

        return result

    def ptr_path(self):
        if self._is_ptr:
            return self
        else:
            result = self.__class__(self)
            result._is_ptr = True
            return result

    def is_concept_path(self):
        return (
            not self.is_ptr_path() and
            isinstance(self._path[-1], s_concepts.Concept)
        )

    def is_atom_path(self):
        return (
            not self.is_ptr_path() and
            isinstance(self._path[-1], s_atoms.Atom)
        )

    def is_ptr_path(self):
        return self._is_ptr

    def is_linkprop_path(self):
        return isinstance(self.rptr(), s_lprops.LinkProperty)

    def is_type_indirection_path(self):
        rptr = self.rptr()
        if rptr is None:
            return False
        else:
            return rptr.shortname in (
                '__type__::indirection',
                '__type__::optindirection',
            )


class InvalidScopeConfiguration(Exception):
    def __init__(self, msg: str, *,
                 offending_node: 'BaseScopeTreeNode',
                 existing_node: 'BaseScopeTreeNode') -> None:
        super().__init__(msg)
        self.offending_node = offending_node
        self.existing_node = existing_node


class BaseScopeTreeNode:
    def __init__(self, *, parent=None):
        self.children = []
        self.contained_paths = set()
        self.set_children = set()
        self._set_parent(parent)

    def _set_parent(self, parent):
        if parent is not None:
            self._parent = weakref.ref(parent)
            if type(parent) == ScopeFenceNode:
                self._parent_fence = self._parent
            else:
                self._parent_fence = parent._parent_fence
        else:
            self._parent = None
            self._parent_fence = None

    @property
    def name(self):
        raise NotImplementedError

    @property
    def parent(self):
        if self._parent is None:
            return None
        else:
            return self._parent()

    @property
    def parent_fence(self):
        if self._parent_fence is None:
            return None
        else:
            return self._parent_fence()

    fence = parent_fence

    @property
    def paths(self):
        return [
            n.path_id for n in self.children if getattr(n, 'path_id', None)
        ]

    def get_all_paths(self):
        return {n.path_id for n in self._get_all_paths()}

    def _get_all_paths(self, include_subpaths=False):
        paths = set()

        if getattr(self, 'path_id', None):
            paths.add(self)
            self_is_path = True
        else:
            self_is_path = False

        for c in self.children:
            if include_subpaths or not self_is_path or c.path_id is None:
                paths.update(c._get_all_paths(
                    include_subpaths=include_subpaths))

        return paths

    def _get_subpaths(self):
        if self.path_id is None:
            return []

        node = self
        paths = []

        while node is not None:
            paths.append(node)
            for child in node.children:
                if child.path_id:
                    node = child
                    break
            else:
                break

        return paths

    def get_all_visible(self) -> typing.Set[PathId]:
        paths = set()

        node = self

        while node is not None:
            if node.path_id:
                paths.add(node.path_id)
            else:
                for c in node.children:
                    if c.path_id:
                        paths.add(c.path_id)

            node = node.parent

        return paths

    def remove_child(self, node):
        self.children.remove(node)
        self.set_children.discard(node)

    def _remove_descendants(self, path_id, respect_fences=False, *,
                            min_depth=0, depth=0):
        descendants = []
        if path_id in self.contained_paths:
            return None

        for child in list(self.children):
            if type(child) is ScopeFenceNode:
                if respect_fences:
                    continue

            if child.children:
                descendants_in_child = child._remove_descendants(
                    path_id, respect_fences=respect_fences, depth=depth + 1)
                if descendants_in_child:
                    descendants.extend(descendants_in_child)

            if min_depth <= depth and child.path_id == path_id:
                self.remove_child(child)
                descendants.append(child)

        return descendants

    def find_descendant(self, path_id, *, respect_fences=True):
        for child in list(self.children):
            if type(child) is ScopeFenceNode and respect_fences:
                continue

            if child.path_id == path_id:
                return child
            elif child.children:
                found = child.find_descendant(path_id)
                if found is not None:
                    return found

    def find_child(self, path_id):
        for child in self.children:
            if child.path_id == path_id:
                return child

    def _find_node(self, path_id):
        node = self.is_visible(path_id)
        if node is not None:
            return node
        else:
            return self.find_descendant(path_id)

    def is_visible(self, path_id):
        node = self

        if node.path_id is not None:
            if path_id == node.path_id:
                return node
            else:
                node = node.parent

        while node is not None:
            for child in node.children:
                if child.path_id == path_id:
                    return child

            node = node.parent

    def is_any_prefix_visible(self, path_id) -> bool:
        for prefix in reversed(list(path_id.iter_prefixes())):
            if self.is_visible(prefix):
                return True

        return False

    def is_empty(self):
        return not self.children or all(c.is_empty() for c in self.children)

    def pformat(self):
        if self.children:
            child_formats = []
            for c in self.children:
                cf = c.pformat()
                if cf:
                    child_formats.append(cf)

            if child_formats:
                children = textwrap.indent(',\n'.join(child_formats), '    ')
                return f'"{self.name}": {{\n{children}\n}}'
            else:
                return f'"{self.name}"'
        else:
            return f'"{self.name}"'

    def pdebugformat(self):
        if self.children:
            child_formats = []
            for c in self.children:
                cf = c.pdebugformat()
                if cf:
                    child_formats.append(cf)

            children = textwrap.indent(',\n'.join(child_formats), '    ')
            return f'"{self.fullname}": {{\n{children}\n}}'
        else:
            return f'"{self.fullname}"'


class ScopeBranchNode(BaseScopeTreeNode):
    def __init__(self, *, parent=None):
        super().__init__(parent=parent)
        self.path_id = None
        self.protect_parent = False
        self.unnest_fence = False

    def __repr__(self):
        return f'<ScopeBranchNode at {id(self):0x}>'

    def _unnest_node(self, node, respect_fences=False):
        descendants = self._remove_descendants(
            node.path_id, respect_fences=respect_fences, min_depth=1)
        if descendants:
            d0 = self._merge_nodes(descendants)
            d0._set_parent(self)
            self.children.append(d0)
            return True

        parent_fence = self._parent_fence
        if self.protect_parent or parent_fence is None:
            return False

        parent_fence = parent_fence()

        if isinstance(self, ScopeFenceNode) and self.unnest_fence:
            for prefix in reversed(list(node.path_id.iter_prefixes())):
                if parent_fence.is_visible(prefix):
                    break
                cnode = parent_fence.find_descendant(prefix)

                if cnode is not None and cnode in cnode.parent.set_children:

                    ppath_id = cnode.parent.path_id
                    if isinstance(ppath_id.rptr(), s_lprops.LinkProperty):
                        # Link-property paths are techincally of the same
                        # length as the target path, and so are "visible".
                        break

                    raise InvalidScopeConfiguration(
                        f'reference to {node.path_id.pformat()!r} changes the '
                        f'interpretation of {cnode.path_id.pformat()!r} in '
                        f'an outer scope',
                        offending_node=node,
                        existing_node=cnode
                    )

        if self.parent:
            respect_fences = isinstance(self, ScopeFenceNode) or respect_fences
            return self.parent._unnest_node(
                node, respect_fences=respect_fences)
        else:
            return False

    def add_path(self, path_id):
        """Ensure *path_id* is visible on this scope level."""
        parent = self
        new_node = None
        is_lprop = False

        for prefix in reversed(list(path_id.iter_prefixes(include_ptr=True))):
            if prefix.is_ptr_path():
                is_lprop = True
                continue

            if self.is_visible(prefix):
                # This path is already visible at this level.
                # If we have seen a link property ref for this link,
                # we must ensure that the _source_ of this link is
                # visible on this level to enumerate _all_ existing links,
                # and not just the unique targets.
                if is_lprop:
                    is_lprop = False
                    continue
                else:
                    break

            new_child = ScopePathNode(parent=parent, path_id=prefix)
            if parent is self:
                new_node = new_child

            present = self._unnest_node(new_child)
            if present:
                if is_lprop or prefix.is_linkprop_path():
                    is_lprop = False
                    continue
                else:
                    break

            parent.children.append(new_child)
            if parent.path_id is not None:
                parent.set_children.add(new_child)
            parent = new_child
            is_lprop = False

        return new_node

    def contain_path(self, path_id):
        self.contained_paths.add(path_id)

    def add_fence(self):
        scope_node = ScopeFenceNode(parent=self)
        self.children.append(scope_node)
        return scope_node

    def add_branch(self):
        node = ScopeBranchNode(parent=self)
        self.children.append(node)
        return node

    def mark_as_optional(self, path_id):
        """Indicate that *path_id* is used as an OPTIONAL argument."""
        node = self.is_visible(path_id)
        if node is not None:
            node.optional = True

    def is_optional(self, path_id):
        node = self.is_visible(path_id)
        if node is None:
            node = self.find_child(path_id)
        if node is not None:
            return node.optional

    def _merge_children(self, other):
        for child in other.children:
            if child.path_id:
                existing = self.find_child(child.path_id)
                if existing:
                    child = self._merge_nodes([child, existing])

            self.children.append(child)
            child._set_parent(self)

    def _merge_nodes(self, descendants):
        d0 = descendants[0]
        for descendant in descendants[1:]:
            d0._merge_children(descendant)

        return d0

    def _reattach_descendants(self, other, path_id):
        descendants = other._remove_descendants(path_id)
        if descendants:
            d0 = self._merge_nodes(descendants)
            if (d0.path_id is not None and self.path_id is not None and
                    d0.path_id == self.path_id):
                self._merge_children(d0)
            else:
                d0._set_parent(self)
                self.children.append(d0)
            return True

    def attach_branch(self, node):
        for cpath in node._get_all_paths():
            for cnode in cpath._get_subpaths():
                path_id = cnode.path_id
                ours = self.is_visible(path_id)
                if ours:
                    ours._reattach_descendants(node, path_id)
                    if cnode.optional:
                        ours.optional = True
                    break
                else:
                    if cnode.fence != node:
                        present = self.find_descendant(path_id)
                        if present:
                            node._remove_descendants(path_id)
                            break
                        else:
                            present = self._unnest_node(
                                cnode, respect_fences=True)
                            if present:
                                node._remove_descendants(path_id)
                                break
                    else:
                        if cnode._reattach_descendants(self, path_id):
                            break
                        else:
                            present = self._unnest_node(cnode)
                            if present:
                                node._remove_descendants(path_id)
                                break

        for child in node.children:
            if (child.path_id is not None and self.path_id is not None and
                    child.path_id == self.path_id):
                self._merge_children(child)
            else:
                child._set_parent(self)
                self.children.append(child)

    def unfence(self, node):
        self.remove_child(node)

        for child in node.children:
            if child.path_id:
                self.add_path(child.path_id)
            else:
                child._parent = weakref.ref(self)
                self.children.append(child)

    @property
    def name(self):
        return f'BRANCH'

    @property
    def fullname(self):
        return f'BRANCH 0x{id(self):0x}'

    def pformat(self):
        if self.is_empty():
            return ''
        else:
            return super().pformat()


class ScopePathNode(ScopeBranchNode):
    def __init__(self, *, parent, path_id):
        super().__init__(parent=parent)
        self.path_id = path_id
        self.optional = False

    def __repr__(self):
        return f'<ScopePathNode {self.path_id!r} at {id(self):0x}>'

    @property
    def name(self):
        return f'{self.path_id}{" [OPT]" if self.optional else ""}'

    @property
    def fullname(self):
        return (f'{self.path_id}{" [OPT]" if self.optional else ""} '
                f'0x{id(self):0x}')

    def is_empty(self):
        return False


class ScopeFenceNode(ScopeBranchNode):
    def __repr__(self):
        return f'<ScopeFenceNode at {id(self):0x}>'

    @property
    def name(self):
        return f'FENCE'

    @property
    def fullname(self):
        return f'FENCE 0x{id(self):0x}'

    @property
    def fence(self):
        return self
