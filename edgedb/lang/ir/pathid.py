##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import textwrap
import weakref

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import objects as so
from edgedb.lang.schema import pointers as s_pointers


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
            if not isinstance(initializer, so.NodeClass):
                raise ValueError(
                    f'invalid PathId: bad source: {initializer!r}')
            self._path = (initializer,)
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

    __repr__ = __str__

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

    def is_concept_path(self):
        return (
            not self.is_ptr_path() and
            isinstance(self._path[-1], s_concepts.Concept)
        )

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
                result._namespace = self._namespace
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
        result._norm_path = self._norm_path + (lnk, target)
        result._namespace = self._namespace

        return result

    def ptr_path(self):
        if self._is_ptr:
            return self
        else:
            result = self.__class__(self)
            result._is_ptr = True
            return result

    def is_ptr_path(self):
        return self._is_ptr

    def is_type_indirection_path(self):
        rptr = self.rptr()
        if rptr is None:
            return False
        else:
            return rptr.shortname in (
                '__type__::indirection',
                '__type__::optindirection',
            )


class BaseScopeTreeNode:
    def __init__(self, *, parent=None):
        self.children = []
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
    def fence(self):
        if self._parent_fence is None:
            return None
        else:
            return self._parent_fence()

    @property
    def paths(self):
        return [n.path_id for n in self.children if hasattr(n, 'path_id')]

    def get_all_paths(self):
        paths = set()

        if hasattr(self, 'path_id'):
            paths.add(self.path_id)

        for c in self.children:
            paths.update(c.get_all_paths())

        return paths

    def _remove_child(self, node):
        self.children.remove(node)

        # Reattach grandchildren directly.
        for child in node.children:
            child._parent = weakref.ref(self)
            self.children.append(child)

    def _remove_descendants(self, path_id, respect_fences=False, *, depth=0):
        present = False

        for child in list(self.children):
            if type(child) is ScopeFenceNode:
                if respect_fences:
                    continue
                elif child.ignore_parent and depth == 0:
                    continue

            if child.children:
                present = present or child._remove_descendants(
                    path_id, respect_fences=respect_fences, depth=depth + 1)

            if child.path_id == path_id:
                self._remove_child(child)
                present = True

        return present

    def is_visible(self, path_id):
        node = self

        while node is not None:
            for child in node.children:
                if child.path_id == path_id:
                    return child

            node = node.parent

    def pformat(self):
        if self.children:
            child_formats = []
            for c in self.children:
                cf = c.pformat()
                if cf:
                    child_formats.append(cf)

            children = textwrap.indent(',\n'.join(child_formats), '    ')
            return f'"{self.name}": {{\n{children}\n}}'
        else:
            return f'"{self.name}"'


class ScopePathNode(BaseScopeTreeNode):
    def __init__(self, *, parent, path_id):
        super().__init__(parent=parent)
        self.path_id = path_id

    def __repr__(self):
        return f'<ScopePathNode {self.path_id!r} at {id(self):0x}>'

    @property
    def name(self):
        return f'{self.path_id}'


class ScopeBranchNode(BaseScopeTreeNode):
    def __init__(self, *, parent=None):
        super().__init__(parent=parent)
        self.path_id = None
        self.ignore_parent = False

    def __repr__(self):
        return f'<ScopeBranchNode at {id(self):0x}>'

    def _unnest_node(self, node, respect_fences=False):
        present = self._remove_descendants(
            node.path_id, respect_fences=respect_fences)
        if present:
            self.children.append(node)
            return True

        parent_fence = self._parent_fence
        if parent_fence is not None:
            return parent_fence()._unnest_node(node, respect_fences=True)
        else:
            return False

    def add_path(self, path_id):
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
            if not present:
                parent.children.append(new_child)

            parent = new_child

        return new_node

    def add_fence(self):
        scope_node = ScopeFenceNode(parent=self)
        self.children.append(scope_node)
        return scope_node

    def add_branch(self):
        node = ScopeBranchNode(parent=self)
        self.children.append(node)
        return node

    def attach_child(self, node):
        self.children.append(node)

    @property
    def name(self):
        return f'BRANCH'

    def pformat(self):
        if not self.children:
            return ''
        else:
            return super().pformat()


class ScopeFenceNode(ScopeBranchNode):
    def __repr__(self):
        return f'<ScopeFenceNode at {id(self):0x}>'

    @property
    def name(self):
        return f'FENCE'

    @property
    def fence(self):
        return self
