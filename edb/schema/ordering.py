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
from typing import *

import collections

from edb.common import ordered
from edb.common import topological

from . import delta as sd
from . import functions as s_func
from . import inheriting
from . import name as sn
from . import objects as so
from . import pointers as s_pointers
from . import referencing
from . import types as s_types

if TYPE_CHECKING:
    from . import schema as s_schema


def linearize_delta(
    delta: sd.DeltaRoot,
    old_schema: Optional[s_schema.Schema],
    new_schema: s_schema.Schema,
) -> sd.DeltaRoot:
    """Reorder the *delta* tree in-place to satisfy command dependency order.

    Args:
        delta:
            Input delta command tree.
        old_schema:
            Schema used to resolve original object state.
        new_schema:
            Schema used to resolve final schema state.

    Returns:
        Input delta tree reordered according to topological ordering of
        commands.
    """

    # We take the scatter-sort-gather approach here, where the original
    # tree is broken up into linear branches, which are then sorted
    # and reassembled back into a tree.

    # A map of commands to root->command paths through the tree.
    # Nodes are duplicated so the interior nodes of the path are
    # distinct.
    opmap: Dict[sd.Command, List[sd.Command]] = {}
    strongrefs: Dict[str, str] = {}

    for op in _get_sorted_subcommands(delta):
        _break_down(opmap, strongrefs, [delta, op])

    depgraph: Dict[Tuple[str, str], Dict[str, Any]] = {}
    renames: Dict[str, str] = {}
    renames_r: Dict[str, str] = {}
    deletions: Set[str] = set()

    for op in opmap:
        if isinstance(op, sd.RenameObject):
            renames[op.classname] = op.new_name
            renames_r[op.new_name] = op.classname
        elif isinstance(op, sd.DeleteObject):
            deletions.add(op.classname)

    for op, opbranch in opmap.items():
        if isinstance(op, sd.AlterObject) and not op.get_subcommands():
            continue

        _trace_op(op, opbranch, depgraph, renames,
                  renames_r, strongrefs, old_schema, new_schema)

    depgraph = dict(
        filter(lambda i: i[1].get('item') is not None, depgraph.items()))

    everything = set(depgraph)
    for item in depgraph.values():
        item['deps'] = item['deps'] & everything

    sortedlist = [i[1] for i in topological.sort(depgraph, return_record=True)]
    reconstructed = reconstruct_tree(sortedlist, depgraph)
    delta.replace_all(reconstructed.get_subcommands())
    return delta


def reconstruct_tree(
    sortedlist: List[Dict[str, Any]],
    depgraph: Dict[Tuple[str, str], Dict[str, Any]],
) -> sd.DeltaRoot:

    result = sd.DeltaRoot()
    # Child to parent mapping.
    parents: Dict[sd.Command, sd.Command] = {}
    # A mapping of commands to their dependencies.
    dependencies: Dict[sd.Command, Set[sd.Command]] = (
        collections.defaultdict(set))
    # Current address of command within a tree in the form of
    # a tuple of indexes where each index represents relative
    # position within the tree rank.
    offsets: Dict[sd.Command, Tuple[int, ...]] = {}
    # Object commands indexed by command type and object name,
    # where each entry represents the latest seen command of the type
    # for a particular object.  Implicit commands are not included in
    # this mapping.
    opindex: Dict[
        Tuple[Type[sd.ObjectCommand[so.Object]], str],
        sd.ObjectCommand[so.Object]
    ] = {}

    def ok_to_attach_to(
        op_to_attach: sd.Command,
        op_to_attach_to: sd.ObjectCommand[so.Object],
    ) -> bool:
        """Determine if a given command can be attached to another.

        Returns True, if *op_to_attach* can be attached to *op_to_attach_to*
        without violating the dependency order.
        """
        tgt_offset = offsets[op_to_attach_to]
        tgt_offset_len = len(tgt_offset)
        deps = dependencies[op_to_attach]
        return all(offsets[dep][:tgt_offset_len] <= tgt_offset for dep in deps)

    def attach(
        opbranch: List[sd.Command],
        new_parent: sd.Command,
        slice_start: int = 1,
        as_implicit: bool = False,
    ) -> None:
        """Attach a portion of a given command branch to another parent.

        Args:
            opbranch:
                Command branch to attach to *new_parent*.
            new_parent:
                Command node to attach the specified portion of *opbranch* to.
            slice_start:
                Offset into *opbranch* that determines which commands
                get attached.
            as_implicit:
                If True, the command branch is considered to be implicit,
                i.e. it is not recorded in the command index.
        """
        parent = opbranch[slice_start]
        op = opbranch[-1]
        offset_within_parent = new_parent.get_nonattr_subcommand_count()
        if not isinstance(new_parent, sd.DeltaRoot):
            parent_offset = offsets[new_parent] + (offset_within_parent,)
        else:
            parent_offset = (offset_within_parent,)
        new_parent.add(parent)
        old_parent = parents[parent]
        old_parent.discard(parent)
        parents[parent] = new_parent

        for i in range(slice_start, len(opbranch)):
            op = opbranch[i]
            if isinstance(op, sd.ObjectCommand) and not as_implicit:
                ancestor_key = (type(op), op.classname)
                opindex[ancestor_key] = op

            if op in offsets:
                op_offset = offsets[op][slice_start:]
            else:
                op_offset = (0,) * (i - slice_start)

            offsets[op] = parent_offset + op_offset

    def maybe_replace_preceding(
        op: sd.ObjectCommand[so.Object],
    ) -> bool:
        """Possibly merge and replace an earlier command with *op*.

        If *op* is a DELETE command, or an ALTER command that has no
        subcommands, and there is an earlier ALTER command operating
        on the same object as *op*, merge that command into *op* and
        replace it with *op*.

        Returns:
            True if merge and replace happened, False otherwise.
        """
        if not (
            isinstance(op, sd.DeleteObject)
            or (
                isinstance(op, sd.AlterObject)
                and op.get_nonattr_subcommand_count() == 0
            )
        ):
            return False

        alter_cmd_cls = sd.ObjectCommandMeta.get_command_class(
            sd.AlterObject, op.get_schema_metaclass())

        if alter_cmd_cls is None:
            # ALTER isn't even defined for this object class, bail.
            return False

        alter_key = ((alter_cmd_cls), op.classname)
        alter_op = opindex.get(alter_key)
        if alter_op is None:
            # No preceding ALTER, bail.
            return False

        if (
            not ok_to_attach_to(op, alter_op)
            or (
                isinstance(parents[op], sd.DeltaRoot)
                != isinstance(parents[alter_op], sd.DeltaRoot)
            )
        ):
            return False

        for alter_sub in reversed(alter_op.get_prerequisites()):
            op.prepend_prerequisite(alter_sub)
            parents[alter_sub] = op

        for alter_sub in reversed(
            alter_op.get_subcommands(include_prerequisites=False)
        ):
            op.prepend(alter_sub)
            parents[alter_sub] = op

        attached_root = parents[alter_op]
        attached_root.replace(alter_op, op)
        opindex[alter_key] = op
        opindex[type(op), op.classname] = op
        offsets[op] = offsets[alter_op]
        parents[op] = attached_root

        return True

    def maybe_attach_to_preceding(
        opbranch: List[sd.Command],
        parent_candidates: List[str],
        allowed_op_types: List[Type[sd.ObjectCommand[so.Object]]],
        as_implicit: bool = False,
        slice_start: int = 1,
    ) -> bool:
        """Find a parent and attach a given portion of command branch to it.

        Args:
            opbranch:
                Command branch to consider.
            parent_candidates:
                A list of parent object names to consider when looking for
                a parent command.
            allowed_op_types:
                A list of command types to consider when looking for a
                parent command.
            as_implicit:
                If True, the command branch is considered to be implicit,
                i.e. it is not recorded in the command index.
            slice_start:
                Offset into *opbranch* that determines which commands
                get attached.
        """

        for candidate in parent_candidates:
            for op_type in allowed_op_types:
                parent_op = opindex.get((op_type, candidate))

                if parent_op is not None and ok_to_attach_to(op, parent_op):
                    attach(
                        opbranch,
                        parent_op,
                        as_implicit=as_implicit,
                        slice_start=slice_start,
                    )
                    return True

        return False

    # First, build parents and dependencies maps.
    for info in sortedlist:
        opbranch = info['item']
        op = opbranch[-1]
        for j, pop in enumerate(opbranch[1:]):
            parents[pop] = opbranch[j]
        for dep in info['deps']:
            dep_item = depgraph[dep]
            dep_stack = dep_item['item']
            dep_op = dep_stack[-1]
            dependencies[op].add(dep_op)

    for info in sortedlist:
        opbranch = info['item']
        op = opbranch[-1]
        # Elide empty ALTER statements from output.
        if isinstance(op, sd.AlterObject) and not op.get_subcommands():
            continue

        # If applicable, replace a preceding command with this op.
        if maybe_replace_preceding(op):
            continue

        if (
            isinstance(op, sd.ObjectCommand)
            and not isinstance(op, sd.CreateObject)
            and info['implicit_ancestors']
        ):
            # This command is deemed to be an implicit effect of another
            # command, such as when alteration is propagated through the
            # inheritance chain.  If so, find a command that operates on
            # a parent object and attach this branch to it.
            allowed_ops = [type(op)]
            if isinstance(op, sd.DeleteObject):
                allowed_ops.append(op.get_other_command_class(sd.DeleteObject))

            if maybe_attach_to_preceding(
                opbranch,
                info['implicit_ancestors'],
                allowed_ops,
                as_implicit=True,
            ):
                continue

        # Walking the branch toward root, see if there's a matching
        # branch prefix we could attach to.
        for depth, ancestor_op in enumerate(reversed(opbranch[1:-1])):
            assert isinstance(ancestor_op, sd.ObjectCommand)

            allowed_ops = []
            create_cmd_t = ancestor_op.get_other_command_class(sd.CreateObject)
            if type(ancestor_op) != create_cmd_t:
                allowed_ops.append(create_cmd_t)
            allowed_ops.append(type(ancestor_op))

            if maybe_attach_to_preceding(
                opbranch,
                [ancestor_op.classname],
                allowed_ops,
                slice_start=len(opbranch) - (depth + 1),
            ):
                break
        else:
            # No branches to attach to, so attach to root.
            attach(opbranch, result)

    return result


def _command_key(cmd: sd.Command) -> Any:
    if isinstance(cmd, sd.ObjectCommand):
        return (cmd.get_schema_metaclass().__name__, cmd.classname)
    elif isinstance(cmd, sd.AlterObjectProperty):
        return ('.field', cmd.property)
    else:
        return ('_generic', type(cmd).__name__)


def _get_sorted_subcommands(cmd: sd.Command) -> List[sd.Command]:
    subcommands = list(cmd.get_subcommands())
    subcommands.sort(key=_command_key)
    return subcommands


def _break_down(
    opmap: Dict[sd.Command, List[sd.Command]],
    strongrefs: Dict[str, str],
    opbranch: List[sd.Command],
) -> None:
    if len(opbranch) > 2:
        new_opbranch = _extract_op(opbranch)
    else:
        new_opbranch = opbranch

    op = new_opbranch[-1]

    for sub_op in _get_sorted_subcommands(op):
        if isinstance(sub_op, (referencing.ReferencedObjectCommand,
                               sd.RenameObject,
                               inheriting.RebaseInheritingObject)):
            _break_down(opmap, strongrefs, new_opbranch + [sub_op])
        elif (
            isinstance(sub_op, sd.AlterObjectProperty)
            and not isinstance(op, sd.DeleteObject)
        ):
            assert isinstance(op, sd.ObjectCommand)
            mcls = op.get_schema_metaclass()
            field = mcls.get_field(sub_op.property)
            # Break a possible reference cycle
            # (i.e. Type.rptr <-> Pointer.target)
            if (
                field.weak_ref
                or (
                    isinstance(op, sd.AlterObject)
                    and issubclass(field.type, so.Object)
                )
            ):
                _break_down(opmap, strongrefs, new_opbranch + [sub_op])
        elif isinstance(sub_op, referencing.StronglyReferencedObjectCommand):
            assert isinstance(op, sd.ObjectCommand)
            strongrefs[sub_op.classname] = op.classname

    opmap[op] = new_opbranch


def _trace_op(
    op: sd.Command,
    opbranch: List[sd.Command],
    depgraph: Dict[Tuple[str, str], Dict[str, Any]],
    renames: Dict[str, str],
    renames_r: Dict[str, str],
    strongrefs: Dict[str, str],
    old_schema: Optional[s_schema.Schema],
    new_schema: s_schema.Schema,
) -> None:
    deps: ordered.OrderedSet[Tuple[str, str]] = ordered.OrderedSet()
    graph_key: str
    implicit_ancestors: List[str] = []

    if isinstance(op, sd.CreateObject):
        tag = 'create'
    elif isinstance(op, sd.AlterObject):
        tag = 'alter'
    elif isinstance(op, sd.RenameObject):
        tag = 'rename'
    elif isinstance(op, inheriting.RebaseInheritingObject):
        tag = 'rebase'
    elif isinstance(op, sd.DeleteObject):
        tag = 'delete'
    elif isinstance(op, referencing.AlterOwned):
        tag = 'alterowned'
    elif isinstance(op, sd.AlterObjectProperty):
        tag = 'field'
    else:
        raise RuntimeError(
            f'unexpected delta command type at top level: {op!r}'
        )

    if isinstance(op, (sd.DeleteObject, referencing.AlterOwned)):
        assert old_schema is not None
        obj = get_object(old_schema, op)
        refs = _get_referrers(old_schema, obj, strongrefs)
        for ref in refs:
            ref_name = ref.get_name(old_schema)
            if (
                (
                    isinstance(obj, referencing.ReferencedObject)
                    and obj.get_referrer(old_schema) == ref
                )
            ):
                # If the referrer is enclosing the object
                # (i.e. the reference is a refdict reference),
                # we sort the referrer operation first.
                try:
                    ref_item = depgraph[('delete', ref_name)]
                except KeyError:
                    ref_item = depgraph[('delete', ref_name)] = {'deps': set()}

                ref_item['deps'].add((tag, op.classname))

            elif (
                isinstance(ref, referencing.ReferencedInheritingObject)
                and (
                    op.classname
                    in {
                        b.get_name(old_schema)
                        for b in ref.get_implicit_ancestors(old_schema)
                    }
                )
                and (
                    not isinstance(ref, s_pointers.Pointer)
                    or not ref.get_is_from_alias(old_schema)
                )
            ):
                # If the ref is an implicit descendant (i.e. an inherited ref),
                # we also sort it _after_ the parent, because we'll pull
                # it as a child of the parent op at the time of tree
                # reassembly.
                try:
                    ref_item = depgraph[('delete', ref_name)]
                except KeyError:
                    ref_item = depgraph[('delete', ref_name)] = {'deps': set()}

                ref_item['deps'].add((tag, op.classname))

            elif (
                isinstance(ref, referencing.ReferencedObject)
                and ref.get_referrer(old_schema) == obj
            ):
                # Skip refdict.backref_attr to avoid dependency cycles.
                continue

            else:
                # Otherwise, things must be deleted _after_ their referrers
                # have been deleted or altered.
                deps.add(('delete', ref.get_name(old_schema)))

        if isinstance(obj, referencing.ReferencedObject):
            referrer = obj.get_referrer(old_schema)
            if referrer is not None:
                assert isinstance(referrer, so.QualifiedObject)
                referrer_name: str = referrer.get_name(old_schema)
                if referrer_name in renames_r:
                    referrer_name = renames_r[referrer_name]
                deps.add(('rebase', referrer_name))

                if (
                    isinstance(obj, referencing.ReferencedInheritingObject)
                    and (
                        not isinstance(obj, s_pointers.Pointer)
                        or not obj.get_is_from_alias(old_schema)
                    )
                ):
                    for ancestor in obj.get_implicit_ancestors(old_schema):
                        ancestor_name = ancestor.get_name(old_schema)
                        implicit_ancestors.append(ancestor_name)

                        try:
                            anc_item = depgraph[('delete', ancestor_name)]
                        except KeyError:
                            anc_item = {'deps': set()}
                            depgraph[('delete', ancestor_name)] = anc_item

                        anc_item['deps'].add((
                            'alterowned', op.classname,
                        ))

        graph_key = op.classname

    elif isinstance(op, sd.AlterObjectProperty):
        if isinstance(op.new_value, (so.Object, so.ObjectShell)):
            nvn = op.new_value.get_name(new_schema)
            if nvn is not None:
                deps.add(('create', nvn))
                deps.add(('alter', nvn))
                if nvn in renames_r:
                    deps.add(('rename', renames_r[nvn]))

        parent_op = opbranch[-2]
        assert isinstance(parent_op, sd.ObjectCommand)
        graph_key = f'{parent_op.classname}%%{op.property}'
        deps.add(('create', parent_op.classname))

        if isinstance(op.old_value, (so.Object, so.ObjectShell)):
            assert old_schema is not None
            ovn = op.old_value.get_name(old_schema)
            nvn = op.new_value.get_name(new_schema)
            if ovn != nvn:
                try:
                    ov_item = depgraph[('delete', ovn)]
                except KeyError:
                    ov_item = depgraph[('delete', ovn)] = {'deps': set()}

                ov_item['deps'].add((tag, graph_key))

    elif isinstance(op, sd.ObjectCommand):
        # If the object was renamed, use the new name, else use regular.
        name = renames.get(op.classname, op.classname)
        obj = get_object(new_schema, op, name)

        if tag == 'rename':
            # On renames, we want to delete any references before we
            # do the rename. This is because for functions and
            # constraints we implicitly rename the object when
            # something it references is renamed, and this implicit
            # rename can interfere with a CREATE/DELETE pair.  So we
            # make sure to put the DELETE before the RENAME of a
            # referenced object. (An improvement would be to elide a
            # CREATE/DELETE pair when it could be implicitly handled
            # by a rename).
            assert old_schema
            old_obj = get_object(old_schema, op, op.classname)
            for ref in _get_referrers(old_schema, old_obj, strongrefs):
                deps.add(('delete', ref.get_name(old_schema)))

        refs = _get_referrers(new_schema, obj, strongrefs)
        for ref in refs:
            ref_name = ref.get_name(new_schema)
            if ref_name in renames_r:
                ref_name = renames_r[ref_name]

            if ((isinstance(ref, referencing.ReferencedObject)
                    and ref.get_referrer(new_schema) == obj)
                    or (isinstance(obj, referencing.ReferencedObject)
                        and obj.get_referrer(new_schema) == ref)):
                # Ignore refs generated by refdict backref.
                continue

            try:
                item = depgraph[('create', ref_name)]
            except KeyError:
                item = depgraph[('create', ref_name)] = {
                    'deps': set(),
                }

            item['deps'].add(('create', op.classname))
            item['deps'].add(('alter', op.classname))
            item['deps'].add(('rename', op.classname))

            try:
                item = depgraph[('alter', ref_name)]
            except KeyError:
                item = depgraph[('alter', ref_name)] = {
                    'deps': set(),
                }

            item['deps'].add(('create', op.classname))
            item['deps'].add(('alter', op.classname))
            item['deps'].add(('rename', op.classname))

            try:
                item = depgraph[('rebase', ref_name)]
            except KeyError:
                item = depgraph[('rebase', ref_name)] = {
                    'deps': set(),
                }

            item['deps'].add(('create', op.classname))
            item['deps'].add(('alter', op.classname))
            item['deps'].add(('rename', op.classname))

            try:
                item = depgraph[('rename', ref_name)]
            except KeyError:
                item = depgraph[('rename', ref_name)] = {
                    'deps': set(),
                }

            item['deps'].add(('create', op.classname))
            item['deps'].add(('alter', op.classname))

        if tag in ('create', 'alter'):
            # In a delete/create cycle, deletion must obviously
            # happen first.
            deps.add(('delete', op.classname))

            if isinstance(obj, s_func.Function) and old_schema is not None:
                old_funcs = old_schema.get_functions(
                    sn.shortname_from_fullname(op.classname),
                    default=(),
                )
                for old_func in old_funcs:
                    deps.add(('delete', old_func.get_name(old_schema)))

        if tag == 'alter':
            # Alteration must happen after creation, if any.
            deps.add(('create', op.classname))
            deps.add(('rename', op.classname))
            deps.add(('rebase', op.classname))

        if isinstance(obj, referencing.ReferencedObject):
            referrer = obj.get_referrer(new_schema)
            if referrer is not None:
                assert isinstance(referrer, so.QualifiedObject)
                referrer_name = referrer.get_name(new_schema)
                if referrer_name in renames_r:
                    referrer_name = renames_r[referrer_name]
                deps.add(('create', referrer_name))
                deps.add(('rebase', referrer_name))

                if isinstance(obj, referencing.ReferencedInheritingObject):
                    implicit_ancestors = [
                        b.get_name(new_schema)
                        for b in obj.get_implicit_ancestors(new_schema)
                    ]

                    if not isinstance(op, sd.CreateObject):
                        assert old_schema is not None
                        old_obj = get_object(old_schema, op)
                        assert isinstance(
                            old_obj,
                            referencing.ReferencedInheritingObject,
                        )
                        implicit_ancestors += [
                            b.get_name(old_schema)
                            for b in old_obj.get_implicit_ancestors(old_schema)
                        ]

        graph_key = op.classname

    else:
        raise AssertionError(f'unexpected op type: {op!r}')

    try:
        item = depgraph[(tag, graph_key)]
    except KeyError:
        item = depgraph[(tag, graph_key)] = {'deps': set()}

    item['item'] = opbranch
    item['deps'].update(deps)
    item['implicit_ancestors'] = [
        renames_r.get(a, a) for a in implicit_ancestors
    ]


def get_object(
    schema: s_schema.Schema,
    op: sd.ObjectCommand[so.Object],
    name: Optional[str] = None,
) -> so.Object:
    metaclass = op.get_schema_metaclass()
    if name is None:
        name = op.classname

    if issubclass(metaclass, s_types.Collection):
        if sn.Name.is_qualified(name):
            return schema.get(name)
        else:
            t_id = s_types.type_id_from_name(name)
            assert t_id is not None
            return schema.get_by_id(t_id)
    elif not issubclass(metaclass, so.QualifiedObject):
        obj = schema.get_global(metaclass, name)
        assert isinstance(obj, so.Object)
        return obj
    else:
        return schema.get(name)


def _get_referrers(
    schema: s_schema.Schema,
    obj: so.Object,
    strongrefs: Dict[str, str],
) -> List[so.Object]:
    refs = schema.get_referrers(obj)
    result: Set[so.Object] = set()

    for ref in refs:
        if not ref.is_blocking_ref(schema, obj):
            continue

        parent_ref = strongrefs.get(ref.get_name(schema))
        if parent_ref is not None:
            referrer: so.Object = schema.get(parent_ref)
        else:
            referrer = ref

        result.add(referrer)

    return list(sorted(
        result,
        key=lambda o: (type(o).__name__, o.get_name(schema)),
    ))


def _extract_op(stack: Sequence[sd.Command]) -> List[sd.Command]:
    parent_op = stack[0]
    new_stack = [parent_op]

    for stack_op in stack[1:-1]:
        assert isinstance(stack_op, sd.ObjectCommand)
        alter_class = stack_op.get_other_command_class(sd.AlterObject)
        alter_delta = alter_class(
            classname=stack_op.classname,
            ddl_identity=stack_op.ddl_identity,
            annotations=stack_op.annotations,
        )
        parent_op.add(alter_delta)
        parent_op = alter_delta
        new_stack.append(parent_op)

    stack[-2].discard(stack[-1])
    parent_op.add(stack[-1])
    new_stack.append(stack[-1])

    return new_stack
