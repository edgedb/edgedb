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

import collections

from edb.common import topological

from . import delta as sd
from . import inheriting
from . import pointers
from . import referencing


def linearize_delta(delta, old_schema, new_schema):
    """Sort delta operations to dependency order."""

    opmap = {}

    for op in delta.get_subcommands():
        _break_down(opmap, [delta, op])

    depgraph = {}
    renames = {}
    renames_r = {}

    for op in opmap:
        if isinstance(op, sd.RenameObject):
            renames[op.classname] = op.new_name
            renames_r[op.new_name] = op.classname

    for op, opstack in opmap.items():
        _trace_op(op, opstack, depgraph, renames,
                  renames_r, old_schema, new_schema)

    depgraph = dict(
        filter(lambda i: i[1].get('item') is not None, depgraph.items()))

    ordered = list(topological.sort(depgraph, allow_unresolved=True,
                                    return_record=True))

    parents = {}
    dependencies = collections.defaultdict(set)
    max_offset = len(ordered)
    offsets = {}
    ops = []

    for key, info in ordered:
        op = info['op']
        parent = opstack[1]
        for dep in info['deps']:
            dep_item = depgraph.get(dep)
            if dep_item is None:
                continue
            dep_op = dep_item['op']
            dep_stack = opmap[dep_op]
            dep_parent = dep_stack[1]
            if dep_parent.classname != parent.classname:
                dependencies[op].add(dep_op)

    for key, info in ordered:
        op = info['op']
        if (isinstance(op, sd.AlterObject)
                and not len(op.get_subcommands())):
            continue

        opstack = opmap[op]
        parent = opstack[1]

        reattachment_offset = 1

        for offset, ancestor_op in enumerate(reversed(opstack[1:-1])):
            mcls = ancestor_op.get_schema_metaclass()
            create_cmd_cls = sd.ObjectCommandMeta.get_command_class_or_die(
                sd.CreateObject, mcls)
            ancestor_key = (create_cmd_cls, ancestor_op.classname)

            attached_parent = parents.get(ancestor_key)
            if attached_parent is not None:
                parent_offset = offsets[attached_parent]
                deps = dependencies.get(op)

                ok_to_reattach = (
                    not deps
                    or all(offsets.get(dep, max_offset) < parent_offset
                           for dep in deps)
                )

                if ok_to_reattach:
                    reattachment_offset = -(offset + 1)
                    attached_parent.add(opstack[reattachment_offset])
                    offset = parent_offset

                break

            ancestor_key = (type(ancestor_op), ancestor_op.classname)
            attached_parent = parents.get(ancestor_key)
            if attached_parent is not None:
                parent_offset = offsets[attached_parent]
                deps = dependencies.get(op)
                ok_to_reattach = (
                    not deps
                    or all(offsets.get(dep, max_offset) < parent_offset
                           for dep in deps)
                )

                if ok_to_reattach:
                    reattachment_offset = -(offset + 1)
                    attached_parent.add(opstack[reattachment_offset])
                    offset = parent_offset

                break

        if reattachment_offset == 1:
            # Haven't seen this op branch yet
            ops.append(parent)
            offset = len(ops) - 1

        for op in opstack[reattachment_offset:]:
            ancestor_key = (type(op), op.classname)
            parents[ancestor_key] = op
            offsets[op] = offset

    delta.replace(ops)

    return delta


def _break_down(opmap, opstack):
    if len(opstack) > 2:
        new_opstack = _extract_op(opstack)
    else:
        new_opstack = opstack

    op = new_opstack[-1]

    for sub_op in op.get_subcommands():
        if isinstance(sub_op, (referencing.ReferencedObjectCommand,
                               sd.RenameObject,
                               inheriting.RebaseInheritingObject)):
            _break_down(opmap, opstack + [sub_op])

    opmap[op] = new_opstack


def _trace_op(op, opstack, depgraph, renames, renames_r,
              old_schema, new_schema):
    deps = set()

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
    elif isinstance(op, sd.AlterObjectProperty):
        return
    else:
        raise RuntimeError(
            f'unexpected delta command type at top level: {op!r}'
        )

    if tag == 'delete':
        # Things must be deleted _after_ their referrers have
        # been deleted or altered.
        obj = old_schema.get(op.classname)
        refs = old_schema.get_referrers(old_schema.get(op.classname))
        for ref in refs:
            if (isinstance(ref, pointers.Pointer)
                    and ref.is_endpoint_pointer(old_schema)):
                # Ignore special link properties
                continue

            ref_name = ref.get_name(old_schema)
            if (isinstance(obj, referencing.ReferencedObject)
                    and obj.get_referrer(old_schema) == ref):
                try:
                    ref_item = depgraph[('delete', ref_name)]
                except KeyError:
                    ref_item = depgraph[('delete', ref_name)] = {
                        'deps': set(),
                    }

                ref_item['deps'].add((tag, op.classname))

            elif (isinstance(ref, referencing.ReferencedObject)
                    and ref.get_referrer(old_schema) == obj):
                continue
            else:
                deps.add(('delete', ref.get_name(old_schema)))

        if isinstance(obj, referencing.ReferencedObject):
            referrer = obj.get_referrer(old_schema)
            if referrer is not None:
                referrer_name = referrer.get_name(old_schema)
                if referrer_name in renames_r:
                    referrer_name = renames_r[referrer_name]
                deps.add(('rebase', referrer_name))

    else:
        obj = new_schema.get(op.classname, None)
        if obj is None:
            rename = renames.get(op.classname)
            if rename is not None:
                obj = new_schema.get(rename)
            else:
                obj = new_schema.get(op.classname)

        refs = new_schema.get_referrers(obj)
        for ref in refs:
            if (isinstance(ref, pointers.Pointer)
                    and ref.is_endpoint_pointer(new_schema)):
                # Ignore special link properties
                continue

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

            try:
                item = depgraph[('alter', ref_name)]
            except KeyError:
                item = depgraph[('alter', ref_name)] = {
                    'deps': set(),
                }

            item['deps'].add(('create', op.classname))
            item['deps'].add(('alter', op.classname))

            try:
                item = depgraph[('rebase', ref_name)]
            except KeyError:
                item = depgraph[('rebase', ref_name)] = {
                    'deps': set(),
                }

            item['deps'].add(('create', op.classname))
            item['deps'].add(('alter', op.classname))

        if tag in ('create', 'alter'):
            # In a delete/create cycle, deletion must obviously
            # happen first.
            deps.add(('delete', op.classname))

        if tag == 'alter':
            # Alteration must happen after creation, if any.
            deps.add(('create', op.classname))
            deps.add(('rename', op.classname))
            deps.add(('rebase', op.classname))

        if isinstance(obj, referencing.ReferencedObject):
            referrer = obj.get_referrer(new_schema)
            if referrer is not None:
                referrer_name = referrer.get_name(new_schema)
                if referrer_name in renames_r:
                    referrer_name = renames_r[referrer_name]
                deps.add(('create', referrer_name))
                deps.add(('rebase', referrer_name))

    try:
        item = depgraph[(tag, op.classname)]
    except KeyError:
        item = depgraph[(tag, op.classname)] = {'deps': set()}

    item['item'] = opstack[1]
    item['op'] = op
    item['tag'] = tag
    item['deps'].update(deps)


def _extract_op(stack):
    parent_op = stack[0]
    new_stack = [parent_op]

    for stack_op in stack[1:-1]:
        alter_class = sd.ObjectCommandMeta.get_command_class(
            sd.AlterObject, stack_op.get_schema_metaclass())

        alter_delta = alter_class(classname=stack_op.classname)
        parent_op.add(alter_delta)
        parent_op = alter_delta
        new_stack.append(parent_op)

    stack[-2].discard(stack[-1])
    parent_op.add(stack[-1])
    new_stack.append(stack[-1])

    return new_stack
