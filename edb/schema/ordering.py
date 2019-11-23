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
from typing import *  # NoQA

import collections

from edb.common import topological

from . import delta as sd
from . import functions as s_func
from . import inheriting
from . import name as sn
from . import objects as so
from . import referencing

if TYPE_CHECKING:
    from . import schema as s_schema
    CommandT = TypeVar("CommandT", bound=sd.Command)


def linearize_delta(
    delta: CommandT,
    old_schema: Optional[s_schema.Schema],
    new_schema: s_schema.Schema
) -> CommandT:
    """Sort delta operations to dependency order."""

    opmap: Dict[sd.ObjectCommand, List[sd.ObjectCommand]] = {}
    strongrefs: Dict[str, str] = {}

    for op in delta.get_subcommands():
        _break_down(opmap, strongrefs, [delta, op])

    depgraph: Dict[Tuple[str, sn.Name], Dict[str, Any]] = {}
    renames: Dict[sn.Name, sn.Name] = {}
    renames_r: Dict[sn.Name, sn.Name] = {}

    for op in opmap:
        if isinstance(op, sd.RenameObject):
            renames[op.classname] = op.new_name
            renames_r[op.new_name] = op.classname

    for op, opstack in opmap.items():
        _trace_op(op, opstack, depgraph, renames,
                  renames_r, strongrefs, old_schema, new_schema)

    depgraph = dict(
        filter(lambda i: i[1].get('item') is not None, depgraph.items()))

    ordered = list(topological.sort(depgraph, allow_unresolved=True,
                                    return_record=True))

    parents: Dict[Tuple[Type[sd.ObjectCommand], sn.Name], sd.ObjectCommand]
    dependencies: Dict[sd.ObjectCommand, Set[sd.ObjectCommand]]
    parents = {}
    dependencies = collections.defaultdict(set)
    max_offset = len(ordered)
    offsets: Dict[sd.ObjectCommand, int] = {}
    ops: List[sd.ObjectCommand] = []

    for _key, info in ordered:
        op = info['op']
        opstack = opmap[op]
        parent = opstack[1]
        for dep in info['deps']:
            dep_item = depgraph.get(dep)
            if dep_item is None:
                continue
            dep_op = dep_item['op']
            dep_stack = opmap[dep_op]
            dep_parent = dep_stack[1]
            if ((dep_item['tag'], dep_parent.classname)
                    != (info['tag'], parent.classname)):
                dependencies[op].add(dep_op)

    for _key, info in ordered:
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
            if not isinstance(op, sd.AlterObjectProperty):
                ancestor_key = (type(op), op.classname)
                parents[ancestor_key] = op

            offsets[op] = offset

    delta.replace(ops)

    return delta


def _break_down(opmap, strongrefs, opstack):
    if len(opstack) > 2:
        new_opstack = _extract_op(opstack)
    else:
        new_opstack = opstack

    op = new_opstack[-1]

    for sub_op in op.get_subcommands():
        if isinstance(sub_op, (referencing.ReferencedObjectCommand,
                               sd.RenameObject,
                               inheriting.RebaseInheritingObject)):
            _break_down(opmap, strongrefs, new_opstack + [sub_op])
        elif isinstance(sub_op, sd.AlterObjectProperty):
            mcls = op.get_schema_metaclass()
            field = mcls.get_field(sub_op.property)
            # Break a possible reference cycle
            # (i.e. Type.rptr <-> Pointer.target)
            if field.weak_ref:
                _break_down(opmap, strongrefs, new_opstack + [sub_op])
        elif isinstance(sub_op, referencing.StronglyReferencedObjectCommand):
            strongrefs[sub_op.classname] = op.classname

    opmap[op] = new_opstack


def _trace_op(
    op: sd.ObjectCommand,
    opstack: List[sd.ObjectCommand],
    depgraph: Dict[Tuple[str, sn.Name], Dict[str, Any]],
    renames: Dict[sn.Name, sn.Name],
    renames_r: Dict[sn.Name, sn.Name],
    strongrefs: Dict[str, str],
    old_schema: Optional[s_schema.Schema],
    new_schema: s_schema.Schema,
) -> None:
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
        tag = 'field'
    else:
        raise RuntimeError(
            f'unexpected delta command type at top level: {op!r}'
        )

    if isinstance(op, sd.DeleteObject):
        assert old_schema is not None
        # Things must be deleted _after_ their referrers have
        # been deleted or altered.
        obj = get_object(old_schema, op, op.classname)
        refs = _get_referrers(
            old_schema, get_object(old_schema, op, op.classname), strongrefs)
        for ref in refs:
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

        graph_key = op.classname

    elif isinstance(op, sd.AlterObjectProperty):
        if isinstance(op.new_value, so.Object):
            deps.add(('create', op.new_value.name))
            deps.add(('alter', op.new_value.name))

        graph_key = (opstack[-2].classname, op.property)

    else:
        # If the object was renamed, use the new name, else use regular.
        name = renames.get(op.classname, op.classname)
        obj = get_object(new_schema, op, name)

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

            if isinstance(obj, s_func.Function) and old_schema is not None:
                old_funcs = old_schema.get_functions(
                    sn.shortname_from_fullname(op.classname),
                    default=[])
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
                referrer_name = referrer.get_name(new_schema)
                if referrer_name in renames_r:
                    referrer_name = renames_r[referrer_name]
                deps.add(('create', referrer_name))
                deps.add(('rebase', referrer_name))

        graph_key = op.classname

    try:
        item = depgraph[(tag, graph_key)]
    except KeyError:
        item = depgraph[(tag, graph_key)] = {'deps': set()}

    item['item'] = opstack[1]
    item['op'] = op
    item['tag'] = tag
    item['deps'].update(deps)


def get_object(
    schema: s_schema.Schema,
    op: sd.ObjectCommand,
    name: str
) -> so.Object:
    metaclass = op.get_schema_metaclass()

    if issubclass(metaclass, so.UnqualifiedObject):
        return schema.get_global(metaclass, name)
    else:
        return schema.get(name)


def _get_referrers(
    schema: s_schema.Schema,
    obj: so.Object,
    strongrefs: Dict[str, str],
) -> Set[so.Object]:
    refs = schema.get_referrers(obj)
    result = set()

    for ref in refs:
        if not ref.is_blocking_ref(schema, obj):
            continue

        parent_ref = strongrefs.get(ref.get_name(schema))
        if parent_ref is not None:
            result.add(schema.get(parent_ref))
        else:
            result.add(ref)

    return result


def _extract_op(stack: List[sd.ObjectCommand]) -> List[sd.ObjectCommand]:
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
