#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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
import functools
import json
import uuid

import immutables

from edb.common import verutils
from edb.common import uuidgen

from edb.schema import abc as s_abc
from edb.schema import expr as s_expr
from edb.schema import functions as s_func
from edb.schema import name as s_name
from edb.schema import objects as s_obj
from edb.schema import operators as s_oper
from edb.schema import schema as s_schema
from edb.schema import version as s_ver

from . import structure as sr_struct


SchemaClassLayout = Dict[Type[s_obj.Object], sr_struct.SchemaTypeLayout]


def parse_into(
    base_schema: s_schema.Schema,
    schema: s_schema.FlatSchema,
    data: Union[str, bytes],
    schema_class_layout: SchemaClassLayout,
) -> s_schema.FlatSchema:
    """Parse JSON-encoded schema objects and populate the schema with them.

    Args:
        schema:
            A schema instance to use as a starting point.
        data:
            A JSON-encoded schema object data as returned
            by an introspection query.
        schema_class_layout:
            A mapping describing schema class layout in the reflection,
            as returned from
            :func:`schema.reflection.structure.generate_structure`.

    Returns:
        A schema instance including objects encoded in the provided
        JSON sequence.
    """

    id_to_type = {}
    id_to_data = {}
    name_to_id = {}
    shortname_to_id = collections.defaultdict(set)
    globalname_to_id = {}
    dict_of_dicts: Callable[
        [],
        Dict[Tuple[Type[s_obj.Object], str], Dict[uuid.UUID, None]],
    ] = functools.partial(collections.defaultdict, dict)  # type: ignore
    refs_to: Dict[
        uuid.UUID,
        Dict[Tuple[Type[s_obj.Object], str], Dict[uuid.UUID, None]]
    ] = collections.defaultdict(dict_of_dicts)

    objects: Dict[uuid.UUID, Tuple[s_obj.Object, Dict[str, Any]]] = {}
    objid: uuid.UUID

    for entry in json.loads(data):
        _, _, clsname = entry['_tname'].rpartition('::')
        mcls = s_obj.ObjectMeta.maybe_get_schema_class(clsname)
        if mcls is None:
            raise ValueError(
                f'unexpected type in schema reflection: {clsname}')
        objid = uuidgen.UUID(entry['id'])
        objects[objid] = (mcls._create_from_id(objid), entry)

    refdict_updates = {}

    for objid, (obj, entry) in objects.items():
        mcls = type(obj)
        name = s_name.name_from_string(entry['name__internal'])
        layout = schema_class_layout[mcls]

        if (
            base_schema.has_object(objid)
            and not isinstance(obj, s_ver.BaseSchemaVersion)
        ):
            continue

        if isinstance(obj, s_obj.QualifiedObject):
            name_to_id[name] = objid
        else:
            name = s_name.UnqualName(str(name))
            globalname_to_id[mcls, name] = objid

        if isinstance(obj, (s_func.Function, s_oper.Operator)):
            shortname = mcls.get_shortname_static(name)
            shortname_to_id[mcls, shortname].add(objid)

        id_to_type[objid] = type(obj).__name__

        all_fields = mcls.get_schema_fields()
        objdata: List[Any] = [None] * len(all_fields)
        val: Any
        refid: uuid.UUID

        for k, v in entry.items():
            desc = layout.get(k)
            if desc is None:
                continue

            fn = desc.fieldname
            field = all_fields.get(fn)
            if field is None:
                continue
            findex = field.index

            if desc.storage is not None:
                if v is None:
                    pass
                elif desc.storage.ptrkind == 'link':
                    refid = uuidgen.UUID(v['id'])
                    newobj = objects.get(refid)
                    if newobj is not None:
                        val = newobj[0]
                    else:
                        val = base_schema.get_by_id(refid)
                    objdata[findex] = val.schema_reduce()
                    refs_to[val.id][mcls, fn][objid] = None

                elif desc.storage.ptrkind == 'multi link':
                    ftype = mcls.get_field(fn).type
                    if issubclass(ftype, s_obj.ObjectDict):
                        refids = ftype._container(
                            uuidgen.UUID(e['value']) for e in v)
                        refkeys = tuple(e['name'] for e in v)
                        val = ftype(refids, refkeys, _private_init=True)
                    else:
                        refids = ftype._container(
                            uuidgen.UUID(e['id']) for e in v)
                        val = ftype(refids, _private_init=True)
                    objdata[findex] = val.schema_reduce()
                    for refid in refids:
                        refs_to[refid][mcls, fn][objid] = None

                elif desc.storage.shadow_ptrkind:
                    val = entry[f'{k}__internal']
                    ftype = mcls.get_field(fn).type
                    if val is not None and type(val) is not ftype:
                        if issubclass(ftype, s_expr.Expression):
                            val = _parse_expression(val, objid, k)
                            for refid in val.refs.ids(schema):
                                refs_to[refid][mcls, fn][objid] = None
                        elif issubclass(ftype, s_expr.ExpressionList):
                            exprs = []
                            for e_dict in val:
                                e = _parse_expression(e_dict, objid, k)
                                assert e.refs is not None
                                for refid in e.refs.ids(schema):
                                    refs_to[refid][mcls, fn][objid] = None
                                exprs.append(e)
                            val = ftype(exprs)
                        elif issubclass(ftype, s_obj.Object):
                            val = val.id
                        elif issubclass(ftype, s_name.Name):
                            val = s_name.name_from_string(val)
                        else:
                            val = ftype(val)

                    if issubclass(ftype, s_abc.Reducible):
                        val = val.schema_reduce()
                    objdata[findex] = val

                else:
                    ftype = mcls.get_field(fn).type
                    if type(v) is not ftype:
                        if issubclass(ftype, verutils.Version):
                            objdata[findex] = _parse_version(v)
                        else:
                            objdata[findex] = ftype(v)
                    else:
                        objdata[findex] = v

            elif desc.is_refdict:
                ftype = mcls.get_field(fn).type
                refids = ftype._container(uuidgen.UUID(e['id']) for e in v)
                for refid in refids:
                    refs_to[refid][mcls, fn][objid] = None

                val = ftype(refids, _private_init=True)
                objdata[findex] = val.schema_reduce()
                if desc.properties:
                    for e_dict in v:
                        refdict_updates[uuidgen.UUID(e_dict['id'])] = {
                            p: pv for p in desc.properties
                            if (pv := e_dict[f'@{p}']) is not None
                        }

        id_to_data[objid] = tuple(objdata)

    for objid, updates in refdict_updates.items():
        if updates:
            sclass = s_obj.ObjectMeta.get_schema_class(id_to_type[objid])
            updated_data = list(id_to_data[objid])
            for fn, v in updates.items():
                field = sclass.get_schema_field(fn)
                updated_data[field.index] = v
            id_to_data[objid] = tuple(updated_data)

    with schema._refs_to.mutate() as mm:
        for referred_id, refdata in refs_to.items():
            try:
                refs = mm[referred_id]
            except KeyError:
                refs = immutables.Map((
                    (k, immutables.Map(r)) for k, r in refdata.items()
                ))
            else:
                refs_update = {}
                for k, referrers in refdata.items():
                    try:
                        rt = refs[k]
                    except KeyError:
                        rt = immutables.Map(referrers)
                    else:
                        rt = rt.update(referrers)
                    refs_update[k] = rt

                refs = refs.update(refs_update)

            mm[referred_id] = refs

    schema = schema._replace(
        id_to_type=schema._id_to_type.update(id_to_type),
        id_to_data=schema._id_to_data.update(id_to_data),
        name_to_id=schema._name_to_id.update(name_to_id),
        shortname_to_id=schema._shortname_to_id.update(
            (k, frozenset(v)) for k, v in shortname_to_id.items()
        ),
        globalname_to_id=schema._globalname_to_id.update(globalname_to_id),
        refs_to=mm.finish(),
    )

    return schema


def _parse_expression(
    val: Dict[str, Any], id: uuid.UUID, field: str
) -> s_expr.Expression:
    refids = frozenset(
        uuidgen.UUID(r) for r in val['refs']
    )
    expr = s_expr.Expression(
        text=val['text'],
        refs=s_obj.ObjectSet(
            refids,
            _private_init=True,
        ),
    )
    expr.set_origin(id, field)
    return expr


def _parse_version(val: Dict[str, Any]) -> verutils.Version:
    return verutils.Version(
        major=val['major'],
        minor=val['minor'],
        stage=getattr(verutils.VersionStage, val['stage'].upper()),
        stage_no=val['stage_no'],
        local=tuple(val['local']),
    )
