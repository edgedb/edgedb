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

import json
import textwrap

from edb.server import defines

from ..common import quote_ident as qi
from ..common import quote_literal as ql

from . import base


class DDLOperation(base.Command):
    pass


class NonTransactionalDDLOperation(DDLOperation):
    def generate_self_block(
        self,
        block: base.PLBlock,
    ) -> Optional[base.PLBlock]:
        block.add_command(self.code(block))
        block.set_non_transactional()
        self_block = block.add_block()
        return self_block


class SchemaObjectOperation(DDLOperation):
    def __init__(
            self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

        self.name = name
        self.opid = name

    def __repr__(self):
        return '<edb.sync.%s %s>' % (self.__class__.__name__, self.name)


class Comment(DDLOperation):
    def __init__(self, object, text, **kwargs):
        super().__init__(**kwargs)

        self.object = object
        self.text = text

    def code(self, block: base.PLBlock) -> str:
        object_type = self.object.get_type()
        object_id = self.object.get_id()

        code = 'COMMENT ON {type} {id} IS {text}'.format(
            type=object_type, id=object_id,
            text=ql(self.text))

        return code


class ReassignOwned(DDLOperation):
    def __init__(self, old_role, new_role, **kwargs):
        super().__init__(**kwargs)
        self.old_role = old_role
        self.new_role = new_role

    def qi(self, ident: str) -> str:
        if ident.upper() in ('CURRENT_USER', 'SESSION_USER'):
            return ident
        else:
            return qi(ident)

    def code(self, block: base.PLBlock) -> str:
        return (
            f'REASSIGN OWNED BY {self.qi(self.old_role)} '
            f'TO {self.qi(self.new_role)}'
        )


class GetMetadata(base.Command):
    def __init__(self, object):
        super().__init__()
        self.object = object

    def code(self, block: base.PLBlock) -> str:
        oid = self.object.get_oid()
        is_shared = self.object.is_shared()
        if isinstance(oid, base.Query):
            qry = oid.text
            classoid = block.declare_var('oid')
            objoid = block.declare_var('oid')
            objsubid = block.declare_var('oid')
            block.add_command(
                qry + f' INTO {classoid}, {objoid}, {objsubid}')
        else:
            objoid, classoid, objsubid = oid

        if is_shared:
            return textwrap.dedent(f'''\
                SELECT
                    edgedb.shobj_metadata(
                        {objoid},
                        {classoid}::regclass::text
                    )
                ''')
        elif objsubid:
            return textwrap.dedent(f'''\
                SELECT
                    edgedb.col_metadata(
                        {objoid},
                        {objsubid}
                    )
                ''')
        else:
            return textwrap.dedent(f'''\
                SELECT
                    edgedb.obj_metadata(
                        {objoid},
                        {classoid}::regclass::text,
                    )
                ''')


class PutMetadata(DDLOperation):
    def __init__(self, object, metadata, **kwargs):
        super().__init__(**kwargs)
        self.object = object
        self.metadata = metadata

    def __repr__(self):
        return \
            '<{mod}.{cls} {object!r} {metadata!r}>'.format(
                mod=self.__class__.__module__,
                cls=self.__class__.__name__,
                object=self.object,
                metadata=self.metadata)


class SetMetadata(PutMetadata):
    def code(self, block: base.PLBlock) -> str:
        metadata = self.metadata

        object_type = self.object.get_type()
        object_id = self.object.get_id()

        prefix = ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)
        desc = ql(json.dumps(metadata))
        comment = f'E{prefix} || {desc}'

        return textwrap.dedent(f'''\
            EXECUTE 'COMMENT ON {object_type} {object_id} IS ' ||
                quote_literal({comment});
        ''')


class UpdateMetadata(PutMetadata):
    def code(self, block: base.PLBlock) -> str:
        metadata_qry = GetMetadata(self.object).code(block)
        prefix = ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)
        json_v = block.declare_var('jsonb')
        upd_v = block.declare_var('text')
        meta_v = block.declare_var('jsonb')
        block.add_command(f'{json_v} := ({metadata_qry});')
        upd_metadata = ql(json.dumps(self.metadata))
        block.add_command(f'{meta_v} := {upd_metadata}::jsonb')

        block.add_command(textwrap.dedent(f'''\
            IF {json_v} IS NOT NULL THEN
                {upd_v} := E{prefix} || ({json_v} || {upd_metadata})::text;
            ELSE
                {upd_v} := E{prefix} || {upd_metadata}::text;
            END IF;
        '''))

        object_type = self.object.get_type()
        object_id = self.object.get_id()

        return textwrap.dedent(f'''\
            IF {upd_v} IS NOT NULL THEN
                EXECUTE 'COMMENT ON {object_type} {object_id} IS ' ||
                    quote_literal({upd_v});
            END IF;
        ''')


class UpdateMetadataSection(PutMetadata):
    def __init__(self, object, metadata, *, section, **kwargs):
        super().__init__(object, metadata, **kwargs)
        self.section = section

    def code(self, block: base.PLBlock) -> str:
        metadata_qry = GetMetadata(self.object).code(block)
        prefix = ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)
        json_v = block.declare_var('jsonb')
        upd_v = block.declare_var('text')
        meta_v = block.declare_var('jsonb')
        block.add_command(f'{json_v} := ({metadata_qry});')
        upd_metadata = ql(json.dumps(self.metadata))
        block.add_command(
            f"{meta_v} := jsonb_strip_nulls(jsonb_build_object(\n"
            f"    {ql(self.section)},\n"
            f"    COALESCE({json_v} -> {ql(self.section)}, '{{}}')"
            f" || {upd_metadata}::jsonb\n"
            f"))"
        )

        block.add_command(textwrap.dedent(f'''\
            IF {json_v} IS NOT NULL THEN
                {upd_v} := E{prefix} || ({json_v} || {meta_v})::text;
            ELSE
                {upd_v} := E{prefix} || {meta_v}::text;
            END IF;
        '''))

        object_type = self.object.get_type()
        object_id = self.object.get_id()

        return textwrap.dedent(f'''\
            IF {upd_v} IS NOT NULL THEN
                EXECUTE 'COMMENT ON {object_type} {object_id} IS ' ||
                    quote_literal({upd_v});
            END IF;
        ''')


class CreateObject(SchemaObjectOperation):

    def __init__(self, object, **kwargs):
        super().__init__(object.get_id(), **kwargs)
        self.object = object

    def generate_extra(self, block: base.PLBlock) -> None:
        super().generate_extra(block)
        if self.object.metadata:
            mdata = SetMetadata(self.object, self.object.metadata)
            block.add_command(mdata.code(block))


class RenameObject(SchemaObjectOperation):
    def __init__(self, object, *, new_name, **kwargs):
        super().__init__(name=object.name, **kwargs)
        self.object = object
        self.altered_object = object.copy()
        self.altered_object.rename(new_name)
        self.new_name = new_name

    def generate_extra(self, block: base.PLBlock) -> None:
        super().generate_extra(block)
        if self.object.metadata:
            mdata = UpdateMetadata(
                self.altered_object, self.altered_object.metadata)
            block.add_command(mdata.code(block))


class AlterObject(SchemaObjectOperation):
    def __init__(self, object, **kwargs):
        super().__init__(object.get_id(), **kwargs)
        self.object = object

    def generate_extra(self, block: base.PLBlock) -> None:
        super().generate_extra(block)
        if self.object.metadata:
            mdata = SetMetadata(self.object, self.object.metadata)
            block.add_command(mdata.code(block))


class DropObject(SchemaObjectOperation):

    def __init__(self, object, **kwargs):
        super().__init__(object.get_id(), **kwargs)
        self.object = object
