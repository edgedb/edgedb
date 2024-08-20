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

import json
import textwrap

from edb.server import defines

from ..common import quote_ident as qi
from ..common import quote_literal as ql

from . import base


class DDLOperation(base.Command):
    pass


class NonTransactionalDDLOperation(DDLOperation):
    def generate(
        self,
        block: base.SQLBlock,
    ) -> None:
        block.add_command(self.code())
        block.set_non_transactional()
        self_block = block.add_block()

        self.generate_extra(self_block)
        self_block.conditions = self.conditions
        self_block.neg_conditions = self.neg_conditions


class SchemaObjectOperation(DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions)
        self.name = name
        self.opid = name

    def __repr__(self):
        return '<edb.sync.%s %s>' % (self.__class__.__name__, self.name)


class Comment(DDLOperation):
    def __init__(self, object, text, **kwargs):
        super().__init__(**kwargs)

        self.object = object
        self.text = text

    def code(self) -> str:
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

    def code(self) -> str:
        return (
            f'REASSIGN OWNED BY {self.qi(self.old_role)} '
            f'TO {self.qi(self.new_role)}'
        )


class GetMetadata(base.Command):
    def __init__(self, object):
        super().__init__()
        self.object = object

    def code_with_block(self, block: base.PLBlock) -> str:
        from .. import trampoline

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
            q = textwrap.dedent(f'''\
                SELECT
                    edgedb_VER.shobj_metadata(
                        {objoid},
                        {classoid}::regclass::text
                    )
                ''')
        elif objsubid:
            q = textwrap.dedent(f'''\
                SELECT
                    edgedb_VER.col_metadata(
                        {objoid},
                        {objsubid}
                    )
                ''')
        else:
            q = textwrap.dedent(f'''\
                SELECT
                    edgedb_VER.obj_metadata(
                        {objoid},
                        {classoid}::regclass::text,
                    )
                ''')

        return trampoline.fixup_query(q)


class GetSingleDBMetadata(base.Command):
    def __init__(self, dbname, **kwargs):
        super().__init__(**kwargs)
        self.dbname = dbname

    def code(self) -> str:
        from .. import trampoline

        key = f'{self.dbname}metadata'
        return textwrap.dedent(trampoline.fixup_query(f'''\
            SELECT
                json
            FROM
                edgedbinstdata_VER.instdata
            WHERE
                key = {ql(key)}
        '''))


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


class PutSingleDBMetadata(DDLOperation):
    def __init__(self, dbname, metadata, **kwargs):
        super().__init__(**kwargs)
        self.dbname = dbname
        self.metadata = metadata

    @property
    def key(self):
        return f'{self.dbname}metadata'

    def __repr__(self):
        return \
            '<{mod}.{cls} Branch({dbname!r}) {metadata!r}>'.format(
                mod=self.__class__.__module__,
                cls=self.__class__.__name__,
                dbname=self.dbname,
                metadata=self.metadata)


class SetMetadata(PutMetadata):
    def creation_code(self) -> str:
        metadata = self.metadata

        object_type = self.object.get_type()
        object_id = self.object.get_id()

        prefix = ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)
        desc = ql(json.dumps(metadata))
        comment = f'E{prefix} || {desc}'

        return textwrap.dedent(f'''\
            'COMMENT ON {object_type} {object_id} IS '
            || quote_literal({comment})
        ''')

    def code(self) -> str:
        return 'EXECUTE ' + self.creation_code() + ';'


class SetSingleDBMetadata(PutSingleDBMetadata):
    def code(self) -> str:
        from .. import trampoline

        metadata = ql(json.dumps(self.metadata))
        return textwrap.dedent(trampoline.fixup_query(f'''\
            UPDATE
                edgedbinstdata_VER.instdata
            SET
                json = {metadata}
            WHERE
                key = {ql(self.key)};
        '''))


class UpdateMetadata(PutMetadata):
    def code_with_block(self, block: base.PLBlock) -> str:
        metadata_qry = GetMetadata(self.object).code_with_block(block)
        prefix = ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)
        json_v = block.declare_var('jsonb')
        upd_v = block.declare_var('text')
        meta_v = block.declare_var('jsonb')
        block.add_command(f'{json_v} := ({metadata_qry});')
        upd_metadata = ql(json.dumps(self.metadata))
        block.add_command(f'{meta_v} := {upd_metadata}::jsonb')

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


class UpdateSingleDBMetadata(PutSingleDBMetadata):
    def code_with_block(self, block: base.PLBlock) -> str:
        from .. import trampoline

        metadata_qry = GetSingleDBMetadata(self.dbname).code_with_block(block)
        json_v = block.declare_var('jsonb')
        meta_v = block.declare_var('jsonb')
        block.add_command(f'{json_v} := ({metadata_qry});')
        upd_metadata = ql(json.dumps(self.metadata))
        block.add_command(f'{meta_v} := {upd_metadata}::jsonb')

        return textwrap.dedent(trampoline.fixup_query(f'''\
            UPDATE
                edgedbinstdata_VER.instdata
            SET
                json = {json_v} || {meta_v}
            WHERE
                key = {ql(self.key)}
        '''))


class UpdateMetadataSectionMixin:
    def __init__(self, *args, section, **kwargs):
        super().__init__(*args, **kwargs)
        self.section = section

    def _metadata_query(self) -> base.Command:
        raise NotImplementedError

    def _merge(self, block):
        metadata_qry = self._metadata_query().code_with_block(block)
        json_v = block.declare_var('jsonb')
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
        return json_v, meta_v


class UpdateMetadataSection(UpdateMetadataSectionMixin, PutMetadata):
    def _metadata_query(self) -> base.Command:
        return GetMetadata(self.object)

    def code_with_block(self, block: base.PLBlock) -> str:
        json_v, meta_v = self._merge(block)
        upd_v = block.declare_var('text')
        prefix = ql(defines.EDGEDB_VISIBLE_METADATA_PREFIX)
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


class UpdateSingleDBMetadataSection(
    UpdateMetadataSectionMixin, PutSingleDBMetadata
):
    def _metadata_query(self) -> base.Command:
        return GetSingleDBMetadata(self.dbname)

    def code_with_block(self, block: base.PLBlock) -> str:
        from .. import trampoline

        json_v, meta_v = self._merge(block)
        return textwrap.dedent(trampoline.fixup_query(f'''\
            UPDATE
                edgedbinstdata_VER.instdata
            SET
                json = {json_v} || {meta_v}
            WHERE
                key = {ql(self.key)}
        '''))


class CreateObject(SchemaObjectOperation):

    def __init__(self, object, **kwargs):
        super().__init__(object.get_id(), **kwargs)
        self.object = object

    def generate_extra(self, block: base.PLBlock) -> None:
        super().generate_extra(block)
        if self.object.metadata:
            mdata = SetMetadata(self.object, self.object.metadata)
            block.add_command(mdata.code_with_block(block))


class AlterObject(SchemaObjectOperation):
    def __init__(self, object, **kwargs):
        super().__init__(object.get_id(), **kwargs)
        self.object = object

    def generate_extra(self, block: base.PLBlock) -> None:
        super().generate_extra(block)
        if self.object.metadata:
            mdata = SetMetadata(self.object, self.object.metadata)
            block.add_command(mdata.code_with_block(block))


class DropObject(SchemaObjectOperation):

    def __init__(self, object, **kwargs):
        super().__init__(object.get_id(), **kwargs)
        self.object = object
