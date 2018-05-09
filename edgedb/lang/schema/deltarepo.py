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


from edgedb.lang import edgeql
from edgedb.lang import schema as so
from edgedb.lang.schema import delta as sd
from edgedb.lang.schema import database as s_db


class MetaDeltaRepository:

    def read_delta(self, id, compat_mode=False):
        raise NotImplementedError

    def load_delta(self, id, compat_mode=False):
        d = self.read_delta(id, compat_mode=compat_mode)
        if d.script:
            delta_script = edgeql.parse_block(d.script)

            alter_db = s_db.AlterDatabase()
            context = sd.CommandContext()

            with context(s_db.DatabaseCommandContext(alter_db)):
                for ddl in delta_script:
                    ddl = edgeql.deoptimize(ddl)
                    cmd = sd.Command.from_ast(ddl, context=context)
                    alter_db.add(cmd)

            d.deltas = [alter_db]

        return d

    def delta_ref_to_id(self, ref):
        raise NotImplementedError

    def resolve_delta_ref(self, ref):
        ref = sd.DeltaRef.parse(ref)
        if not ref:
            raise sd.DeltaRefError('unknown revision: %s' % ref)
        return self.delta_ref_to_id(ref)

    def update_delta_ref(self, ref, id):
        raise NotImplementedError

    def write_delta(self, delta_obj):
        delta_set = sd.DeltaSet(deltas=[delta_obj])
        return self.write_delta_set(delta_set)

    def write_delta_set(self, delta_set):
        raise NotImplementedError

    def get_delta(self, id='HEAD', compat_mode=False):
        id = self.resolve_delta_ref(id)
        if id:
            return self.load_delta(id, compat_mode=compat_mode)
        else:
            return None

    def get_deltas(self, start_rev, end_rev=None, take_closest_snapshot=False):
        start_rev = start_rev
        end_rev = end_rev or self.resolve_delta_ref('HEAD')

        deltas = self.walk_deltas(end_rev, start_rev, reverse=True,
                                  take_closest_snapshot=take_closest_snapshot)
        return sd.DeltaSet(deltas)

    def walk_deltas(self, end_rev, start_rev, reverse=False,
                    take_closest_snapshot=False,
                    compat_mode=False):
        current_rev = end_rev

        if not reverse:
            while current_rev and current_rev != start_rev:
                delta = self.load_delta(current_rev, compat_mode=compat_mode)
                yield delta
                current_rev = delta.parent_id
        else:
            deltas = []

            while current_rev and current_rev != start_rev:
                delta = self.load_delta(current_rev, compat_mode=compat_mode)
                deltas.append(delta)
                if delta.snapshot is not None and take_closest_snapshot:
                    break
                current_rev = delta.parent_id

            for delta in reversed(deltas):
                yield delta

    def upgrade(self, start_rev=None, end_rev=None,
                new_format_ver=sd.Delta.CURRENT_FORMAT_VERSION):

        if end_rev is None:
            end_rev = self.get_delta(id='HEAD', compat_mode=True).id

        schema = so.Schema()

        context = sd.DeltaUpgradeContext(sd.Delta.CURRENT_FORMAT_VERSION)
        for d in self.walk_deltas(end_rev, start_rev, reverse=True,
                                  compat_mode=True):
            d.upgrade(context, schema)
            d.apply(schema)
            d.checksum = schema.get_checksum()
            self.write_delta(d)

    def get_schema(self, delta_obj):
        deltas = self.get_deltas(None, delta_obj.id,
                                 take_closest_snapshot=True)
        schema = so.Schema()
        deltas.apply(schema)
        return schema

    def get_schema_at(self, ref):
        if ref is None:
            return so.Schema()
        else:
            delta = self.load_delta(ref)
            return self.get_schema(delta)

    def get_snapshot_at(self, ref):
        org_delta = self.get_delta(ref)
        full_delta = self.cumulative_delta(None, org_delta.id)
        snapshot = sd.Delta(parent_id=org_delta.parent_id,
                            checksum=org_delta.checksum,
                            deltas=org_delta.deltas,
                            formatver=sd.Delta.CURRENT_FORMAT_VERSION,
                            comment=org_delta.comment, snapshot=full_delta)
        return snapshot

    def _cumulative_delta(self, ref1, ref2):
        d = None
        v1 = self.load_delta(ref1) if ref1 else None

        if isinstance(ref2, so.Schema):
            v2 = None
            v2_schema = ref2
        else:
            v2 = self.load_delta(ref2)
            v2_schema = self.get_schema(v2)

        if v1 is not None:
            v1_schema = self.get_schema(v1)
        else:
            v1_schema = so.Schema()

        if v1 is None or v1.checksum != v2_schema.get_checksum():
            d = sd.delta_schemas(v2_schema, v1_schema)
        else:
            d = None

        return v1, v1_schema, v2, v2_schema, d

    def cumulative_delta(self, ref1, ref2):
        return self._cumulative_delta(ref1, ref2)[4]

    def calculate_delta(self, ref1, ref2, *, comment=None,
                        preprocess=None, postprocess=None):
        v1, v1_schema, v2, v2_schema, d = self._cumulative_delta(ref1, ref2)

        if d is None and (preprocess is not None or postprocess is not None):
            d = s_db.AlterDatabase()

        if d is not None:
            parent_id = v1.id if v1 else None

            checksum = v2_schema.get_checksum()
            checksum_details = None

            return sd.Delta(parent_id=parent_id, checksum=checksum,
                            checksum_details=checksum_details,
                            comment=comment, deltas=[d],
                            preprocess=preprocess,
                            postprocess=postprocess,
                            formatver=sd.Delta.CURRENT_FORMAT_VERSION)
        else:
            return None


class DeltaProvider:
    def __init__(self, deltarepo):
        self.deltarepo = deltarepo

    def getschema(self):
        raise NotImplementedError

    def is_dirty(self):
        schema = self.getschema()
        delta = self.deltarepo.get_delta('HEAD')
        if delta:
            return schema.get_checksum() != delta.checksum
        else:
            return True

    def record_delta(self, *, preprocess=None, postprocess=None, comment=None):
        ref1 = self.deltarepo.resolve_delta_ref('HEAD')
        delta_obj = self.deltarepo.calculate_delta(
            ref1, self.getschema(), comment=comment,
            preprocess=preprocess, postprocess=postprocess)
        if delta_obj:
            self.deltarepo.write_delta(delta_obj)
            self.deltarepo.update_delta_ref('HEAD', delta_obj.id)

    def process_delta(self, delta, schema):
        delta.apply(schema)
        return delta
