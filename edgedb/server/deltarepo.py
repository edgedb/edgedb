##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos import proto, delta


class MetaDeltaRepository:

    def load_delta(self, id):
        raise NotImplementedError

    def delta_ref_to_id(self, ref):
        raise NotImplementedError

    def resolve_delta_ref(self, ref):
        ref = delta.DeltaRef.parse(ref)
        if not ref:
            raise delta.DeltaRefError('unknown revision: %s' % ref)
        return self.delta_ref_to_id(ref)

    def update_delta_ref(self, ref, id):
        raise NotImplementedError

    def write_delta(self, delta_obj):
        delta_set = delta.DeltaSet(deltas=[delta_obj])
        return self.write_delta_set(delta_set)

    def write_delta_set(self, delta_set):
        raise NotImplementedError

    def get_delta(self, id='HEAD'):
        id = self.resolve_delta_ref(id)
        if id:
            return self.load_delta(id)
        else:
            return None

    def get_deltas(self, start_rev, end_rev=None, take_closest_snapshot=False):
        start_rev = start_rev
        end_rev = end_rev or self.resolve_delta_ref('HEAD')

        deltas = self.walk_deltas(end_rev, start_rev, reverse=True,
                                  take_closest_snapshot=take_closest_snapshot)
        return delta.DeltaSet(deltas)

    def walk_deltas(self, end_rev, start_rev, reverse=False, take_closest_snapshot=False):
        current_rev = end_rev

        if not reverse:
            while current_rev and current_rev != start_rev:
                delta = self.load_delta(current_rev)
                yield delta
                current_rev = delta.parent_id
        else:
            deltas = []

            while current_rev and current_rev != start_rev:
                delta = self.load_delta(current_rev)
                deltas.append(delta)
                if delta.snapshot is not None and take_closest_snapshot:
                    break
                current_rev = delta.parent_id

            for delta in reversed(deltas):
                yield delta

    def upgrade(self, start_rev=None, end_rev=None,
                      new_format_ver=delta.Delta.CURRENT_FORMAT_VERSION):

        if end_rev is None:
            end_rev = self.get_delta(id='HEAD').id

        context = delta.DeltaUpgradeContext(delta.Delta.CURRENT_FORMAT_VERSION)
        for d in self.walk_deltas(end_rev, start_rev, reverse=True):
            d.upgrade(context)
            self.write_delta(d)

        self.update_checksums()

    def update_checksums(self):
        start_rev = None
        end_rev = self.get_delta(id='HEAD').id

        schema = proto.ProtoSchema()

        for d in self.walk_deltas(end_rev, start_rev, reverse=True):
            d.apply(schema)
            d.checksum = schema.get_checksum()
            self.write_delta(d)

    def get_meta(self, delta_obj):
        deltas = self.get_deltas(None, delta_obj.id, take_closest_snapshot=True)
        meta = proto.ProtoSchema()
        deltas.apply(meta)
        return meta

    def get_meta_at(self, ref):
        delta = self.load_delta(ref)
        return self.get_meta(delta)

    def get_snapshot_at(self, ref):
        org_delta = self.get_delta(ref)
        full_delta = self.cumulative_delta(None, org_delta.id)
        snapshot = delta.Delta(parent_id=org_delta.parent_id, checksum=org_delta.checksum,
                               deltas=org_delta.deltas,
                               formatver=delta.Delta.CURRENT_FORMAT_VERSION,
                               comment=org_delta.comment, snapshot=full_delta)
        return snapshot

    def _cumulative_delta(self, ref1, ref2):
        delta = None
        v1 = self.load_delta(ref1) if ref1 else None

        if isinstance(ref2, proto.ProtoSchema):
            v2 = None
            v2_meta = ref2
        else:
            v2 = self.load_delta(ref2)
            v2_meta = self.get_meta(v2)

        if v1 is not None:
            v1_meta = self.get_meta(v1)
        else:
            v1_meta = proto.ProtoSchema()

        if v1 is None or v1.checksum != v2_meta.get_checksum():
            delta = v2_meta.delta(v1_meta)
        else:
            delta = None

        return v1, v1_meta, v2, v2_meta, delta

    def cumulative_delta(self, ref1, ref2):
        return self._cumulative_delta(ref1, ref2)[4]

    def calculate_delta(self, ref1, ref2, *, comment=None, preprocess=None, postprocess=None):
        v1, v1_meta, v2, v2_meta, d = self._cumulative_delta(ref1, ref2)

        if d is None and (preprocess is not None or postprocess is not None):
            d = delta.AlterRealm()

        if d is not None:
            d.preprocess = preprocess
            d.postprocess = postprocess

            parent_id = v1.id if v1 else None
            checksum = v2_meta.get_checksum()

            return delta.Delta(parent_id=parent_id, checksum=checksum,
                               comment=comment, deltas=[d],
                               formatver=delta.Delta.CURRENT_FORMAT_VERSION)
        else:
            return None
