##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos import proto, delta


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
        raise NotImplementedError

    def get_delta(self, id='HEAD'):
        id = self.resolve_delta_ref(id)
        if id:
            return self.load_delta(id)
        else:
            return None

    def get_deltas(self, start_rev, end_rev=None):
        start_rev = start_rev
        end_rev = end_rev or self.resolve_delta_ref('HEAD')

        return delta.DeltaSet(reversed(list(self.walk_deltas(start_rev, end_rev))))

    def walk_deltas(self, start_rev, end_rev):
        current_rev = end_rev

        while current_rev and current_rev != start_rev:
            delta = self.load_delta(current_rev)
            yield delta
            current_rev = delta.parent_id

    def get_meta(self, delta_obj):
        deltas = self.get_deltas(None, delta_obj.id)
        meta = proto.RealmMeta(load_builtins=False)
        deltas.apply(meta)
        return meta

    def get_meta_at(self, ref):
        delta = self.load_delta(ref)
        return self.get_meta(delta)

    def _cumulative_delta(self, ref1, ref2):
        delta = None
        v1 = self.load_delta(ref1) if ref1 else None

        if isinstance(ref2, proto.RealmMeta):
            v2 = None
            v2_meta = ref2
        else:
            v2 = self.load_delta(ref2)
            v2_meta = self.get_meta(v2)

        if v1 and v1.checksum == v2_meta.get_checksum():
            return None

        if v1:
            v1_meta = self.get_meta(v1)
        else:
            v1_meta = proto.RealmMeta(load_builtins=False)

        delta = v2_meta.delta(v1_meta)

        return v1, v1_meta, v2, v2_meta, delta

    def cumulative_delta(self, ref1, ref2):
        cdelta = self._cumulative_delta(ref1, ref2)
        if cdelta:
            return cdelta[4]
        else:
            return None

    def calculate_delta(self, ref1, ref2, comment=None):
        cdelta = self._cumulative_delta(ref1, ref2)

        if cdelta:
            v1, v1_meta, v2, v2_meta, d = cdelta

            parent_id = v1.id if v1 else None
            checksum = v2_meta.get_checksum()

            return delta.Delta(parent_id=parent_id, checksum=checksum,
                               comment=comment, deltas=[d])
        else:
            return None


class MetaBackend:
    def __init__(self, deltarepo):
        self.deltarepo = deltarepo

    def getmeta(self):
        raise NotImplementedError

    def is_dirty(self):
        meta = self.getmeta()
        delta = self.deltarepo.get_delta('HEAD')
        if delta:
            return meta.get_checksum() != delta.checksum
        else:
            return True

    def record_delta(self, comment=None):
        ref1 = self.deltarepo.resolve_delta_ref('HEAD')
        delta_obj = self.deltarepo.calculate_delta(ref1, self.getmeta(), comment)
        if delta_obj:
            self.deltarepo.write_delta(delta_obj)
            self.deltarepo.update_delta_ref('HEAD', delta_obj.id)

    def process_delta(self, delta, meta):
        delta.apply(meta)
        return delta


class DataBackend:
    pass
